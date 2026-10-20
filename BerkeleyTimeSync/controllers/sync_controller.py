"""Independent Berkeley round orchestration; no sockets, timers or GUI threads.

Configured controllers return one-shot SyncDispatch actions. Pass prepare_send
as the TCP send factory AFTER the transport's send lock is acquired; capture
receive monotonic_ns and virtual clock_time_ms in the TCP receive thread.
Do not timestamp at enqueue/dequeue time. No transport retry is scheduled here.

All public methods serialize through an RLock: the owner drives state/receive
events while dispatch workers may call prepare_send. No network I/O occurs
under this lock. Leadership changes invalidate queued actions and stop owned
slews. Controllers never import or call another application controller.

SyncController() without registry/clock remains disabled for the existing Node
boundary. Configuring and routing it into NodeController is a separate step.
Periodic 5/10-second scheduling also belongs to that later integration.
"""

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from time import monotonic_ns
from typing import Callable, Dict, Iterable, Optional, Tuple
from uuid import UUID, uuid4

from ..core.berkeley import BerkeleyPlan, ClockMeasurement, build_plan
from ..core.clock import ClockSnapshot, VirtualClock
from ..core.election import ElectionSnapshot
from ..core.models import ClockState, LeaderIdentity, NodeIdentity, NodeRole, PeerStatus, RoundKey, SyncRoundState
from ..core.outlier_filter import filter_outliers
from ..core.peer_registry import PeerRegistry
from ..core.protocol import AckStatus, MAX_INTEGER, Message, MessageType, ProtocolError
from ..core.sync_session import ReportSample, SessionSnapshot, SessionStateError, SyncSession


_TERMINAL = frozenset({SyncRoundState.COMPLETED, SyncRoundState.PARTIAL,
                       SyncRoundState.FAILED, SyncRoundState.CANCELLED})
_TYPES = frozenset({MessageType.SYNC_REQUEST, MessageType.TIME_REPORT,
                    MessageType.SYNC_ADJUST, MessageType.TIME_ACK, MessageType.SYNC_CANCEL})


class SyncStateError(RuntimeError):
    """The requested operation/action is no longer valid in the current state."""


class WorkerState(str, Enum):
    WAITING_ADJUSTMENT = "WAITING_ADJUSTMENT"
    SLEWING = "SLEWING"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class SyncDispatch:
    token: UUID
    type: MessageType
    target: NodeIdentity
    round_key: RoundKey
    deadline_ns: int


@dataclass(frozen=True)
class WorkerSnapshot:
    round_key: RoundKey
    state: WorkerState
    deadline_ns: int
    request_id: UUID
    adjustment_id: Optional[UUID]
    correction_ms: Optional[int]


@dataclass(frozen=True)
class SyncSnapshot:
    available: bool
    leader: Optional[LeaderIdentity]
    role: NodeRole
    session: Optional[SessionSnapshot] = None
    worker: Optional[WorkerSnapshot] = None
    clock: Optional[ClockSnapshot] = None
    plan: Optional[BerkeleyPlan] = None
    applied_worker_ids: Tuple[int, ...] = ()
    pending_dispatches: int = 0


@dataclass(frozen=True)
class SyncResult:
    detail: str
    outbound: Tuple[SyncDispatch, ...] = ()


@dataclass
class _Pending:
    action: SyncDispatch
    fixed: Optional[Message] = None


@dataclass
class _Worker:
    request: Message
    received_ms: int
    deadline_ns: int
    state: WorkerState = WorkerState.WAITING_ADJUSTMENT
    report: Optional[Message] = None
    report_clock: Optional[ClockSnapshot] = None
    command: Optional[Message] = None
    ack: Optional[Message] = None


def _positive_ms(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 60000:
        raise ValueError("{} must be in [1, 60000] milliseconds".format(name))
    return value


class SyncController:
    """Master and Worker synchronization on the same process-local virtual clock.

    Pass BOTH registry and clock to enable. The registry's local identity is
    pinned for this process. Use one monotonic source for this controller and
    VirtualClock. Default membership selects ONLINE discovery entries; the
    future orchestrator may explicitly pass heartbeat-eligible participants.

    Collection, computation and ACK budgets include dispatch delays; RTT does
    not include time waiting to prepare a request. REQUEST.timeout_ms carries
    their TOTAL budget, giving Workers enough time for partial collection.
    COMPLETED means commands acknowledged, not measured clock convergence.
    APPLIED is a Worker report of slew completion, also not a convergence proof.
    """

    def __init__(self, registry: Optional[PeerRegistry] = None,
                 clock: Optional[VirtualClock] = None, *,
                 report_timeout_ms: int = 2000, computation_timeout_ms: int = 2000,
                 ack_timeout_ms: int = 2000, min_workers: int = 1,
                 max_rtt_ms: float = 500.0, max_spread_ms: float = 10000.0,
                 min_cluster_size: int = 2, max_event_age_ms: int = 1000,
                 monotonic_source: Callable[[], int] = monotonic_ns) -> None:
        if (registry is None) != (clock is None):
            raise ValueError("Provide both registry and VirtualClock, or neither")
        if registry is not None and (not isinstance(registry, PeerRegistry)
                                     or not isinstance(clock, VirtualClock)):
            raise TypeError("Expected PeerRegistry and VirtualClock")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self._report_ms = _positive_ms(report_timeout_ms, "report_timeout_ms")
        self._compute_ms = _positive_ms(computation_timeout_ms, "computation_timeout_ms")
        self._ack_ms = _positive_ms(ack_timeout_ms, "ack_timeout_ms")
        self._total_ms = self._report_ms + self._compute_ms + self._ack_ms
        if self._total_ms > 60000:
            raise ValueError("Total round budget must not exceed 60000 ms")
        if type(min_workers) is not int or not 1 <= min_workers <= 5:
            raise ValueError("min_workers must be in [1, 5]")
        self._age_ns = _positive_ms(max_event_age_ms, "max_event_age_ms") * 1000000
        # Reuse filter configuration validation rather than duplicating its math.
        filter_outliers(RoundKey(LeaderIdentity(NodeIdentity(1)), 1), (),
                        max_rtt_ms=max_rtt_ms, max_spread_ms=max_spread_ms,
                        min_cluster_size=min_cluster_size)
        self._filter_args = dict(max_rtt_ms=max_rtt_ms, max_spread_ms=max_spread_ms,
                                 min_cluster_size=min_cluster_size)
        self._min_workers = min_workers
        self._registry, self._clock = registry, clock
        self._local = registry.local_identity if registry is not None else None
        self._monotonic_source = monotonic_source
        self._lock = RLock()
        self._last_ns = None  # type: Optional[int]
        self._leader = None  # type: Optional[LeaderIdentity]
        self._role = NodeRole.UNASSIGNED
        self._session = None  # type: Optional[SyncSession]
        self._worker = None  # type: Optional[_Worker]
        self._plan = None  # type: Optional[BerkeleyPlan]
        self._polls = {}  # type: Dict[int, Tuple[Message, int]]
        self._measurements = {}  # type: Dict[int, ClockMeasurement]
        self._pending = {}  # type: Dict[UUID, _Pending]
        self._seen = {}  # type: Dict[UUID, Message]
        self._round_numbers = {}  # type: Dict[LeaderIdentity, int]
        self._worker_high_water = {}  # type: Dict[LeaderIdentity, int]
        self._applied = set()
        self._notified_failure = None  # type: Optional[RoundKey]
        self._owns_slew = False
        self._now()

    def _now(self) -> int:
        now = self._monotonic_source()
        if type(now) is not int or now < 0 or (self._last_ns is not None and now < self._last_ns):
            raise ValueError("Invalid or backwards monotonic source")
        self._last_ns = now
        return now

    def _available(self) -> bool:
        return (self._registry is not None and self._leader is not None
                and self._role in (NodeRole.MASTER, NodeRole.WORKER))

    def _clock_read(self) -> ClockSnapshot:
        reading = self._clock.snapshot()
        if not 0 <= reading.time_ms <= MAX_INTEGER:
            raise ValueError("Virtual clock is outside protocol range")
        return reading

    @staticmethod
    def _bounded_set(mapping: Dict, key, value) -> None:
        if key not in mapping and len(mapping) >= 256:
            mapping.pop(next(iter(mapping)))
        mapping[key] = value

    def _remember(self, message: Message) -> None:
        self._bounded_set(self._seen, message.message_id, message)

    def _stop_slew(self) -> None:
        if self._owns_slew:
            self._clock.schedule_correction(0)
            self._owns_slew = False

    def leadership_changed(self, election: ElectionSnapshot) -> SyncResult:
        if not isinstance(election, ElectionSnapshot):
            raise TypeError("Expected ElectionSnapshot")
        with self._lock:
            if self._local is not None and election.leader is not None:
                local_master = election.leader.node == self._local
                if ((election.role == NodeRole.MASTER) != local_master
                        or election.role not in (NodeRole.MASTER, NodeRole.WORKER)):
                    raise ValueError("Role/leader does not match the configured local identity")
            changed = (self._leader, self._role) != (election.leader, election.role)
            if changed:
                if self._session is not None:
                    self._session.cancel("Leadership changed")
                if self._worker is not None:
                    self._worker.state = WorkerState.CANCELLED
                self._pending.clear()
                if self._clock is not None:
                    self._stop_slew()
            self._leader, self._role = election.leader, election.role
            return SyncResult("Leadership changed; old synchronization invalidated" if changed
                              else "Leadership unchanged")

    def snapshot(self) -> SyncSnapshot:
        with self._lock:
            self._now()
            worker = None
            if self._worker is not None:
                value = self._worker
                worker = WorkerSnapshot(value.request.round_key, value.state, value.deadline_ns,
                                        value.request.message_id,
                                        value.command.message_id if value.command else None,
                                        value.command.payload["correction_ms"] if value.command else None)
            return SyncSnapshot(
                self._available(), self._leader, self._role,
                self._session.snapshot() if self._session is not None else None,
                worker, self._clock.snapshot() if self._clock is not None else None,
                self._plan, tuple(sorted(self._applied)), len(self._pending))

    def _queue(self, kind: MessageType, target: NodeIdentity, key: RoundKey,
               deadline: int, fixed: Optional[Message] = None) -> Tuple[SyncDispatch, ...]:
        if fixed is not None and any(p.fixed == fixed for p in self._pending.values()):
            return ()
        if len(self._pending) >= 64:
            raise SyncStateError("Synchronization dispatch capacity reached")
        action = SyncDispatch(uuid4(), kind, target, key, deadline)
        self._pending[action.token] = _Pending(action, fixed)
        return (action,)

    def _peer_current(self, identity: NodeIdentity) -> bool:
        peer = self._registry.get(identity.node_id)
        return peer is not None and peer.identity == identity

    def start_round(self, participants: Optional[Iterable[NodeIdentity]] = None) -> SyncResult:
        with self._lock:
            now = self._now()
            if not self._available() or self._role != NodeRole.MASTER:
                raise SyncStateError("Only the configured current Master may start a round")
            if self._clock.is_paused:
                raise SyncStateError("Resume the virtual clock before synchronizing")
            if self._session is not None and self._session.state not in _TERMINAL:
                raise SyncStateError("A synchronization round is already active")
            members = (tuple(participants) if participants is not None else
                       (self._local,) + tuple(p.identity for p in self._registry.snapshot()
                                              if p.status == PeerStatus.ONLINE))
            key = RoundKey(self._leader, self._round_numbers.get(self._leader, 0) + 1)
            session = SyncSession(
                key, members, report_timeout_ms=self._report_ms,
                computation_timeout_ms=self._compute_ms, ack_timeout_ms=self._ack_ms,
                min_workers=self._min_workers, monotonic_source=self._monotonic_source)
            if any(p != self._local and not self._peer_current(p) for p in session.snapshot().participants):
                raise SyncStateError("Round participants must match current discovery identities")
            self._pending.clear()
            self._session, self._plan = session, None
            self._polls.clear()
            self._measurements.clear()
            self._applied.clear()
            self._bounded_set(self._round_numbers, self._leader, key.round_id)
            actions = ()
            for member in session.snapshot().participants:
                if member != self._local:
                    actions += self._queue(MessageType.SYNC_REQUEST, member, key,
                                           session.snapshot().report_deadline_ns)
            return SyncResult("Synchronization round started", actions)

    def prepare_send(self, action: SyncDispatch) -> Message:
        """One-shot TCP factory. Register T1/RTT origin under the send lock.

        Never reuse an action after send failure: delivery may be ambiguous.
        Other actions (including duplicate-command ACKs) get separate tokens.
        """
        with self._lock:
            now = self._now()
            pending = self._pending.get(action.token) if isinstance(action, SyncDispatch) else None
            if pending is None or pending.action != action:
                raise SyncStateError("Unknown, invalidated or already prepared dispatch")
            if (not self._available() or action.round_key.leader != self._leader
                    or now >= action.deadline_ns or not self._peer_current(action.target)):
                self._pending.pop(action.token)
                raise SyncStateError("Dispatch context expired or peer changed")
            if action.type == MessageType.SYNC_REQUEST:
                session = self._session.snapshot()
                if session.round_key != action.round_key or session.state != SyncRoundState.COLLECTING:
                    raise SyncStateError("Request no longer belongs to a collecting round")
                reading = self._clock_read()
                message = Message(
                    action.type, self._local,
                    {"master_time_ms": reading.time_ms, "timeout_ms": self._total_ms,
                     "participant_ids": tuple(p.node_id for p in session.participants)},
                    target_node_id=action.target.node_id, leader=self._leader,
                    round_id=action.round_key.round_id)
                self._session.record_request(message, sent_monotonic_ns=reading.monotonic_ns)
                self._polls[action.target.node_id] = (message, reading.monotonic_ns)
            elif action.type == MessageType.TIME_REPORT and pending.fixed is None:
                worker = self._worker
                if (worker is None or worker.request.round_key != action.round_key
                        or worker.state != WorkerState.WAITING_ADJUSTMENT):
                    raise SyncStateError("Report context is no longer active")
                reading = self._clock_read()
                message = Message(
                    action.type, self._local,
                    {"received_time_ms": worker.received_ms, "sent_time_ms": reading.time_ms},
                    target_node_id=action.target.node_id, leader=self._leader,
                    round_id=action.round_key.round_id, reply_to=worker.request.message_id)
                worker.report, worker.report_clock = message, reading
            else:
                message = pending.fixed
                if message is None:
                    raise SyncStateError("Dispatch has no prepared wire message")
                if action.type == MessageType.SYNC_ADJUST:
                    session = self._session.snapshot()
                    if session.round_key != action.round_key or session.state != SyncRoundState.WAITING_ACK:
                        raise SyncStateError("Adjustment no longer belongs to ACK phase")
                elif action.type != MessageType.SYNC_CANCEL:
                    if self._worker is None or self._worker.request.round_key != action.round_key:
                        raise SyncStateError("Worker response belongs to an obsolete round")
            self._pending.pop(action.token)
            return message

    @staticmethod
    def _consistent(reference: ClockSnapshot, current: ClockSnapshot) -> bool:
        duration = current.monotonic_ns - reference.monotonic_ns
        elapsed = current.time_ms - reference.time_ms
        return duration >= 0 and 0 <= elapsed <= 2 * duration / 1000000 + 1

    def _cancel_packets(self, reason: str) -> Tuple[SyncDispatch, ...]:
        if self._session is None or self._leader != self._session.snapshot().round_key.leader:
            return ()
        actions = ()
        key = self._session.snapshot().round_key
        deadline = self._now() + self._total_ms * 1000000
        for member in self._session.snapshot().participants:
            if member != self._local and self._peer_current(member):
                message = Message(MessageType.SYNC_CANCEL, self._local, {"reason": reason},
                                  target_node_id=member.node_id, leader=self._leader,
                                  round_id=key.round_id)
                actions += self._queue(message.type, member, key, deadline, message)
        return actions

    def cancel(self, reason: str = "Synchronization cancelled") -> SyncResult:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            raise ValueError("Cancellation reason must be 1..512 nonblank characters")
        with self._lock:
            self._now()
            self._pending.clear()
            if self._session is not None:
                self._session.cancel(reason)
            if self._worker is not None:
                self._worker.state = WorkerState.CANCELLED
            if self._clock is not None:
                self._stop_slew()
            actions = self._cancel_packets(reason) if self._role == NodeRole.MASTER else ()
            return SyncResult(reason, actions)

    def _compute(self) -> SyncResult:
        snapshot = self._session.snapshot()
        try:
            plan = build_plan(snapshot, self._measurements.values(), **self._filter_args)
            self._plan = plan
            if not plan.ready:
                return self._fail("No unambiguous Berkeley consensus")
            reading = self._clock_read()
            if reading.is_paused:
                return self._fail("Master virtual clock paused during collection")
            for measurement in self._measurements.values():
                reference = ClockSnapshot(measurement.master_received_time_ms,
                                          measurement.report.received_monotonic_ns,
                                          0, ClockState.NORMAL, False)
                if not self._consistent(reference, reading):
                    return self._fail("Master clock changed after measurement")
            # Revalidate BEFORE changing any clock or exposing adjustments.
            self._session.begin_ack_wait(plan.adjustments)
            self._clock.schedule_correction(plan.master_correction_ms)
            self._owns_slew = True
            deadline = self._session.snapshot().ack_deadline_ns
            actions = ()
            members = {p.node_id: p for p in snapshot.participants}
            for message in plan.adjustments:
                actions += self._queue(message.type, members[message.target_node_id],
                                       snapshot.round_key, deadline, message)
            return SyncResult("Berkeley plan committed; acknowledgments pending", actions)
        except (ValueError, SessionStateError) as exc:
            return self._fail("Berkeley computation failed: {}".format(exc)[:512])

    def _fail(self, reason: str) -> SyncResult:
        self._session.fail(reason)
        self._pending.clear()
        return SyncResult(reason, self._cancel_packets(reason))

    def tick(self) -> SyncResult:
        with self._lock:
            now = self._now()
            if not self._available():
                return SyncResult("Synchronization is not configured or no leader is active")
            self._pending = {k: p for k, p in self._pending.items() if now < p.action.deadline_ns}
            if self._role == NodeRole.MASTER and self._session is not None:
                state = self._session.snapshot()
                if state.state not in _TERMINAL and any(
                    p != self._local and not self._peer_current(p) for p in state.participants
                ):
                    return self.cancel("Participant process identity changed")
                if state.state == SyncRoundState.COMPUTING:
                    return self._compute()
                if state.state == SyncRoundState.FAILED and not self._plan:
                    # One cancellation batch, not a new batch every timer tick.
                    if self._pending:
                        return SyncResult("Round failed; cancellation already pending")
                    if state.round_key != self._notified_failure:
                        self._notified_failure = state.round_key
                        return SyncResult(state.reason or "Round failed",
                                          self._cancel_packets(state.reason or "Round failed"))
            worker = self._worker
            if worker is not None and self._role == NodeRole.WORKER:
                if not self._peer_current(self._leader.node):
                    return self.cancel("Master process identity changed")
                if worker.state == WorkerState.WAITING_ADJUSTMENT and now >= worker.deadline_ns:
                    worker.state = WorkerState.EXPIRED
                    self._pending.clear()
                    return SyncResult("Worker adjustment deadline expired")
                if worker.state == WorkerState.SLEWING and self._clock.state == ClockState.NORMAL:
                    worker.state = WorkerState.APPLIED
                    self._owns_slew = False
                    worker.ack = self._ack(worker.command, AckStatus.APPLIED)
                    return SyncResult("Worker slew finished",
                                      self._queue_ack(worker.ack))
            if self._owns_slew and self._clock.state == ClockState.NORMAL:
                self._owns_slew = False
            return SyncResult("Synchronization timers checked")

    def _ack(self, command: Message, status: AckStatus, reason: Optional[str] = None) -> Message:
        reading = self._clock_read()
        payload = {"status": status.value, "clock_state": reading.state.value,
                   "clock_time_ms": reading.time_ms}
        if reason is not None:
            payload["reason"] = reason
        return Message(MessageType.TIME_ACK, self._local, payload,
                       target_node_id=command.source.node_id, leader=self._leader,
                       round_id=command.round_id, reply_to=command.message_id)

    def _queue_ack(self, message: Message) -> Tuple[SyncDispatch, ...]:
        return self._queue(message.type, self._leader.node, message.round_key,
                           self._now() + self._ack_ms * 1000000, message)

    def handle_tcp(self, message: Message, *, received_monotonic_ns: Optional[int] = None,
                   clock_time_ms: Optional[int] = None) -> Optional[SyncResult]:
        if not isinstance(message, Message):
            raise TypeError("Expected Message")
        if message.type not in _TYPES:
            return None
        with self._lock:
            now = self._now()
            if self._registry is None:
                return SyncResult("Synchronization is not integrated yet")
            if (not self._available() or message.leader != self._leader
                    or message.target_node_id != self._local.node_id
                    or not self._peer_current(message.source)):
                return SyncResult("Rejected synchronization context or process identity")
            received = now if received_monotonic_ns is None else received_monotonic_ns
            if type(received) is not int or not 0 <= received <= now:
                raise ProtocolError("Invalid receive monotonic timestamp")
            if now - received > self._age_ns:
                return SyncResult("Stale synchronization event")
            previous = self._seen.get(message.message_id)
            if previous is not None and previous != message:
                raise ProtocolError("Synchronization message ID reused with different contents")
            if message.type in (MessageType.SYNC_REQUEST, MessageType.TIME_REPORT):
                if type(clock_time_ms) is not int or not 0 <= clock_time_ms <= MAX_INTEGER:
                    raise ProtocolError("Capture the virtual clock at TCP receive, not queue handling")
            if self._role == NodeRole.MASTER:
                return self._handle_master(message, received, clock_time_ms)
            return self._handle_worker(message, received, clock_time_ms)

    def _handle_master(self, message: Message, received: int, clock_ms: Optional[int]) -> SyncResult:
        if message.type not in (MessageType.TIME_REPORT, MessageType.TIME_ACK):
            return SyncResult("Wrong synchronization direction for Master")
        if self._session is None or message.round_key != self._session.snapshot().round_key:
            return SyncResult("Stale or unknown synchronization round")
        if message.type == MessageType.TIME_REPORT:
            if message.source.node_id in self._measurements:
                previous = self._measurements[message.source.node_id].report.response
                if (previous.source != message.source or previous.payload != message.payload
                        or previous.reply_to != message.reply_to):
                    raise ProtocolError("Worker already supplied a different clock sample")
                return SyncResult("Duplicate report; original measurement retained")
            poll = self._polls.get(message.source.node_id)
            if poll is None:
                raise ProtocolError("Worker did not receive a registered poll")
            measurement = ClockMeasurement(ReportSample(poll[0], message, poll[1], received), clock_ms)
            try:
                added = self._session.record_report(message, received_monotonic_ns=received)
            except SessionStateError:
                return SyncResult("Report arrived after collection phase")
            if added:
                self._measurements[message.source.node_id] = measurement
                self._remember(message)
            return SyncResult("Worker clock report recorded")
        try:
            added = self._session.record_ack(message)
        except SessionStateError:
            return SyncResult("ACK arrived after its deadline")
        self._remember(message)
        if message.payload["status"] == AckStatus.APPLIED.value:
            self._applied.add(message.source.node_id)
        return SyncResult("Worker ACK recorded" if added else "Duplicate/completion ACK recorded")

    def _handle_worker(self, message: Message, received: int, clock_ms: Optional[int]) -> SyncResult:
        if message.type not in (MessageType.SYNC_REQUEST, MessageType.SYNC_ADJUST, MessageType.SYNC_CANCEL):
            return SyncResult("Wrong synchronization direction for Worker")
        high = self._worker_high_water.get(self._leader, 0)
        worker = self._worker
        if message.type == MessageType.SYNC_CANCEL:
            if message.round_id < high:
                return SyncResult("Stale cancellation ignored")
            self._bounded_set(self._worker_high_water, self._leader, message.round_id)
            self._pending.clear()
            if worker is not None:
                worker.state = WorkerState.CANCELLED
            self._stop_slew()
            self._remember(message)
            return SyncResult("Master cancelled synchronization")
        if message.type == MessageType.SYNC_REQUEST:
            if worker is not None and message.round_key == worker.request.round_key:
                if message != worker.request:
                    raise ProtocolError("Round already has a different request")
                if worker.state == WorkerState.WAITING_ADJUSTMENT and worker.report is not None:
                    return SyncResult("Duplicate request; cached report retained",
                                      self._queue(MessageType.TIME_REPORT, self._leader.node,
                                                  message.round_key, worker.deadline_ns, worker.report))
                return SyncResult("Duplicate or closed request ignored")
            if message.round_id <= high:
                return SyncResult("Stale or cancelled request ignored")
            if message.payload["timeout_ms"] > 60000:
                raise ProtocolError("Worker round budget exceeds 60000 ms")
            self._bounded_set(self._worker_high_water, self._leader, message.round_id)
            self._pending.clear()
            self._worker = _Worker(message, clock_ms,
                                   received + message.payload["timeout_ms"] * 1000000)
            self._remember(message)
            if self._clock.is_paused:
                self._worker.state = WorkerState.REJECTED
                return SyncResult("Paused Worker cannot report a running clock")
            return SyncResult("Worker poll accepted",
                              self._queue(MessageType.TIME_REPORT, self._leader.node,
                                          message.round_key, self._worker.deadline_ns))
        if worker is None or message.round_key != worker.request.round_key:
            return SyncResult("Adjustment has no current Worker poll")
        if worker.state in (WorkerState.CANCELLED, WorkerState.EXPIRED):
            return SyncResult("Adjustment belongs to a cancelled or expired Worker round")
        if worker.command is not None:
            if worker.command != message:
                raise ProtocolError("A different adjustment already exists for this round")
            return SyncResult("Duplicate adjustment; correction not applied twice",
                              self._queue_ack(worker.ack) if worker.ack is not None else ())
        if (worker.state != WorkerState.WAITING_ADJUSTMENT or self._now() >= worker.deadline_ns):
            return SyncResult("Adjustment arrived after Worker deadline or cancellation")
        if worker.report is None or message.reply_to != worker.report.message_id:
            raise ProtocolError("Adjustment does not reference the prepared Worker report")
        reading = self._clock_read()
        reason = None
        if reading.is_paused:
            reason = "Worker virtual clock is paused"
        elif not self._consistent(worker.report_clock, reading):
            reason = "Worker clock changed after its report"
        if reason is not None:
            worker.state = WorkerState.REJECTED
            worker.command = message
            worker.ack = self._ack(message, AckStatus.REJECTED, reason)
        else:
            self._clock.schedule_correction(message.payload["correction_ms"])
            self._owns_slew = True
            worker.command = message
            worker.state = (WorkerState.APPLIED if self._clock.state == ClockState.NORMAL
                            else WorkerState.SLEWING)
            status = AckStatus.APPLIED if worker.state == WorkerState.APPLIED else AckStatus.ACCEPTED
            worker.ack = self._ack(message, status)
        self._remember(message)
        return SyncResult("Worker adjustment handled", self._queue_ack(worker.ack))
