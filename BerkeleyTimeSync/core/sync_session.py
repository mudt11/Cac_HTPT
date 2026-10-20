"""Master-side lifetime of one bounded Berkeley synchronization round.

This module records messages and local timing; it does not send packets,
compute corrections or change clocks. Participants are pinned by node ID and
boot ID. Controllers must serialize send registration before dispatch and
cancel the session if its Master tenure is superseded.
"""

from dataclasses import dataclass
from threading import RLock
from time import monotonic_ns
from typing import Callable, Dict, Iterable, Optional, Set, Tuple
from uuid import UUID

from .models import NodeIdentity, RoundKey, SyncRoundState
from .protocol import AckStatus, Message, MessageType, ProtocolError, validate_context


_TERMINAL = frozenset({
    SyncRoundState.COMPLETED, SyncRoundState.PARTIAL,
    SyncRoundState.FAILED, SyncRoundState.CANCELLED,
})
_POSITIVE_ACKS = frozenset({AckStatus.ACCEPTED.value, AckStatus.APPLIED.value})


class SessionStateError(RuntimeError):
    """An operation cannot be performed in the current round phase."""


def _integer(value: int, name: str, minimum: int = 0) -> None:
    if type(value) is not int:
        raise TypeError("{} must be an integer".format(name))
    if value < minimum:
        raise ValueError("{} must be at least {}".format(name, minimum))


@dataclass(frozen=True)
class ReportSample:
    """Correlated report with requester-local monotonic send/receive times."""

    request: Message
    response: Message
    sent_monotonic_ns: int
    received_monotonic_ns: int

    @property
    def rtt_ns(self) -> int:
        return self.received_monotonic_ns - self.sent_monotonic_ns


@dataclass(frozen=True)
class SessionSnapshot:
    """Immutable progress view; COMPLETED acknowledges commands, not convergence."""

    round_key: RoundKey
    participants: Tuple[NodeIdentity, ...]
    state: SyncRoundState
    started_ns: int
    report_deadline_ns: int
    computation_deadline_ns: Optional[int]
    ack_deadline_ns: Optional[int]
    requests: Tuple[Message, ...]
    reports: Tuple[ReportSample, ...]
    adjustments: Tuple[Message, ...]
    acknowledgments: Tuple[Message, ...]
    reason: Optional[str]

    @property
    def missing_report_ids(self) -> Tuple[int, ...]:
        reported = {sample.response.source.node_id for sample in self.reports}
        master_id = self.round_key.leader.node.node_id
        return tuple(p.node_id for p in self.participants
                     if p.node_id != master_id and p.node_id not in reported)

    @property
    def missing_ack_ids(self) -> Tuple[int, ...]:
        answered = {ack.source.node_id for ack in self.acknowledgments}
        return tuple(m.target_node_id for m in self.adjustments
                     if m.target_node_id not in answered)


class SyncSession:
    """One round with collection, computation and acknowledgment deadlines.

    A missing Worker never extends a deadline. Collection continues with at
    least min_workers reports; the default permits one Worker plus the Master.
    Missing/excluded participants or rejected commands prevent COMPLETED.
    All rejections or no positive ACK before expiry produce FAILED.

    A new report must be handled before the collection deadline. An optional
    captured arrival timestamp measures RTT only; it cannot reopen an expired
    round. APPLIED notifications after an initial positive ACK are left to
    the controller's clock/convergence tracking and do not rewrite this round.
    """

    def __init__(
        self, round_key: RoundKey, participants: Iterable[NodeIdentity], *,
        report_timeout_ms: int = 2000, computation_timeout_ms: int = 2000,
        ack_timeout_ms: int = 2000, min_workers: int = 1,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not isinstance(round_key, RoundKey):
            raise TypeError("round_key must be a RoundKey")
        members = tuple(participants)
        if not 2 <= len(members) <= 6 or any(not isinstance(p, NodeIdentity) for p in members):
            raise ValueError("A round requires 2 to 6 NodeIdentity participants")
        by_id = {p.node_id: p for p in members}
        if len(by_id) != len(members):
            raise ValueError("Participants contain duplicate node IDs")
        master = round_key.leader.node
        if by_id.get(master.node_id) != master:
            raise ValueError("Participants must contain the exact Master identity")
        for name, value in (
            ("report_timeout_ms", report_timeout_ms),
            ("computation_timeout_ms", computation_timeout_ms),
            ("ack_timeout_ms", ack_timeout_ms), ("min_workers", min_workers),
        ):
            _integer(value, name, 1)
        if min_workers > len(members) - 1:
            raise ValueError("min_workers exceeds the Worker count")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self._key = round_key
        self._participants = tuple(by_id[k] for k in sorted(by_id))
        self._workers = {k: p for k, p in by_id.items() if k != master.node_id}
        self._lock = RLock()
        self._monotonic_source = monotonic_source
        self._started_ns = monotonic_source()
        _integer(self._started_ns, "monotonic reading")
        self._last_now_ns = self._started_ns
        self._report_deadline_ns = self._started_ns + report_timeout_ms * 1000000
        self._computation_timeout_ns = computation_timeout_ms * 1000000
        self._ack_timeout_ns = ack_timeout_ms * 1000000
        self._computation_deadline_ns: Optional[int] = None
        self._ack_deadline_ns: Optional[int] = None
        self._min_workers = min_workers
        self._state = SyncRoundState.COLLECTING
        self._reason: Optional[str] = None
        self._requests: Dict[int, Tuple[Message, int]] = {}
        self._reports: Dict[int, ReportSample] = {}
        self._adjustments: Dict[int, Message] = {}
        self._acks: Dict[int, Message] = {}
        self._seen: Dict[UUID, Message] = {}

    def _now(self) -> int:
        now = self._monotonic_source()
        _integer(now, "monotonic reading")
        if now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    def _computing(self, at_ns: int) -> None:
        self._state = SyncRoundState.COMPUTING
        self._computation_deadline_ns = at_ns + self._computation_timeout_ns

    def _finish_acks(self) -> None:
        positive = sum(ack.payload["status"] in _POSITIVE_ACKS for ack in self._acks.values())
        if not positive:
            self._state = SyncRoundState.FAILED
            self._reason = "No Worker acknowledged accepting an adjustment"
        elif positive == len(self._workers):
            self._state = SyncRoundState.COMPLETED
        else:
            self._state = SyncRoundState.PARTIAL
            self._reason = "Some participants were missing, excluded or did not accept"

    def _expire(self, now: int) -> None:
        if self._state == SyncRoundState.COLLECTING and now >= self._report_deadline_ns:
            if len(self._reports) < self._min_workers:
                self._state = SyncRoundState.FAILED
                self._reason = "Insufficient Worker reports before the collection deadline"
            else:
                self._computing(self._report_deadline_ns)
        if self._state == SyncRoundState.COMPUTING and now >= self._computation_deadline_ns:
            self._state = SyncRoundState.FAILED
            self._reason = "Computation deadline expired"
        if self._state == SyncRoundState.WAITING_ACK and now >= self._ack_deadline_ns:
            self._finish_acks()

    def _phase(self, expected: SyncRoundState) -> None:
        if self._state != expected:
            raise SessionStateError("Round is {}, expected {}".format(self._state.value, expected.value))

    def _message(self, message: Message, expected: MessageType) -> None:
        if not isinstance(message, Message) or message.type != expected:
            raise ProtocolError("Unexpected message type for this operation")
        validate_context(message, expected_round=self._key)

    def _worker_response(self, message: Message, expected: MessageType) -> None:
        self._message(message, expected)
        if self._workers.get(message.source.node_id) != message.source:
            raise ProtocolError("Sender is not a pinned Worker incarnation")
        validate_context(message, recipient_id=self._key.leader.node.node_id)

    def _duplicate(self, message: Message) -> bool:
        previous = self._seen.get(message.message_id)
        if previous is not None and previous != message:
            raise ProtocolError("A message ID was reused with different content")
        return previous is not None

    def record_request(self, message: Message, *, sent_monotonic_ns: Optional[int] = None) -> bool:
        """Register one poll per Worker before sending; retransmit the same message."""
        self._message(message, MessageType.SYNC_REQUEST)
        if message.target_node_id not in self._workers:
            raise ProtocolError("Poll recipient is not a pinned Worker")
        if set(message.payload["participant_ids"]) != {p.node_id for p in self._participants}:
            raise ProtocolError("Poll participant list differs from the round snapshot")
        with self._lock:
            now = self._now()
            self._expire(now)
            if self._duplicate(message):
                return False
            self._phase(SyncRoundState.COLLECTING)
            if message.target_node_id in self._requests:
                raise ProtocolError("A different poll is already registered for this Worker")
            sent = now if sent_monotonic_ns is None else sent_monotonic_ns
            _integer(sent, "sent_monotonic_ns")
            if not self._started_ns <= sent <= now:
                raise ValueError("Poll send time is outside the current round interval")
            self._requests[message.target_node_id] = (message, sent)
            self._seen[message.message_id] = message
            return True

    def record_report(self, message: Message, *, received_monotonic_ns: Optional[int] = None) -> bool:
        """Keep the first correlated sample; exact retries never replace its RTT."""
        self._worker_response(message, MessageType.TIME_REPORT)
        with self._lock:
            now = self._now()
            self._expire(now)
            if self._duplicate(message):
                return False
            self._phase(SyncRoundState.COLLECTING)
            node_id = message.source.node_id
            request = self._requests.get(node_id)
            if request is None or message.reply_to != request[0].message_id:
                raise ProtocolError("Report does not answer this Worker's registered poll")
            previous = self._reports.get(node_id)
            if previous is not None:
                if previous.response.payload != message.payload:
                    raise ProtocolError("Worker already supplied a different sample")
                return False
            received = now if received_monotonic_ns is None else received_monotonic_ns
            _integer(received, "received_monotonic_ns")
            if not request[1] <= received <= now:
                raise ValueError("Report receive time is outside its measured poll interval")
            self._reports[node_id] = ReportSample(request[0], message, request[1], received)
            self._seen[message.message_id] = message
            if len(self._reports) == len(self._workers):
                self._computing(now)
            return True

    def begin_ack_wait(self, adjustments: Iterable[Message]) -> None:
        """Atomically register the full adjustment plan before dispatching it.

        The engine may exclude unusable samples. Such a round can finish only
        PARTIAL. No target may lack a report, and every command must reference
        that report. The ACK deadline includes command dispatch time.
        """
        plan = tuple(adjustments)
        with self._lock:
            now = self._now()
            self._expire(now)
            self._phase(SyncRoundState.COMPUTING)
            if not plan:
                raise ValueError("An adjustment plan requires at least one Worker")
            by_id: Dict[int, Message] = {}
            ids: Set[UUID] = set()
            for message in plan:
                self._message(message, MessageType.SYNC_ADJUST)
                sample = self._reports.get(message.target_node_id)
                if sample is None or message.reply_to != sample.response.message_id:
                    raise ProtocolError("Adjustment does not reference its target's report")
                if message.target_node_id in by_id or message.message_id in ids:
                    raise ProtocolError("Adjustment plan contains a duplicate target or message ID")
                if self._duplicate(message):
                    raise ProtocolError("Adjustment message ID is already used")
                by_id[message.target_node_id] = message
                ids.add(message.message_id)
            self._adjustments = by_id
            self._seen.update((message.message_id, message) for message in plan)
            self._ack_deadline_ns = now + self._ack_timeout_ns
            self._state = SyncRoundState.WAITING_ACK

    def record_ack(self, message: Message) -> bool:
        """Record the first response to an adjustment without waiting for slewing."""
        self._worker_response(message, MessageType.TIME_ACK)
        with self._lock:
            now = self._now()
            self._expire(now)
            if self._duplicate(message):
                return False
            command = self._adjustments.get(message.source.node_id)
            if command is None or message.reply_to != command.message_id:
                raise ProtocolError("ACK does not answer its Worker's adjustment command")
            previous = self._acks.get(message.source.node_id)
            if previous is not None:
                was_positive = previous.payload["status"] in _POSITIVE_ACKS
                is_positive = message.payload["status"] in _POSITIVE_ACKS
                if was_positive != is_positive:
                    raise ProtocolError("Worker ACK contradicts its earlier response")
                return False
            self._phase(SyncRoundState.WAITING_ACK)
            self._acks[message.source.node_id] = message
            self._seen[message.message_id] = message
            if len(self._acks) == len(self._adjustments):
                self._finish_acks()
            return True

    def _close(self, state: SyncRoundState, reason: str) -> bool:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            raise ValueError("A closing reason must be 1 to 512 nonblank characters")
        with self._lock:
            self._expire(self._now())
            if self._state in _TERMINAL:
                return False
            self._state = state
            self._reason = reason
            return True

    def cancel(self, reason: str) -> bool:
        """Cancel an active round, for example when its Master changes."""
        return self._close(SyncRoundState.CANCELLED, reason)

    def fail(self, reason: str) -> bool:
        """Close an active round when the engine cannot produce a valid result."""
        return self._close(SyncRoundState.FAILED, reason)

    @property
    def state(self) -> SyncRoundState:
        return self.snapshot().state

    def refresh(self) -> SyncRoundState:
        """Evaluate deadlines; the controller supplies the timer, not a thread here."""
        return self.snapshot().state

    def snapshot(self) -> SessionSnapshot:
        with self._lock:
            self._expire(self._now())
            return SessionSnapshot(
                self._key, self._participants, self._state, self._started_ns,
                self._report_deadline_ns, self._computation_deadline_ns,
                self._ack_deadline_ns,
                tuple(self._requests[k][0] for k in sorted(self._requests)),
                tuple(self._reports[k] for k in sorted(self._reports)),
                tuple(self._adjustments[k] for k in sorted(self._adjustments)),
                tuple(self._acks[k] for k in sorted(self._acks)), self._reason,
            )
