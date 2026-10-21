"""Owner-thread monitoring snapshots; never changes synchronization or clocks.

The orchestrator supplies immutable membership/heartbeat/sync/clock views.
One publication per second is a replaceable observation, not a reliable work
queue. TCP admission against discovery remains the orchestrator's duty.
Snapshot ages use the receiver's clock only after receipt; remote absolute
monotonic timestamps are never compared across machines.
"""

from collections import OrderedDict
from dataclasses import dataclass, replace
from time import monotonic_ns
from typing import Optional, Tuple

from ..core.cluster_status import AdjustmentStatus, ClockSample, ClusterStatus, NodeStatus, RoundStatus
from ..core.models import ClockState, LeaderIdentity, NodeIdentity, NodeRole, PeerInfo, PeerStatus, SyncRoundState
from ..core.protocol import Message, MessageType, ProtocolError, serialize_packet


@dataclass(frozen=True)
class ClusterSnapshot:
    leader: Optional[LeaderIdentity]
    status: Optional[ClusterStatus]
    received_monotonic_ns: Optional[int]
    age_ms: Optional[int]
    stale: bool


@dataclass(frozen=True)
class ClusterUpdate:
    detail: str
    outbound: Tuple[Message, ...] = ()
    deadline_ns: Optional[int] = None
    accepted: bool = False


class ClusterController:
    def __init__(self, identity: NodeIdentity, display_name: str, ip: str, *,
                 interval_ms: int = 1000, stale_after_ms: int = 4000,
                 max_event_age_ms: int = 1000, monotonic_source=monotonic_ns):
        PeerInfo(identity, display_name, ip)
        for value in (interval_ms, stale_after_ms, max_event_age_ms):
            if type(value) is not int or not 1 <= value <= 60000:
                raise ValueError("Monitoring durations must be integers in [1, 60000]")
        if stale_after_ms < interval_ms:
            raise ValueError("stale_after_ms must be at least interval_ms")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self.identity, self._name, self._ip = identity, display_name, ip
        self._source = monotonic_source
        self._interval_ns = interval_ms * 1000000
        self._stale_ns = stale_after_ms * 1000000
        self._event_age_ns = max_event_age_ms * 1000000
        self._last_ns = None
        self._leader = None
        self._role = NodeRole.UNASSIGNED
        self._since_ns = 0
        self._next_ns = 0
        self._status = None
        self._received_ns = None
        self._samples = {}
        self._adjustments = {}
        self._received_marks = OrderedDict()
        self._published_marks = OrderedDict()
        self._now()

    def _now(self):
        now = self._source()
        if type(now) is not int or now < 0 or (self._last_ns is not None and now < self._last_ns):
            raise ValueError("Invalid/backwards monitoring monotonic clock")
        self._last_ns = now
        return now

    @staticmethod
    def _remember(mapping, key, value):
        mapping[key] = value
        mapping.move_to_end(key)
        while len(mapping) > 32:
            mapping.popitem(last=False)

    def leadership_changed(self, election):
        now = self._now()
        leader, role = election.leader, election.role
        if leader is not None:
            expected = NodeRole.MASTER if leader.node == self.identity else NodeRole.WORKER
            if role != expected:
                raise ValueError("Monitoring role disagrees with the current Master")
        if (leader, role) == (self._leader, self._role):
            return
        self._leader, self._role = leader, role
        self._status, self._received_ns = None, None
        self._samples.clear()
        self._adjustments.clear()
        self._next_ns = now
        previous = self._published_marks.get(leader)
        self._since_ns = previous[0] if previous is not None else now

    def clear(self):
        self._leader, self._role = None, NodeRole.UNASSIGNED
        self._status, self._received_ns = None, None
        self._samples.clear()
        self._adjustments.clear()

    def snapshot(self):
        now = self._now()
        age = None if self._received_ns is None else now - self._received_ns
        return ClusterSnapshot(self._leader, self._status, self._received_ns,
                               age // 1000000 if age is not None else None,
                               age is None or age >= self._stale_ns)

    @staticmethod
    def _brief(reason):
        return None if reason is None else reason if len(reason) <= 128 else reason[:127] + "…"

    def _capture_samples(self, session, plan, applied_ids, current, now):
        """Retain real samples across collection phases, pinned to boot ID/round."""
        if plan is not None and plan.round_key != session.round_key:
            plan = None
        selected, reasons = {}, {}
        if plan is not None and plan.round_key == session.round_key:
            selected.update((s.identity, s) for s in plan.filter_result.accepted)
            reasons.update((r.sample.identity, r.reason.value) for r in plan.filter_result.rejected)
            selected.update((r.sample.identity, r.sample) for r in plan.filter_result.rejected)
        for report in session.reports:
            identity = report.response.source
            if (current.get(identity.node_id) != identity
                    or not self._since_ns <= report.received_monotonic_ns <= now):
                continue
            estimate = selected.get(identity)
            sample = ClockSample(session.round_key.round_id, report.response.payload["sent_time_ms"], 0,
                report.rtt_ns / 1000000, estimate.offset_ms if estimate else None,
                reasons.get(identity, "KEPT") if estimate else None)
            self._samples[identity] = (sample, report.received_monotonic_ns)
        # This is an actual captured Master T1, not a made-up remote timestamp.
        references = [r for r in session.reports if self._since_ns <= r.sent_monotonic_ns <= now]
        if references:
            reference = min(references, key=lambda r: r.sent_monotonic_ns)
            reason = reasons.get(self.identity, "KEPT") if self.identity in selected else None
            sample = ClockSample(session.round_key.round_id, reference.request.payload["master_time_ms"],
                                 0, 0, 0, reason)
            self._samples[self.identity] = (sample, reference.sent_monotonic_ns)
        acks = {ack.source: ack for ack in session.acknowledgments}
        members = {identity.node_id: identity for identity in session.participants}
        for command in session.adjustments:
            identity = members[command.target_node_id]
            stored = self._samples.get(identity)
            if current.get(identity.node_id) != identity or not stored or stored[0].round_id != command.round_id:
                continue
            ack = acks.get(identity)
            status = ack.payload["status"] if ack else None
            state = ClockState(ack.payload["clock_state"]) if ack else None
            if status == "ACCEPTED" and identity.node_id in applied_ids:
                status, state = "APPLIED", ClockState.NORMAL
            self._adjustments[identity] = AdjustmentStatus(command.round_id, command.payload["correction_ms"],
                status, state, self._brief(ack.payload.get("reason")) if ack else None)
        if plan is not None and plan.ready and session.adjustments and self.identity in self._samples:
            self._adjustments[self.identity] = AdjustmentStatus(session.round_key.round_id, plan.master_correction_ms)
        for identity, adjustment in tuple(self._adjustments.items()):
            stored = self._samples.get(identity)
            if stored is None or stored[0].round_id != adjustment.round_id:
                self._adjustments.pop(identity)

    def tick(self, *, peers, heartbeat, synchronization, clock):
        now = self._now()
        if self._role != NodeRole.MASTER or self._leader is None or now < self._next_ns:
            return ClusterUpdate("Monitoring publication not due")
        self._next_ns = now + self._interval_ns
        peers = tuple(peers)
        current = {p.identity.node_id: p.identity for p in peers}
        current[self.identity.node_id] = self.identity
        if len(current) != len(peers) + 1:
            raise ValueError("Duplicate/local identity in monitoring membership")
        for identity in tuple(self._samples):
            if current.get(identity.node_id) != identity:
                self._samples.pop(identity)
                self._adjustments.pop(identity, None)
        session = synchronization.session
        if session is not None and session.round_key.leader != self._leader:
            session = None
        if session is None:
            self._samples.clear()
            self._adjustments.clear()
        else:
            self._capture_samples(session, synchronization.plan, synchronization.applied_worker_ids, current, now)
        watches = {p.identity: p for p in heartbeat.peers} if heartbeat and heartbeat.leader == self._leader else {}
        rows = []
        local = PeerInfo(self.identity, self._name, self._ip, role=NodeRole.MASTER)
        for peer in sorted((local,) + peers, key=lambda p: p.identity.node_id):
            identity = peer.identity
            watch = watches.get(identity)
            last = watch.last_heartbeat_ns if watch else None
            age = ((now - last) // 1000000 if last is not None and self._since_ns <= last <= now else None)
            stored = self._samples.get(identity)
            sample = None
            if stored:
                sample = replace(stored[0], age_ms=(now - stored[1]) // 1000000)
            rows.append(NodeStatus(identity, peer.display_name, peer.ip,
                NodeRole.MASTER if identity == self.identity else NodeRole.WORKER,
                PeerStatus.ONLINE if identity == self.identity else watch.status if watch else PeerStatus.SUSPECT,
                watch.rtt_ms if watch and age is not None else None, age,
                sample, self._adjustments.get(identity)))
        round_status = None
        if session is not None:
            remaining = (max(0, (session.ack_deadline_ns - now + 999999) // 1000000)
                         if session.state == SyncRoundState.WAITING_ACK and session.ack_deadline_ns is not None else None)
            round_status = RoundStatus(session.round_key.round_id, session.state, session.participants,
                session.missing_report_ids, session.missing_ack_ids, remaining, self._brief(session.reason))
        previous = self._published_marks.get(self._leader)
        sequence = previous[1] + 1 if previous else 1
        status = ClusterStatus(sequence, clock.time_ms, (now - self._since_ns) // 1000000,
            tuple(rows), round_status, clock.state, clock.remaining_correction_ms)
        status.validate_leader(self._leader)
        messages = tuple(Message(MessageType.CLUSTER_STATUS, self.identity, status.to_payload(),
                                 target_node_id=peer.identity.node_id, leader=self._leader) for peer in peers)
        # Validate each complete frame now; failure cannot poison synchronization.
        for message in messages:
            serialize_packet(message)
        self._remember(self._published_marks, self._leader, (self._since_ns, sequence))
        self._status, self._received_ns = status, now
        return ClusterUpdate("Cluster monitoring published", messages, now + self._interval_ns, True)

    def handle_tcp(self, message: Message, *, received_monotonic_ns: int):
        now = self._now()
        if message.type != MessageType.CLUSTER_STATUS:
            return ClusterUpdate("Not a cluster monitoring message")
        if (self._role != NodeRole.WORKER or self._leader is None or message.leader != self._leader
                or message.source != self._leader.node or message.target_node_id != self.identity.node_id):
            return ClusterUpdate("Rejected monitoring Master/tenure/recipient")
        if type(received_monotonic_ns) is not int or not 0 <= received_monotonic_ns <= now:
            raise ProtocolError("Invalid monitoring receipt timestamp")
        if now - received_monotonic_ns > self._event_age_ns:
            return ClusterUpdate("Rejected stale queued monitoring event")
        status = ClusterStatus.from_payload(message.payload)
        status.validate_leader(self._leader)
        local = next((row for row in status.nodes if row.identity.node_id == self.identity.node_id), None)
        if local is None or local.identity != self.identity:
            return ClusterUpdate("Rejected monitoring for a different recipient incarnation")
        previous = self._received_marks.get(self._leader)
        round_id = status.round.round_id if status.round else 0
        if previous is not None:
            sequence, elapsed, previous_round = previous
            if status.sequence <= sequence:
                return ClusterUpdate("Ignored duplicate/out-of-order monitoring sequence")
            if status.elapsed_ms < elapsed or round_id < previous_round:
                return ClusterUpdate("Rejected regressing monitoring time/round")
        self._remember(self._received_marks, self._leader, (status.sequence, status.elapsed_ms, round_id))
        self._status, self._received_ns = status, received_monotonic_ns
        return ClusterUpdate("Cluster monitoring accepted", accepted=True)
