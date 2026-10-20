"""Heartbeat, Bully and manual leadership policy; no socket dispatch or GUI."""

from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable, Optional, Tuple

from ..core.election import BullyElection, ElectionMode, ElectionSnapshot, ElectionState, ElectionUpdate
from ..core.models import NodeRole, PeerInfo
from ..core.peer_registry import PeerRegistry
from ..core.protocol import Message, MessageType
from ..network.heartbeat import HeartbeatService, HeartbeatSnapshot, TickResult
from ..network.udp_transport import DatagramEvent, UDPTransport


_ELECTION_TYPES = frozenset({
    MessageType.ELECTION, MessageType.OK, MessageType.COORDINATOR, MessageType.FORCE_MASTER,
})


@dataclass(frozen=True)
class LeadershipCycle:
    heartbeat: TickResult
    updates: Tuple[ElectionUpdate, ...]


class LeadershipController:
    """Return election actions for NodeController to publish and send.

    This controller never imports another controller. Same-ID probe retries
    retain the original phase deadline. Heartbeat leader changes are applied
    here immediately, while membership/advertisement updates are orchestrated
    outside this class. Methods run on one owner thread.
    """

    def __init__(self, registry: PeerRegistry, clock_source: Callable[[], int], *,
                 monotonic_source: Callable[[], int] = monotonic_ns) -> None:
        self.election = BullyElection(registry, monotonic_source=monotonic_source)
        self._registry = registry
        self._clock_source, self._monotonic_source = clock_source, monotonic_source
        self._heartbeat = None  # type: Optional[HeartbeatService]
        self._last = self.election.snapshot()
        self._probes = {}
        self._next_probe_retry_ns = 0
        self._next_manual_announce_ns = 0

    def bind(self, transport: UDPTransport) -> None:
        self._heartbeat = HeartbeatService(
            transport, self._registry, self._clock_source, leader=self._last.leader,
            monotonic_source=self._monotonic_source)

    def snapshot(self) -> ElectionSnapshot:
        return self.election.snapshot()

    def heartbeat_snapshot(self) -> Optional[HeartbeatSnapshot]:
        return self._heartbeat.snapshot() if self._heartbeat is not None else None

    @staticmethod
    def accepts(message: Message) -> bool:
        return message.type in _ELECTION_TYPES

    def _record(self, update: ElectionUpdate) -> ElectionUpdate:
        previous, current = self._last, update.snapshot
        if ((previous.leader, previous.mode, previous.election_id)
                != (current.leader, current.mode, current.election_id)):
            self._probes.clear()
            self._next_probe_retry_ns = self._monotonic_source() + 500000000
        if self._heartbeat is not None and previous.leader != current.leader:
            self._heartbeat.set_leader(current.leader)
        for message in update.outbound:
            if message.type == MessageType.ELECTION:
                self._probes[message.target_node_id] = message
        self._last = current
        return update

    def handle_udp(self, event: DatagramEvent):
        return self._heartbeat.handle(event)

    def handle_tcp(self, message: Message, received_monotonic_ns: int) -> Tuple[ElectionUpdate, ...]:
        updates = []
        if message.type == MessageType.ELECTION and self.snapshot().state == ElectionState.IDLE:
            # Establish an incumbent before answering the first lower probe,
            # including when the highest-ID node has not reached startup grace.
            updates.append(self._record(self.election.start()))
        updates.append(self._record(self.election.handle(
            message, received_monotonic_ns=received_monotonic_ns)))
        return tuple(updates)

    def membership_updated(self, peer: PeerInfo, *, ready: bool) -> Optional[ElectionUpdate]:
        election = self.snapshot()
        if (ready and election.mode == ElectionMode.AUTOMATIC and election.leader is not None
                and (peer.identity.node_id > election.leader.node.node_id
                     or (peer.identity.node_id == election.leader.node.node_id
                         and peer.identity != election.leader.node))):
            return self._record(self.election.start(expected_leader=election.leader))
        return None

    def force_master(self, reason: str) -> ElectionUpdate:
        update = self._record(self.election.force_master(reason))
        self._next_manual_announce_ns = self._monotonic_source() + 2000000000
        return update

    def tick(self, *, ready: bool) -> LeadershipCycle:
        heartbeat = self._heartbeat.tick()
        updates = []
        if heartbeat.snapshot.master_timed_out:
            updates.append(self._record(self.election.leader_timed_out(heartbeat.snapshot.leader)))
        if ready and self.snapshot().state == ElectionState.IDLE:
            updates.append(self._record(self.election.start()))
        updates.append(self._record(self.election.tick()))
        now, election = self._monotonic_source(), self.snapshot()
        if (election.role == NodeRole.CANDIDATE and election.deadline_ns is not None
                and self._next_probe_retry_ns <= now < election.deadline_ns):
            self._next_probe_retry_ns = now + 500000000
            updates.append(ElectionUpdate(tuple(self._probes.values()), election, False,
                                          "Retry current election probes without extending deadline"))
        if (election.mode == ElectionMode.MANUAL and election.role == NodeRole.MASTER
                and now >= self._next_manual_announce_ns):
            self._next_manual_announce_ns = now + 2000000000
            updates.append(self._record(self.election.force_master(election.manual_reason)))
        return LeadershipCycle(heartbeat, tuple(updates))
