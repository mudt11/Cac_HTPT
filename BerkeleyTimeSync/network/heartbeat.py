"""Controller-driven Master PING / Worker PONG failure detection.

Call tick, handle, snapshot and set_leader on one controller thread. This
service does not drain the shared UDP queue, start threads or run elections.
The controller supplies a validated leader; discovery cannot select one here.
Heartbeat timers are independent of PeerRegistry discovery-contact timestamps.

A Master monitors correlated PONGs; Workers monitor unique PINGs from their
current Master tenure. Timeout is suspicion of lost heartbeat, not proof of
crash. The controller must use this snapshot for heartbeat state rather than
overwrite it with the registry's broader discovery reachability state.
"""

from dataclasses import dataclass
from enum import Enum
from ipaddress import IPv4Address
from time import monotonic_ns
from typing import Callable, Dict, Optional, Tuple
from uuid import UUID

from ..core.models import LeaderIdentity, NodeIdentity, NodeRole, PeerInfo, PeerStatus
from ..core.peer_registry import PeerRegistry, StalePeerError
from ..core.protocol import MAX_INTEGER, Message, MessageType, ProtocolError
from .udp_transport import DatagramEvent, EventType, SendReceipt, UDPTransport


class HeartbeatStatus(str, Enum):
    RECEIVED = "RECEIVED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class HeartbeatPeer:
    identity: NodeIdentity
    role: NodeRole
    status: PeerStatus
    monitoring_since_ns: int
    last_heartbeat_ns: Optional[int]
    deadline_ns: int
    rtt_ms: Optional[float]


@dataclass(frozen=True)
class HeartbeatSnapshot:
    leader: Optional[LeaderIdentity]
    peers: Tuple[HeartbeatPeer, ...]
    master_timed_out: bool


@dataclass(frozen=True)
class SendFailure:
    identity: NodeIdentity
    detail: str


@dataclass(frozen=True)
class TickResult:
    sent: Tuple[SendReceipt, ...]
    failures: Tuple[SendFailure, ...]
    changes: Tuple[HeartbeatPeer, ...]
    snapshot: HeartbeatSnapshot


@dataclass(frozen=True)
class HeartbeatResult:
    status: HeartbeatStatus
    peer: Optional[HeartbeatPeer] = None
    reply_receipt: Optional[SendReceipt] = None
    detail: Optional[str] = None


@dataclass
class _Watch:
    identity: NodeIdentity
    address: Tuple[str, int]
    since_ns: int
    last_ns: Optional[int] = None
    next_ping_ns: int = 0
    rtt_ms: Optional[float] = None


@dataclass(frozen=True)
class _Probe:
    identity: NodeIdentity
    address: Tuple[str, int]
    sent_ns: int
    deadline_ns: int


def _milliseconds(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 60000:
        raise ValueError("{} must be in [1, 60000] milliseconds".format(name))
    return value * 1000000


def _endpoint(peer: PeerInfo) -> Tuple[str, int]:
    ip = IPv4Address(peer.ip)
    if ip.is_unspecified or ip.is_multicast or str(ip) == "255.255.255.255":
        raise ValueError("Heartbeat requires a usable peer IPv4 endpoint")
    return peer.ip, peer.udp_port


class HeartbeatService:
    """Track at most five peers, pinning boot IDs, endpoints and leader epoch.

    Defaults: PING every 2 s, SUSPECT after 4 s without a heartbeat, Worker
    OFFLINE at 6 s and Master OFFLINE at 8 s. Before the first heartbeat, a
    watched node is SUSPECT and gets the same timeout grace from monitoring
    start. Reapplying the same leader never resets that grace or the timers.

    Supply the same virtual-clock reader to this service and UDPTransport so
    received_time_ms is captured at recv, not when the queue is processed.
    PING/PONG measure heartbeat RTT only; they never correct any clock.
    Duplicate suppression is bounded to 256 messages / 30 s, not authentication.
    """

    def __init__(
        self, transport: UDPTransport, registry: PeerRegistry,
        clock_source: Callable[[], int], *, leader: Optional[LeaderIdentity] = None,
        interval_ms: int = 2000, suspect_after_ms: int = 4000,
        worker_timeout_ms: int = 6000, master_timeout_ms: int = 8000,
        reply_timeout_ms: int = 2000, max_event_age_ms: int = 1000,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not isinstance(transport, UDPTransport) or not isinstance(registry, PeerRegistry):
            raise TypeError("Expected UDPTransport and PeerRegistry")
        if transport.local_identity != registry.local_identity:
            raise ValueError("Transport and registry must share one process identity")
        if not transport.is_running:
            raise ValueError("Bind the UDP transport before starting heartbeat")
        if not callable(clock_source) or not callable(monotonic_source):
            raise TypeError("Clock readers must be callable")
        self._interval_ns = _milliseconds(interval_ms, "interval_ms")
        self._suspect_ns = _milliseconds(suspect_after_ms, "suspect_after_ms")
        self._worker_ns = _milliseconds(worker_timeout_ms, "worker_timeout_ms")
        self._master_ns = _milliseconds(master_timeout_ms, "master_timeout_ms")
        self._reply_ns = _milliseconds(reply_timeout_ms, "reply_timeout_ms")
        self._event_age_ns = _milliseconds(max_event_age_ms, "max_event_age_ms")
        if not self._interval_ns <= self._suspect_ns < self._worker_ns <= self._master_ns:
            raise ValueError("Require interval <= suspect < worker timeout <= master timeout")
        if self._reply_ns > self._worker_ns:
            raise ValueError("Reply timeout must not exceed the Worker timeout")
        self._transport, self._registry = transport, registry
        self._local = transport.local_identity
        self._clock_source, self._monotonic_source = clock_source, monotonic_source
        self._leader = None  # type: Optional[LeaderIdentity]
        self._last_now_ns = None  # type: Optional[int]
        self._watches = {}  # type: Dict[int, _Watch]
        self._pending = {}  # type: Dict[UUID, _Probe]
        self._seen = {}  # type: Dict[Tuple[NodeIdentity, UUID], Tuple[Message, int]]
        self._reported = {}  # type: Dict[NodeIdentity, PeerStatus]
        self._now()
        self.set_leader(leader)

    def _now(self) -> int:
        now = self._monotonic_source()
        if type(now) is not int or now < 0:
            raise ValueError("monotonic_source must return nonnegative integer nanoseconds")
        if self._last_now_ns is not None and now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    def _clock_ms(self) -> int:
        value = self._clock_source()
        if type(value) is not int or not 0 <= value <= MAX_INTEGER:
            raise ValueError("clock_source returned invalid milliseconds")
        return value

    @property
    def _is_master(self) -> bool:
        return self._leader is not None and self._leader.node == self._local

    def set_leader(self, leader: Optional[LeaderIdentity]) -> None:
        """Apply an election/controller decision; clear old-tenure correlations."""
        if leader is not None and not isinstance(leader, LeaderIdentity):
            raise TypeError("leader must be LeaderIdentity or None")
        if leader == self._leader:
            return
        if leader is not None:
            if leader.node.node_id == self._local.node_id:
                if leader.node != self._local:
                    raise ValueError("Local Master has another process identity")
            else:
                peer = self._registry.get(leader.node.node_id)
                if peer is None or peer.identity != leader.node:
                    raise ValueError("Discover and validate the Master identity first")
                _endpoint(peer)
        now = self._now()
        self._leader = leader
        self._watches.clear()
        self._pending.clear()
        self._seen.clear()
        self._reported.clear()
        self._reconcile(now)

    def _reconcile(self, now: int) -> None:
        if self._leader is None:
            return
        if self._is_master:
            peers = self._registry.snapshot()
            wanted = {peer.identity.node_id for peer in peers}
            self._watches = {key: value for key, value in self._watches.items() if key in wanted}
        else:
            peer = self._registry.get(self._leader.node.node_id)
            # If a validated restart replaced the registry entry, keep watching
            # the old Master until the controller supplies a new leader epoch.
            peers = (peer,) if peer is not None and peer.identity == self._leader.node else ()
        for peer in peers:
            address = _endpoint(peer)
            watch = self._watches.get(peer.identity.node_id)
            if watch is None or watch.identity != peer.identity:
                self._watches[peer.identity.node_id] = _Watch(peer.identity, address, now)
            elif watch.address != address:
                watch.address = address
                # Endpoint changes do not prove fresh heartbeat contact.
                self._pending = {key: value for key, value in self._pending.items()
                                 if value.identity != peer.identity}
        identities = {watch.identity for watch in self._watches.values()}
        self._pending = {key: value for key, value in self._pending.items()
                         if value.identity in identities and now < value.deadline_ns}
        self._seen = {key: value for key, value in self._seen.items()
                      if key[0] in identities and now < value[1]}
        self._reported = {key: value for key, value in self._reported.items() if key in identities}

    def _view(self, watch: _Watch, now: int) -> HeartbeatPeer:
        timeout = self._worker_ns if self._is_master else self._master_ns
        reference = watch.since_ns if watch.last_ns is None else watch.last_ns
        elapsed = now - reference
        if elapsed >= timeout:
            status = PeerStatus.OFFLINE
        elif watch.last_ns is None or elapsed >= self._suspect_ns:
            status = PeerStatus.SUSPECT
        else:
            status = PeerStatus.ONLINE
        return HeartbeatPeer(
            watch.identity, NodeRole.WORKER if self._is_master else NodeRole.MASTER,
            status, watch.since_ns, watch.last_ns, reference + timeout, watch.rtt_ms)

    def snapshot(self) -> HeartbeatSnapshot:
        """Current state without consuming transitions returned by tick."""
        now = self._now()
        self._reconcile(now)
        peers = tuple(self._view(self._watches[key], now) for key in sorted(self._watches))
        expired = not self._is_master and any(p.status == PeerStatus.OFFLINE for p in peers)
        return HeartbeatSnapshot(self._leader, peers, expired)

    def tick(self) -> TickResult:
        """Probe even offline Workers for recovery; return send errors per peer.

        master_timed_out is a level condition. The controller must guard against
        starting multiple simultaneous elections. No socket error alone changes
        liveness, and discovery/GUI polling never resets heartbeat deadlines.
        """
        self._reconcile(self._now())
        sent, failures = [], []
        if self._is_master:
            for node_id in sorted(self._watches):
                watch = self._watches[node_id]
                now = self._now()
                if now < watch.next_ping_ns:
                    continue
                watch.next_ping_ns = now + self._interval_ns
                prepared = []

                def factory() -> Message:
                    message = Message(
                        MessageType.PING, self._local, {"master_time_ms": self._clock_ms()},
                        target_node_id=watch.identity.node_id, leader=self._leader)
                    prepared.append(message)
                    return message

                try:
                    receipt = self._transport.send(watch.address[0], factory, port=watch.address[1])
                    message = prepared[0]
                    if len(self._pending) >= 32:
                        self._pending.pop(next(iter(self._pending)))
                    self._pending[message.message_id] = _Probe(
                        watch.identity, watch.address, receipt.sent_monotonic_ns,
                        receipt.sent_monotonic_ns + self._reply_ns)
                    sent.append(receipt)
                except (OSError, RuntimeError, ValueError) as exc:
                    failures.append(SendFailure(watch.identity, "{}: {}".format(type(exc).__name__, exc)))
        snapshot = self.snapshot()
        changes = tuple(peer for peer in snapshot.peers
                        if self._reported.get(peer.identity) != peer.status)
        for peer in snapshot.peers:
            self._reported[peer.identity] = peer.status
        return TickResult(tuple(sent), tuple(failures), changes, snapshot)

    def handle(self, event: DatagramEvent) -> Optional[HeartbeatResult]:
        """Validate tenure, identity, endpoint, receive age and PONG correlation.

        A valid PING counts as Master contact even if its PONG cannot be sent.
        Missing receive-clock capture rejects a PING explicitly, so incorrect
        transport configuration cannot silently fabricate worker timestamps.
        """
        if not isinstance(event, DatagramEvent):
            raise TypeError("Expected a DatagramEvent")
        message = event.message
        if event.type != EventType.MESSAGE or message is None or message.type not in (
            MessageType.PING, MessageType.PONG,
        ):
            return None
        now = self._now()
        self._reconcile(now)
        if type(event.monotonic_ns) is not int or not 0 <= event.monotonic_ns <= now:
            raise ProtocolError("Invalid local heartbeat receive timestamp")
        if now - event.monotonic_ns > self._event_age_ns:
            return HeartbeatResult(HeartbeatStatus.STALE, detail="Heartbeat event is too old")
        if self._leader is None or message.leader != self._leader:
            return HeartbeatResult(HeartbeatStatus.REJECTED, detail="Another Master tenure")
        expected_type = MessageType.PONG if self._is_master else MessageType.PING
        if message.type != expected_type or message.target_node_id != self._local.node_id:
            return HeartbeatResult(HeartbeatStatus.REJECTED, detail="Wrong direction or recipient")
        watch = self._watches.get(message.source.node_id)
        registered = self._registry.get(message.source.node_id)
        if (watch is None or watch.identity != message.source or registered is None
                or registered.identity != message.source or event.address != watch.address):
            return HeartbeatResult(HeartbeatStatus.REJECTED, detail="Unknown identity or endpoint")
        if event.monotonic_ns < watch.since_ns:
            return HeartbeatResult(HeartbeatStatus.STALE, detail="Heartbeat predates monitoring")
        key = (message.source, message.message_id)
        previous = self._seen.get(key)
        if previous is not None:
            if previous[0] != message:
                raise ProtocolError("Heartbeat message ID reused with different contents")
            return HeartbeatResult(HeartbeatStatus.DUPLICATE, peer=self._view(watch, now))
        probe = None
        if self._is_master:
            probe = self._pending.get(message.reply_to)
            if (probe is None or probe.identity != message.source or probe.address != event.address
                    or event.monotonic_ns < probe.sent_ns):
                return HeartbeatResult(HeartbeatStatus.STALE, detail="Unknown or expired PING")
        elif type(event.clock_time_ms) is not int or not 0 <= event.clock_time_ms <= MAX_INTEGER:
            raise ProtocolError("Configure UDP receive clock capture for PONG timestamps")
        try:
            self._registry.mark_seen(message.source)
        except (KeyError, StalePeerError):
            return HeartbeatResult(HeartbeatStatus.REJECTED, detail="Peer identity changed")
        watch.last_ns = max(watch.last_ns or 0, event.monotonic_ns)
        if probe is not None:
            watch.rtt_ms = (event.monotonic_ns - probe.sent_ns) / 1000000.0
            self._pending.pop(message.reply_to)
        if len(self._seen) >= 256:
            self._seen.pop(next(iter(self._seen)))
        self._seen[key] = (message, now + 30000000000)
        receipt, detail = None, None
        if not self._is_master:
            def factory() -> Message:
                return Message(
                    MessageType.PONG, self._local,
                    {"received_time_ms": event.clock_time_ms, "sent_time_ms": self._clock_ms()},
                    target_node_id=message.source.node_id, leader=self._leader,
                    reply_to=message.message_id)
            try:
                receipt = self._transport.send(watch.address[0], factory, port=watch.address[1])
            except (OSError, RuntimeError, ValueError) as exc:
                detail = "PONG send failed: {}: {}".format(type(exc).__name__, exc)
        return HeartbeatResult(
            HeartbeatStatus.RECEIVED, self._view(watch, now), receipt, detail)
