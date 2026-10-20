"""Controller-driven LAN discovery over the shared UDP transport.

Call tick periodically and route UDP events through handle on ONE controller
thread. This module never drains the UDP queue, starts threads, elects a Master
or replaces a different boot_id. PING/PONG and ordinary transport errors are
left for other handlers. Local advertisements are supplied by the controller.

The actual UDP sender endpoint is used for membership and replies, ignoring
advertised IP/UDP port for routing. TCP port and role remain advertised hints;
advertised_leader is not a leadership decision. There is no authentication.
Duplicate suppression is bounded and temporary, not cryptographic replay
protection. Discovery contact shows reachability, not successful clock sync.
"""

from dataclasses import dataclass
from enum import Enum
from ipaddress import IPv4Address
from time import monotonic_ns
from typing import Callable, Dict, Optional, Set, Tuple
from uuid import UUID

from ..core.models import LeaderIdentity, NodeIdentity, NodeRole, PeerInfo
from ..core.peer_registry import NodeIdConflictError, PeerRegistry, StalePeerError
from ..core.protocol import Message, MessageType, ProtocolError
from .udp_transport import DatagramEvent, EventType, SendReceipt, UDPTransport


class DiscoveryStatus(str, Enum):
    UPDATED = "UPDATED"
    DUPLICATE = "DUPLICATE"
    IGNORED = "IGNORED"
    STALE = "STALE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class DiscoveryResult:
    status: DiscoveryStatus
    candidate: Optional[PeerInfo] = None
    observed: Optional[PeerInfo] = None
    advertised_leader: Optional[LeaderIdentity] = None
    conflicting_identity: Optional[NodeIdentity] = None
    reply_receipt: Optional[SendReceipt] = None
    detail: Optional[str] = None


@dataclass
class _Probe:
    deadline_ns: int
    responders: Set[NodeIdentity]


def _milliseconds(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 60000:
        raise ValueError("{} must be in [1, 60000] milliseconds".format(name))
    return value * 1000000


class DiscoveryService:
    """Periodic discovery, reply correlation and conservative membership updates.

    The UDP transport must already be bound, with an advertised local UDP port
    matching its real port. Production defaults use port 8888. Tests may use
    ephemeral ports and a caller-selected loopback broadcast destination.
    Retry every two seconds by default; there is no guarantee of discovery
    within a fixed time on a lossy or blocked LAN.

    Different boot IDs, including offline peers, return CONFLICT. A controller
    may separately validate a restart and call PeerRegistry.replace_offline.
    Delayed/unknown replies never refresh membership. Local role changes use
    set_advertisement; they do not reset pending probes or choose a leader.
    """

    def __init__(
        self, transport: UDPTransport, registry: PeerRegistry, local_peer: PeerInfo,
        *, leader: Optional[LeaderIdentity] = None,
        broadcast_ip: str = "255.255.255.255", discovery_port: int = 8888,
        interval_ms: int = 2000, reply_timeout_ms: int = 3000,
        max_event_age_ms: int = 1000, duplicate_window_ms: int = 30000,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not isinstance(transport, UDPTransport) or not isinstance(registry, PeerRegistry):
            raise TypeError("Expected UDPTransport and PeerRegistry")
        if transport.local_identity != registry.local_identity:
            raise ValueError("Transport and registry must share one process identity")
        IPv4Address(broadcast_ip)
        if type(discovery_port) is not int or not 1 <= discovery_port <= 65535:
            raise ValueError("Invalid discovery port")
        self._interval_ns = _milliseconds(interval_ms, "interval_ms")
        self._reply_timeout_ns = _milliseconds(reply_timeout_ms, "reply_timeout_ms")
        self._event_age_ns = _milliseconds(max_event_age_ms, "max_event_age_ms")
        self._duplicate_ns = _milliseconds(duplicate_window_ms, "duplicate_window_ms")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self._transport = transport
        self._registry = registry
        self._broadcast_ip = broadcast_ip
        self._discovery_port = discovery_port
        self._monotonic_source = monotonic_source
        self._last_now_ns = None  # type: Optional[int]
        self._next_probe_ns = 0
        self._pending = {}  # type: Dict[UUID, _Probe]
        self._seen = {}  # type: Dict[Tuple[NodeIdentity, UUID], Tuple[Message, int]]
        self.set_advertisement(local_peer, leader=leader)
        self._now()

    def _now(self) -> int:
        now = self._monotonic_source()
        if type(now) is not int or now < 0:
            raise ValueError("monotonic_source must return nonnegative integer nanoseconds")
        if self._last_now_ns is not None and now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    def set_advertisement(
        self, local_peer: PeerInfo, *, leader: Optional[LeaderIdentity] = None,
    ) -> None:
        """Validate atomically; controller supplies the current role/known leader."""
        if not isinstance(local_peer, PeerInfo):
            raise TypeError("local_peer must be PeerInfo")
        if local_peer.identity != self._transport.local_identity:
            raise ValueError("Advertisement must describe this process")
        address = self._transport.address
        if not self._transport.is_running or address is None:
            raise ValueError("Bind the UDP transport before configuring discovery")
        if local_peer.udp_port != address[1]:
            raise ValueError("Advertised UDP port differs from the bound port")
        ip = IPv4Address(local_peer.ip)
        if ip.is_unspecified or ip.is_multicast or str(ip) == "255.255.255.255":
            raise ValueError("Advertise a usable local interface address")
        if address[0] != "0.0.0.0" and address[0] != local_peer.ip:
            raise ValueError("Advertisement differs from the bound interface")
        payload = {
            "display_name": local_peer.display_name, "ip": local_peer.ip,
            "tcp_port": local_peer.tcp_port, "udp_port": local_peer.udp_port,
            "role": local_peer.role.value,
        }
        # Reuse protocol checks for a Master advertisement and matching tenure.
        Message(MessageType.DISCOVER, local_peer.identity, payload, leader=leader)
        self._local_peer, self._leader = local_peer, leader
        self._payload = payload

    def _prune(self, now: int) -> None:
        self._pending = {key: value for key, value in self._pending.items()
                         if now < value.deadline_ns}
        self._seen = {key: value for key, value in self._seen.items() if now < value[1]}

    def tick(self, *, force: bool = False) -> Optional[SendReceipt]:
        """Send when due; propagate socket errors and wait a period before retry.

        force is an explicit rescan, bounded to 32 outstanding probe IDs. Each
        probe can accept distinct replies from all five remote nodes. No send
        failure or discovery timeout itself removes a peer from the registry.
        """
        if type(force) is not bool:
            raise TypeError("force must be bool")
        now = self._now()
        self._prune(now)
        if not force and now < self._next_probe_ns:
            return None
        request = Message(
            MessageType.DISCOVER, self._local_peer.identity, self._payload,
            leader=self._leader,
        )
        if len(self._pending) >= 32:
            self._pending.pop(next(iter(self._pending)))
        self._pending[request.message_id] = _Probe(now + self._reply_timeout_ns, set())
        self._next_probe_ns = now + self._interval_ns
        try:
            return self._transport.broadcast(
                request, broadcast_ip=self._broadcast_ip, port=self._discovery_port)
        except Exception:
            self._pending.pop(request.message_id, None)
            raise

    def _reply(self, request: Message, address: Tuple[str, int]) -> SendReceipt:
        reply = Message(
            MessageType.DISCOVER_REPLY, self._local_peer.identity, self._payload,
            target_node_id=request.source.node_id, leader=self._leader,
            reply_to=request.message_id,
        )
        return self._transport.send(address[0], reply, port=address[1])

    def handle(self, event: DatagramEvent) -> Optional[DiscoveryResult]:
        """Handle a discovery event without taking ownership of the UDP queue.

        ProtocolError identifies an invalid event for the controller to log.
        Reply send failures return detail while retaining valid observed contact.
        A duplicate DISCOVER is answered again (a prior reply may be lost), but
        does not refresh liveness. Duplicate replies do not refresh it either.
        """
        if not isinstance(event, DatagramEvent):
            raise TypeError("Expected a DatagramEvent")
        message = event.message
        if message is None or message.type not in (
            MessageType.DISCOVER, MessageType.DISCOVER_REPLY,
        ):
            return None
        # UDP exposes a conflicting local node ID through an ERROR event.
        local_conflict = (message.source.node_id == self._local_peer.identity.node_id
                          and message.source != self._local_peer.identity)
        if event.type != EventType.MESSAGE and not (
            event.type == EventType.ERROR and local_conflict
        ):
            return None
        now = self._now()
        self._prune(now)
        if type(event.monotonic_ns) is not int or not 0 <= event.monotonic_ns <= now:
            raise ProtocolError("Invalid local receive timestamp")
        if now - event.monotonic_ns > self._event_age_ns:
            return DiscoveryResult(DiscoveryStatus.STALE, detail="Discovery event is too old")
        if message.source == self._local_peer.identity:
            return DiscoveryResult(DiscoveryStatus.IGNORED, detail="Local discovery echo")
        if not local_conflict and message.target_node_id not in (
            None, self._local_peer.identity.node_id,
        ):
            return DiscoveryResult(DiscoveryStatus.IGNORED, detail="Another recipient")
        address = event.address
        if not isinstance(address, tuple) or len(address) != 2:
            raise ProtocolError("Discovery requires a sender endpoint")
        try:
            ip = IPv4Address(address[0])
        except (ValueError, TypeError) as exc:
            raise ProtocolError("Invalid sender IPv4 address") from exc
        if (not isinstance(address[0], str) or ip.is_unspecified or ip.is_multicast
                or str(ip) == "255.255.255.255" or type(address[1]) is not int
                or not 1 <= address[1] <= 65535):
            raise ProtocolError("Invalid sender endpoint")
        candidate = PeerInfo(
            message.source, message.payload["display_name"], str(ip),
            tcp_port=message.payload["tcp_port"], udp_port=address[1],
            role=NodeRole(message.payload["role"]),
        )
        if local_conflict:
            return DiscoveryResult(
                DiscoveryStatus.CONFLICT, candidate=candidate,
                conflicting_identity=self._local_peer.identity,
                detail="Another process uses the local node ID")
        key = (message.source, message.message_id)
        previous = self._seen.get(key)
        if previous is not None and previous[0] != message:
            raise ProtocolError("Discovery message ID reused with different contents")
        probe = None
        if message.type == MessageType.DISCOVER_REPLY:
            probe = self._pending.get(message.reply_to)
            if probe is None:
                return DiscoveryResult(
                    DiscoveryStatus.STALE, candidate=candidate,
                    detail="Unknown or expired discovery request")
            if message.source in probe.responders:
                return DiscoveryResult(DiscoveryStatus.DUPLICATE, candidate=candidate)
        duplicate = previous is not None
        observed = None
        if not duplicate:
            try:
                observed = self._registry.observe(candidate)
            except NodeIdConflictError as exc:
                return DiscoveryResult(
                    DiscoveryStatus.CONFLICT, candidate=candidate,
                    conflicting_identity=exc.current_identity, detail=str(exc))
            except StalePeerError as exc:
                return DiscoveryResult(DiscoveryStatus.STALE, candidate=candidate, detail=str(exc))
            if len(self._seen) >= 256:
                self._seen.pop(next(iter(self._seen)))
            self._seen[key] = (message, now + self._duplicate_ns)
            if probe is not None:
                probe.responders.add(message.source)
        receipt, detail = None, None
        if message.type == MessageType.DISCOVER:
            try:
                receipt = self._reply(message, address)
            except (OSError, RuntimeError) as exc:
                detail = "Discovery reply failed: {}: {}".format(type(exc).__name__, exc)
        return DiscoveryResult(
            DiscoveryStatus.DUPLICATE if duplicate else DiscoveryStatus.UPDATED,
            candidate=candidate, observed=observed, advertised_leader=message.leader,
            reply_receipt=receipt, detail=detail,
        )
