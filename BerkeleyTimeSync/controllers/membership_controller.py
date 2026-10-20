"""Discovery, peer metadata and explicit restart acceptance; no elections."""

from typing import Callable, Optional, Tuple
from time import monotonic_ns
from uuid import UUID

from ..core.models import LeaderIdentity, NodeIdentity, NodeRole, PeerInfo
from ..core.peer_registry import PeerRegistry
from ..core.protocol import Message
from ..network.discovery import DiscoveryService, DiscoveryResult, DiscoveryStatus
from ..network.tcp_transport import ConnectionInfo
from ..network.udp_transport import DatagramEvent, UDPTransport


class MembershipController:
    """Own discovery/conflict handling on the Node controller's owner thread.

    Methods return metadata/results; closing sockets and applying leadership
    decisions belong to NodeController. registry remains thread-safe for read
    access by TCP dispatch workers.
    """

    def __init__(self, registry: PeerRegistry, *,
                 monotonic_source: Callable[[], int] = monotonic_ns) -> None:
        self.registry = registry
        self._monotonic_source = monotonic_source
        self._discovery = None  # type: Optional[DiscoveryService]
        self._conflicts = {}

    def bind(self, transport: UDPTransport, local_peer: PeerInfo, *,
             broadcast_ip: str, discovery_port: int) -> None:
        self._discovery = DiscoveryService(
            transport, self.registry, local_peer, broadcast_ip=broadcast_ip,
            discovery_port=discovery_port, monotonic_source=self._monotonic_source)

    def set_advertisement(self, peer: PeerInfo, leader: Optional[LeaderIdentity]) -> None:
        self._discovery.set_advertisement(peer, leader=leader)

    def tick(self) -> None:
        self._discovery.tick()

    def handle_udp(self, event: DatagramEvent) -> Optional[DiscoveryResult]:
        result = self._discovery.handle(event)
        if result is not None and result.status == DiscoveryStatus.CONFLICT:
            self._conflicts[result.candidate.identity.node_id] = result.candidate
        return result

    def conflicts(self) -> Tuple[PeerInfo, ...]:
        return tuple(self._conflicts[key] for key in sorted(self._conflicts))

    def accept_restart(self, expected: NodeIdentity) -> PeerInfo:
        """CAS replacement after external validation; never replace a live peer."""
        if not isinstance(expected, NodeIdentity):
            raise TypeError("expected must be NodeIdentity")
        candidate = self._conflicts.get(expected.node_id)
        if candidate is None:
            raise ValueError("No discovery restart candidate for this node ID")
        observed = self.registry.replace_offline(expected, candidate)
        self._conflicts.pop(expected.node_id)
        return observed

    def refresh_roles(self, leader: Optional[LeaderIdentity]) -> None:
        """Apply the orchestrator's decision, without refreshing contact time."""
        for peer in self.registry.snapshot():
            role = (NodeRole.UNASSIGNED if leader is None else
                    NodeRole.MASTER if peer.identity == leader.node else NodeRole.WORKER)
            if peer.role != role:
                self.registry.update_role(peer.identity, role)

    def admits_tcp(self, connection: ConnectionInfo, message: Message) -> bool:
        peer = self.registry.get(message.source.node_id)
        return (peer is not None and peer.identity == message.source
                and connection.peer == message.source and peer.ip == connection.address[0]
                and (connection.incoming or peer.tcp_port == connection.address[1]))

    def obsolete_connections(self, connections: Tuple[ConnectionInfo, ...]) -> Tuple[UUID, ...]:
        obsolete = []
        for connection in connections:
            if connection.peer is None:
                continue
            peer = self.registry.get(connection.peer.node_id)
            if (peer is None or peer.identity != connection.peer or peer.ip != connection.address[0]
                    or (not connection.incoming and peer.tcp_port != connection.address[1])):
                obsolete.append(connection.connection_id)
        return tuple(obsolete)
