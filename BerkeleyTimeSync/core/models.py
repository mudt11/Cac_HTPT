"""Immutable domain models shared by Master and Worker nodes.

These models contain no networking or GUI operations. UUIDs remain UUID
objects here; the protocol layer is responsible for their JSON encoding.
"""

from dataclasses import dataclass, field
from enum import Enum
from ipaddress import IPv4Address
from typing import Optional
from uuid import UUID, uuid4


class NodeRole(str, Enum):
    """The node's current responsibility, independent of reachability."""

    UNASSIGNED = "UNASSIGNED"
    WORKER = "WORKER"
    CANDIDATE = "CANDIDATE"
    MASTER = "MASTER"


class PeerStatus(str, Enum):
    """Reachability of a peer as observed by the local node."""

    ONLINE = "ONLINE"
    SUSPECT = "SUSPECT"
    OFFLINE = "OFFLINE"


class SyncRoundState(str, Enum):
    """Progress of one synchronization round.

    COMPLETED means the required adjustment commands were acknowledged,
    not that every clock has already finished slewing.
    """

    COLLECTING = "COLLECTING"
    COMPUTING = "COMPUTING"
    WAITING_ACK = "WAITING_ACK"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ClockState(str, Enum):
    """Whether a virtual clock is currently applying a gradual correction."""

    NORMAL = "NORMAL"
    SLEWING = "SLEWING"


def _require_int(
    name: str, value: int, minimum: int, maximum: Optional[int] = None
) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("{} must be an integer".format(name))
    if value < minimum or (maximum is not None and value > maximum):
        if maximum is None:
            raise ValueError("{} must be at least {}".format(name, minimum))
        raise ValueError(
            "{} must be between {} and {}".format(name, minimum, maximum)
        )


@dataclass(frozen=True)
class NodeIdentity:
    """Identity of one process incarnation of a node numbered 1 through 6.

    Create once at application startup and reuse throughout that process.
    Restoring an identity from a message requires its explicit boot_id.
    """

    node_id: int
    boot_id: UUID = field(default_factory=uuid4)

    def __post_init__(self) -> None:
        _require_int("node_id", self.node_id, 1, 6)
        if not isinstance(self.boot_id, UUID):
            raise TypeError("boot_id must be a UUID")


@dataclass(frozen=True)
class LeaderIdentity:
    """Identity of a Master tenure, renewed on each promotion.

    The epoch distinguishes two tenures even when the same process regains
    leadership. It identifies a tenure; it does not order competing leaders.
    """

    node: NodeIdentity
    leader_epoch: UUID = field(default_factory=uuid4)

    def __post_init__(self) -> None:
        if not isinstance(self.node, NodeIdentity):
            raise TypeError("node must be a NodeIdentity")
        if not isinstance(self.leader_epoch, UUID):
            raise TypeError("leader_epoch must be a UUID")


@dataclass(frozen=True)
class RoundKey:
    """Unique round identity within a specific Master process and tenure.

    Equality includes the Master's node_id, boot_id, leader_epoch and round_id.
    The first round in a tenure is numbered 1.
    """

    leader: LeaderIdentity
    round_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.leader, LeaderIdentity):
            raise TypeError("leader must be a LeaderIdentity")
        _require_int("round_id", self.round_id, 1)


@dataclass(frozen=True)
class PeerInfo:
    """Snapshot of a peer in the local membership table.

    last_seen_monotonic_ns belongs to the observing process and must never
    be compared with another machine's monotonic clock or sent as wall time.
    Role and reachability are separate: an unreachable peer may still have
    MASTER as its last observed role. Use dataclasses.replace for updates.
    """

    identity: NodeIdentity
    display_name: str
    ip: str
    tcp_port: int = 9999
    udp_port: int = 8888
    role: NodeRole = NodeRole.UNASSIGNED
    status: PeerStatus = PeerStatus.SUSPECT
    last_seen_monotonic_ns: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, NodeIdentity):
            raise TypeError("identity must be a NodeIdentity")
        if not isinstance(self.display_name, str):
            raise TypeError("display_name must be a string")
        if not self.display_name.strip():
            raise ValueError("display_name must not be blank")
        if not isinstance(self.ip, str):
            raise TypeError("ip must be a string")
        IPv4Address(self.ip)
        _require_int("tcp_port", self.tcp_port, 1, 65535)
        _require_int("udp_port", self.udp_port, 1, 65535)
        if not isinstance(self.role, NodeRole):
            raise TypeError("role must be a NodeRole")
        if not isinstance(self.status, PeerStatus):
            raise TypeError("status must be a PeerStatus")
        if self.last_seen_monotonic_ns is not None:
            _require_int("last_seen_monotonic_ns", self.last_seen_monotonic_ns, 0)
