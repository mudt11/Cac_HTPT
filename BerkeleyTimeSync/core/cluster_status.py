"""Immutable monitoring snapshots, separate from Berkeley adjustment commands.

This module validates data only. It does not send packets, accept leadership,
set clocks or decide freshness. The receiver must verify the current Master,
tenure and increasing sequence before publishing a snapshot to the GUI.

elapsed_ms is a duration since this Master tenure began, not a remote absolute
monotonic timestamp. sample.age_ms is its age when this snapshot was built.
Receivers add their locally measured time since receipt; transit delay remains
unknown. None represents unavailable data and must never become a fake zero.
"""

from dataclasses import dataclass
from ipaddress import IPv4Address
from math import isfinite
from typing import Mapping, Optional, Tuple
from uuid import UUID

from .models import ClockState, LeaderIdentity, NodeIdentity, NodeRole, PeerStatus, SyncRoundState


_MAX = (1 << 53) - 1
_FILTERS = frozenset({"KEPT", "HIGH_RTT", "OUTSIDE_CLUSTER", "INSUFFICIENT_CONSENSUS", "AMBIGUOUS_CONSENSUS"})
_ACKS = frozenset({"ACCEPTED", "APPLIED", "REJECTED"})


class ClusterStatusError(ValueError):
    """Monitoring payload has invalid fields or inconsistent identities."""


def _integer(value, name, minimum=0):
    if type(value) is not int or not minimum <= value <= _MAX:
        raise ClusterStatusError("{} must be an integer in [{}, {}]".format(name, minimum, _MAX))


def _number(value, name, nonnegative=False):
    if value is None:
        return
    if (type(value) not in (int, float) or abs(value) > _MAX or not isfinite(value)
            or (nonnegative and value < 0)):
        raise ClusterStatusError("{} must be a finite {}number or null".format(name, "nonnegative " if nonnegative else ""))


def _text(value, name, maximum):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ClusterStatusError("Invalid {}".format(name))
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ClusterStatusError("Invalid Unicode in {}".format(name)) from exc


def _fields(data, names):
    if not isinstance(data, Mapping) or set(data) != set(names.split()):
        raise ClusterStatusError("Monitoring object has missing or unknown fields")


def _enum(kind, value):
    if not isinstance(value, str):
        raise ClusterStatusError("Expected {} string".format(kind.__name__))
    try:
        return kind(value)
    except ValueError as exc:
        raise ClusterStatusError("Unknown {}".format(kind.__name__)) from exc


def _identity(data):
    _fields(data, "node_id boot_id")
    _integer(data["node_id"], "node_id", 1)
    try:
        if not isinstance(data["boot_id"], str):
            raise ValueError("boot_id must be text")
        boot = UUID(data["boot_id"])
        if str(boot) != data["boot_id"]:
            raise ValueError("boot_id must be canonical")
        return NodeIdentity(data["node_id"], boot)
    except (ValueError, TypeError) as exc:
        raise ClusterStatusError("Invalid Node identity") from exc


def _identity_data(identity):
    return {"node_id": identity.node_id, "boot_id": str(identity.boot_id)}


def _identities(values, minimum=1):
    if not isinstance(values, (list, tuple)) or not minimum <= len(values) <= 6:
        raise ClusterStatusError("Expected {} to 6 Node identities".format(minimum))
    result = tuple(values)
    if any(not isinstance(p, NodeIdentity) for p in result):
        raise ClusterStatusError("Expected NodeIdentity")
    if len({p.node_id for p in result}) != len(result):
        raise ClusterStatusError("Repeated Node ID, including mixed boot IDs")
    return result


@dataclass(frozen=True)
class ClockSample:
    round_id: int
    clock_time_ms: int
    age_ms: int
    rtt_ms: Optional[float] = None
    offset_ms: Optional[float] = None
    filter_reason: Optional[str] = None

    def __post_init__(self):
        _integer(self.round_id, "sample.round_id", 1)
        _integer(self.clock_time_ms, "sample.clock_time_ms")
        _integer(self.age_ms, "sample.age_ms")
        _number(self.rtt_ms, "sample.rtt_ms", True)
        _number(self.offset_ms, "sample.offset_ms")
        if self.filter_reason is not None:
            if not isinstance(self.filter_reason, str) or self.filter_reason not in _FILTERS:
                raise ClusterStatusError("Unknown filter reason")
            if self.offset_ms is None:
                raise ClusterStatusError("Filter classification requires a measured offset")

    def to_payload(self):
        return {"round_id": self.round_id, "clock_time_ms": self.clock_time_ms, "age_ms": self.age_ms,
                "rtt_ms": self.rtt_ms, "offset_ms": self.offset_ms, "filter_reason": self.filter_reason}

    @classmethod
    def from_payload(cls, data):
        _fields(data, "round_id clock_time_ms age_ms rtt_ms offset_ms filter_reason")
        return cls(**dict(data))


@dataclass(frozen=True)
class AdjustmentStatus:
    round_id: int
    correction_ms: int
    ack_status: Optional[str] = None
    clock_state: Optional[ClockState] = None
    reason: Optional[str] = None

    def __post_init__(self):
        _integer(self.round_id, "adjustment.round_id", 1)
        _integer(self.correction_ms, "adjustment.correction_ms", -_MAX)
        if self.ack_status is None:
            if self.clock_state is not None or self.reason is not None:
                raise ClusterStatusError("No ACK: its clock state and reason must be null")
            return
        if not isinstance(self.ack_status, str) or self.ack_status not in _ACKS:
            raise ClusterStatusError("Unknown ACK status")
        if not isinstance(self.clock_state, ClockState):
            raise ClusterStatusError("ACK requires a recorded ClockState")
        if self.ack_status == "APPLIED" and self.clock_state != ClockState.NORMAL:
            raise ClusterStatusError("APPLIED cannot still be slewing")
        if self.reason is not None:
            _text(self.reason, "ACK reason", 512)
        elif self.ack_status == "REJECTED":
            raise ClusterStatusError("REJECTED requires a reason")

    def to_payload(self):
        return {"round_id": self.round_id, "correction_ms": self.correction_ms,
                "ack_status": self.ack_status,
                "clock_state": self.clock_state.value if self.clock_state is not None else None,
                "reason": self.reason}

    @classmethod
    def from_payload(cls, data):
        _fields(data, "round_id correction_ms ack_status clock_state reason")
        values = dict(data)
        if values["clock_state"] is not None:
            values["clock_state"] = _enum(ClockState, values["clock_state"])
        return cls(**values)


@dataclass(frozen=True)
class NodeStatus:
    identity: NodeIdentity
    display_name: str
    ip: str
    role: NodeRole
    status: PeerStatus
    heartbeat_rtt_ms: Optional[float] = None
    heartbeat_age_ms: Optional[int] = None
    sample: Optional[ClockSample] = None
    adjustment: Optional[AdjustmentStatus] = None

    def __post_init__(self):
        if not isinstance(self.identity, NodeIdentity):
            raise ClusterStatusError("Node status requires an identity")
        _text(self.display_name, "display_name", 128)
        if not isinstance(self.ip, str):
            raise ClusterStatusError("ip must be text")
        try:
            ip = IPv4Address(self.ip)
            if ip.is_unspecified or ip.is_multicast or int(ip) == 0xffffffff:
                raise ValueError("Not a unicast endpoint")
        except ValueError as exc:
            raise ClusterStatusError("Invalid Node IPv4 endpoint") from exc
        if not isinstance(self.role, NodeRole) or not isinstance(self.status, PeerStatus):
            raise ClusterStatusError("Invalid Node role or reachability status")
        _number(self.heartbeat_rtt_ms, "heartbeat_rtt_ms", True)
        if self.heartbeat_age_ms is not None:
            _integer(self.heartbeat_age_ms, "heartbeat_age_ms")
        if self.heartbeat_rtt_ms is not None and self.heartbeat_age_ms is None:
            raise ClusterStatusError("Heartbeat RTT requires its sample age")
        if self.sample is not None and not isinstance(self.sample, ClockSample):
            raise ClusterStatusError("Invalid clock sample")
        if self.adjustment is not None:
            if not isinstance(self.adjustment, AdjustmentStatus):
                raise ClusterStatusError("Invalid adjustment status")
            if self.sample is None or self.adjustment.round_id != self.sample.round_id:
                raise ClusterStatusError("Adjustment must refer to this Node's measured round")

    def to_payload(self):
        return {"identity": _identity_data(self.identity), "display_name": self.display_name, "ip": self.ip,
                "role": self.role.value, "status": self.status.value,
                "heartbeat_rtt_ms": self.heartbeat_rtt_ms, "heartbeat_age_ms": self.heartbeat_age_ms,
                "sample": self.sample.to_payload() if self.sample else None,
                "adjustment": self.adjustment.to_payload() if self.adjustment else None}

    @classmethod
    def from_payload(cls, data):
        _fields(data, "identity display_name ip role status heartbeat_rtt_ms heartbeat_age_ms sample adjustment")
        values = dict(data)
        values["identity"] = _identity(values["identity"])
        values["role"] = _enum(NodeRole, values["role"])
        values["status"] = _enum(PeerStatus, values["status"])
        if values["sample"] is not None:
            values["sample"] = ClockSample.from_payload(values["sample"])
        if values["adjustment"] is not None:
            values["adjustment"] = AdjustmentStatus.from_payload(values["adjustment"])
        return cls(**values)


@dataclass(frozen=True)
class RoundStatus:
    round_id: int
    state: SyncRoundState
    participants: Tuple[NodeIdentity, ...]
    missing_report_ids: Tuple[int, ...] = ()
    missing_ack_ids: Tuple[int, ...] = ()
    ack_remaining_ms: Optional[int] = None
    reason: Optional[str] = None

    def __post_init__(self):
        _integer(self.round_id, "round.round_id", 1)
        if not isinstance(self.state, SyncRoundState):
            raise ClusterStatusError("Invalid round state")
        object.__setattr__(self, "participants", _identities(self.participants, 2))
        member_ids = {p.node_id for p in self.participants}
        for name in ("missing_report_ids", "missing_ack_ids"):
            values = getattr(self, name)
            if not isinstance(values, (list, tuple)) or len(values) > 5:
                raise ClusterStatusError("Invalid missing-Node list")
            for value in values:
                _integer(value, name, 1)
            if len(set(values)) != len(values) or not set(values).issubset(member_ids):
                raise ClusterStatusError("Missing-Node IDs must be distinct participants")
            object.__setattr__(self, name, tuple(values))
        if self.ack_remaining_ms is not None:
            _integer(self.ack_remaining_ms, "ack_remaining_ms")
        if self.reason is not None:
            _text(self.reason, "round reason", 512)

    def to_payload(self):
        return {"round_id": self.round_id, "state": self.state.value,
                "participants": [_identity_data(p) for p in self.participants],
                "missing_report_ids": list(self.missing_report_ids), "missing_ack_ids": list(self.missing_ack_ids),
                "ack_remaining_ms": self.ack_remaining_ms, "reason": self.reason}

    @classmethod
    def from_payload(cls, data):
        _fields(data, "round_id state participants missing_report_ids missing_ack_ids ack_remaining_ms reason")
        values = dict(data)
        if not isinstance(values["participants"], (list, tuple)) or not 2 <= len(values["participants"]) <= 6:
            raise ClusterStatusError("Invalid participant array")
        values["participants"] = tuple(_identity(p) for p in values["participants"])
        values["state"] = _enum(SyncRoundState, values["state"])
        return cls(**values)


@dataclass(frozen=True)
class ClusterStatus:
    sequence: int
    master_time_ms: int
    elapsed_ms: int
    nodes: Tuple[NodeStatus, ...]
    round: Optional[RoundStatus] = None
    master_clock_state: Optional[ClockState] = None
    master_remaining_ms: Optional[float] = None

    def __post_init__(self):
        _integer(self.sequence, "sequence", 1)
        _integer(self.master_time_ms, "master_time_ms")
        _integer(self.elapsed_ms, "elapsed_ms")
        _number(self.master_remaining_ms, "master_remaining_ms")
        if (self.master_clock_state is None) != (self.master_remaining_ms is None):
            raise ClusterStatusError("Master clock state and remaining correction must be supplied together")
        if self.master_clock_state is not None:
            if not isinstance(self.master_clock_state, ClockState):
                raise ClusterStatusError("Invalid Master clock state")
            if ((self.master_clock_state == ClockState.NORMAL) != (self.master_remaining_ms == 0)):
                raise ClusterStatusError("Master clock state contradicts its remaining correction")
        if not isinstance(self.nodes, (list, tuple)) or not 1 <= len(self.nodes) <= 6:
            raise ClusterStatusError("A snapshot contains 1 to 6 known Nodes")
        nodes = tuple(self.nodes)
        if any(not isinstance(node, NodeStatus) for node in nodes):
            raise ClusterStatusError("Expected NodeStatus")
        _identities(tuple(n.identity for n in nodes))
        if sum(n.role == NodeRole.MASTER for n in nodes) != 1:
            raise ClusterStatusError("A snapshot requires exactly one Master")
        object.__setattr__(self, "nodes", nodes)
        if self.round is not None and not isinstance(self.round, RoundStatus):
            raise ClusterStatusError("Invalid round summary")
        for node in nodes:
            if node.sample is not None:
                if self.round is None or node.sample.round_id > self.round.round_id:
                    raise ClusterStatusError("Sample belongs to a future/unknown round")
                if node.sample.age_ms > self.elapsed_ms:
                    raise ClusterStatusError("Sample predates the current Master tenure")
            if node.heartbeat_age_ms is not None and node.heartbeat_age_ms > self.elapsed_ms:
                raise ClusterStatusError("Heartbeat predates the current Master tenure")

    def validate_leader(self, leader: LeaderIdentity):
        if not isinstance(leader, LeaderIdentity):
            raise ClusterStatusError("Expected current Master identity")
        master = next(n for n in self.nodes if n.role == NodeRole.MASTER)
        if master.identity != leader.node:
            raise ClusterStatusError("Snapshot Master differs from envelope Master")
        if self.round:
            if leader.node not in self.round.participants:
                raise ClusterStatusError("Round must include this Master incarnation")
            if leader.node.node_id in self.round.missing_report_ids + self.round.missing_ack_ids:
                raise ClusterStatusError("Master cannot be a missing Worker")
            current = {node.identity.node_id: node.identity for node in self.nodes}
            for node in self.nodes:
                if node.sample and node.sample.round_id == self.round.round_id:
                    if node.identity not in self.round.participants:
                        raise ClusterStatusError("Current-round sample is not from a pinned participant")
            for participant in self.round.participants:
                if participant.node_id in current and current[participant.node_id] != participant:
                    # Old round membership can coexist with a restarted Node,
                    # but its samples must not be attached to the new process.
                    node = next(n for n in self.nodes if n.identity.node_id == participant.node_id)
                    if ((node.sample and node.sample.round_id == self.round.round_id)
                            or (node.adjustment and node.adjustment.round_id == self.round.round_id)):
                        raise ClusterStatusError("Old incarnation's round data attached to a restarted Node")

    def to_payload(self):
        return {"sequence": self.sequence, "master_time_ms": self.master_time_ms,
                "elapsed_ms": self.elapsed_ms, "nodes": [node.to_payload() for node in self.nodes],
                "round": self.round.to_payload() if self.round else None,
                "master_clock_state": self.master_clock_state.value if self.master_clock_state is not None else None,
                "master_remaining_ms": self.master_remaining_ms}

    @classmethod
    def from_payload(cls, data):
        _fields(data, "sequence master_time_ms elapsed_ms nodes round master_clock_state master_remaining_ms")
        if not isinstance(data["nodes"], (list, tuple)) or not 1 <= len(data["nodes"]) <= 6:
            raise ClusterStatusError("Invalid Node array")
        return cls(data["sequence"], data["master_time_ms"], data["elapsed_ms"],
                   tuple(NodeStatus.from_payload(node) for node in data["nodes"]),
                   RoundStatus.from_payload(data["round"]) if data["round"] is not None else None,
                   _enum(ClockState, data["master_clock_state"]) if data["master_clock_state"] is not None else None,
                   data["master_remaining_ms"])

    def frozen_payload(self):
        """Deep freeze for Message.payload; to_payload() returns a detached JSON copy."""
        from types import MappingProxyType

        def freeze(value):
            if isinstance(value, dict):
                return MappingProxyType({key: freeze(item) for key, item in value.items()})
            if isinstance(value, list):
                return tuple(freeze(item) for item in value)
            return value

        return freeze(self.to_payload())
