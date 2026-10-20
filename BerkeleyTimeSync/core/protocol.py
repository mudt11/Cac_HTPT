"""Versioned JSON messages for discovery, Berkeley synchronization and election.

TCP uses newline-delimited JSON; UDP carries one JSON object per datagram.
Structural validation does not authenticate senders or elect a Master. The
controller must check the current context and track processed message IDs.
Monotonic timestamps are local measurements and are never wire timestamps.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from ipaddress import IPv4Address
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from uuid import UUID, uuid4

from .models import ClockState, LeaderIdentity, NodeIdentity, NodeRole, RoundKey


PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 16384
MAX_DATAGRAM_BYTES = 1200
MAX_INTEGER = (1 << 53) - 1


class ProtocolError(ValueError):
    """A message, framing operation or expected context is invalid."""


class Transport(str, Enum):
    TCP = "TCP"
    UDP = "UDP"


class MessageType(str, Enum):
    DISCOVER = "DISCOVER"
    DISCOVER_REPLY = "DISCOVER_REPLY"
    PING = "PING"
    PONG = "PONG"
    SYNC_REQUEST = "SYNC_REQUEST"
    TIME_REPORT = "TIME_REPORT"
    SYNC_ADJUST = "SYNC_ADJUST"
    TIME_ACK = "TIME_ACK"
    SYNC_CANCEL = "SYNC_CANCEL"
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"
    FORCE_MASTER = "FORCE_MASTER"


class AckStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"


_SYNC_TYPES = frozenset({
    MessageType.SYNC_REQUEST, MessageType.TIME_REPORT, MessageType.SYNC_ADJUST,
    MessageType.TIME_ACK, MessageType.SYNC_CANCEL,
})
_UDP_TYPES = frozenset({
    MessageType.DISCOVER, MessageType.DISCOVER_REPLY,
    MessageType.PING, MessageType.PONG,
})
_REPLY_TYPES = frozenset({
    MessageType.DISCOVER_REPLY, MessageType.PONG, MessageType.TIME_REPORT,
    MessageType.SYNC_ADJUST, MessageType.TIME_ACK, MessageType.OK,
})
_MASTER_TYPES = frozenset({
    MessageType.PING, MessageType.SYNC_REQUEST, MessageType.SYNC_ADJUST,
    MessageType.SYNC_CANCEL, MessageType.COORDINATOR, MessageType.FORCE_MASTER,
})
_WORKER_TYPES = frozenset({
    MessageType.PONG, MessageType.TIME_REPORT, MessageType.TIME_ACK,
})
_SCHEMAS = {
    MessageType.DISCOVER: (
        ("display_name", "ip", "tcp_port", "udp_port", "role"), ()),
    MessageType.DISCOVER_REPLY: (
        ("display_name", "ip", "tcp_port", "udp_port", "role"), ()),
    MessageType.PING: (("master_time_ms",), ()),
    MessageType.PONG: (("received_time_ms", "sent_time_ms"), ()),
    MessageType.SYNC_REQUEST: (
        ("master_time_ms", "timeout_ms", "participant_ids"), ()),
    MessageType.TIME_REPORT: (("received_time_ms", "sent_time_ms"), ()),
    MessageType.SYNC_ADJUST: (("correction_ms",), ()),
    MessageType.TIME_ACK: (("status", "clock_state", "clock_time_ms"), ("reason",)),
    MessageType.SYNC_CANCEL: (("reason",), ()),
    MessageType.ELECTION: (("election_id",), ()),
    MessageType.OK: (("election_id",), ()),
    MessageType.COORDINATOR: (("election_id",), ()),
    MessageType.FORCE_MASTER: (("reason",), ()),
}


def _integer(value: Any, name: str, minimum: int, maximum: int = MAX_INTEGER) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ProtocolError("{} must be an integer in [{}, {}]".format(
            name, minimum, maximum))


def _text(value: Any, name: str, maximum: int = 512) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ProtocolError("{} must be nonblank text of at most {} characters".format(
            name, maximum))
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ProtocolError("{} contains invalid Unicode".format(name)) from exc


def _uuid(value: Any, name: str) -> UUID:
    if not isinstance(value, str):
        raise ProtocolError("{} must be a UUID string".format(name))
    try:
        result = UUID(value)
    except ValueError as exc:
        raise ProtocolError("{} must be a UUID string".format(name)) from exc
    if str(result) != value:
        raise ProtocolError("{} must use canonical UUID spelling".format(name))
    return result


def _fields(value: Any, required: Tuple[str, ...], optional: Tuple[str, ...] = ()) -> None:
    if not isinstance(value, Mapping) or any(not isinstance(k, str) for k in value):
        raise ProtocolError("Expected an object with string keys")
    keys = set(value)
    if not set(required).issubset(keys) or keys - set(required) - set(optional):
        raise ProtocolError("Object has missing or unknown fields")


def _payload(message_type: MessageType, payload: Any) -> Mapping[str, Any]:
    _fields(payload, *_SCHEMAS[message_type])
    values = dict(payload)
    for name, value in values.items():
        if name in {"master_time_ms", "received_time_ms", "sent_time_ms", "clock_time_ms"}:
            _integer(value, name, 0)
        elif name == "correction_ms":
            _integer(value, name, -MAX_INTEGER)
        elif name == "timeout_ms":
            _integer(value, name, 1)
        elif name in {"tcp_port", "udp_port"}:
            _integer(value, name, 1, 65535)
        elif name == "ip":
            _text(value, name, 15)
            try:
                IPv4Address(value)
            except ValueError as exc:
                raise ProtocolError("ip must be an IPv4 address") from exc
        elif name in {"display_name", "reason"}:
            _text(value, name, 128 if name == "display_name" else 512)
        elif name == "election_id":
            _uuid(value, name)
        elif name in {"role", "status", "clock_state"}:
            enum_type = {"role": NodeRole, "status": AckStatus, "clock_state": ClockState}[name]
            if not isinstance(value, str):
                raise ProtocolError("{} must be a string".format(name))
            try:
                values[name] = enum_type(value).value
            except ValueError as exc:
                raise ProtocolError("Unknown {}".format(name)) from exc
        elif name == "participant_ids":
            if not isinstance(value, (list, tuple)) or not 2 <= len(value) <= 6:
                raise ProtocolError("participant_ids must contain 2 to 6 distinct node IDs")
            for node_id in value:
                _integer(node_id, "participant_id", 1, 6)
            if len(set(value)) != len(value):
                raise ProtocolError("participant_ids contains a duplicate node ID")
            values[name] = tuple(value)
    if "received_time_ms" in values and values["sent_time_ms"] < values["received_time_ms"]:
        raise ProtocolError("Reply time must not precede request receipt time")
    if message_type == MessageType.TIME_ACK:
        if values["status"] == AckStatus.REJECTED.value and "reason" not in values:
            raise ProtocolError("A rejected adjustment requires a reason")
        if values["status"] == AckStatus.APPLIED.value and values["clock_state"] != ClockState.NORMAL.value:
            raise ProtocolError("An applied adjustment cannot still be slewing")
    return MappingProxyType(values)


@dataclass(frozen=True)
class Message:
    """Validated message; timestamps and corrections in payload are milliseconds.

    TIME_REPORT/PONG capture the virtual clock at request receipt and reply
    send. Requests are correlated through reply_to; RTT durations are stored
    locally by the requester. ACCEPTED acknowledges scheduling a correction;
    APPLIED reports its completion. Neither replaces convergence measurement.
    """

    type: MessageType
    source: NodeIdentity
    payload: Mapping[str, Any]
    target_node_id: Optional[int] = None
    leader: Optional[LeaderIdentity] = None
    round_id: Optional[int] = None
    message_id: UUID = field(default_factory=uuid4)
    reply_to: Optional[UUID] = None

    def __post_init__(self) -> None:
        if not isinstance(self.type, MessageType) or not isinstance(self.source, NodeIdentity):
            raise ProtocolError("type and source must be MessageType and NodeIdentity")
        if not isinstance(self.message_id, UUID):
            raise ProtocolError("message_id must be a UUID")
        if self.reply_to is not None and not isinstance(self.reply_to, UUID):
            raise ProtocolError("reply_to must be a UUID")
        if self.leader is not None and not isinstance(self.leader, LeaderIdentity):
            raise ProtocolError("leader must be a LeaderIdentity")
        if self.target_node_id is not None:
            _integer(self.target_node_id, "target_node_id", 1, 6)
            if self.target_node_id == self.source.node_id:
                raise ProtocolError("A message cannot target its own node ID")
        if self.type in _SYNC_TYPES:
            _integer(self.round_id, "round_id", 1)
        elif self.round_id is not None:
            raise ProtocolError("Only synchronization messages may carry round_id")
        if self.type in _SYNC_TYPES | _MASTER_TYPES | _WORKER_TYPES and self.leader is None:
            raise ProtocolError("This message requires a Master identity")
        if self.type in {MessageType.ELECTION, MessageType.OK} and self.leader is not None:
            raise ProtocolError("Election requests and replies must not declare a Master")
        if self.type in _REPLY_TYPES and self.reply_to is None:
            raise ProtocolError("This response requires reply_to")
        broadcast_types = {MessageType.DISCOVER, MessageType.COORDINATOR, MessageType.FORCE_MASTER}
        if self.type not in broadcast_types and self.target_node_id is None:
            raise ProtocolError("This message requires a target_node_id")
        if self.type in _MASTER_TYPES and self.source != self.leader.node:
            raise ProtocolError("Sender does not match the declared Master identity")
        if self.type in _WORKER_TYPES:
            if self.target_node_id != self.leader.node.node_id or self.source.node_id == self.leader.node.node_id:
                raise ProtocolError("A Worker response must target its Master")
        if self.type == MessageType.ELECTION and self.target_node_id <= self.source.node_id:
            raise ProtocolError("Bully ELECTION must target a higher node ID")
        if self.type == MessageType.OK and self.target_node_id >= self.source.node_id:
            raise ProtocolError("Bully OK must target a lower node ID")
        payload = _payload(self.type, self.payload)
        if self.type == MessageType.SYNC_REQUEST:
            if self.source.node_id not in payload["participant_ids"] or self.target_node_id not in payload["participant_ids"]:
                raise ProtocolError("The Master and recipient must belong to participant_ids")
        if self.type in {MessageType.DISCOVER, MessageType.DISCOVER_REPLY}:
            if payload["role"] == NodeRole.MASTER.value:
                if self.leader is None or self.source != self.leader.node:
                    raise ProtocolError("A Master advertisement requires its matching identity")
            elif self.leader is not None and self.source.node_id == self.leader.node.node_id:
                raise ProtocolError("A non-Master advertisement cannot identify itself as Master")
        object.__setattr__(self, "payload", payload)

    @property
    def round_key(self) -> Optional[RoundKey]:
        if self.round_id is None:
            return None
        return RoundKey(self.leader, self.round_id)

    @property
    def transport(self) -> Transport:
        return Transport.UDP if self.type in _UDP_TYPES else Transport.TCP

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "version": PROTOCOL_VERSION,
            "type": self.type.value,
            "message_id": str(self.message_id),
            "source": {"node_id": self.source.node_id, "boot_id": str(self.source.boot_id)},
            "payload": {k: list(v) if isinstance(v, tuple) else v for k, v in self.payload.items()},
        }
        if self.target_node_id is not None:
            data["target_node_id"] = self.target_node_id
        if self.leader is not None:
            data["leader"] = {
                "node_id": self.leader.node.node_id,
                "boot_id": str(self.leader.node.boot_id),
                "leader_epoch": str(self.leader.leader_epoch),
            }
        if self.round_id is not None:
            data["round_id"] = self.round_id
        if self.reply_to is not None:
            data["reply_to"] = str(self.reply_to)
        return data


def _node(value: Any) -> NodeIdentity:
    _fields(value, ("node_id", "boot_id"))
    _integer(value["node_id"], "node_id", 1, 6)
    return NodeIdentity(value["node_id"], _uuid(value["boot_id"], "boot_id"))


def _leader(value: Any) -> LeaderIdentity:
    _fields(value, ("node_id", "boot_id", "leader_epoch"))
    node = _node({"node_id": value["node_id"], "boot_id": value["boot_id"]})
    return LeaderIdentity(node, _uuid(value["leader_epoch"], "leader_epoch"))


def _unique_object(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("JSON contains a duplicate object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ProtocolError("JSON contains a non-finite number")


def _transport(value: Union[str, Transport]) -> Transport:
    try:
        return Transport(value)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("Transport must be TCP or UDP") from exc


def serialize_packet(message: Message, *, transport: Union[str, Transport] = Transport.TCP) -> bytes:
    """Encode one message, including the TCP delimiter when appropriate."""
    channel = _transport(transport)
    if not isinstance(message, Message) or message.transport != channel:
        raise ProtocolError("Message does not belong on this transport")
    raw = json.dumps(message.to_dict(), ensure_ascii=False, allow_nan=False,
                     separators=(",", ":")).encode("utf-8")
    limit = MAX_FRAME_BYTES if channel == Transport.TCP else MAX_DATAGRAM_BYTES
    if len(raw) > limit:
        raise ProtocolError("Encoded message exceeds the transport size limit")
    return raw + b"\n" if channel == Transport.TCP else raw


def parse_packet(raw: Union[str, bytes], *, transport: Union[str, Transport] = Transport.TCP) -> Message:
    """Parse one complete frame/datagram; reject invalid input with ProtocolError."""
    channel = _transport(transport)
    limit = MAX_FRAME_BYTES if channel == Transport.TCP else MAX_DATAGRAM_BYTES
    if not isinstance(raw, (str, bytes)):
        raise ProtocolError("Packet must be text or bytes")
    try:
        encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
        if channel == Transport.TCP and encoded.endswith(b"\n"):
            encoded = encoded[:-1]
            if encoded.endswith(b"\r"):
                encoded = encoded[:-1]
        if not encoded or len(encoded) > limit:
            raise ProtocolError("Packet is empty or exceeds the transport size limit")
        data = json.loads(encoded.decode("utf-8"), object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
        _fields(data, ("version", "type", "message_id", "source", "payload"),
                ("target_node_id", "leader", "round_id", "reply_to"))
        _integer(data["version"], "version", PROTOCOL_VERSION, PROTOCOL_VERSION)
        message = Message(
            type=MessageType(data["type"]),
            source=_node(data["source"]),
            payload=data["payload"],
            target_node_id=data.get("target_node_id"),
            leader=_leader(data["leader"]) if data.get("leader") is not None else None,
            round_id=data.get("round_id"),
            message_id=_uuid(data["message_id"], "message_id"),
            reply_to=_uuid(data["reply_to"], "reply_to") if data.get("reply_to") is not None else None,
        )
        if message.transport != channel:
            raise ProtocolError("Message does not belong on this transport")
        return message
    except ProtocolError:
        raise
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ProtocolError("Invalid JSON message or field value") from exc


def validate_context(
    message: Message, *, recipient_id: Optional[int] = None,
    expected_leader: Optional[LeaderIdentity] = None,
    expected_round: Optional[RoundKey] = None,
) -> None:
    """Check a parsed message against locally known context before acting on it.

    A broadcast is valid for any recipient. Passing expected_round rejects
    messages without a round as well as stale Master incarnations/tenures.
    Duplicate IDs, reply correlation and membership remain controller duties.
    """
    if not isinstance(message, Message):
        raise ProtocolError("Expected a Message")
    if recipient_id is not None:
        _integer(recipient_id, "recipient_id", 1, 6)
        if message.target_node_id is not None and message.target_node_id != recipient_id:
            raise ProtocolError("Message targets another node")
    if expected_leader is not None:
        if not isinstance(expected_leader, LeaderIdentity) or message.leader != expected_leader:
            raise ProtocolError("Message belongs to another Master tenure")
    if expected_round is not None:
        if not isinstance(expected_round, RoundKey) or message.round_key != expected_round:
            raise ProtocolError("Message belongs to another synchronization round")


class TCPFrameBuffer:
    """Per-connection byte buffer, decoding UTF-8 only after a full frame exists.

    feed returns raw frames so callers can reject one malformed JSON message
    without losing the next. An oversized frame invalidates this buffer: the
    caller should close that connection. finish detects a truncated final frame.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._closed = False

    def feed(self, chunk: bytes) -> List[bytes]:
        if self._closed:
            raise ProtocolError("TCP frame buffer is closed")
        if not isinstance(chunk, bytes):
            raise ProtocolError("TCP chunks must be bytes")
        frames = []
        start = 0
        while start < len(chunk):
            end = chunk.find(b"\n", start)
            stop = end if end >= 0 else len(chunk)
            # Permit CRLF with a maximum-sized JSON body.
            total = len(self._buffer) + stop - start
            last_byte = chunk[stop - 1:stop] if stop > start else self._buffer[-1:]
            if total > MAX_FRAME_BYTES and not (total == MAX_FRAME_BYTES + 1 and last_byte == b"\r"):
                self._closed = True
                self._buffer.clear()
                raise ProtocolError("TCP frame exceeds the size limit")
            self._buffer.extend(chunk[start:stop])
            if end < 0:
                break
            frame = bytes(self._buffer)
            frames.append(frame[:-1] if frame.endswith(b"\r") else frame)
            self._buffer.clear()
            start = end + 1
        return frames

    def finish(self) -> None:
        self._closed = True
        if self._buffer:
            raise ProtocolError("Connection ended with an incomplete TCP frame")
