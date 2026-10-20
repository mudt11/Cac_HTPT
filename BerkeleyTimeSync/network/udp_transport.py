"""UDP datagrams for discovery and heartbeat, independent of TCP and the GUI.

Bind completes before the receive thread starts. Each datagram carries exactly
one shared-protocol message, capped at MAX_DATAGRAM_BYTES. The transport does
not mark peers connected, retry packets, deduplicate messages or elect a Master.
Successful sendto only means the local socket accepted the datagram.

The controller chooses the LAN interface/broadcast address and validates peer
identity, reply correlation and Master tenure. Advertised IPs are not trusted;
receive events retain the actual sender endpoint. No sender authentication is
provided. An optional virtual-clock reader must be fast and thread-safe.
"""

import math
import socket
import threading
from dataclasses import dataclass
from enum import Enum
from ipaddress import IPv4Address
from queue import Full, Queue
from time import monotonic_ns
from typing import Callable, Optional, Tuple, Union
from uuid import UUID

from ..core.models import NodeIdentity
from ..core.protocol import (
    MAX_DATAGRAM_BYTES, MAX_INTEGER, Message, MessageType, ProtocolError,
    Transport, parse_packet, serialize_packet,
)


class EventType(str, Enum):
    MESSAGE = "MESSAGE"
    ERROR = "ERROR"
    STOPPED = "STOPPED"


class TransportClosedError(RuntimeError):
    """The UDP transport is not running."""


class EventOverflowError(RuntimeError):
    """The bounded event queue dropped events; inspect lost_events."""


@dataclass(frozen=True)
class DatagramEvent:
    type: EventType
    address: Optional[Tuple[str, int]]
    monotonic_ns: int
    message: Optional[Message] = None
    clock_time_ms: Optional[int] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class SendReceipt:
    message_id: UUID
    sent_monotonic_ns: int
    bytes_sent: int


def _duration(value: float, name: str, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("{} must be a finite duration".format(name))
    if not math.isfinite(value) or not 0 <= value <= 60 or (not allow_zero and value == 0):
        raise ValueError("{} is outside the supported duration range".format(name))
    return float(value)


def _port(value: int, allow_zero: bool = False) -> None:
    if type(value) is not int or not (0 if allow_zero else 1) <= value <= 65535:
        raise ValueError("Invalid UDP port")


class UDPTransport:
    """One-shot lifecycle: start, send/broadcast/get_event, stop.

    The default bind is 0.0.0.0:8888; bind a specific interface when necessary.
    port=0 is available for isolated tests. No SO_REUSEADDR is enabled, avoiding
    multiple processes silently sharing a node's UDP port on Windows.
    Overflow drops individual events and is reported on the next get_event;
    the socket stays alive. OS/network packet loss cannot be counted here.
    """

    def __init__(
        self, local_identity: NodeIdentity, *, host: str = "0.0.0.0",
        port: int = 8888, io_timeout: float = 0.5, event_capacity: int = 256,
        clock_source: Optional[Callable[[], int]] = None,
    ) -> None:
        if not isinstance(local_identity, NodeIdentity):
            raise TypeError("local_identity must be NodeIdentity")
        IPv4Address(host)
        _port(port, allow_zero=True)
        if type(event_capacity) is not int or not 1 <= event_capacity <= 65536:
            raise ValueError("event_capacity must be in [1, 65536]")
        if clock_source is not None and not callable(clock_source):
            raise TypeError("clock_source must be callable")
        self.local_identity = local_identity
        self._host, self._port = host, port
        self._io_timeout = _duration(io_timeout, "io_timeout")
        self._clock_source = clock_source
        self._events = Queue(maxsize=event_capacity)  # type: Queue
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._socket = None  # type: Optional[socket.socket]
        self._thread = None  # type: Optional[threading.Thread]
        self._address = None  # type: Optional[Tuple[str, int]]
        self._started = False
        self._stopped = False
        self._lost_events = 0
        self._overflow_pending = False

    @property
    def address(self) -> Optional[Tuple[str, int]]:
        with self._lock:
            return self._address

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._socket is not None and not self._stopped

    @property
    def lost_events(self) -> int:
        with self._lock:
            return self._lost_events

    def _emit(self, event: DatagramEvent) -> None:
        try:
            self._events.put_nowait(event)
        except Full:
            with self._lock:
                self._lost_events += 1
                self._overflow_pending = True

    def get_event(self, timeout: float = 0.0) -> DatagramEvent:
        """Return an event or raise queue.Empty / EventOverflowError."""
        timeout = _duration(timeout, "timeout", allow_zero=True)
        with self._lock:
            if self._overflow_pending:
                self._overflow_pending = False
                raise EventOverflowError("UDP events lost: {}".format(self._lost_events))
        return self._events.get(timeout=timeout)

    def start(self) -> Tuple[str, int]:
        with self._lock:
            if self._started or self._stopped:
                raise TransportClosedError("Create a new UDP transport after start/stop")
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.settimeout(self._io_timeout)
                sock.bind((self._host, self._port))
                self._address = sock.getsockname()
                self._socket = sock
                thread = threading.Thread(
                    target=self._receive_loop, args=(sock,),
                    daemon=True, name="BerkeleyUDP",
                )
                self._thread = thread
                thread.start()
                self._started = True
            except (OSError, RuntimeError):
                self._socket = None
                self._address = None
                self._thread = None
                sock.close()
                raise
            return self._address

    def _finish(self, sock: socket.socket, detail: str) -> None:
        with self._lock:
            if self._socket is not sock:
                return
            self._socket = None
            self._stopped = True
            sock.close()
            self._emit(DatagramEvent(EventType.STOPPED, None, monotonic_ns(), detail=detail))

    def _receive_loop(self, sock: socket.socket) -> None:
        detail = "UDP receiver stopped"
        try:
            while True:
                with self._lock:
                    if self._socket is not sock:
                        return
                try:
                    # Read a whole IPv4 UDP payload, then enforce the protocol
                    # cap. This avoids truncation/WSAEMSGSIZE on large packets.
                    raw, address = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError as exc:
                    with self._lock:
                        if self._socket is not sock:
                            return
                    # Windows may report ICMP port-unreachable for an earlier
                    # UDP send. This is not proof the whole cluster is offline.
                    if getattr(exc, "winerror", None) == 10054:
                        self._emit(DatagramEvent(
                            EventType.ERROR, None, monotonic_ns(), detail=str(exc)))
                        continue
                    raise
                received_ns = monotonic_ns()
                clock_ms = self._clock_source() if self._clock_source is not None else None
                if self._clock_source is not None and (
                    type(clock_ms) is not int or not 0 <= clock_ms <= MAX_INTEGER
                ):
                    raise ValueError("clock_source returned invalid milliseconds")
                try:
                    if len(raw) > MAX_DATAGRAM_BYTES:
                        raise ProtocolError("UDP datagram exceeds the size limit")
                    message = parse_packet(raw, transport=Transport.UDP)
                except ProtocolError as exc:
                    with self._lock:
                        if self._socket is sock:
                            self._emit(DatagramEvent(
                                EventType.ERROR, address, received_ns, detail=str(exc)))
                    continue
                with self._lock:
                    if self._socket is not sock:
                        return
                    if message.source == self.local_identity:
                        continue  # Broadcast echo from this process.
                    if message.source.node_id == self.local_identity.node_id:
                        self._emit(DatagramEvent(
                            EventType.ERROR, address, received_ns, message, clock_ms,
                            "Remote process uses the local node ID"))
                        continue
                    if message.target_node_id not in (None, self.local_identity.node_id):
                        continue  # A targeted broadcast may reach other nodes.
                    self._emit(DatagramEvent(
                        EventType.MESSAGE, address, received_ns, message, clock_ms))
        except Exception as exc:
            detail = "{}: {}".format(type(exc).__name__, exc)
            with self._lock:
                if self._socket is sock:
                    self._emit(DatagramEvent(EventType.ERROR, None, monotonic_ns(), detail=detail))
        finally:
            self._finish(sock, detail)

    def send(
        self, ip: str, message: Union[Message, Callable[[], Message]], *,
        port: int = 8888,
    ) -> SendReceipt:
        """Send one datagram; use a fast factory to sample a clock after lock wait.

        The receipt marks preparation start and includes serialization time in
        a subsequent RTT measurement. Send errors propagate to the caller and
        do not imply any peer has acknowledged receipt. Keep this off the GUI.
        """
        IPv4Address(ip)
        _port(port)
        if not self._send_lock.acquire(timeout=self._io_timeout):
            raise TimeoutError("Timed out waiting for another UDP send")
        try:
            with self._lock:
                sock = self._socket
                if sock is None or self._stopped:
                    raise TransportClosedError("UDP transport is not running")
            sent_ns = monotonic_ns()
            prepared = message() if callable(message) else message
            raw = serialize_packet(prepared, transport=Transport.UDP)
            if prepared.source != self.local_identity:
                raise ProtocolError("Outbound source must match the local process")
            with self._lock:
                if self._socket is not sock:
                    raise TransportClosedError("UDP transport stopped during preparation")
            count = sock.sendto(raw, (ip, port))
            if count != len(raw):
                raise OSError("Incomplete UDP send")
            return SendReceipt(prepared.message_id, sent_ns, count)
        finally:
            self._send_lock.release()

    def broadcast(
        self, message: Message, *, broadcast_ip: str = "255.255.255.255",
        port: int = 8888,
    ) -> SendReceipt:
        """Send DISCOVER to a caller-selected LAN broadcast address.

        The limited broadcast default follows OS routing. Supply the actual
        interface/subnet broadcast when the machine has multiple adapters.
        This transport does not assume any fixed private subnet.
        """
        if not isinstance(message, Message) or message.type != MessageType.DISCOVER:
            raise ProtocolError("Only DISCOVER is sent through broadcast()")
        if message.target_node_id is not None:
            raise ProtocolError("Broadcast discovery must not target one node")
        return self.send(broadcast_ip, message, port=port)

    def stop(self, timeout: float = 2.0) -> None:
        """Close the socket, stop receiving and join the thread within timeout."""
        timeout = _duration(timeout, "timeout")
        with self._lock:
            self._stopped = True
            sock, thread = self._socket, self._thread
        if sock is not None:
            self._finish(sock, "Transport stopped")
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
            if thread.is_alive():
                raise TimeoutError("UDP receive thread has not stopped before the deadline")
