"""Bounded, bidirectional TCP transport for the shared JSON protocol.

Each node listens on port 9999 and may also initiate connections. Consumers
drain events from their controller thread; network threads never touch a GUI.
connect/send may block up to their configured timeout and belong off the GUI.
There is no automatic retry: a failed send may have delivered a partial or
complete frame, so session IDs and idempotence remain controller duties.

An observed source identity is pinned per connection, not authenticated.
Membership, Master tenure, round validity and discovery remain higher layers.
Receive timestamps mark the recv chunk completing a frame, before queuing.
An optional fast, thread-safe virtual-clock reader captures the fourth clock
timestamp there; it must not call GUI code or perform blocking operations.
"""

import math
import socket
import threading
from dataclasses import dataclass, replace
from enum import Enum
from ipaddress import IPv4Address
from queue import Full, Queue
from time import monotonic, monotonic_ns
from typing import Callable, Dict, Optional, Tuple, Union
from uuid import UUID, uuid4

from ..core.models import NodeIdentity
from ..core.protocol import (
    MAX_INTEGER, Message, ProtocolError, TCPFrameBuffer, parse_packet,
    serialize_packet, validate_context,
)


class EventType(str, Enum):
    CONNECTED = "CONNECTED"
    MESSAGE = "MESSAGE"
    ERROR = "ERROR"
    DISCONNECTED = "DISCONNECTED"


class TransportClosedError(RuntimeError):
    """The transport or requested connection is no longer active."""


class EventOverflowError(RuntimeError):
    """Events were lost; the controller must invalidate affected work."""


@dataclass(frozen=True)
class ConnectionInfo:
    connection_id: UUID
    address: Tuple[str, int]
    incoming: bool
    peer: Optional[NodeIdentity] = None


@dataclass(frozen=True)
class TransportEvent:
    type: EventType
    connection: ConnectionInfo
    monotonic_ns: int
    message: Optional[Message] = None
    clock_time_ms: Optional[int] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class SendReceipt:
    message_id: UUID
    sent_monotonic_ns: int


class _Connection:
    def __init__(self, sock: socket.socket, info: ConnectionInfo) -> None:
        self.sock = sock
        self.info = info
        self.send_lock = threading.Lock()


def _positive_seconds(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("{} must be a finite positive duration".format(name))
    if not math.isfinite(value) or not 0 < value <= 60:
        raise ValueError("{} must be in (0, 60] seconds".format(name))
    return float(value)


def _port(value: int, allow_zero: bool = False) -> None:
    if type(value) is not int or not (0 if allow_zero else 1) <= value <= 65535:
        raise ValueError("Invalid TCP port")


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


class TCPTransport:
    """One-shot lifecycle: start, connect/send/get_event, then stop.

    port=0 is supported for isolated loopback tests; production defaults to
    9999. Idle recv timeouts only allow shutdown checks, not failure detection.
    max_connections includes in-progress outbound connects. A full event queue
    closes the producing connection and sets a sticky overflow flag; get_event
    raises once per observed overflow batch before returning more queued data.
    The caller must cancel active sessions on overflow rather than trust ACKs.
    """

    def __init__(
        self, local_identity: NodeIdentity, *, host: str = "0.0.0.0",
        port: int = 9999, connect_timeout: float = 2.0,
        io_timeout: float = 0.5, max_connections: int = 16,
        event_capacity: int = 256,
        clock_source: Optional[Callable[[], int]] = None,
    ) -> None:
        if not isinstance(local_identity, NodeIdentity):
            raise TypeError("local_identity must be NodeIdentity")
        IPv4Address(host)
        _port(port, allow_zero=True)
        for name, value in (("max_connections", max_connections),
                            ("event_capacity", event_capacity)):
            if type(value) is not int or not 1 <= value <= 65536:
                raise ValueError("{} must be in [1, 65536]".format(name))
        if clock_source is not None and not callable(clock_source):
            raise TypeError("clock_source must be callable")
        self.local_identity = local_identity
        self._host, self._port = host, port
        self._connect_timeout = _positive_seconds(connect_timeout, "connect_timeout")
        self._io_timeout = _positive_seconds(io_timeout, "io_timeout")
        self._max_connections = max_connections
        self._clock_source = clock_source
        self._events = Queue(maxsize=event_capacity)  # type: Queue
        self._lock = threading.RLock()
        self._connections = {}  # type: Dict[UUID, _Connection]
        self._pending = set()
        self._threads = set()
        self._listener = None  # type: Optional[socket.socket]
        self._address = None  # type: Optional[Tuple[str, int]]
        self._stopped = False
        self._lost_events = 0
        self._overflow_pending = False

    @property
    def address(self) -> Optional[Tuple[str, int]]:
        with self._lock:
            return self._address

    @property
    def lost_events(self) -> int:
        with self._lock:
            return self._lost_events

    def connections(self) -> Tuple[ConnectionInfo, ...]:
        with self._lock:
            return tuple(c.info for c in self._connections.values())

    def _require_running(self) -> None:
        if self._stopped or self._listener is None:
            raise TransportClosedError("TCP transport is not running")

    def _spawn(self, target: Callable, *args: object) -> None:
        def run() -> None:
            try:
                target(*args)
            finally:
                with self._lock:
                    self._threads.discard(threading.current_thread())
        thread = threading.Thread(target=run, daemon=True, name="BerkeleyTCP")
        self._threads.add(thread)
        try:
            thread.start()
        except RuntimeError:
            self._threads.discard(thread)
            raise

    def start(self) -> Tuple[str, int]:
        with self._lock:
            if self._stopped or self._listener is not None:
                raise TransportClosedError("Create a new transport after start/stop")
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Windows SO_REUSEADDR can permit a competing listener to bind.
                if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                listener.settimeout(self._io_timeout)
                listener.bind((self._host, self._port))
                listener.listen(self._max_connections)
                self._listener = listener
                self._address = listener.getsockname()
                self._spawn(self._accept_loop, listener)
            except (OSError, RuntimeError):
                self._listener = None
                self._address = None
                listener.close()
                raise
            return self._address

    def _emit(self, event: TransportEvent) -> bool:
        try:
            self._events.put_nowait(event)
            return True
        except Full:
            with self._lock:
                self._lost_events += 1
                self._overflow_pending = True
            return False

    def get_event(self, timeout: float = 0.0) -> TransportEvent:
        """Return one event, or raise queue.Empty; never call GUI from workers."""
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError("timeout must be a finite nonnegative duration")
        if not math.isfinite(timeout) or not 0 <= timeout <= 60:
            raise ValueError("timeout must be in [0, 60] seconds")
        with self._lock:
            if self._overflow_pending:
                self._overflow_pending = False
                raise EventOverflowError("TCP events lost: {}".format(self._lost_events))
        return self._events.get(timeout=timeout)

    def _adopt(
        self, sock: socket.socket, address: Tuple[str, int], incoming: bool,
        peer: Optional[NodeIdentity] = None,
    ) -> UUID:
        # Caller owns the socket and closes it if adoption raises.
        with self._lock:
            self._require_running()
            if len(self._connections) + len(self._pending) >= self._max_connections:
                raise ConnectionError("TCP connection capacity reached")
            sock.settimeout(self._io_timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            info = ConnectionInfo(uuid4(), address, incoming, peer)
            connection = _Connection(sock, info)
            self._connections[info.connection_id] = connection
            try:
                if not self._emit(TransportEvent(EventType.CONNECTED, info, monotonic_ns())):
                    raise EventOverflowError("TCP event queue is full")
                self._spawn(self._read_loop, connection)
            except (RuntimeError, OSError):
                self._connections.pop(info.connection_id, None)
                raise
            return info.connection_id

    def connect(
        self, ip: str, port: int = 9999, *,
        expected_peer: Optional[NodeIdentity] = None,
    ) -> UUID:
        IPv4Address(ip)
        _port(port)
        if expected_peer is not None:
            if not isinstance(expected_peer, NodeIdentity):
                raise TypeError("expected_peer must be NodeIdentity")
            if expected_peer.node_id == self.local_identity.node_id:
                raise ValueError("Cannot connect to the local node ID")
        with self._lock:
            self._require_running()
            if len(self._connections) + len(self._pending) >= self._max_connections:
                raise ConnectionError("TCP connection capacity reached")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._pending.add(sock)
        try:
            sock.settimeout(self._connect_timeout)
            sock.connect((ip, port))
            with self._lock:
                self._pending.discard(sock)
                return self._adopt(sock, (ip, port), False, expected_peer)
        except (OSError, RuntimeError):
            _close_socket(sock)
            raise
        finally:
            with self._lock:
                self._pending.discard(sock)

    def _accept_loop(self, listener: socket.socket) -> None:
        while True:
            try:
                sock, address = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                self._adopt(sock, address, True)
            except (OSError, RuntimeError):
                _close_socket(sock)

    def _drop(self, connection: _Connection, detail: str) -> None:
        with self._lock:
            if self._connections.pop(connection.info.connection_id, None) is None:
                return
            info = connection.info
        _close_socket(connection.sock)
        self._emit(TransportEvent(EventType.DISCONNECTED, info, monotonic_ns(), detail=detail))

    def disconnect(self, connection_id: UUID) -> None:
        with self._lock:
            connection = self._connections.get(connection_id)
        if connection is not None:
            self._drop(connection, "Local disconnect")

    def _read_loop(self, connection: _Connection) -> None:
        buffer = TCPFrameBuffer()
        detail = "Peer closed connection"
        try:
            while True:
                try:
                    chunk = connection.sock.recv(4096)
                except socket.timeout:
                    continue
                received_ns = monotonic_ns()
                if not chunk:
                    buffer.finish()
                    break
                clock_ms = self._clock_source() if self._clock_source else None
                if clock_ms is not None and (
                    type(clock_ms) is not int or not 0 <= clock_ms <= MAX_INTEGER
                ):
                    raise ValueError("clock_source returned invalid milliseconds")
                for raw in buffer.feed(chunk):
                    try:
                        message = parse_packet(raw)
                        validate_context(message, recipient_id=self.local_identity.node_id)
                        if message.source.node_id == self.local_identity.node_id:
                            raise ProtocolError("Received local node ID on remote connection")
                    except ProtocolError as exc:
                        if not self._emit(TransportEvent(
                            EventType.ERROR, connection.info, received_ns, detail=str(exc)
                        )):
                            raise EventOverflowError("TCP event queue is full")
                        continue
                    with self._lock:
                        if connection.info.connection_id not in self._connections:
                            return
                        if connection.info.peer is None:
                            connection.info = replace(connection.info, peer=message.source)
                        elif message.source != connection.info.peer:
                            raise ProtocolError("Source identity changed on TCP connection")
                        info = connection.info
                        # Keep enqueue ordered before any concurrent drop of
                        # this connection; MESSAGE must not follow DISCONNECTED.
                        if not self._emit(TransportEvent(
                            EventType.MESSAGE, info, received_ns, message, clock_ms
                        )):
                            raise EventOverflowError("TCP event queue is full")
        except Exception as exc:
            # Includes failures of the injected clock reader; don't lose a
            # receive thread silently or let it update application state.
            detail = "{}: {}".format(type(exc).__name__, exc)
            with self._lock:
                active = connection.info.connection_id in self._connections
            if active:
                self._emit(TransportEvent(
                    EventType.ERROR, connection.info, monotonic_ns(), detail=detail
                ))
        finally:
            self._drop(connection, detail)

    def send(
        self, connection_id: UUID,
        message: Union[Message, Callable[[], Message]],
    ) -> SendReceipt:
        """Serialize sends on this connection; success is not an application ACK.

        For timing-sensitive polls/replies, pass a fast message factory that
        samples the virtual clock. It runs after acquiring the send lock. The
        receipt marks preparation start, excluding lock wait but including
        serialization/send time, not remote arrival. A prebuilt Message is
        suitable for adjustments/elections that do not measure a clock offset.
        """
        with self._lock:
            self._require_running()
            connection = self._connections.get(connection_id)
        if connection is None:
            raise TransportClosedError("Unknown TCP connection")
        if not connection.send_lock.acquire(timeout=self._io_timeout):
            raise TimeoutError("Timed out waiting for another TCP send")
        try:
            with self._lock:
                if connection_id not in self._connections:
                    raise TransportClosedError("TCP connection was closed")
            sent_ns = monotonic_ns()
            prepared = message() if callable(message) else message
            raw = serialize_packet(prepared)
            if prepared.source != self.local_identity:
                raise ProtocolError("Outbound source must match the local process")
            with self._lock:
                if connection_id not in self._connections:
                    raise TransportClosedError("TCP connection was closed during preparation")
                peer = connection.info.peer
                if peer is not None and prepared.target_node_id not in (None, peer.node_id):
                    raise ProtocolError("Outbound message targets another connection")
            try:
                connection.sock.sendall(raw)
            except OSError as exc:
                self._drop(connection, "Send failed: {}".format(exc))
                raise
            return SendReceipt(prepared.message_id, sent_ns)
        finally:
            connection.send_lock.release()

    def stop(self, timeout: float = 2.0) -> None:
        """Close listener, active and pending sockets; join within one deadline."""
        timeout = _positive_seconds(timeout, "timeout")
        deadline = monotonic() + timeout
        with self._lock:
            self._stopped = True
            listener, self._listener = self._listener, None
            pending = tuple(self._pending)
            connections = tuple(self._connections.values())
            threads = tuple(self._threads)
        if listener is not None:
            _close_socket(listener)
        for sock in pending:
            _close_socket(sock)
        for connection in connections:
            self._drop(connection, "Transport stopped")
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(max(0.0, deadline - monotonic()))
        if any(t.is_alive() for t in threads if t is not threading.current_thread()):
            raise TimeoutError("TCP threads have not stopped before the deadline")
