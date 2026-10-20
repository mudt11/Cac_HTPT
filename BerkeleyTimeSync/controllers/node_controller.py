"""Node lifecycle and event distribution across independent feature controllers.

start/poll/force_master/accept_restart/stop run on ONE owner thread, outside
the GUI. The caller drives poll regularly; no hidden controller loop is started.
TCP dispatch has one bounded queue/worker per remote ID so dead endpoints do
not block the heartbeat loop. Workers never mutate election or GUI state.
snapshot and drain_notices may be read by the GUI from another thread.

Synchronization uses transport receive timestamps and send-lock factories.
Automatic rounds run only on the Master, without overlapping active sessions.
An election startup grace allows discovery to run; it is not proof all six
machines have been found. Restart conflicts require explicit accept_restart.
Transport event overflow stops this node rather than campaigning on lost OKs.
"""

import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum
from ipaddress import IPv4Address
from queue import Empty, Full, Queue
from time import monotonic, monotonic_ns
from typing import Callable, Dict, Optional, Tuple, Union

from ..core.clock import VirtualClock
from ..core.election import ElectionSnapshot, ElectionUpdate
from ..core.models import NodeIdentity, NodeRole, PeerInfo, PeerStatus, SyncRoundState
from ..core.peer_registry import PeerRegistry
from ..core.protocol import Message, ProtocolError
from ..network.discovery import DiscoveryStatus
from ..network.heartbeat import HeartbeatSnapshot
from .membership_controller import MembershipController
from .leadership_controller import LeadershipController
from .sync_controller import SyncController, SyncDispatch, SyncResult, SyncSnapshot, SyncStateError
from ..network.tcp_transport import (
    TCPTransport, EventType as TCPEventType, EventOverflowError as TCPOverflowError,
)
from ..network.udp_transport import (
    UDPTransport, EventType as UDPEventType, EventOverflowError as UDPOverflowError,
)


class NoticeKind(str, Enum):
    LIFECYCLE = "LIFECYCLE"
    DISCOVERY = "DISCOVERY"
    ELECTION = "ELECTION"
    HEARTBEAT = "HEARTBEAT"
    CONFLICT = "CONFLICT"
    ERROR = "ERROR"
    UNSUPPORTED = "UNSUPPORTED"
    SYNC = "SYNC"


@dataclass(frozen=True)
class ControllerNotice:
    kind: NoticeKind
    detail: str
    monotonic_ns: int
    peer: Optional[NodeIdentity] = None


@dataclass(frozen=True)
class NodeSnapshot:
    identity: NodeIdentity
    running: bool
    ready: bool
    election: ElectionSnapshot
    peers: Tuple[PeerInfo, ...]
    heartbeat: Optional[HeartbeatSnapshot]
    conflicts: Tuple[PeerInfo, ...]
    tcp_address: Optional[Tuple[str, int]]
    udp_address: Optional[Tuple[str, int]]
    dropped_notices: int
    synchronization: Optional[SyncSnapshot] = None
    auto_sync: bool = True
    sync_interval_s: int = 10


@dataclass(frozen=True)
class _Dispatch:
    message: Union[Message, SyncDispatch]
    peer: PeerInfo
    generation: int
    deadline_ns: int


class NodeController:
    """Own one process identity and its TCP/UDP services (one-shot lifecycle).

    ip is the selected LAN interface address advertised to peers, never chosen
    through an Internet route. bind_host=0.0.0.0 receives broadcasts on Windows;
    use a specific bind_host for isolated tests. Physical nodes default to TCP
    9999 / UDP 8888; zero ports are allowed only for local test arrangements.
    Manual commands must be serialized as required by BullyElection policy.
    """

    def __init__(
        self, identity: NodeIdentity, display_name: str, ip: str, *,
        bind_host: str = "0.0.0.0", tcp_port: int = 9999, udp_port: int = 8888,
        broadcast_ip: str = "255.255.255.255", discovery_port: int = 8888,
        startup_wait_ms: int = 3000, clock: Optional[VirtualClock] = None,
        auto_sync: bool = True, sync_interval_s: int = 10,
        connect_timeout: float = 0.5, monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not isinstance(identity, NodeIdentity):
            raise TypeError("identity must be NodeIdentity")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        if clock is not None and not isinstance(clock, VirtualClock):
            raise TypeError("clock must be VirtualClock")
        if type(startup_wait_ms) is not int or not 0 <= startup_wait_ms <= 60000:
            raise ValueError("startup_wait_ms must be in [0, 60000]")
        if type(auto_sync) is not bool:
            raise TypeError("auto_sync must be bool")
        if type(sync_interval_s) is not int or sync_interval_s not in (5, 10):
            raise ValueError("sync_interval_s must be 5 or 10")
        # Validate advertised metadata without using zero test bind ports.
        PeerInfo(identity, display_name, ip)
        address = IPv4Address(ip)
        if address.is_unspecified or address.is_multicast or str(address) == "255.255.255.255":
            raise ValueError("Choose a usable LAN interface address")
        IPv4Address(broadcast_ip)
        if type(discovery_port) is not int or not 1 <= discovery_port <= 65535:
            raise ValueError("Invalid discovery port")
        self.identity = identity
        self.clock = clock if clock is not None else VirtualClock(monotonic_source=monotonic_source)
        self._name, self._ip = display_name, ip
        self._monotonic_source = monotonic_source
        self._last_now_ns = None  # type: Optional[int]
        self._registry = PeerRegistry(identity, monotonic_source=monotonic_source)
        self.membership = MembershipController(self._registry, monotonic_source=monotonic_source)
        self.leadership = LeadershipController(self._registry, self.clock.now_ms,
                                                monotonic_source=monotonic_source)
        self.sync = SyncController(self._registry, self.clock, monotonic_source=monotonic_source)
        self._auto_sync = auto_sync
        self._sync_interval_s = sync_interval_s
        self._next_sync_ns = 0
        self._tcp = TCPTransport(identity, host=bind_host, port=tcp_port,
                                 connect_timeout=connect_timeout, io_timeout=0.25,
                                 clock_source=self.clock.now_ms)
        self._udp = UDPTransport(identity, host=bind_host, port=udp_port,
                                 io_timeout=0.25, clock_source=self.clock.now_ms)
        self._broadcast_ip, self._discovery_port = broadcast_ip, discovery_port
        self._startup_ns = startup_wait_ms * 1000000
        self._ready_at_ns = 0
        self._ready = False
        self._running = False
        self._closed = False
        self._owner = None  # type: Optional[int]
        self._generation = 0
        self._last_applied = self.leadership.snapshot()
        self._lock = threading.RLock()
        self._notices = deque(maxlen=256)
        self._dropped_notices = 0
        self._stop_event = threading.Event()
        self._queues = {i: Queue(maxsize=32) for i in range(1, 7) if i != identity.node_id}
        self._workers = []
        self._snapshot = None  # type: Optional[NodeSnapshot]
        self._publish()

    def _now(self) -> int:
        now = self._monotonic_source()
        if type(now) is not int or now < 0:
            raise ValueError("Invalid monotonic nanoseconds")
        if self._last_now_ns is not None and now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    def _check_owner(self) -> None:
        if self._owner is not None and self._owner != threading.get_ident():
            raise RuntimeError("Drive controller commands on its owner thread, outside the GUI")

    def _require_running(self) -> None:
        self._check_owner()
        if not self._running:
            raise RuntimeError("Node controller is not running")

    def _notice(self, kind: NoticeKind, detail: str, peer: Optional[NodeIdentity] = None) -> None:
        # Worker notices use real local receipt time, never mutate _last_now_ns.
        notice = ControllerNotice(kind, detail, monotonic_ns(), peer)
        with self._lock:
            if len(self._notices) == self._notices.maxlen:
                self._dropped_notices += 1
            self._notices.append(notice)

    def drain_notices(self) -> Tuple[ControllerNotice, ...]:
        with self._lock:
            notices = tuple(self._notices)
            self._notices.clear()
            return notices

    def snapshot(self) -> NodeSnapshot:
        """Return the last immutable owner-thread publication, safe for GUI reads."""
        with self._lock:
            return self._snapshot

    def _publish(self) -> None:
        heartbeat = self.leadership.heartbeat_snapshot()
        snapshot = NodeSnapshot(
            self.identity, self._running, self._ready, self.leadership.snapshot(),
            self._registry.snapshot(), heartbeat,
            self.membership.conflicts(),
            self._tcp.address, self._udp.address, self._dropped_notices, self.sync.snapshot(),
            self._auto_sync, self._sync_interval_s)
        with self._lock:
            self._snapshot = snapshot

    def start(self) -> NodeSnapshot:
        self._check_owner()
        if self._running or self._closed:
            raise RuntimeError("Create a new controller after start/stop")
        self._owner = threading.get_ident()
        try:
            self._tcp.start()
            self._udp.start()
            self.membership.bind(self._udp, self._local_peer(),
                                 broadcast_ip=self._broadcast_ip, discovery_port=self._discovery_port)
            self.leadership.bind(self._udp)
            self._ready_at_ns = self._now() + self._startup_ns
            with self._lock:
                self._running = True
            for node_id in self._queues:
                worker = threading.Thread(target=self._dispatch_loop, args=(node_id,),
                                          daemon=True, name="BerkeleyDispatch-{}".format(node_id))
                worker.start()
                self._workers.append(worker)
            self._notice(NoticeKind.LIFECYCLE, "TCP/UDP bound; discovering membership")
            self.membership.tick()
            self._publish()
            return self.snapshot()
        except Exception:
            self.stop()
            raise

    def _local_peer(self, election: Optional[ElectionSnapshot] = None) -> PeerInfo:
        election = self.leadership.snapshot() if election is None else election
        return PeerInfo(self.identity, self._name, self._ip,
                        tcp_port=self._tcp.address[1], udp_port=self._udp.address[1],
                        role=election.role)

    def _apply_election(self, update: ElectionUpdate) -> None:
        current, previous = update.snapshot, self._last_applied
        old_key = (previous.leader, previous.mode, previous.election_id)
        new_key = (current.leader, current.mode, current.election_id)
        if old_key != new_key:
            with self._lock:
                self._generation += 1
        if current != previous:
            self.membership.set_advertisement(self._local_peer(current), current.leader)
            self.membership.refresh_roles(current.leader)
            self.sync.leadership_changed(current)
            if (current.leader, current.role) != (previous.leader, previous.role):
                self._next_sync_ns = self._now() + self._sync_interval_s * 1000000000
            self._notice(NoticeKind.ELECTION, update.detail)
        self._last_applied = current
        now = self._now()
        deadline = current.deadline_ns if current.deadline_ns is not None else now + 3000000000
        for message in update.outbound:
            self._enqueue(message, deadline)

    def _enqueue(self, message: Union[Message, SyncDispatch], deadline: int) -> None:
        target = message.target if isinstance(message, SyncDispatch) else None
        peer = self._registry.get(target.node_id if target else message.target_node_id)
        if peer is None or (target is not None and peer.identity != target):
            self._notice(NoticeKind.ERROR, "TCP recipient has no matching discovery entry")
            return
        job = _Dispatch(message, peer, self._generation, deadline)
        try:
            self._queues[peer.identity.node_id].put_nowait(job)
        except Full:
            self._notice(NoticeKind.ERROR, "TCP dispatch queue full; message not sent", peer.identity)

    def _job_current(self, job: _Dispatch) -> bool:
        with self._lock:
            if not self._running or job.generation != self._generation:
                return False
        if self._monotonic_source() >= job.deadline_ns:
            return False
        peer = self._registry.get(job.peer.identity.node_id)
        return (peer is not None and peer.identity == job.peer.identity
                and (peer.ip, peer.tcp_port) == (job.peer.ip, job.peer.tcp_port))

    def _dispatch_loop(self, node_id: int) -> None:
        while not self._stop_event.is_set():
            try:
                job = self._queues[node_id].get(timeout=0.1)
            except Empty:
                continue
            try:
                if not self._job_current(job):
                    continue
                candidates = [c for c in self._tcp.connections()
                              if c.peer == job.peer.identity and c.address[0] == job.peer.ip
                              and (c.incoming or c.address[1] == job.peer.tcp_port)]
                connection_id = (candidates[0].connection_id if candidates else
                                 self._tcp.connect(job.peer.ip, job.peer.tcp_port,
                                                   expected_peer=job.peer.identity))
                if self._job_current(job):
                    if isinstance(job.message, SyncDispatch):
                        self._tcp.send(connection_id, lambda: self.sync.prepare_send(job.message))
                    else:
                        self._tcp.send(connection_id, job.message)
            except SyncStateError:
                # Expired/invalidated one-shot synchronization actions are discarded.
                pass
            except Exception as exc:
                if not self._stop_event.is_set():
                    self._notice(NoticeKind.ERROR, "TCP dispatch failed: {}: {}".format(
                        type(exc).__name__, exc), job.peer.identity)
            finally:
                self._queues[node_id].task_done()

    def _close_obsolete_connections(self) -> None:
        for connection_id in self.membership.obsolete_connections(self._tcp.connections()):
            self._tcp.disconnect(connection_id)

    def _membership_updated(self, peer: PeerInfo) -> None:
        self._close_obsolete_connections()
        self.membership.refresh_roles(self.leadership.snapshot().leader)
        update = self.leadership.membership_updated(peer, ready=self._ready)
        if update is not None:
            self._apply_election(update)

    def _udp_event(self, event) -> None:
        result = self.membership.handle_udp(event)
        if result is not None:
            if result.status == DiscoveryStatus.CONFLICT:
                self._notice(NoticeKind.CONFLICT, result.detail, result.candidate.identity)
                if result.candidate.identity.node_id == self.identity.node_id:
                    self._notice(NoticeKind.ERROR, "Local node ID conflict; stopping ambiguous node")
                    self.stop()
                return
            if result.status == DiscoveryStatus.UPDATED:
                self._membership_updated(result.observed)
            if result.detail:
                self._notice(NoticeKind.DISCOVERY, result.detail,
                             result.candidate.identity if result.candidate else None)
            return
        if event.type == UDPEventType.STOPPED:
            self._notice(NoticeKind.ERROR, "UDP stopped; stopping controller")
            self.stop()
        elif event.type == UDPEventType.ERROR:
            self._notice(NoticeKind.ERROR, event.detail or "UDP error")
        else:
            heartbeat = self.leadership.handle_udp(event)
            if heartbeat is not None and heartbeat.detail:
                self._notice(NoticeKind.HEARTBEAT, heartbeat.detail,
                             event.message.source if event.message else None)

    def _tcp_event(self, event) -> None:
        if event.type == TCPEventType.ERROR:
            self._notice(NoticeKind.ERROR, event.detail or "TCP error")
            return
        if event.type != TCPEventType.MESSAGE:
            return
        message = event.message
        if not self.membership.admits_tcp(event.connection, message):
            self._notice(NoticeKind.ERROR, "TCP source does not match discovered membership", message.source)
            self._tcp.disconnect(event.connection.connection_id)
            return
        if self.leadership.accepts(message):
            for update in self.leadership.handle_tcp(message, event.monotonic_ns):
                self._apply_election(update)
        else:
            result = self.sync.handle_tcp(message, received_monotonic_ns=event.monotonic_ns,
                                          clock_time_ms=event.clock_time_ms)
            if result is not None:
                self._sync_result(result)
            else:
                self._notice(NoticeKind.UNSUPPORTED, "Unsupported TCP message", message.source)

    def _sync_result(self, result: SyncResult, *, quiet: bool = False) -> None:
        for action in result.outbound:
            self._enqueue(action, action.deadline_ns)
        if not quiet or result.outbound:
            self._notice(NoticeKind.SYNC, result.detail)

    def _start_sync_round(self) -> None:
        heartbeat = self.leadership.heartbeat_snapshot()
        participants = [self.identity]
        if heartbeat is not None:
            for peer in heartbeat.peers:
                known = self._registry.get(peer.identity.node_id)
                if (peer.status == PeerStatus.ONLINE and known is not None
                        and known.identity == peer.identity and peer.identity != self.identity):
                    participants.append(peer.identity)
        self._sync_result(self.sync.start_round(tuple(participants)))
        self._next_sync_ns = self._now() + self._sync_interval_s * 1000000000

    def _sync_tick(self, now: int) -> None:
        self._sync_result(self.sync.tick(), quiet=True)
        state = self.sync.snapshot()
        if (not self._auto_sync or not self._ready or state.role != NodeRole.MASTER
                or now < self._next_sync_ns):
            return
        if state.session is not None and state.session.state not in (
                SyncRoundState.COMPLETED, SyncRoundState.PARTIAL,
                SyncRoundState.FAILED, SyncRoundState.CANCELLED):
            return
        self._next_sync_ns = now + self._sync_interval_s * 1000000000
        try:
            self._start_sync_round()
        except (SyncStateError, ValueError) as exc:
            self._notice(NoticeKind.SYNC, "Automatic round deferred: {}".format(exc))

    def poll(self, max_events: int = 64) -> NodeSnapshot:
        """Drain bounded batches from BOTH transports, then run due timers."""
        self._require_running()
        if type(max_events) is not int or not 1 <= max_events <= 4096:
            raise ValueError("max_events must be in [1, 4096]")
        for transport, handler in ((self._udp, self._udp_event), (self._tcp, self._tcp_event)):
            for _ in range(max_events):
                if not self._running:
                    return self.snapshot()
                try:
                    event = transport.get_event()
                except Empty:
                    break
                except (UDPOverflowError, TCPOverflowError) as exc:
                    self._notice(NoticeKind.ERROR, "Transport events lost; stopping node: {}".format(exc))
                    self.stop()
                    return self.snapshot()
                try:
                    handler(event)
                except (ProtocolError, ValueError, KeyError) as exc:
                    self._notice(NoticeKind.ERROR, "Rejected event: {}".format(exc))
        if not self._running:
            return self.snapshot()
        now = self._now()
        try:
            self.membership.tick()
            if not self._ready and now >= self._ready_at_ns:
                self._ready = True
            cycle = self.leadership.tick(ready=self._ready)
            for failure in cycle.heartbeat.failures:
                self._notice(NoticeKind.ERROR, failure.detail, failure.identity)
            for change in cycle.heartbeat.changes:
                self._notice(NoticeKind.HEARTBEAT, change.status.value, change.identity)
            for update in cycle.updates:
                self._apply_election(update)
            self._sync_tick(now)
        except (OSError, RuntimeError, ValueError) as exc:
            self._notice(NoticeKind.ERROR, "Timer/service failure; stopping node: {}".format(exc))
            self.stop()
            return self.snapshot()
        self._publish()
        return self.snapshot()

    def request_sync(self) -> NodeSnapshot:
        """Start one round using only heartbeat-confirmed Workers."""
        self._require_running()
        self._start_sync_round()
        self._publish()
        return self.snapshot()

    def configure_sync(self, *, auto_sync: bool, interval_s: int) -> NodeSnapshot:
        """Configure automatic rounds on the controller owner thread."""
        self._require_running()
        if type(auto_sync) is not bool:
            raise TypeError("auto_sync must be bool")
        if type(interval_s) is not int or interval_s not in (5, 10):
            raise ValueError("interval_s must be 5 or 10")
        self._auto_sync, self._sync_interval_s = auto_sync, interval_s
        self._next_sync_ns = self._now() + interval_s * 1000000000
        self._publish()
        return self.snapshot()

    def force_master(self, reason: str) -> NodeSnapshot:
        """Trusted operator command on the selected node, not a GUI-thread call."""
        self._require_running()
        self._apply_election(self.leadership.force_master(reason))
        self._publish()
        return self.snapshot()

    def accept_restart(self, expected: NodeIdentity) -> NodeSnapshot:
        """Explicit operator/controller acceptance of a validated offline restart.

        Timeout alone is not proof of restart. The caller must validate the
        retained conflict candidate before invoking this compare-and-swap action.
        A live/suspect old process cannot be replaced by this method.
        """
        self._require_running()
        candidate = self.membership.accept_restart(expected)
        self._membership_updated(candidate)
        self._notice(NoticeKind.DISCOVERY, "Validated offline restart accepted", candidate.identity)
        self._publish()
        return self.snapshot()

    def stop(self, timeout: float = 3.0) -> NodeSnapshot:
        """Stop all owned transports/workers within a shared shutdown deadline."""
        self._check_owner()
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 60:
            raise ValueError("timeout must be in (0, 60] seconds")
        deadline = monotonic() + timeout
        with self._lock:
            self._running = False
            self._closed = True
            self._generation += 1
        self._stop_event.set()
        self.sync.cancel("Node stopped")
        failures = []
        for transport in (self._tcp, self._udp):
            try:
                transport.stop(timeout=max(0.001, deadline - monotonic()))
            except (OSError, RuntimeError) as exc:
                failures.append(str(exc))
        for worker in self._workers:
            worker.join(max(0.0, deadline - monotonic()))
        if any(worker.is_alive() for worker in self._workers):
            failures.append("TCP dispatch workers did not stop")
        self._notice(NoticeKind.LIFECYCLE, "Node stopped")
        self._publish()
        if failures:
            raise TimeoutError("; ".join(failures))
        return self.snapshot()
