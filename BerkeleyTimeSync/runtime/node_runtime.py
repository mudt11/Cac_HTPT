"""Nonblocking GUI boundary around the single-owner NodeController.

Create a runtime per start/stop cycle. GUI code submits commands and reads
immutable snapshots/events; it never calls controller methods or Tk callbacks
from this worker. stop() signals shutdown without joining a network thread.
Command success means the operation was accepted locally, not a remote ACK or
clock convergence. Network/session progress remains in NodeSnapshot.
"""

import math
import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Full, Queue
from time import monotonic_ns
from typing import Callable, Optional, Tuple
from uuid import UUID, uuid4

from ..controllers.node_controller import ControllerNotice, NodeController, NodeSnapshot
from ..core.clock import ClockSnapshot
from ..core.models import NodeIdentity


class RuntimeState(str, Enum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class CommandKind(str, Enum):
    SYNC_NOW = "SYNC_NOW"
    CONFIGURE_SYNC = "CONFIGURE_SYNC"
    FORCE_MASTER = "FORCE_MASTER"
    ACCEPT_RESTART = "ACCEPT_RESTART"
    INJECT_OFFSET = "INJECT_OFFSET"
    RESET_CLOCK = "RESET_CLOCK"
    PAUSE_CLOCK = "PAUSE_CLOCK"
    RESUME_CLOCK = "RESUME_CLOCK"


@dataclass(frozen=True)
class NodeConfig:
    """Explicit interface selection; no Internet route lookup is performed.

    Controller configuration errors are reported asynchronously as FAILED.
    Physical/VM nodes use the default ports; zero bind ports are for local QA.
    """

    node_id: int
    display_name: str
    ip: str
    bind_host: str = "0.0.0.0"
    tcp_port: int = 9999
    udp_port: int = 8888
    broadcast_ip: str = "255.255.255.255"
    discovery_port: int = 8888
    auto_sync: bool = True
    sync_interval_s: int = 10


@dataclass(frozen=True)
class RuntimeEvent:
    """Bounded event stream; command_id correlates an asynchronous result."""

    kind: str
    detail: str
    monotonic_ns: int
    command_id: Optional[UUID] = None
    command: Optional[CommandKind] = None
    success: Optional[bool] = None
    notice: Optional[ControllerNotice] = None


@dataclass(frozen=True)
class RuntimeSnapshot:
    state: RuntimeState
    node: Optional[NodeSnapshot]
    clock: Optional[ClockSnapshot]
    pending_commands: int
    dropped_events: int
    last_error: Optional[str]


@dataclass(frozen=True)
class _Command:
    identity: UUID
    kind: CommandKind
    arguments: tuple


class NodeRuntime:
    """One owner thread, bounded commands and priority shutdown.

    start(), stop(), submit(), snapshot(), and drain_events() are safe for GUI
    calls. wait_stopped() is available for headless shutdown only, never a GUI
    callback. A queue rejection raises immediately; command failures become
    COMMAND events and do not stop the service. Unexpected service failures
    stop transports and leave the runtime FAILED with an error message.
    """

    def __init__(self, config: NodeConfig, *, poll_interval_s: float = 0.02,
                 command_capacity: int = 64, event_capacity: int = 512,
                 controller_factory: Callable = NodeController) -> None:
        if not isinstance(config, NodeConfig):
            raise TypeError("config must be NodeConfig")
        if (type(poll_interval_s) not in (int, float)
                or not math.isfinite(poll_interval_s)
                or not 0.005 <= poll_interval_s <= 0.25):
            raise ValueError("poll_interval_s must be between 0.005 and 0.25")
        for value in (command_capacity, event_capacity):
            if type(value) is not int or not 1 <= value <= 65536:
                raise ValueError("Queue capacities must be integers in [1, 65536]")
        if not callable(controller_factory):
            raise TypeError("controller_factory must be callable")
        self.config = config
        self._factory = controller_factory
        self._interval = float(poll_interval_s)
        self._commands = Queue(maxsize=command_capacity)
        self._events = deque(maxlen=event_capacity)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._done = threading.Event()
        self._thread = None
        self._state = RuntimeState.CREATED
        self._node = None
        self._clock = None
        self._dropped = 0
        self._error = None

    def _event(self, kind: str, detail: str, **kwargs) -> None:
        event = RuntimeEvent(kind, detail, monotonic_ns(), **kwargs)
        with self._lock:
            if len(self._events) == self._events.maxlen:
                self._dropped += 1
            self._events.append(event)

    def snapshot(self) -> RuntimeSnapshot:
        with self._lock:
            return RuntimeSnapshot(self._state, self._node, self._clock,
                                   self._commands.qsize(), self._dropped, self._error)

    def drain_events(self) -> Tuple[RuntimeEvent, ...]:
        with self._lock:
            events = tuple(self._events)
            self._events.clear()
            return events

    def start(self) -> None:
        with self._lock:
            if self._state != RuntimeState.CREATED:
                raise RuntimeError("Create a new runtime after start/stop")
            self._state = RuntimeState.STARTING
            self._thread = threading.Thread(target=self._run, daemon=True,
                                            name="BerkeleyNodeRuntime")
            try:
                self._thread.start()
            except RuntimeError as exc:
                self._state = RuntimeState.FAILED
                self._error = str(exc)
                self._done.set()
                raise

    def stop(self) -> None:
        """Request shutdown immediately, including while startup is in progress."""
        with self._lock:
            if self._state in (RuntimeState.STOPPED, RuntimeState.FAILED):
                return
            self._stop.set()
            if self._state == RuntimeState.CREATED:
                self._state = RuntimeState.STOPPED
                self._done.set()
            else:
                self._state = RuntimeState.STOPPING

    def wait_stopped(self, timeout: float = 5.0) -> bool:
        """Bounded wait for CLI/tests; GUI should poll snapshot() instead."""
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 <= timeout <= 60:
            raise ValueError("timeout must be a finite number in [0, 60]")
        return self._done.wait(timeout)

    def submit(self, kind: CommandKind, *arguments) -> UUID:
        """Queue an operation without waiting; Full means it was not accepted.

        Arguments: CONFIGURE_SYNC(bool, 5|10), FORCE_MASTER(reason),
        ACCEPT_RESTART(NodeIdentity), INJECT_OFFSET(integer milliseconds).
        Other operations take no arguments. Values are checked on the owner
        thread and failures are returned in COMMAND events.
        """
        if not isinstance(kind, CommandKind):
            raise TypeError("kind must be CommandKind")
        command = _Command(uuid4(), kind, tuple(arguments))
        # Payloads are immutable scalars/identities, never GUI-owned containers.
        if any(type(a) not in (bool, int, str) and not isinstance(a, NodeIdentity)
               for a in command.arguments):
            raise TypeError("Command arguments must be immutable scalar values or NodeIdentity")
        with self._lock:
            if self._state != RuntimeState.RUNNING:
                raise RuntimeError("Runtime is not accepting commands")
            try:
                self._commands.put_nowait(command)
            except Full as exc:
                raise Full("Runtime command queue is full; command was not accepted") from exc
        return command.identity

    def _capture(self, controller) -> None:
        node, clock = controller.snapshot(), controller.clock.snapshot()
        with self._lock:
            self._node, self._clock = node, clock
        for notice in controller.drain_notices():
            self._event("NOTICE", notice.detail, notice=notice)

    def _execute(self, controller, command: _Command) -> None:
        kind, args = command.kind, command.arguments
        if kind == CommandKind.CONFIGURE_SYNC:
            if len(args) != 2:
                raise ValueError("CONFIGURE_SYNC requires auto_sync and interval_s")
            controller.configure_sync(auto_sync=args[0], interval_s=args[1])
        else:
            operations = {
                CommandKind.SYNC_NOW: controller.request_sync,
                CommandKind.FORCE_MASTER: controller.force_master,
                CommandKind.ACCEPT_RESTART: controller.accept_restart,
                CommandKind.INJECT_OFFSET: controller.clock.inject_offset,
                CommandKind.RESET_CLOCK: controller.clock.reset_to_system_time,
                CommandKind.PAUSE_CLOCK: controller.clock.pause,
                CommandKind.RESUME_CLOCK: controller.clock.resume,
            }
            operations[kind](*args)

    def _run(self) -> None:
        controller = None
        failed = False
        try:
            if self._stop.is_set():
                return
            c = self.config
            controller = self._factory(
                NodeIdentity(c.node_id), c.display_name, c.ip,
                bind_host=c.bind_host, tcp_port=c.tcp_port, udp_port=c.udp_port,
                broadcast_ip=c.broadcast_ip, discovery_port=c.discovery_port,
                auto_sync=c.auto_sync, sync_interval_s=c.sync_interval_s)
            controller.start()
            self._capture(controller)
            with self._lock:
                if not self._stop.is_set():
                    self._state = RuntimeState.RUNNING
            self._event("LIFECYCLE", "Node runtime started")
            while not self._stop.is_set():
                # Limit command work each cycle so heartbeat timers keep running.
                for _ in range(8):
                    if self._stop.is_set():
                        break
                    try:
                        command = self._commands.get_nowait()
                    except Empty:
                        break
                    try:
                        self._execute(controller, command)
                    except Exception as exc:
                        self._event("COMMAND", "{}: {}".format(type(exc).__name__, exc),
                                    command_id=command.identity, command=command.kind, success=False)
                    else:
                        self._event("COMMAND", "Command accepted locally",
                                    command_id=command.identity, command=command.kind, success=True)
                    finally:
                        self._commands.task_done()
                if self._stop.is_set():
                    break
                controller.poll()
                self._capture(controller)
                if not self._node.running:
                    raise RuntimeError("Node controller stopped unexpectedly; inspect notices")
                self._stop.wait(self._interval)
        except Exception as exc:
            failed = True
            with self._lock:
                self._error = "{}: {}".format(type(exc).__name__, exc)
            self._event("ERROR", self._error)
        finally:
            if controller is not None:
                try:
                    controller.stop()
                except Exception as exc:
                    failed = True
                    detail = "Shutdown failed: {}: {}".format(type(exc).__name__, exc)
                    with self._lock:
                        self._error = "{}; {}".format(self._error, detail) if self._error else detail
                    self._event("ERROR", detail)
                try:
                    self._capture(controller)
                except Exception as exc:
                    failed = True
                    with self._lock:
                        self._error = "Final snapshot failed: {}: {}".format(type(exc).__name__, exc)
                    self._event("ERROR", self._error)
            while True:
                try:
                    command = self._commands.get_nowait()
                except Empty:
                    break
                self._event("COMMAND", "Command cancelled because runtime stopped",
                            command_id=command.identity, command=command.kind, success=False)
                self._commands.task_done()
            with self._lock:
                self._state = RuntimeState.FAILED if failed else RuntimeState.STOPPED
            self._event("LIFECYCLE", "Node runtime {}".format(self._state.value))
            self._done.set()
