"""Runtime ownership/shutdown tests using fake controllers and no sockets."""

import threading
import time
import unittest
from queue import Full
from types import SimpleNamespace
from unittest.mock import patch

from BerkeleyTimeSync.core.clock import VirtualClock
from BerkeleyTimeSync.runtime import CommandKind, NodeConfig, NodeRuntime, RuntimeState


class FakeController:
    def __init__(self, identity, name, ip, **options):
        self.clock = VirtualClock(initial_time_ms=100000)
        self.running = False
        self.owner = None
        self.calls = []
        self.interval = options["sync_interval_s"]

    def owned(self, operation):
        if threading.get_ident() != self.owner:
            raise AssertionError("Controller used outside its owner thread")
        self.calls.append((operation, threading.get_ident()))

    def start(self):
        self.owner = threading.get_ident()
        self.owned("start")
        self.running = True

    def poll(self):
        self.owned("poll")

    def stop(self):
        self.owned("stop")
        self.running = False

    def snapshot(self):
        return SimpleNamespace(running=self.running, sync_interval_s=self.interval)

    def drain_notices(self):
        return ()

    def configure_sync(self, *, auto_sync, interval_s):
        self.owned("configure")
        if type(auto_sync) is not bool or interval_s not in (5, 10):
            raise ValueError("Invalid sync configuration")
        self.interval = interval_s

    def request_sync(self):
        self.owned("sync")

    def force_master(self, reason):
        self.owned("master")

    def accept_restart(self, expected):
        self.owned("restart")


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.no_sockets = patch("socket.socket", side_effect=AssertionError("Runtime tests must not open sockets"))
        self.no_sockets.start()
        self.addCleanup(self.no_sockets.stop)
        self.config = NodeConfig(1, "QA", "127.0.0.1", auto_sync=False)
        self.runtimes = []
        self.gates = []
        self.addCleanup(self.shutdown)

    def shutdown(self):
        for gate in self.gates:
            gate.set()
        for runtime in self.runtimes:
            runtime.stop()
            self.assertTrue(runtime.wait_stopped(3), "Runtime thread leaked")

    def create(self, controller_class=FakeController, **options):
        instances = []

        def factory(*args, **kwargs):
            controller = controller_class(*args, **kwargs)
            instances.append(controller)
            return controller

        runtime = NodeRuntime(self.config, controller_factory=factory, **options)
        self.runtimes.append(runtime)
        return runtime, instances

    def until(self, predicate):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if predicate():
                return
            threading.Event().wait(0.005)
        self.fail("Timed out waiting for runtime publication")

    def command_events(self, runtime, ids):
        results = {}

        def received():
            results.update((e.command_id, e) for e in runtime.drain_events() if e.command_id in ids)
            return set(results) == set(ids)

        self.until(received)
        return results

    def test_commands_run_on_one_thread_and_return_correlated_results(self):
        runtime, instances = self.create()
        runtime.start()
        self.until(lambda: runtime.snapshot().state == RuntimeState.RUNNING)
        ids = (runtime.submit(CommandKind.SYNC_NOW),
               runtime.submit(CommandKind.CONFIGURE_SYNC, False, 5),
               runtime.submit(CommandKind.FORCE_MASTER, "QA"))
        results = self.command_events(runtime, ids)
        self.assertTrue(all(event.success for event in results.values()))
        runtime.stop()
        self.assertTrue(runtime.wait_stopped(3))
        controller = instances[0]
        self.assertNotEqual(controller.owner, threading.get_ident())
        self.assertEqual({owner for _, owner in controller.calls}, {controller.owner})
        self.assertEqual(controller.interval, 5)
        self.assertEqual(controller.calls[-1][0], "stop")

    def test_bad_command_does_not_stop_later_commands(self):
        runtime, _ = self.create()
        runtime.start()
        self.until(lambda: runtime.snapshot().state == RuntimeState.RUNNING)
        bad = runtime.submit(CommandKind.CONFIGURE_SYNC, False, 7)
        good = runtime.submit(CommandKind.SYNC_NOW)
        events = self.command_events(runtime, (bad, good))
        self.assertFalse(events[bad].success)
        self.assertTrue(events[good].success)
        self.assertEqual(runtime.snapshot().state, RuntimeState.RUNNING)

    def test_stop_before_start_and_one_shot_lifecycle(self):
        runtime, instances = self.create()
        runtime.stop()
        self.assertTrue(runtime.wait_stopped(0))
        self.assertEqual(runtime.snapshot().state, RuntimeState.STOPPED)
        self.assertEqual(instances, [])
        with self.assertRaises(RuntimeError):
            runtime.start()
        with self.assertRaises(RuntimeError):
            runtime.submit(CommandKind.SYNC_NOW)

    def test_stop_during_startup_does_not_publish_running_afterwards(self):
        entered, release = threading.Event(), threading.Event()
        self.gates.append(release)

        class SlowStart(FakeController):
            def start(self):
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("Startup gate timed out")
                super().start()

        runtime, _ = self.create(SlowStart)
        runtime.start()
        self.assertTrue(entered.wait(3))
        runtime.stop()
        self.assertEqual(runtime.snapshot().state, RuntimeState.STOPPING)
        release.set()
        self.assertTrue(runtime.wait_stopped(3))
        self.assertEqual(runtime.snapshot().state, RuntimeState.STOPPED)

    def test_full_queue_and_shutdown_cancel_pending_command(self):
        entered, release = threading.Event(), threading.Event()
        self.gates.append(release)

        class SlowPoll(FakeController):
            def poll(self):
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("Poll gate timed out")
                super().poll()

        runtime, _ = self.create(SlowPoll, command_capacity=1)
        runtime.start()
        self.assertTrue(entered.wait(3))
        pending = runtime.submit(CommandKind.SYNC_NOW)
        with self.assertRaises(Full):
            runtime.submit(CommandKind.SYNC_NOW)
        runtime.stop()
        release.set()
        self.assertTrue(runtime.wait_stopped(3))
        events = [e for e in runtime.drain_events() if e.command_id == pending]
        self.assertEqual(len(events), 1)
        self.assertFalse(events[0].success)
        self.assertEqual(runtime.snapshot().pending_commands, 0)

    def test_service_failure_is_reported_and_controller_stopped(self):
        class BrokenPoll(FakeController):
            def poll(self):
                raise OSError("Simulated service failure")

        runtime, instances = self.create(BrokenPoll)
        runtime.start()
        self.assertTrue(runtime.wait_stopped(3))
        self.assertEqual(runtime.snapshot().state, RuntimeState.FAILED)
        self.assertIn("Simulated service failure", runtime.snapshot().last_error)
        self.assertFalse(instances[0].running)
        self.assertTrue(any(e.kind == "ERROR" for e in runtime.drain_events()))

    def test_shutdown_failure_is_not_reported_as_success(self):
        class BrokenStop(FakeController):
            def stop(self):
                super().stop()
                raise TimeoutError("Simulated incomplete shutdown")

        runtime, _ = self.create(BrokenStop)
        runtime.start()
        self.until(lambda: runtime.snapshot().state == RuntimeState.RUNNING)
        runtime.stop()
        self.assertTrue(runtime.wait_stopped(3))
        self.assertEqual(runtime.snapshot().state, RuntimeState.FAILED)
        self.assertIn("Shutdown failed", runtime.snapshot().last_error)

    def test_event_overflow_is_counted(self):
        runtime, _ = self.create(event_capacity=1)
        runtime.start()
        self.until(lambda: runtime.snapshot().state == RuntimeState.RUNNING)
        runtime.stop()
        self.assertTrue(runtime.wait_stopped(3))
        self.assertGreater(runtime.snapshot().dropped_events, 0)
        self.assertEqual(len(runtime.drain_events()), 1)

    def test_factory_failure_completes_without_a_controller(self):
        def broken(*args, **kwargs):
            raise ValueError("Invalid interface")

        runtime = NodeRuntime(self.config, controller_factory=broken)
        self.runtimes.append(runtime)
        runtime.start()
        self.assertTrue(runtime.wait_stopped(3))
        self.assertEqual(runtime.snapshot().state, RuntimeState.FAILED)
        self.assertIn("Invalid interface", runtime.snapshot().last_error)


if __name__ == "__main__":
    unittest.main()
