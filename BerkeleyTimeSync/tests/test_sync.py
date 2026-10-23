"""Deterministic synchronization integration without TCP/UDP or wall-time sleeps.

The fixture delivers real protocol messages between real SyncControllers using
a simulated monotonic timeline. It does not test LAN discovery or election.
"""

import unittest
from collections import deque
from dataclasses import replace
from unittest.mock import patch

from BerkeleyTimeSync.controllers.sync_controller import SyncController, SyncStateError, WorkerState
from BerkeleyTimeSync.core.clock import VirtualClock
from BerkeleyTimeSync.core.election import ElectionSnapshot, ElectionState
from BerkeleyTimeSync.core.models import ClockState, LeaderIdentity, NodeIdentity, NodeRole, PeerInfo, SyncRoundState
from BerkeleyTimeSync.core.outlier_filter import RejectionReason
from BerkeleyTimeSync.core.peer_registry import PeerRegistry
from BerkeleyTimeSync.core.protocol import MAX_INTEGER, MessageType, ProtocolError


class Timeline:
    def __init__(self):
        self.ns = 1000000000

    def __call__(self):
        return self.ns

    def advance(self, milliseconds):
        self.ns += milliseconds * 1000000


class Cluster:
    def __init__(self, offsets=None):
        self.time = Timeline()
        offsets = offsets or {1: 5000, 2: -3000, 3: 2000, 4: 0, 5: -1000, 6: 0}
        self.identities = {i: NodeIdentity(i) for i in offsets}
        self.leader = LeaderIdentity(self.identities[max(offsets)])
        self.clocks, self.registries, self.nodes = {}, {}, {}
        for i, identity in self.identities.items():
            clock = VirtualClock(initial_time_ms=86399000 + offsets[i], monotonic_source=self.time)
            registry = PeerRegistry(identity, monotonic_source=self.time)
            for other, peer in self.identities.items():
                if other != i:
                    registry.observe(PeerInfo(peer, "Node {}".format(other), "10.0.0.{}".format(other)))
            self.clocks[i], self.registries[i] = clock, registry
            self.nodes[i] = SyncController(registry, clock, monotonic_source=self.time)
        self.set_leader(self.leader)

    @property
    def master(self):
        return self.leader.node.node_id

    def set_leader(self, leader):
        self.leader = leader
        for i, node in self.nodes.items():
            role = NodeRole.MASTER if self.identities[i] == leader.node else NodeRole.WORKER
            node.leadership_changed(ElectionSnapshot(
                ElectionState.LEADER if role == NodeRole.MASTER else ElectionState.FOLLOWER,
                role, leader, None, None, (), ()))

    def start(self):
        return self.nodes[self.master].start_round(tuple(self.identities.values()))

    def deliver(self, sender, action, *, delay_ms=2):
        message = self.nodes[sender].prepare_send(action)
        target = message.target_node_id
        self.time.advance(delay_ms)
        result = self.nodes[target].handle_tcp(
            message, received_monotonic_ns=self.time(), clock_time_ms=self.clocks[target].now_ms())
        return message, result

    def pump(self, sender, result, *, drop=None, latency=None):
        queue = deque((sender, action) for action in result.outbound)
        delivered = []
        for _ in range(200):
            if not queue:
                computed = self.nodes[self.master].tick()
                queue.extend((self.master, a) for a in computed.outbound)
                if not queue:
                    return delivered
            source, action = queue.popleft()
            if drop and drop(source, action):
                continue
            delay = latency(source, action) if latency else 2
            message, response = self.deliver(source, action, delay_ms=delay)
            delivered.append(message)
            queue.extend((message.target_node_id, a) for a in response.outbound)
        raise AssertionError("Simulation did not quiesce")


class SynchronizationTests(unittest.TestCase):
    def setUp(self):
        guard = patch("socket.socket", side_effect=AssertionError("Sync tests must not open sockets"))
        guard.start()
        self.addCleanup(guard.stop)

    def test_six_nodes_step_and_remeasure_across_midnight(self):
        cluster = Cluster()
        cluster.time.advance(1000)
        cluster.pump(cluster.master, cluster.start())
        snapshot = cluster.nodes[6].snapshot()
        self.assertEqual(snapshot.session.state, SyncRoundState.COMPLETED)
        self.assertEqual(len(snapshot.session.acknowledgments), 5)
        self.assertTrue(all(a.payload["status"] == "APPLIED" for a in snapshot.session.acknowledgments))
        # Fixed RTT/symmetric paths give known offsets: average = +500 ms.
        self.assertEqual(snapshot.plan.master_correction_ms, 500)
        self.assertEqual({m.target_node_id: m.payload["correction_ms"] for m in snapshot.plan.adjustments},
                         {1: -4500, 2: 3500, 3: -1500, 4: 500, 5: 1500})
        readings = [clock.now_ms() for clock in cluster.clocks.values()]
        self.assertLessEqual(max(readings) - min(readings), 1)
        self.assertGreater(min(readings), 86400000)
        self.assertTrue(all(c.state == ClockState.NORMAL for c in cluster.clocks.values()))
        self.assertEqual(cluster.nodes[6].snapshot().applied_worker_ids, (1, 2, 3, 4, 5))
        cluster.pump(6, cluster.start())
        plan = cluster.nodes[6].snapshot().plan
        self.assertEqual(plan.master_correction_ms, 0)
        self.assertTrue(all(m.payload["correction_ms"] == 0 for m in plan.adjustments))

    def test_round_cannot_overlap(self):
        cluster = Cluster()
        cluster.start()
        with self.assertRaises(SyncStateError):
            cluster.start()

    def test_dispatch_timestamp_excludes_time_waiting_to_send(self):
        cluster = Cluster({1: 1000, 6: 0})
        action = cluster.start().outbound[0]
        cluster.time.advance(100)
        request, response = cluster.deliver(6, action)
        self.assertEqual(request.payload["master_time_ms"], 86399100)
        cluster.pump(1, response)
        sample = cluster.nodes[6].snapshot().session.reports[0]
        self.assertEqual(sample.rtt_ns, 4000000)

    def test_dispatch_token_is_one_shot(self):
        cluster = Cluster({1: 1000, 6: 0})
        action = cluster.start().outbound[0]
        cluster.nodes[6].prepare_send(action)
        with self.assertRaises(SyncStateError):
            cluster.nodes[6].prepare_send(action)

    def test_duplicate_adjustment_does_not_apply_step_twice(self):
        cluster = Cluster({1: 1000, 6: 0})
        messages = cluster.pump(6, cluster.start())
        adjustment = next(m for m in messages if m.type == MessageType.SYNC_ADJUST)
        cluster.time.advance(100)
        before = cluster.clocks[1].snapshot()
        response = cluster.nodes[1].handle_tcp(adjustment)
        after = cluster.clocks[1].snapshot()
        self.assertEqual(after.remaining_correction_ns, before.remaining_correction_ns)
        self.assertEqual(after.time_ms, before.time_ms)
        self.assertIn("not applied twice", response.detail)

    def test_missing_reports_fail_within_deadline(self):
        cluster = Cluster()
        cluster.start()
        cluster.time.advance(2000)
        cluster.nodes[6].tick()
        self.assertEqual(cluster.nodes[6].snapshot().session.state, SyncRoundState.FAILED)

    def test_missing_worker_allows_partial_round_without_hanging(self):
        cluster = Cluster()
        cluster.pump(6, cluster.start(), drop=lambda source, a: source == 2 and a.type == MessageType.TIME_REPORT)
        cluster.time.advance(2000)
        cluster.pump(6, cluster.nodes[6].tick())
        snapshot = cluster.nodes[6].snapshot().session
        self.assertEqual(snapshot.state, SyncRoundState.PARTIAL)
        self.assertEqual(snapshot.missing_report_ids, (2,))
        self.assertEqual(len(snapshot.acknowledgments), 4)

    def test_missing_ack_never_claims_completed(self):
        cluster = Cluster()
        cluster.pump(6, cluster.start(), drop=lambda source, a: source == 2 and a.type == MessageType.TIME_ACK)
        self.assertEqual(cluster.nodes[6].snapshot().session.state, SyncRoundState.WAITING_ACK)
        cluster.time.advance(2000)
        cluster.nodes[6].tick()
        session = cluster.nodes[6].snapshot().session
        self.assertEqual(session.state, SyncRoundState.PARTIAL)
        self.assertEqual(session.missing_ack_ids, (2,))

    def test_report_cannot_be_attached_to_another_round(self):
        cluster = Cluster({1: 1000, 6: 0})
        request, reply = cluster.deliver(6, cluster.start().outbound[0])
        report = cluster.nodes[1].prepare_send(reply.outbound[0])
        wrong = replace(report, round_id=request.round_id + 1)
        response = cluster.nodes[6].handle_tcp(wrong, clock_time_ms=cluster.clocks[6].now_ms())
        self.assertIn("unknown", response.detail)
        self.assertEqual(cluster.nodes[6].snapshot().session.reports, ())

    def test_wrong_report_correlation_is_rejected(self):
        cluster = Cluster({1: 1000, 6: 0})
        _, reply = cluster.deliver(6, cluster.start().outbound[0])
        report = cluster.nodes[1].prepare_send(reply.outbound[0])
        with self.assertRaises(ProtocolError):
            cluster.nodes[6].handle_tcp(replace(report, reply_to=report.message_id),
                                       clock_time_ms=cluster.clocks[6].now_ms())

    def test_high_rtt_is_excluded_from_averaging_and_adjustment(self):
        cluster = Cluster()
        replies = {}
        for action in cluster.start().outbound:
            request, result = cluster.deliver(6, action)
            worker_id = request.target_node_id
            replies[worker_id] = cluster.nodes[worker_id].prepare_send(result.outbound[0])
        # Deliver healthy replies before the independently delayed packet.
        # A serial 600 ms wait in pump() would also inflate every later RTT.
        for worker_id in (2, 3, 4, 5, 1):
            cluster.time.advance(600 if worker_id == 1 else 2)
            cluster.nodes[6].handle_tcp(replies[worker_id], received_monotonic_ns=cluster.time(),
                                        clock_time_ms=cluster.clocks[6].now_ms())
        cluster.pump(6, cluster.nodes[6].tick())
        snapshot = cluster.nodes[6].snapshot()
        rejected = {r.sample.identity.node_id: r.reason for r in snapshot.plan.filter_result.rejected}
        self.assertEqual(rejected[1], RejectionReason.HIGH_RTT)
        self.assertNotIn(1, {m.target_node_id for m in snapshot.plan.adjustments})
        self.assertEqual(snapshot.session.state, SyncRoundState.PARTIAL)

    def test_large_offset_contributes_to_mean(self):
        cluster = Cluster({1: 60000, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0})
        cluster.pump(6, cluster.start())
        plan = cluster.nodes[6].snapshot().plan
        self.assertEqual(plan.master_correction_ms, 10000)
        self.assertEqual(plan.filter_result.rejected, ())
        correction = next(m for m in plan.adjustments if m.target_node_id == 1)
        self.assertEqual(correction.payload["correction_ms"], -50000)

    def test_two_nodes_seven_minutes_apart_average_and_converge(self):
        cluster = Cluster({1: 447175, 2: 0})
        cluster.pump(2, cluster.start())
        snapshot = cluster.nodes[2].snapshot()
        self.assertEqual(snapshot.session.state, SyncRoundState.COMPLETED)
        self.assertEqual(snapshot.plan.filter_result.rejected, ())
        self.assertEqual(snapshot.plan.master_correction_ms, 223588)
        self.assertEqual(snapshot.plan.adjustments[0].payload["correction_ms"], -223588)
        self.assertTrue(all(c.state == ClockState.NORMAL for c in cluster.clocks.values()))
        self.assertEqual(snapshot.session.acknowledgments[0].payload["status"], "APPLIED")
        self.assertLessEqual(abs(cluster.clocks[1].now_ms() - cluster.clocks[2].now_ms()), 1)

    def test_worker_rejects_step_outside_protocol_range_without_changing_clock(self):
        for correction in (-MAX_INTEGER, MAX_INTEGER):
            with self.subTest(correction=correction):
                cluster = Cluster({1: 1000, 6: 0})
                cluster.pump(6, cluster.start(), drop=lambda source, a: a.type == MessageType.SYNC_ADJUST)
                command = cluster.nodes[6].snapshot().plan.adjustments[0]
                command = replace(command, payload={"correction_ms": correction})
                before = cluster.clocks[1].snapshot()
                response = cluster.nodes[1].handle_tcp(command)
                self.assertEqual(cluster.clocks[1].snapshot(), before)
                ack = cluster.nodes[1].prepare_send(response.outbound[0])
                self.assertEqual(ack.payload["status"], "REJECTED")

    def test_step_replaces_pending_slew_and_preserves_fractional_time(self):
        timeline = Timeline()
        clock = VirtualClock(initial_time_ms=10000, monotonic_source=timeline)
        clock.schedule_correction(1000)
        timeline.advance(5)
        clock.apply_correction(-100)
        self.assertEqual(clock.now_ms(), 9905)
        self.assertEqual(clock.state, ClockState.NORMAL)
        timeline.ns += 500000
        self.assertEqual(clock.now_ms(), 9906)
        timeline.advance(1000)
        self.assertEqual(clock.now_ms(), 10906)

    def test_step_zero_pause_resume_and_invalid_input(self):
        timeline = Timeline()
        clock = VirtualClock(initial_time_ms=10000, monotonic_source=timeline)
        clock.schedule_correction(-1000)
        clock.apply_correction(0)
        self.assertEqual(clock.snapshot().remaining_correction_ns, 0)
        clock.pause()
        clock.apply_correction(225000)
        timeline.advance(1000)
        self.assertTrue(clock.is_paused)
        self.assertEqual(clock.now_ms(), 235000)
        clock.apply_correction(-225000)
        clock.resume()
        timeline.advance(1)
        self.assertEqual(clock.now_ms(), 10001)
        clock.schedule_correction(1000)
        before = clock.snapshot()
        for value in (True, 1.5, None):
            with self.assertRaises(TypeError):
                clock.apply_correction(value)
            self.assertEqual(clock.snapshot(), before)
        with self.assertRaises(ValueError):
            clock.apply_correction(-10002)
        self.assertEqual(clock.snapshot(), before)

    def test_leadership_change_cancels_round_and_old_dispatch(self):
        cluster = Cluster()
        old_action = cluster.start().outbound[0]
        cluster.set_leader(LeaderIdentity(cluster.identities[5]))
        self.assertEqual(cluster.nodes[6].snapshot().session.state, SyncRoundState.CANCELLED)
        with self.assertRaises(SyncStateError):
            cluster.nodes[6].prepare_send(old_action)
        cluster.pump(5, cluster.start())
        self.assertEqual(cluster.nodes[5].snapshot().session.state, SyncRoundState.COMPLETED)

    def test_old_master_adjustment_is_rejected_after_handover(self):
        cluster = Cluster()
        messages = cluster.pump(6, cluster.start())
        old = next(m for m in messages if m.type == MessageType.SYNC_ADJUST and m.target_node_id == 1)
        cluster.set_leader(LeaderIdentity(cluster.identities[5]))
        before = cluster.clocks[1].snapshot()
        response = cluster.nodes[1].handle_tcp(old)
        self.assertIn("Rejected", response.detail)
        self.assertEqual(cluster.clocks[1].snapshot(), before)

    def test_negative_slew_keeps_time_advancing(self):
        timeline = Timeline()
        clock = VirtualClock(initial_time_ms=86400000, monotonic_source=timeline)
        clock.schedule_correction(-1000)
        previous = clock.now_ms()
        for _ in range(100):
            timeline.advance(100)
            current = clock.now_ms()
            self.assertGreater(current, previous)
            previous = current
        self.assertEqual(clock.now_ms(), 86409000)
        self.assertEqual(clock.state, ClockState.NORMAL)


if __name__ == "__main__":
    unittest.main()
