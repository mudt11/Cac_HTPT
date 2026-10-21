"""Monitoring flow tests with simulated time/messages and mocked TCP only."""

import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from BerkeleyTimeSync.controllers.cluster_controller import ClusterController
from BerkeleyTimeSync.controllers.node_controller import NodeController, NoticeKind
from BerkeleyTimeSync.core.election import ElectionSnapshot, ElectionState
from BerkeleyTimeSync.core.models import LeaderIdentity, NodeIdentity, NodeRole, PeerInfo, PeerStatus, SyncRoundState
from BerkeleyTimeSync.core.protocol import Message, MessageType, ProtocolError, parse_packet, serialize_packet
from BerkeleyTimeSync.network.heartbeat import HeartbeatPeer, HeartbeatSnapshot
from BerkeleyTimeSync.network.tcp_transport import EventType
from BerkeleyTimeSync.tests.test_sync import Cluster, Timeline


class ClusterControllerTests(unittest.TestCase):
    def setUp(self):
        guard = patch("socket.socket", side_effect=AssertionError("Monitoring tests must not open sockets"))
        guard.start()
        self.addCleanup(guard.stop)
        self.sim = Cluster()
        self.features = {i: ClusterController(identity, "Node {}".format(i), "10.0.0.{}".format(i),
            monotonic_source=self.sim.time) for i, identity in self.sim.identities.items()}
        for i, feature in self.features.items():
            feature.leadership_changed(self.election(i))

    def election(self, i, leader=None):
        leader = leader or self.sim.leader
        role = NodeRole.MASTER if self.sim.identities[i] == leader.node else NodeRole.WORKER
        return ElectionSnapshot(ElectionState.LEADER if role == NodeRole.MASTER else ElectionState.FOLLOWER,
                                role, leader, None, None, (), ())

    def heartbeat(self):
        now = self.sim.time()
        peers = tuple(HeartbeatPeer(identity, NodeRole.WORKER, PeerStatus.ONLINE,
            1000000000, now, now + 6000000000, 4.0)
            for i, identity in self.sim.identities.items() if i != self.sim.master)
        return HeartbeatSnapshot(self.sim.leader, peers, False)

    def publish(self, *, heartbeat=True):
        master = self.sim.master
        return self.features[master].tick(peers=self.sim.registries[master].snapshot(),
            heartbeat=self.heartbeat() if heartbeat else None,
            synchronization=self.sim.nodes[master].snapshot(), clock=self.sim.clocks[master].snapshot())

    def for_worker(self, result, i=1):
        return next(message for message in result.outbound if message.target_node_id == i)

    def receive(self, message, i=1, stamp=None):
        # Exercise serialization as well as the receiving controller boundary.
        return self.features[i].handle_tcp(parse_packet(serialize_packet(message)),
            received_monotonic_ns=self.sim.time() if stamp is None else stamp)

    def modified(self, message, **fields):
        from BerkeleyTimeSync.core.cluster_status import ClusterStatus
        status = replace(ClusterStatus.from_payload(message.payload), **fields)
        return replace(message, payload=status.to_payload(), message_id=uuid4())

    def node_controller(self, i):
        node = NodeController(self.sim.identities[i], "Node {}".format(i), "10.0.0.{}".format(i),
            clock=self.sim.clocks[i], monotonic_source=self.sim.time, auto_sync=False)
        for j, identity in self.sim.identities.items():
            if j != i:
                node._registry.observe(PeerInfo(identity, "Node {}".format(j), "10.0.0.{}".format(j)))
        election = self.election(i)
        node.leadership.snapshot = lambda: election
        node.leadership.heartbeat_snapshot = lambda: None
        node.cluster.leadership_changed(election)
        node.sync.leadership_changed(election)
        node._running = True
        return node

    def test_initial_snapshot_has_six_known_nodes_but_no_fake_clock_samples(self):
        result = self.publish()
        self.assertEqual(len(result.outbound), 5)
        self.assertTrue(result.accepted)
        for i in range(1, 6):
            self.assertTrue(self.receive(self.for_worker(result, i), i).accepted)
            snapshot = self.features[i].snapshot()
            self.assertEqual(len(snapshot.status.nodes), 6)
            self.assertTrue(all(row.sample is None for row in snapshot.status.nodes))
            self.assertIsNone(snapshot.status.round)
            self.assertFalse(snapshot.stale)

    def test_publication_is_limited_to_one_batch_per_second(self):
        first = self.publish()
        self.assertTrue(first.accepted)
        for _ in range(9):
            self.sim.time.advance(100)
            self.assertFalse(self.publish().outbound)
        self.sim.time.advance(100)
        second = self.publish()
        self.assertEqual(self.for_worker(second).payload["sequence"], 2)

    def test_worker_never_publishes_a_master_snapshot(self):
        result = self.features[1].tick(peers=(), heartbeat=None,
            synchronization=self.sim.nodes[1].snapshot(), clock=self.sim.clocks[1].snapshot())
        self.assertFalse(result.outbound)
        self.assertIsNone(self.features[1].snapshot().status)

    def test_measured_offsets_and_adjustments_match_the_real_sync_plan(self):
        self.sim.pump(6, self.sim.start())
        result = self.publish()
        self.assertTrue(self.receive(self.for_worker(result)).accepted)
        status = self.features[1].snapshot().status
        plan = self.sim.nodes[6].snapshot().plan
        rows = {row.identity.node_id: row for row in status.nodes}
        self.assertEqual(rows[1].sample.offset_ms, 5000)
        self.assertEqual(rows[2].sample.offset_ms, -3000)
        self.assertEqual(rows[1].adjustment.correction_ms, -4500)
        self.assertEqual(rows[6].adjustment.correction_ms, plan.master_correction_ms)
        self.assertEqual(rows[1].adjustment.ack_status, "ACCEPTED")
        self.assertEqual(status.round.state, SyncRoundState.COMPLETED)
        self.assertEqual(rows[6].sample.offset_ms, 0)

    def test_applied_is_updated_without_rewriting_historical_measurements(self):
        self.sim.pump(6, self.sim.start())
        first = self.publish()
        original = self.for_worker(first).payload["nodes"][0]["sample"]
        self.sim.time.advance(60000)
        for i in range(1, 6):
            self.sim.pump(i, self.sim.nodes[i].tick())
        result = self.publish()
        self.receive(self.for_worker(result))
        status = self.features[1].snapshot().status
        row = next(row for row in status.nodes if row.identity.node_id == 1)
        self.assertEqual(row.adjustment.ack_status, "APPLIED")
        self.assertEqual(row.sample.clock_time_ms, original["clock_time_ms"])
        self.assertEqual(row.sample.offset_ms, original["offset_ms"])
        self.assertGreaterEqual(row.sample.age_ms, 60000)
        self.assertEqual(status.master_remaining_ms, 0)

    def test_previous_samples_survive_new_collection_with_explicit_round_labels(self):
        self.sim.pump(6, self.sim.start())
        self.publish()
        self.sim.time.advance(1000)
        self.sim.start()
        result = self.publish()
        self.receive(self.for_worker(result))
        status = self.features[1].snapshot().status
        self.assertEqual(status.round.round_id, 2)
        self.assertEqual(status.round.state, SyncRoundState.COLLECTING)
        self.assertTrue(all(row.sample.round_id == 1 for row in status.nodes))
        self.assertTrue(all(row.sample.age_ms >= 1000 for row in status.nodes))

    def test_duplicate_or_older_sequence_does_not_refresh_age(self):
        first = self.for_worker(self.publish())
        self.receive(first)
        received = self.features[1].snapshot().received_monotonic_ns
        self.sim.time.advance(1000)
        self.assertFalse(self.receive(first).accepted)
        self.assertEqual(self.features[1].snapshot().received_monotonic_ns, received)
        self.assertEqual(self.features[1].snapshot().age_ms, 1000)
        second = self.for_worker(self.publish())
        self.assertTrue(self.receive(second).accepted)
        self.assertFalse(self.receive(first).accepted)
        self.assertEqual(self.features[1].snapshot().status.sequence, 2)

    def test_higher_sequence_cannot_regress_time(self):
        self.publish()
        self.sim.time.advance(1000)
        second = self.for_worker(self.publish())
        self.receive(second)
        bad = self.modified(second, sequence=3, elapsed_ms=0)
        self.assertFalse(self.receive(bad).accepted)
        self.assertEqual(self.features[1].snapshot().status.sequence, 2)
        self.sim.time.advance(1000)
        self.assertTrue(self.receive(self.for_worker(self.publish())).accepted)

    def test_higher_sequence_cannot_regress_round(self):
        self.sim.pump(6, self.sim.start())
        first = self.for_worker(self.publish())
        self.sim.time.advance(1000)
        self.sim.start()
        second = self.for_worker(self.publish())
        self.receive(second)
        from BerkeleyTimeSync.core.cluster_status import ClusterStatus
        old_round = ClusterStatus.from_payload(first.payload).round
        current = ClusterStatus.from_payload(second.payload)
        bad = self.modified(second, sequence=3, round=old_round)
        self.assertFalse(self.receive(bad).accepted)
        self.assertEqual(self.features[1].snapshot().status, current)

    def test_old_master_is_rejected_and_snapshot_cleared_after_handover(self):
        old = self.for_worker(self.publish())
        self.receive(old)
        leader = LeaderIdentity(self.sim.identities[5])
        self.sim.set_leader(leader)
        for i, feature in self.features.items():
            feature.leadership_changed(self.election(i))
        self.assertIsNone(self.features[1].snapshot().status)
        self.assertFalse(self.receive(old).accepted)
        self.assertTrue(self.receive(self.for_worker(self.publish())).accepted)
        self.assertEqual(self.features[1].snapshot().leader, leader)

    def test_watermark_survives_temporary_loss_of_leader(self):
        message = self.for_worker(self.publish())
        self.receive(message)
        self.features[1].leadership_changed(SimpleNamespace(leader=None, role=NodeRole.CANDIDATE))
        self.assertIsNone(self.features[1].snapshot().status)
        self.features[1].leadership_changed(self.election(1))
        self.assertFalse(self.receive(message).accepted)
        self.sim.time.advance(1000)
        self.assertTrue(self.receive(self.for_worker(self.publish())).accepted)

    def test_sender_sequence_and_elapsed_survive_reconfirmation_of_same_tenure(self):
        self.publish()
        self.sim.time.advance(1000)
        self.publish()
        self.features[6].leadership_changed(SimpleNamespace(leader=None, role=NodeRole.CANDIDATE))
        self.features[6].leadership_changed(self.election(6))
        result = self.publish()
        self.assertEqual(self.for_worker(result).payload["sequence"], 3)
        self.assertGreaterEqual(self.for_worker(result).payload["elapsed_ms"], 1000)

    def test_wrong_recipient_and_restarted_recipient_do_not_accept_old_view(self):
        message = self.for_worker(self.publish())
        self.assertFalse(self.receive(message, i=2).accepted)
        restarted = ClusterController(NodeIdentity(1), "Restart", "10.0.0.1", monotonic_source=self.sim.time)
        restarted.leadership_changed(self.election(1))
        self.assertFalse(restarted.handle_tcp(message, received_monotonic_ns=self.sim.time()).accepted)

    def test_stale_event_and_future_receipt_are_not_published(self):
        message = self.for_worker(self.publish())
        stamp = self.sim.time()
        self.sim.time.advance(1001)
        self.assertFalse(self.receive(message, stamp=stamp).accepted)
        self.assertIsNone(self.features[1].snapshot().status)
        with self.assertRaises(ProtocolError):
            self.receive(message, stamp=self.sim.time() + 1)

    def test_remote_elapsed_is_not_compared_to_local_absolute_monotonic_time(self):
        message = self.for_worker(self.publish())
        local = Timeline()
        local.advance(500000)
        receiver = ClusterController(self.sim.identities[1], "Worker", "10.0.0.1", monotonic_source=local)
        receiver.leadership_changed(self.election(1))
        self.assertTrue(receiver.handle_tcp(message, received_monotonic_ns=local()).accepted)
        local.advance(4000)
        snapshot = receiver.snapshot()
        self.assertTrue(snapshot.stale)
        self.assertEqual(snapshot.age_ms, 4000)
        self.assertEqual(snapshot.status.sequence, 1)

    def test_discovery_alone_does_not_claim_heartbeat_online_or_rtt(self):
        self.publish(heartbeat=False)
        rows = self.features[6].snapshot().status.nodes
        self.assertTrue(all(row.status == PeerStatus.SUSPECT and row.heartbeat_rtt_ms is None
                            for row in rows if row.identity != self.sim.identities[6]))

    def test_new_boot_does_not_inherit_old_measurement_or_adjustment(self):
        self.sim.pump(6, self.sim.start())
        self.publish()
        self.sim.time.advance(8000)
        identity = NodeIdentity(1)
        self.sim.registries[6].replace_offline(self.sim.identities[1], PeerInfo(identity, "Restart", "10.0.0.1"))
        self.publish(heartbeat=False)
        row = next(row for row in self.features[6].snapshot().status.nodes if row.identity.node_id == 1)
        self.assertEqual(row.identity, identity)
        self.assertIsNone(row.sample)
        self.assertIsNone(row.adjustment)

    def test_clear_removes_snapshot_and_prevents_receiving_while_stopped(self):
        message = self.for_worker(self.publish())
        self.receive(message)
        self.features[1].clear()
        self.assertIsNone(self.features[1].snapshot().status)
        self.assertFalse(self.receive(message).accepted)

    def test_schema_error_in_publication_is_nonfatal_to_node(self):
        node = self.node_controller(6)
        with patch.object(node.cluster, "tick", side_effect=ValueError("Simulated bad monitoring")):
            node._cluster_tick()
        self.assertTrue(node._running)
        self.assertTrue(any(notice.kind == NoticeKind.CLUSTER for notice in node.drain_notices()))

    def test_regular_poll_publishes_monitoring_without_changing_sync_or_clock(self):
        node = self.node_controller(6)
        node.membership.tick = Mock()
        node.leadership.tick = lambda **kwargs: SimpleNamespace(
            heartbeat=SimpleNamespace(failures=(), changes=()), updates=())
        before = node.clock.snapshot()
        snapshot = node.poll()
        self.assertTrue(snapshot.running)
        self.assertEqual(snapshot.cluster.status.sequence, 1)
        self.assertEqual(len(node._cluster_jobs), 5)
        self.assertIsNone(snapshot.synchronization.session)
        self.assertEqual(node.clock.snapshot(), before)

    def test_stop_discards_pending_monitoring_and_clears_published_view(self):
        node = self.node_controller(6)
        node._cluster_tick()
        self.assertEqual(len(node._cluster_jobs), 5)
        snapshot = node.stop()
        self.assertFalse(snapshot.running)
        self.assertFalse(node._cluster_jobs)
        self.assertIsNone(snapshot.cluster.status)

    def test_node_routes_valid_status_after_membership_checks_and_publishes_snapshot(self):
        message = self.for_worker(self.publish())
        node = self.node_controller(1)
        node.sync.handle_tcp = Mock(side_effect=AssertionError("Monitoring must not enter sync handler"))
        connection = SimpleNamespace(peer=self.sim.identities[6], address=("10.0.0.6", 54321),
                                     incoming=True, connection_id=uuid4())
        event = SimpleNamespace(type=EventType.MESSAGE, connection=connection, message=message,
                                monotonic_ns=self.sim.time())
        node._tcp_event(event)
        node._publish()
        self.assertEqual(node.snapshot().cluster.status.sequence, 1)
        node.sync.handle_tcp.assert_not_called()
        node._tcp = Mock(address=None)
        event.connection = SimpleNamespace(peer=self.sim.identities[6], address=("10.0.0.99", 54321),
                                            incoming=True, connection_id=uuid4())
        node._tcp_event(event)
        node._tcp.disconnect.assert_called_once_with(event.connection.connection_id)

    def test_latest_monitoring_jobs_are_bounded_and_control_queue_has_priority(self):
        node = self.node_controller(6)
        node._cluster_tick()
        self.sim.time.advance(1000)
        node._cluster_tick()
        self.assertEqual(len(node._cluster_jobs), 5)
        self.assertTrue(all(job.message.payload["sequence"] == 2 for job in node._cluster_jobs.values()))
        self.assertTrue(all(queue.qsize() == 0 for queue in node._queues.values()))
        control = Message(MessageType.COORDINATOR, self.sim.identities[6], {"election_id": str(uuid4())},
                          target_node_id=1, leader=self.sim.leader)
        node._enqueue(control, self.sim.time() + 1000000000)
        sent = []

        def send(connection_id, message):
            sent.append(message)
            if len(sent) == 2:
                node._stop_event.set()

        node._tcp = Mock()
        node._tcp.connections.return_value = (SimpleNamespace(peer=self.sim.identities[1],
            address=("10.0.0.1", 9999), incoming=False, connection_id=uuid4()),)
        node._tcp.send.side_effect = send
        node._dispatch_loop(1)
        self.assertEqual([message.type for message in sent], [MessageType.COORDINATOR, MessageType.CLUSTER_STATUS])
        self.assertEqual(sent[1].payload["sequence"], 2)
        self.assertEqual(node._queues[1].unfinished_tasks, 0)
        node._tcp.connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
