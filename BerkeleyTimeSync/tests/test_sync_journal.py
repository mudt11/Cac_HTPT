"""Journal regressions using real synchronization with simulated time only."""

import unittest
from dataclasses import replace
from unittest.mock import patch

from BerkeleyTimeSync.controllers.node_controller import NodeController, NoticeKind
from BerkeleyTimeSync.controllers.sync_controller import SyncResult
from BerkeleyTimeSync.controllers.sync_journal import SyncJournal
from BerkeleyTimeSync.core.models import LeaderIdentity
from BerkeleyTimeSync.core.protocol import MessageType
from BerkeleyTimeSync.tests.test_sync import Cluster


class SyncJournalTests(unittest.TestCase):
    def setUp(self):
        guard = patch("socket.socket", side_effect=AssertionError("No real sockets in journal tests"))
        guard.start()
        self.addCleanup(guard.stop)
        self.sim = Cluster()
        self.journal = SyncJournal()

    def snapshot(self):
        return self.sim.nodes[self.sim.master].snapshot()

    def complete(self):
        self.sim.pump(self.sim.master, self.sim.start())
        return self.snapshot()

    def test_start_identifies_round_tenure_and_process_members(self):
        self.sim.start()
        text = "\n".join(self.journal.ingest(self.snapshot()))
        self.assertIn("Master 6", text)
        self.assertIn(str(self.sim.leader.leader_epoch), text)
        self.assertIn("phiên #1", text)
        self.assertIn(str(self.sim.identities[1].boot_id), text)
        self.assertIn("COLLECTING", text)
        self.assertNotIn("Thiếu báo giờ", text)

    def test_complete_records_measurements_plan_commands_and_acks(self):
        text = "\n".join(self.journal.ingest(self.complete()))
        for value in ("báo giờ", "RTT", "Delta", "KEPT", "Delta trung bình",
                      "hiệu chỉnh Master", "Lập lệnh Node 1", "ACK ACCEPTED", "COMPLETED"):
            self.assertIn(value, text)
        self.assertIn("không phải phép đo hội tụ", text)

    def test_unchanged_snapshot_and_clock_progress_do_not_repeat(self):
        self.journal.ingest(self.complete())
        self.assertEqual(self.journal.ingest(self.snapshot()), ())
        self.sim.time.advance(10)
        self.assertEqual(self.journal.ingest(self.snapshot()), ())

    def test_missing_report_failure_includes_reason_and_missing_nodes_once(self):
        self.sim.start()
        self.journal.ingest(self.snapshot())
        self.sim.time.advance(2001)
        self.sim.nodes[6].tick()
        text = "\n".join(self.journal.ingest(self.snapshot()))
        self.assertIn("FAILED", text)
        self.assertIn("Lý do:", text)
        self.assertIn("Thiếu báo giờ: Node 1, 2, 3, 4, 5", text)
        self.assertEqual(self.journal.ingest(self.snapshot()), ())

    def test_missing_ack_is_distinct_from_rejection(self):
        self.sim.pump(6, self.sim.start(), drop=lambda sender, action: action.type == MessageType.TIME_ACK)
        self.journal.ingest(self.snapshot())
        self.sim.time.advance(2001)
        self.sim.nodes[6].tick()
        text = "\n".join(self.journal.ingest(self.snapshot()))
        self.assertIn("Thiếu ACK:", text)
        self.assertNotIn("REJECTED", text)

    def test_actual_rejection_retains_reason(self):
        state = self.complete()
        ack = state.session.acknowledgments[0]
        rejected = replace(ack, payload={**dict(ack.payload), "status": "REJECTED", "reason": "Clock paused"})
        state = replace(state, session=replace(state.session, acknowledgments=(rejected,)))
        text = "\n".join(self.journal.ingest(state))
        self.assertIn("ACK REJECTED • Lý do: Clock paused", text)

    def test_applied_update_emits_only_new_completion(self):
        state = self.complete()
        self.journal.ingest(state)
        updated = replace(state, applied_worker_ids=(1,))
        lines = self.journal.ingest(updated)
        self.assertEqual(len(lines), 1)
        self.assertIn("Node 1 • ACK APPLIED", lines[0])
        self.assertEqual(self.journal.ingest(updated), ())

    def test_worker_state_and_correction_are_logged_locally(self):
        self.complete()
        worker = self.sim.nodes[1]
        text = "\n".join(self.journal.ingest(worker.snapshot()))
        self.assertIn("Worker cục bộ: SLEWING", text)
        self.assertIn("hiệu chỉnh", text)
        self.sim.time.advance(300000)
        worker.tick()
        lines = self.journal.ingest(worker.snapshot())
        self.assertEqual(len(lines), 1)
        self.assertIn("APPLIED", lines[0])

    def test_outlier_reason_is_logged_with_its_recovery_command(self):
        self.sim = Cluster({1: 60000, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0})
        text = "\n".join(self.journal.ingest(self.complete()))
        self.assertIn("Node 1 • Delta", text)
        self.assertIn("OUTSIDE_CLUSTER", text)
        self.assertIn("Lập lệnh Node 1 • hiệu chỉnh -60000 ms", text)

    def test_master_slew_completion_does_not_reemit_old_measurements(self):
        self.journal.ingest(self.complete())
        self.sim.time.advance(300000)
        self.sim.nodes[6].tick()
        lines = self.journal.ingest(self.snapshot())
        self.assertEqual(len(lines), 1)
        self.assertIn("Đồng hồ Master: NORMAL", lines[0])
        self.assertEqual(self.journal.ingest(self.snapshot()), ())

    def test_new_tenure_round_one_is_not_suppressed(self):
        self.journal.ingest(self.complete())
        self.sim.set_leader(LeaderIdentity(self.sim.identities[6]))
        self.sim.start()
        text = "\n".join(self.journal.ingest(self.snapshot()))
        self.assertIn(str(self.sim.leader.leader_epoch), text)
        self.assertIn("COLLECTING", text)

    def test_new_round_replaces_fingerprints_instead_of_growing_history(self):
        state = self.complete()
        for number in range(1, 201):
            session = replace(state.session, round_key=replace(state.session.round_key, round_id=number),
                              reports=(), adjustments=(), acknowledgments=())
            self.journal.ingest(replace(state, session=session, plan=None))
        self.assertEqual(len(self.journal._marks["master"]), 2)

    def test_cancellation_is_logged_with_reason(self):
        self.sim.start()
        self.journal.ingest(self.snapshot())
        self.sim.nodes[6].cancel("Operator cancelled")
        text = "\n".join(self.journal.ingest(self.snapshot()))
        self.assertIn("CANCELLED", text)
        self.assertIn("Operator cancelled", text)

    def test_owner_integration_records_silent_timer_transition_once(self):
        node = NodeController(self.sim.identities[6], "Master", "10.0.0.6",
                              clock=self.sim.clocks[6], monotonic_source=self.sim.time, auto_sync=False)
        node.sync = self.sim.nodes[6]
        self.sim.start()
        node._sync_result(SyncResult("Started"), quiet=True)
        self.assertTrue(node.drain_notices())
        self.sim.time.advance(2001)
        node._sync_result(node.sync.tick(), quiet=True)
        notices = node.drain_notices()
        self.assertTrue(any(n.kind == NoticeKind.SYNC and "Trạng thái FAILED" in n.detail for n in notices))
        node._sync_result(SyncResult("Timers checked"), quiet=True)
        self.assertFalse(node.drain_notices())

    def test_publish_records_state_changed_by_dispatch_preparation(self):
        node = NodeController(self.sim.identities[6], "Master", "10.0.0.6",
                              clock=self.sim.clocks[6], monotonic_source=self.sim.time, auto_sync=False)
        node.sync = self.sim.nodes[6]
        self.complete()
        node._publish()
        self.assertTrue(any("Delta trung bình" in n.detail for n in node.drain_notices()))
        node._publish()
        self.assertFalse(node.drain_notices())

    def test_none_snapshot_is_safe(self):
        self.assertEqual(self.journal.ingest(None), ())


if __name__ == "__main__":
    unittest.main()
