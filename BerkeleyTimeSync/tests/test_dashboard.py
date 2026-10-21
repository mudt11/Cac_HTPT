"""Dashboard status regressions without a Tk window or network sockets."""

import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from BerkeleyTimeSync.controllers.sync_controller import SyncSnapshot, WorkerSnapshot, WorkerState
from BerkeleyTimeSync.core.election import ElectionSnapshot, ElectionState
from BerkeleyTimeSync.core.models import LeaderIdentity, NodeIdentity, NodeRole, PeerInfo, SyncRoundState
from BerkeleyTimeSync.core.protocol import Message, MessageType
from BerkeleyTimeSync.core.sync_session import ReportSample, SessionSnapshot
from BerkeleyTimeSync.gui.node_table import NodeTable, card_text, project_rows
from BerkeleyTimeSync.network.heartbeat import HeartbeatPeer, HeartbeatSnapshot
from BerkeleyTimeSync.gui.node_window import synchronization_description
from BerkeleyTimeSync.controllers.cluster_controller import ClusterSnapshot
from BerkeleyTimeSync.core.cluster_status import ClockSample, AdjustmentStatus, NodeStatus, RoundStatus, ClusterStatus
from BerkeleyTimeSync.core.models import ClockState, PeerStatus
from BerkeleyTimeSync.gui.convergence_chart import ConvergenceHistory
from BerkeleyTimeSync.gui.node_window import monitoring_description


class DashboardTests(unittest.TestCase):
    def setUp(self):
        guard = patch("socket.socket", side_effect=AssertionError("Dashboard tests must not open sockets"))
        guard.start()
        self.addCleanup(guard.stop)
        self.master, self.worker, self.other = NodeIdentity(6), NodeIdentity(1), NodeIdentity(2)
        self.leader = LeaderIdentity(self.master)
        self.request = Message(MessageType.SYNC_REQUEST, self.master,
            {"master_time_ms": 1000, "timeout_ms": 6000, "participant_ids": [1, 2, 6]},
            target_node_id=1, leader=self.leader, round_id=1)
        self.report = Message(MessageType.TIME_REPORT, self.worker,
            {"received_time_ms": 2001, "sent_time_ms": 2001}, target_node_id=6,
            leader=self.leader, round_id=1, reply_to=self.request.message_id)
        self.adjustment = Message(MessageType.SYNC_ADJUST, self.master,
            {"correction_ms": -500}, target_node_id=1, leader=self.leader,
            round_id=1, reply_to=self.report.message_id)
        self.session = SessionSnapshot(self.request.round_key, (self.worker, self.other, self.master),
            SyncRoundState.WAITING_ACK, 1000000000, 3000000000, 5000000000, 7000000000,
            (self.request,), (ReportSample(self.request, self.report, 1000000000, 1002000000),),
            (self.adjustment,), (), None)
        self.sync = SyncSnapshot(True, self.leader, NodeRole.MASTER, session=self.session)
        self.node = SimpleNamespace(identity=self.master, running=True,
            peers=(PeerInfo(self.worker, "Worker", "10.0.0.1"), PeerInfo(self.other, "Other", "10.0.0.2")),
            heartbeat=None, synchronization=self.sync,
            election=ElectionSnapshot(ElectionState.LEADER, NodeRole.MASTER, self.leader, None, None, (), ()))

    def row(self, *, session=None, now_ns=2000000000, applied=()):
        self.node.synchronization = replace(self.sync, session=session or self.session,
                                            applied_worker_ids=applied)
        return project_rows(self.node, now_ns=now_ns)[0]

    def ack(self, status, reason=None):
        payload = {"status": status, "clock_state": "NORMAL" if status == "APPLIED" else "SLEWING",
                   "clock_time_ms": 2002}
        if reason:
            payload["reason"] = reason
        return Message(MessageType.TIME_ACK, self.worker, payload, target_node_id=6,
                       leader=self.leader, round_id=1, reply_to=self.adjustment.message_id)

    def test_active_round_waits_for_ack(self):
        self.assertEqual(self.row().ack, "CHỜ ACK")

    def test_partial_round_after_deadline_does_not_keep_waiting(self):
        session = replace(self.session, state=SyncRoundState.PARTIAL, reason="Missing ACK")
        self.assertEqual(self.row(session=session, now_ns=7000000000).ack, "HẾT HẠN ACK")

    def test_stale_waiting_snapshot_respects_known_deadline(self):
        self.assertEqual(self.row(now_ns=7000000000).ack, "HẾT HẠN ACK")

    def test_failure_before_deadline_does_not_claim_rejection_or_timeout(self):
        session = replace(self.session, state=SyncRoundState.FAILED, reason="Service failure")
        for now in (2000000000, 8000000000):
            with self.subTest(now=now):
                self.assertEqual(self.row(session=session, now_ns=now).ack, "KHÔNG CÓ ACK")

    def test_cancelled_round_does_not_claim_timeout(self):
        session = replace(self.session, state=SyncRoundState.CANCELLED, reason="Operator cancelled")
        self.assertEqual(self.row(session=session, now_ns=8000000000).ack, "ĐÃ HỦY")

    def test_accepted_is_not_promoted_to_applied_after_deadline(self):
        session = replace(self.session, state=SyncRoundState.PARTIAL, acknowledgments=(self.ack("ACCEPTED"),))
        self.assertEqual(self.row(session=session, now_ns=8000000000).ack, "ACCEPTED")

    def test_applied_notification_promotes_accepted(self):
        session = replace(self.session, acknowledgments=(self.ack("ACCEPTED"),))
        self.assertEqual(self.row(session=session, applied=(1,)).ack, "APPLIED")

    def test_rejection_stays_distinct_from_missing_ack(self):
        session = replace(self.session, state=SyncRoundState.FAILED,
                          acknowledgments=(self.ack("REJECTED", "Clock paused"),))
        self.assertEqual(self.row(session=session, now_ns=8000000000).ack, "REJECTED")

    def test_cancelled_accepted_command_is_not_shown_as_still_slewing(self):
        session = replace(self.session, state=SyncRoundState.CANCELLED,
                          acknowledgments=(self.ack("ACCEPTED"),))
        self.assertEqual(self.row(session=session).ack, "ĐÃ HỦY / ACCEPTED")

    def test_completed_adjustment_keeps_applied_when_session_cancelled_later(self):
        session = replace(self.session, state=SyncRoundState.CANCELLED,
                          acknowledgments=(self.ack("APPLIED"),))
        self.assertEqual(self.row(session=session).ack, "APPLIED")

    def test_missing_report_waits_only_while_collecting(self):
        session = replace(self.session, state=SyncRoundState.COLLECTING, reports=(), adjustments=())
        self.assertEqual(self.row(session=session).ack, "CHỜ BÁO GIỜ")
        session = replace(session, state=SyncRoundState.PARTIAL)
        self.assertEqual(self.row(session=session, now_ns=3000000000).ack, "THIẾU BÁO GIỜ")

    def test_sample_excluded_from_adjustments_does_not_wait_for_ack(self):
        session = replace(self.session, state=SyncRoundState.PARTIAL, adjustments=())
        self.assertEqual(self.row(session=session).ack, "KHÔNG HIỆU CHỈNH")

    def test_new_boot_does_not_inherit_old_ack(self):
        self.node.peers = (PeerInfo(NodeIdentity(1), "Restart", "10.0.0.1"),)
        self.assertEqual(self.row().ack, "KHÔNG THAM GIA")
        self.assertEqual(self.row().correction, "--")

    def test_no_session_retains_unknown_status(self):
        self.node.synchronization = replace(self.sync, session=None)
        self.assertEqual(project_rows(self.node)[0].ack, "--")

    def test_summary_reports_reason_and_both_missing_lists(self):
        session = replace(self.session, state=SyncRoundState.PARTIAL, reason="Worker reply deadline expired")
        progress, detail = synchronization_description(replace(self.sync, session=session), self.leader)
        self.assertIn("PARTIAL", progress)
        self.assertIn("Worker reply deadline expired", detail)
        self.assertIn("Thiếu báo giờ: Node 2", detail)
        self.assertIn("Thiếu ACK: Node 1", detail)

    def test_summary_includes_rejection_reason(self):
        session = replace(self.session, acknowledgments=(self.ack("REJECTED", "Clock paused"),))
        _, detail = synchronization_description(replace(self.sync, session=session), self.leader)
        self.assertIn("Node 1 từ chối: Clock paused", detail)
        self.assertNotIn("Thiếu ACK: Node 1", detail)

    def test_old_master_session_does_not_hide_current_worker_round(self):
        leader = LeaderIdentity(self.other)
        worker = WorkerSnapshot(replace(self.request.round_key, leader=leader), WorkerState.SLEWING,
                                9000000000, uuid4(), uuid4(), -500)
        sync = replace(self.sync, leader=leader, role=NodeRole.WORKER, worker=worker,
                       session=replace(self.session, state=SyncRoundState.CANCELLED))
        progress, detail = synchronization_description(sync, leader)
        self.assertIn("SLEWING", progress)
        self.assertNotIn("CANCELLED", progress)
        self.assertIn("cục bộ", detail)

    def test_old_tenure_is_explicit_and_missing_sync_is_safe(self):
        progress, detail = synchronization_description(self.sync, LeaderIdentity(self.master))
        self.assertIn("phiên cũ", progress)
        self.assertIn("Master trước", detail)
        self.assertEqual(synchronization_description(None, None)[0], "chưa có")

    def telemetry(self, *, offset=250.0, ack="ACCEPTED"):
        self.node.identity = self.worker
        self.node.election = replace(self.node.election, role=NodeRole.WORKER)
        self.node.peers = (PeerInfo(self.master, "Master", "10.0.0.6", role=NodeRole.MASTER),)
        worker = WorkerSnapshot(self.request.round_key, WorkerState.SLEWING,
                               9000000000, uuid4(), uuid4(), -500)
        self.node.synchronization = replace(self.sync, role=NodeRole.WORKER, session=None, worker=worker)
        sample = ClockSample(1, 123456, 500, 2.5, offset, "KEPT" if offset is not None else None)
        remote = NodeStatus(self.other, "Other", "10.0.0.2", NodeRole.WORKER,
                            PeerStatus.ONLINE, 3.5, 100, sample,
                            AdjustmentStatus(1, -100, ack, ClockState.SLEWING) if ack else AdjustmentStatus(1, -100))
        local = NodeStatus(self.worker, "Worker", "10.0.0.1", NodeRole.WORKER, PeerStatus.ONLINE,
                           sample=sample, adjustment=AdjustmentStatus(1, -100, "ACCEPTED", ClockState.SLEWING))
        master = NodeStatus(self.master, "Master", "10.0.0.6", NodeRole.MASTER, PeerStatus.ONLINE)
        phase = RoundStatus(1, SyncRoundState.WAITING_ACK, (self.worker, self.other, self.master),
                            missing_ack_ids=(2,) if not ack else ())
        status = ClusterStatus(1, 123999, 10000, (local, remote, master), phase)
        self.node.cluster = ClusterSnapshot(self.leader, status, 2000000000, 0, False)
        return status

    def test_worker_projects_unknown_remote_nodes_from_master(self):
        self.telemetry()
        row = project_rows(self.node, now_ns=3000000000)[1]
        self.assertEqual(row.name, "Other")
        self.assertEqual(row.contact, "MASTER/ONLINE")
        self.assertEqual(row.offset, "+250.00")
        self.assertEqual(row.sync_rtt, "2.50")
        self.assertEqual(row.sample, "#1 / ≈1.5s")
        self.assertEqual(row.ack, "ACCEPTED")

    def test_telemetry_keeps_actual_local_clock_and_slew(self):
        self.telemetry()
        clock = SimpleNamespace(time_ms=999999, state=ClockState.SLEWING)
        row = project_rows(self.node, clock, local_name="My Node", now_ns=2000000000)[0]
        from BerkeleyTimeSync.gui.node_table import clock_text
        self.assertEqual(row.clock, clock_text(999999))
        self.assertEqual(row.ack, "SLEWING")
        self.assertEqual(row.correction, "-500")
        self.assertEqual(row.name, "My Node")

    def test_stale_monitoring_is_labelled_without_fabricating_timeout(self):
        self.telemetry(ack=None)
        row = project_rows(self.node, now_ns=7000000000)[1]
        self.assertEqual(row.contact, "CŨ/ONLINE")
        self.assertIn("CŨ", row.sample)
        self.assertEqual(row.ack, "CHỜ ACK")
        self.assertIn("DỮ LIỆU CŨ", monitoring_description(self.node, 7000000000))

    def test_previous_master_telemetry_is_ignored(self):
        self.telemetry()
        self.node.election = replace(self.node.election, leader=LeaderIdentity(self.master))
        self.assertEqual(project_rows(self.node)[1].name, "Chưa phát hiện")
        self.assertIn("chưa có dữ liệu", monitoring_description(self.node))
        self.assertFalse(ConvergenceHistory().ingest(self.node))

    def test_restarted_peer_does_not_inherit_telemetry(self):
        self.telemetry()
        self.node.peers += (PeerInfo(NodeIdentity(2), "Restart", "10.0.0.2"),)
        row = project_rows(self.node)[1]
        self.assertEqual(row.name, "Restart")
        self.assertEqual(row.offset, "--")
        history = ConvergenceHistory()
        history.ingest(self.node)
        self.assertFalse(history.points[2])

    def test_chart_deduplicates_repeated_publications(self):
        status = self.telemetry()
        history = ConvergenceHistory()
        self.assertTrue(history.ingest(self.node))
        self.node.cluster = replace(self.node.cluster, status=replace(status, sequence=2))
        self.assertFalse(history.ingest(self.node))
        self.assertEqual(len(history.points[2]), 1)
        self.assertEqual(history.points[2][0].time_s, 9.5)

    def test_chart_accepts_offset_computed_after_initial_report(self):
        self.telemetry(offset=None)
        history = ConvergenceHistory()
        self.assertFalse(history.ingest(self.node))
        self.telemetry(offset=0.0)
        self.assertTrue(history.ingest(self.node))
        self.assertEqual(history.points[2][0].offset_ms, 0.0)

    def test_chart_marks_rejected_samples_and_clears_on_master_change(self):
        status = self.telemetry()
        remote = replace(status.nodes[1], sample=replace(status.nodes[1].sample, filter_reason="HIGH_RTT"))
        self.node.cluster = replace(self.node.cluster, status=replace(status, nodes=(status.nodes[0], remote, status.nodes[2])))
        history = ConvergenceHistory()
        history.ingest(self.node)
        self.assertTrue(history.points[2][0].rejected)
        self.node.election = replace(self.node.election, leader=LeaderIdentity(self.master))
        self.assertFalse(history.ingest(self.node))
        self.assertFalse(any(history.points.values()))

    def test_monitoring_summary_includes_global_failure_details(self):
        status = self.telemetry(ack=None)
        phase = replace(status.round, state=SyncRoundState.PARTIAL, reason="Missing ACK")
        self.node.cluster = replace(self.node.cluster, status=replace(status, round=phase))
        text = monitoring_description(self.node, 2000000000)
        self.assertIn("Master 6", text)
        self.assertIn("Worker ONLINE: 2", text)
        self.assertIn("PARTIAL", text)
        self.assertIn("Thiếu ACK: Node 2", text)
        self.assertIn("Missing ACK", text)
        self.assertEqual(project_rows(self.node, now_ns=2000000000)[1].ack, "HẾT HẠN ACK")

    def test_worker_card_contains_real_metrics_and_sample_provenance(self):
        self.telemetry()
        row = project_rows(self.node, now_ns=3000000000)[1]
        text = card_text(row)
        for expected in ("10.0.0.2", "MASTER/ONLINE", "Ping: 3.50 ms", "Delta: +250.00 ms",
                         "Chỉnh: -100 ms", "ACK: ACCEPTED", "Mẫu: #1 / ≈1.5s"):
            self.assertIn(expected, text)
        self.assertNotIn("Worker ONLINE:", text)

    def test_unknown_card_has_no_fabricated_measurements_or_connections(self):
        text = card_text(project_rows(None)[0])
        self.assertIn("CHƯA BIẾT", text)
        self.assertIn("Ping: -- ms", text)
        self.assertIn("Delta: -- ms", text)
        self.assertIn("Chỉnh: -- ms", text)
        self.assertIn("Mẫu: --", text)

    def test_master_card_counts_only_current_heartbeat_confirmed_workers(self):
        watches = (
            HeartbeatPeer(self.worker, NodeRole.WORKER, PeerStatus.ONLINE, 0, 1000000000, 7000000000, 2.0),
            HeartbeatPeer(self.other, NodeRole.WORKER, PeerStatus.OFFLINE, 0, 1000000000, 7000000000, 3.0),
            HeartbeatPeer(NodeIdentity(3), NodeRole.WORKER, PeerStatus.ONLINE, 0, 1000000000, 7000000000, 4.0))
        self.node.heartbeat = HeartbeatSnapshot(self.leader, watches, False)
        row = project_rows(self.node, now_ns=2000000000)[5]
        self.assertIn("Worker ONLINE: 1/5", card_text(row))
        self.node.peers = (PeerInfo(NodeIdentity(1), "Restart", "10.0.0.1"),)
        row = project_rows(self.node, now_ns=2000000000)[5]
        self.assertEqual(row.connected_workers, "0/5")

    def test_master_card_count_is_unknown_without_current_heartbeat(self):
        self.assertEqual(project_rows(self.node)[5].connected_workers, "--")
        self.node.heartbeat = HeartbeatSnapshot(LeaderIdentity(self.master), (), False)
        self.assertEqual(project_rows(self.node)[5].connected_workers, "--")
        self.node.heartbeat = HeartbeatSnapshot(self.leader, (), False)
        self.assertEqual(project_rows(self.node)[5].connected_workers, "0/5")
        self.node.running = False
        self.assertEqual(project_rows(self.node)[5].connected_workers, "--")

    def test_worker_view_master_count_is_sourced_and_stale_labelled(self):
        self.telemetry()
        row = project_rows(self.node, now_ns=2000000000)[5]
        self.assertIn("Worker ONLINE: 2/5 (Master)", card_text(row))
        row = project_rows(self.node, now_ns=7000000000)[5]
        self.assertIn("CŨ/ONLINE", card_text(row))
        self.assertIn("Worker ONLINE: 2/5 (CŨ)", card_text(row))
        self.assertIn("/ CŨ", card_text(project_rows(self.node, now_ns=7000000000)[1]))

    def test_new_master_does_not_inherit_old_connection_count(self):
        self.telemetry()
        self.node.election = replace(self.node.election, leader=LeaderIdentity(self.master))
        self.assertEqual(project_rows(self.node)[5].connected_workers, "--")

    def test_local_worker_card_preserves_actual_slew_and_clock(self):
        self.telemetry()
        clock = SimpleNamespace(time_ms=999999, state=ClockState.SLEWING)
        row = project_rows(self.node, clock, now_ns=2000000000)[0]
        from BerkeleyTimeSync.gui.node_table import clock_text
        text = card_text(row)
        self.assertIn(clock_text(999999), text)
        self.assertIn("ACK: SLEWING", text)
        self.assertIn("Chỉnh: -500 ms", text)

    def test_widget_update_renders_six_cards_and_master_badge_without_tk(self):
        self.telemetry()
        rows = project_rows(self.node, now_ns=2000000000)
        cards = {i: (Mock(), Mock()) for i in range(1, 7)}
        view = SimpleNamespace(_cards=cards, tree=Mock())
        NodeTable.update_rows(view, rows)
        for i, (_, label) in cards.items():
            self.assertEqual(label.configure.call_args.kwargs["text"], card_text(rows[i - 1]))
        self.assertEqual(cards[6][0].configure.call_args.kwargs["text"], "★ NODE 6")
        self.assertIn("Worker ONLINE: 2/5 (Master)", cards[6][1].configure.call_args.kwargs["text"])


if __name__ == "__main__":
    unittest.main()
