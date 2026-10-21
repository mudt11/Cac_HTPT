"""Monitoring schema and wire checks; no sockets, GUI or controller mutation."""

import json
import unittest
from dataclasses import FrozenInstanceError, replace
from uuid import uuid4

from BerkeleyTimeSync.core.cluster_status import (
    AdjustmentStatus, ClockSample, ClusterStatus, ClusterStatusError, NodeStatus, RoundStatus,
)
from BerkeleyTimeSync.core.models import (
    ClockState, LeaderIdentity, NodeIdentity, NodeRole, PeerStatus, SyncRoundState,
)
from BerkeleyTimeSync.core.protocol import (
    MAX_FRAME_BYTES, Message, MessageType, ProtocolError, Transport,
    parse_packet, serialize_packet, validate_context,
)


class ClusterStatusTests(unittest.TestCase):
    def setUp(self):
        self.master = NodeIdentity(6)
        self.worker = NodeIdentity(1)
        self.leader = LeaderIdentity(self.master)
        self.sample = ClockSample(1, 105000, 20, 4.5, 5000.25, "KEPT")
        self.adjustment = AdjustmentStatus(1, -2500, "ACCEPTED", ClockState.SLEWING)
        self.master_row = NodeStatus(self.master, "Master", "10.0.0.6", NodeRole.MASTER, PeerStatus.ONLINE,
                                     sample=ClockSample(1, 100000, 20, 0, 0, "KEPT"))
        self.worker_row = NodeStatus(self.worker, "Máy Đình Thạch", "10.0.0.1", NodeRole.WORKER,
            PeerStatus.ONLINE, 2.5, 10, self.sample, self.adjustment)
        self.round = RoundStatus(1, SyncRoundState.WAITING_ACK, (self.master, self.worker),
                                 missing_ack_ids=(1,), ack_remaining_ms=1900)
        self.status = ClusterStatus(1, 100020, 1000, (self.master_row, self.worker_row),
                                    self.round, ClockState.SLEWING, 2498.5)

    def message(self, status=None, **kwargs):
        options = dict(target_node_id=1, leader=self.leader)
        options.update(kwargs)
        return Message(MessageType.CLUSTER_STATUS, self.master,
                       (status or self.status).to_payload(), **options)

    def assert_bad_payload(self, data):
        with self.assertRaises(ClusterStatusError):
            ClusterStatus.from_payload(data)
        with self.assertRaises(ProtocolError):
            Message(MessageType.CLUSTER_STATUS, self.master, data, target_node_id=1, leader=self.leader)

    def test_roundtrip_preserves_fractional_measurements_and_unicode(self):
        message = self.message()
        wire = serialize_packet(message)
        self.assertLessEqual(len(wire) - 1, MAX_FRAME_BYTES)
        self.assertEqual(parse_packet(wire), message)
        restored = ClusterStatus.from_payload(parse_packet(wire).payload)
        self.assertEqual(restored, self.status)
        self.assertEqual(restored.nodes[1].sample.offset_ms, 5000.25)

    def test_status_is_tcp_only_and_requires_target(self):
        with self.assertRaises(ProtocolError):
            serialize_packet(self.message(), transport=Transport.UDP)
        with self.assertRaises(ProtocolError):
            self.message(target_node_id=None)
        with self.assertRaises(ProtocolError):
            self.message(target_node_id=6)

    def test_source_must_match_envelope_master(self):
        with self.assertRaises(ProtocolError):
            self.message(leader=LeaderIdentity(self.worker))
        with self.assertRaises(ProtocolError):
            self.message(leader=None)

    def test_rows_cannot_identify_another_master_incarnation(self):
        bad_master = replace(self.master_row, identity=NodeIdentity(6))
        status = replace(self.status, nodes=(bad_master, self.worker_row))
        with self.assertRaises(ProtocolError):
            self.message(status)

    def test_monitoring_is_not_a_sync_adjustment_or_response(self):
        message = self.message()
        self.assertIsNone(message.round_id)
        self.assertIsNone(message.round_key)
        self.assertIsNone(message.reply_to)
        with self.assertRaises(ProtocolError):
            self.message(round_id=1)

    def test_envelope_context_can_reject_old_leader_and_wrong_target(self):
        for context in ({"expected_leader": LeaderIdentity(self.master)}, {"recipient_id": 2}):
            with self.subTest(context=context), self.assertRaises(ProtocolError):
                validate_context(self.message(), **context)
        validate_context(self.message(), recipient_id=1, expected_leader=self.leader)

    def test_sequence_is_a_positive_integer(self):
        for value in (0, -1, True, 1.5, 1 << 53):
            data = self.status.to_payload()
            data["sequence"] = value
            with self.subTest(value=value):
                self.assert_bad_payload(data)

    def test_rejects_unknown_and_missing_fields_at_each_level(self):
        for path in ((), ("nodes", 1), ("nodes", 1, "identity"), ("nodes", 1, "sample"),
                     ("nodes", 1, "adjustment"), ("round",)):
            for mutation in ("add", "remove"):
                data = self.status.to_payload()
                obj = data
                for key in path:
                    obj = obj[key]
                if mutation == "add":
                    obj["unknown"] = 1
                else:
                    del obj[next(iter(obj))]
                with self.subTest(path=path, mutation=mutation):
                    self.assert_bad_payload(data)

    def test_node_array_is_bounded_and_ids_are_unique(self):
        for nodes in ([], [self.master_row.to_payload()] * 7,
                      [self.master_row.to_payload(), self.worker_row.to_payload(), self.worker_row.to_payload()]):
            data = self.status.to_payload()
            data["nodes"] = nodes
            self.assert_bad_payload(data)

    def test_different_boots_do_not_make_duplicate_node_ids_valid(self):
        restarted = replace(self.worker_row, identity=NodeIdentity(1))
        with self.assertRaises(ClusterStatusError):
            replace(self.status, nodes=(self.master_row, self.worker_row, restarted))

    def test_snapshot_requires_exactly_one_master(self):
        for nodes in ((self.worker_row,), (self.master_row, replace(self.worker_row, role=NodeRole.MASTER))):
            with self.subTest(nodes=nodes), self.assertRaises(ClusterStatusError):
                replace(self.status, nodes=nodes)

    def test_unknown_measurements_remain_null_before_first_round(self):
        master = replace(self.master_row, sample=None)
        status = ClusterStatus(1, 100000, 0, (master,))
        message = self.message(status)
        result = ClusterStatus.from_payload(parse_packet(serialize_packet(message)).payload)
        self.assertIsNone(result.round)
        self.assertIsNone(result.nodes[0].sample)
        self.assertIsNone(result.master_clock_state)
        self.assertIsNone(result.master_remaining_ms)

    def test_unknown_worker_can_be_present_without_fabricated_metrics(self):
        worker = replace(self.worker_row, heartbeat_rtt_ms=None, heartbeat_age_ms=None,
                         sample=None, adjustment=None, status=PeerStatus.SUSPECT)
        status = replace(self.status, nodes=(self.master_row, worker))
        restored = ClusterStatus.from_payload(self.message(status).payload)
        self.assertIsNone(restored.nodes[1].sample)
        self.assertIsNone(restored.nodes[1].heartbeat_rtt_ms)

    def test_nonfinite_boolean_and_oversized_measurements_are_rejected(self):
        for field in ("rtt_ms", "offset_ms"):
            for value in (float("nan"), float("inf"), True, 1 << 60):
                data = self.status.to_payload()
                data["nodes"][1]["sample"][field] = value
                with self.subTest(field=field, value=value):
                    self.assert_bad_payload(data)

    def test_rtt_and_sample_ages_are_nonnegative(self):
        for field in ("rtt_ms", "age_ms", "clock_time_ms"):
            data = self.status.to_payload()
            data["nodes"][1]["sample"][field] = -1
            self.assert_bad_payload(data)

    def test_samples_cannot_predate_tenure_or_claim_a_future_round(self):
        for field, value in (("age_ms", 1001), ("round_id", 2)):
            data = self.status.to_payload()
            data["nodes"][1]["sample"][field] = value
            self.assert_bad_payload(data)

    def test_current_round_sample_requires_exact_participant_identity(self):
        restarted = replace(self.worker_row, identity=NodeIdentity(1))
        with self.assertRaises(ProtocolError):
            self.message(replace(self.status, nodes=(self.master_row, restarted)))
        round_without_worker = replace(self.round, participants=(self.master, NodeIdentity(2)),
                                       missing_ack_ids=())
        with self.assertRaises(ProtocolError):
            self.message(replace(self.status, round=round_without_worker))

    def test_restarted_node_may_be_listed_without_inheriting_round_data(self):
        restarted = replace(self.worker_row, identity=NodeIdentity(1), sample=None, adjustment=None)
        message = self.message(replace(self.status, nodes=(self.master_row, restarted)))
        self.assertIsNone(ClusterStatus.from_payload(message.payload).nodes[1].sample)

    def test_retained_previous_round_measurement_is_labelled_separately(self):
        status = replace(self.status, round=replace(self.round, round_id=2, state=SyncRoundState.COLLECTING))
        restored = ClusterStatus.from_payload(self.message(status).payload)
        self.assertEqual(restored.round.round_id, 2)
        self.assertEqual(restored.nodes[1].sample.round_id, 1)

    def test_heartbeat_rtt_requires_age_and_age_must_fit_tenure(self):
        for field, value in (("heartbeat_age_ms", None), ("heartbeat_age_ms", 1001), ("heartbeat_rtt_ms", -1)):
            data = self.status.to_payload()
            data["nodes"][1][field] = value
            self.assert_bad_payload(data)

    def test_adjustment_cannot_refer_to_a_different_or_missing_sample(self):
        data = self.status.to_payload()
        data["nodes"][1]["adjustment"]["round_id"] = 2
        self.assert_bad_payload(data)
        data = self.status.to_payload()
        data["nodes"][1]["sample"] = None
        self.assert_bad_payload(data)

    def test_ack_semantics_preserve_rejection_and_slew_completion(self):
        for status, clock, reason in (("APPLIED", "SLEWING", None), ("REJECTED", "NORMAL", None),
                                      (None, "NORMAL", None), ("UNKNOWN", "NORMAL", None)):
            data = self.status.to_payload()
            data["nodes"][1]["adjustment"].update(ack_status=status, clock_state=clock, reason=reason)
            self.assert_bad_payload(data)
        for status, clock, reason in (("APPLIED", ClockState.NORMAL, None),
                                      ("REJECTED", ClockState.NORMAL, "Clock paused"), (None, None, None)):
            adjustment = replace(self.adjustment, ack_status=status, clock_state=clock, reason=reason)
            payload = self.message(replace(self.status, nodes=(self.master_row, replace(self.worker_row, adjustment=adjustment)))).payload
            self.assertEqual(ClusterStatus.from_payload(payload).nodes[1].adjustment, adjustment)

    def test_master_progress_requires_consistent_state_and_remaining_amount(self):
        for state, amount in (("NORMAL", 1), ("SLEWING", 0), (None, 1), ("NORMAL", None), ("SLEWING", float("nan"))):
            data = self.status.to_payload()
            data.update(master_clock_state=state, master_remaining_ms=amount)
            self.assert_bad_payload(data)
        self.message(replace(self.status, master_clock_state=ClockState.NORMAL, master_remaining_ms=0))

    def test_round_members_and_missing_lists_are_validated(self):
        for field, value in (("missing_report_ids", [1, 1]), ("missing_ack_ids", [2]),
                             ("missing_ack_ids", [True]), ("participants", []), ("ack_remaining_ms", -1)):
            data = self.status.to_payload()
            data["round"][field] = value
            self.assert_bad_payload(data)
        with self.assertRaises(ProtocolError):
            self.message(replace(self.status, round=replace(self.round, missing_report_ids=(6,))))

    def test_round_cannot_name_another_master_boot(self):
        status = replace(self.status, round=replace(self.round, participants=(NodeIdentity(6), self.worker)))
        with self.assertRaises(ProtocolError):
            self.message(status)

    def test_invalid_ipv4_names_enums_and_uuid_are_rejected(self):
        for field, value in (("ip", "0.0.0.0"), ("ip", "224.0.0.1"), ("display_name", " "),
                             ("display_name", "\ud800"), ("role", "UNKNOWN"), ("status", "CONNECTED")):
            data = self.status.to_payload()
            data["nodes"][1][field] = value
            self.assert_bad_payload(data)
        data = self.status.to_payload()
        data["nodes"][1]["identity"]["boot_id"] = "NOT-UUID"
        self.assert_bad_payload(data)

    def test_filter_requires_real_offset_and_known_reason(self):
        for field, value in (("offset_ms", None), ("filter_reason", "ALWAYS_TRUST_MASTER")):
            data = self.status.to_payload()
            data["nodes"][1]["sample"][field] = value
            self.assert_bad_payload(data)

    def test_input_and_export_mutation_cannot_change_message(self):
        data = self.status.to_payload()
        message = Message(MessageType.CLUSTER_STATUS, self.master, data, target_node_id=1, leader=self.leader)
        data["nodes"][1]["sample"]["offset_ms"] = 999
        self.assertEqual(message.payload["nodes"][1]["sample"]["offset_ms"], 5000.25)
        with self.assertRaises(TypeError):
            message.payload["nodes"][1]["sample"]["offset_ms"] = 999
        exported = message.to_dict()
        exported["payload"]["nodes"][1]["display_name"] = "Changed"
        self.assertEqual(ClusterStatus.from_payload(message.payload), self.status)
        with self.assertRaises(FrozenInstanceError):
            self.status.sequence = 2

    def test_six_node_frame_fits_and_oversized_utf8_frame_is_rejected(self):
        rows = [self.master_row, self.worker_row]
        identities = [self.master, self.worker]
        for i in (2, 3, 4, 5):
            identity = NodeIdentity(i)
            identities.append(identity)
            rows.append(replace(self.worker_row, identity=identity, ip="10.0.0.{}".format(i)))
        status = replace(self.status, nodes=rows, round=replace(self.round, participants=identities))
        wire = serialize_packet(self.message(status))
        self.assertLessEqual(len(wire) - 1, MAX_FRAME_BYTES)
        self.assertEqual(len(ClusterStatus.from_payload(parse_packet(wire).payload).nodes), 6)
        long = "🙂" * 512
        rejected = AdjustmentStatus(1, -2500, "REJECTED", ClockState.NORMAL, long)
        huge_rows = tuple(replace(row, display_name="🙂" * 128, adjustment=rejected) for row in rows)
        huge = replace(status, nodes=huge_rows, round=replace(status.round, reason=long))
        with self.assertRaises(ProtocolError):
            serialize_packet(self.message(huge))

    def test_nan_wire_input_is_rejected(self):
        data = self.message().to_dict()
        data["payload"]["nodes"][1]["sample"]["offset_ms"] = float("nan")
        with self.assertRaises(ProtocolError):
            parse_packet(json.dumps(data).encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
