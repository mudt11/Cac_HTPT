"""Protocol regression checks. Run from the repository root with unittest discovery."""

import json
import unittest
from dataclasses import replace
from uuid import uuid4

from BerkeleyTimeSync.core.models import LeaderIdentity, NodeIdentity, RoundKey
from BerkeleyTimeSync.core.protocol import (
    MAX_DATAGRAM_BYTES, MAX_FRAME_BYTES, Message, MessageType, ProtocolError,
    TCPFrameBuffer, Transport, parse_packet, serialize_packet, validate_context,
)


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.master = NodeIdentity(6)
        self.worker = NodeIdentity(1)
        self.leader = LeaderIdentity(self.master)
        self.request = Message(
            MessageType.SYNC_REQUEST, self.master,
            {"master_time_ms": 86399999, "timeout_ms": 6000, "participant_ids": (6, 1)},
            target_node_id=1, leader=self.leader, round_id=1)

    def encode(self, data):
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    def test_tcp_roundtrip_preserves_full_session_identity(self):
        restored = parse_packet(serialize_packet(self.request))
        self.assertEqual(restored, self.request)
        self.assertEqual(restored.round_key, RoundKey(self.leader, 1))

    def test_udp_roundtrip_preserves_vietnamese_name(self):
        discovery = Message(MessageType.DISCOVER, self.worker,
                            {"display_name": "Máy Đình Thạch", "ip": "10.0.0.1",
                             "tcp_port": 9999, "udp_port": 8888, "role": "WORKER"})
        self.assertEqual(parse_packet(serialize_packet(discovery, transport=Transport.UDP),
                                      transport=Transport.UDP), discovery)

    def test_rejects_wrong_transport(self):
        with self.assertRaises(ProtocolError):
            serialize_packet(self.request, transport=Transport.UDP)
        with self.assertRaises(ProtocolError):
            parse_packet(serialize_packet(self.request), transport=Transport.UDP)

    def test_rejects_malformed_json_and_invalid_unicode(self):
        for raw in (b"", b"{", b"null", b"[]", b"\xff", b'{"x":NaN}',
                    b'{"x":Infinity}', b'{"version":1,"version":1}', "\ud800"):
            with self.subTest(raw=repr(raw)), self.assertRaises(ProtocolError):
                parse_packet(raw)

    def test_rejects_missing_unknown_and_invalid_fields(self):
        for field, value in (("version", True), ("version", 2), ("type", "UNKNOWN"),
                             ("round_id", 0), ("round_id", True), ("target_node_id", 7),
                             ("message_id", "not-a-uuid"), ("unexpected", "field")):
            data = self.request.to_dict()
            data[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ProtocolError):
                parse_packet(self.encode(data))
        data = self.request.to_dict()
        del data["source"]
        with self.assertRaises(ProtocolError):
            parse_packet(self.encode(data))

    def test_rejects_invalid_poll_payloads(self):
        for field, value in (("master_time_ms", -1), ("master_time_ms", 1.5),
                             ("timeout_ms", 0), ("participant_ids", [6, 1, 1]),
                             ("participant_ids", [6, 2]), ("participant_ids", [6, True])):
            data = self.request.to_dict()
            data["payload"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ProtocolError):
                parse_packet(self.encode(data))

    def test_rejects_inconsistent_ack_semantics(self):
        for payload in ({"status": "REJECTED", "clock_state": "NORMAL", "clock_time_ms": 1},
                        {"status": "APPLIED", "clock_state": "SLEWING", "clock_time_ms": 1}):
            with self.subTest(payload=payload), self.assertRaises(ProtocolError):
                Message(MessageType.TIME_ACK, self.worker, payload, target_node_id=6,
                        leader=self.leader, round_id=1, reply_to=uuid4())

    def test_context_rejects_other_recipient_boot_epoch_and_round(self):
        wrong_contexts = (
            {"recipient_id": 2},
            {"expected_leader": LeaderIdentity(NodeIdentity(6))},
            {"expected_leader": LeaderIdentity(self.master)},
            {"expected_round": RoundKey(self.leader, 2)},
        )
        for context in wrong_contexts:
            with self.subTest(context=context), self.assertRaises(ProtocolError):
                validate_context(self.request, **context)
        validate_context(self.request, recipient_id=1, expected_round=self.request.round_key)

    def test_reply_requires_correlation_and_ordered_worker_timestamps(self):
        report = Message(MessageType.TIME_REPORT, self.worker,
                         {"received_time_ms": 100, "sent_time_ms": 101}, target_node_id=6,
                         leader=self.leader, round_id=1, reply_to=self.request.message_id)
        with self.assertRaises(ProtocolError):
            replace(report, reply_to=None)
        with self.assertRaises(ProtocolError):
            replace(report, payload={"received_time_ms": 101, "sent_time_ms": 100})

    def test_enforces_tcp_and_udp_size_limits(self):
        for channel, limit in ((Transport.TCP, MAX_FRAME_BYTES),
                               (Transport.UDP, MAX_DATAGRAM_BYTES)):
            with self.subTest(channel=channel), self.assertRaises(ProtocolError):
                parse_packet(b" " * (limit + 1), transport=channel)

    def test_utf8_may_be_split_inside_a_multibyte_character(self):
        message = Message(MessageType.FORCE_MASTER, self.master, {"reason": "Đổi Master"},
                          leader=self.leader)
        raw = serialize_packet(message)
        boundary = raw.index("Đ".encode("utf-8")) + 1
        buffer = TCPFrameBuffer()
        self.assertEqual(buffer.feed(raw[:boundary]), [])
        frames = buffer.feed(raw[boundary:])
        self.assertEqual([parse_packet(frame) for frame in frames], [message])
        buffer.finish()

    def test_coalesced_frames_and_crlf(self):
        raw = serialize_packet(self.request).rstrip(b"\n") + b"\r\n"
        buffer = TCPFrameBuffer()
        self.assertEqual([parse_packet(p) for p in buffer.feed(raw * 3)], [self.request] * 3)
        buffer.finish()

    def test_bad_json_frame_does_not_consume_the_following_valid_frame(self):
        frames = TCPFrameBuffer().feed(b"bad-json\n" + serialize_packet(self.request))
        with self.assertRaises(ProtocolError):
            parse_packet(frames[0])
        self.assertEqual(parse_packet(frames[1]), self.request)

    def test_partial_frame_at_disconnect_is_reported(self):
        buffer = TCPFrameBuffer()
        buffer.feed(serialize_packet(self.request)[:-1])
        with self.assertRaises(ProtocolError):
            buffer.finish()

    def test_oversized_frame_closes_buffer_and_bounds_retained_data(self):
        buffer = TCPFrameBuffer()
        buffer.feed(b"x" * MAX_FRAME_BYTES)
        with self.assertRaises(ProtocolError):
            buffer.feed(b"x")
        with self.assertRaises(ProtocolError):
            buffer.feed(b"{}\n")


if __name__ == "__main__":
    unittest.main()
