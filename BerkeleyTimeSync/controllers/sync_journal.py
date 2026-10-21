"""Bounded, owner-thread journal of observed synchronization changes.

Messages describe recorded state, not successful transport delivery or measured
convergence. No clock mutation, network I/O or persistent file writing occurs.
"""

from ..core.models import NodeRole, SyncRoundState


_TERMINAL = {SyncRoundState.COMPLETED, SyncRoundState.PARTIAL,
             SyncRoundState.FAILED, SyncRoundState.CANCELLED}


def round_context(key):
    return "Master {} / nhiệm kỳ {} / phiên #{}".format(
        key.leader.node.node_id, key.leader.leader_epoch, key.round_id)


class SyncJournal:
    """Retain fingerprints for only the latest Master and Worker rounds."""

    def __init__(self):
        self._keys = {}
        self._marks = {}

    def _changed(self, group, key, field, value):
        if self._keys.get(group) != key:
            self._keys[group] = key
            self._marks[group] = {}
        marks = self._marks[group]
        if field in marks and marks[field] == value:
            return False
        marks[field] = value
        return True

    def context(self, snapshot):
        current = snapshot.worker if snapshot.role == NodeRole.WORKER else snapshot.session
        return round_context(current.round_key) if current else "Đồng bộ"

    def ingest(self, snapshot):
        if snapshot is None:
            return ()
        lines = []
        session = snapshot.session
        if session:
            key = session.round_key
            prefix = round_context(key)

            def emit(field, value, text):
                if self._changed("master", key, field, value):
                    lines.append(prefix + " • " + text)

            participants = tuple((p.node_id, p.boot_id) for p in session.participants)
            emit("participants", participants, "Thành viên: " + ", ".join(
                "Node {} (boot {})".format(p.node_id, p.boot_id) for p in session.participants))
            for report in session.reports:
                emit(("report", report.response.source), report.response.message_id,
                     "Node {} báo giờ {} ms • RTT {:.2f} ms".format(
                         report.response.source.node_id, report.response.payload["sent_time_ms"],
                         report.rtt_ns / 1000000))
            plan = snapshot.plan
            if plan and plan.round_key == key:
                for sample in plan.filter_result.accepted:
                    emit(("filter", sample.identity), (sample.offset_ms, sample.rtt_ms, "KEPT"),
                         "Node {} • Delta {:+.2f} ms • RTT {:.2f} ms • bộ lọc KEPT".format(
                             sample.identity.node_id, sample.offset_ms, sample.rtt_ms))
                for rejected in plan.filter_result.rejected:
                    sample = rejected.sample
                    emit(("filter", sample.identity), (sample.offset_ms, sample.rtt_ms, rejected.reason.value),
                         "Node {} • Delta {:+.2f} ms • RTT {:.2f} ms • bộ lọc {}".format(
                             sample.identity.node_id, sample.offset_ms, sample.rtt_ms, rejected.reason.value))
                if plan.ready:
                    emit("plan", (plan.mean_offset_ms, plan.master_correction_ms),
                         "Delta trung bình {:+.2f} ms • hiệu chỉnh Master {:+d} ms".format(
                             plan.mean_offset_ms, plan.master_correction_ms))
            for command in session.adjustments:
                emit(("command", command.target_node_id), command.message_id,
                     "Lập lệnh Node {} • hiệu chỉnh {:+d} ms".format(
                         command.target_node_id, command.payload["correction_ms"]))
            for ack in session.acknowledgments:
                status = ack.payload["status"]
                if ack.source.node_id in snapshot.applied_worker_ids and status == "ACCEPTED":
                    status = "APPLIED"
                emit(("ack", ack.source), (status, ack.payload.get("reason")),
                     "Node {} • ACK {}{}".format(ack.source.node_id, status,
                         " • Lý do: " + ack.payload["reason"] if ack.payload.get("reason") else ""))
            terminal = session.state in _TERMINAL
            summary = (session.state, session.reason,
                       session.missing_report_ids if terminal else (),
                       session.missing_ack_ids if terminal else ())
            detail = "Trạng thái " + session.state.value
            if session.reason:
                detail += " • Lý do: " + session.reason
            if terminal:
                if session.missing_report_ids:
                    detail += " • Thiếu báo giờ: Node " + ", ".join(map(str, session.missing_report_ids))
                if session.missing_ack_ids:
                    detail += " • Thiếu ACK: Node " + ", ".join(map(str, session.missing_ack_ids))
                detail += " • Kết quả phiên không phải phép đo hội tụ."
            emit("state", summary, detail)
            if (snapshot.role == NodeRole.MASTER and snapshot.leader == key.leader
                    and plan and plan.round_key == key and plan.ready and snapshot.clock):
                emit("clock", snapshot.clock.state, "Đồng hồ Master: " + snapshot.clock.state.value)
        worker = snapshot.worker
        if worker:
            value = (worker.state, worker.correction_ms)
            if self._changed("worker", worker.round_key, "state", value):
                lines.append(round_context(worker.round_key) + " • Worker cục bộ: " + worker.state.value
                             + (" • hiệu chỉnh {:+d} ms".format(worker.correction_ms)
                                if worker.correction_ms is not None else ""))
        return tuple(lines)
