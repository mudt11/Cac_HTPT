"""Six-node dashboard from local state and explicitly labelled Master telemetry."""

from dataclasses import dataclass, replace
from datetime import datetime
from time import monotonic_ns
from tkinter import ttk
from typing import Optional, Tuple

from ..core.models import NodeRole, SyncRoundState


def monitoring_view(node, now_ns=None):
    """Return current-tenure telemetry and locally measured receipt age."""
    cluster = getattr(node, "cluster", None)
    if (cluster is None or cluster.status is None or cluster.leader is None
            or cluster.leader != node.election.leader):
        return None, 0, False
    now_ns = monotonic_ns() if now_ns is None else now_ns
    age = (max(0, now_ns - cluster.received_monotonic_ns) // 1000000
           if cluster.received_monotonic_ns is not None else cluster.age_ms or 0)
    return cluster.status, age, cluster.stale or age >= 4000


def monitoring_ack(item, round_status):
    """Describe the Master's recorded outcome; never infer a remote deadline."""
    adjustment = item.adjustment
    current = round_status and (not adjustment or adjustment.round_id == round_status.round_id)
    status = adjustment.ack_status if adjustment else None
    if status in ("REJECTED", "APPLIED"):
        return status
    if current and round_status.state == SyncRoundState.CANCELLED:
        return "ĐÃ HỦY / ACCEPTED" if status == "ACCEPTED" else "ĐÃ HỦY"
    if status:
        return status
    if not current:
        return "CHƯA CÓ ACK" if adjustment else "--"
    if item.identity not in round_status.participants:
        return "KHÔNG THAM GIA"
    if item.identity.node_id in round_status.missing_report_ids:
        return "CHỜ BÁO GIỜ" if round_status.state == SyncRoundState.COLLECTING else "THIẾU BÁO GIỜ"
    if adjustment:
        if round_status.state == SyncRoundState.WAITING_ACK:
            return "CHỜ ACK"
        if round_status.state == SyncRoundState.PARTIAL and item.identity.node_id in round_status.missing_ack_ids:
            return "HẾT HẠN ACK"
        return "KHÔNG CÓ ACK"
    return "CHỜ HIỆU CHỈNH" if round_status.state in (
        SyncRoundState.COLLECTING, SyncRoundState.COMPUTING) else "KHÔNG HIỆU CHỈNH"


def worker_ack_status(session, identity, applied_ids, now_ns: int) -> str:
    """Describe the recorded command outcome without keeping closed waits alive.

    A timeout is absence of confirmation, not evidence of Worker rejection.
    ACCEPTED remains ACCEPTED after the ACK deadline; only an actual APPLIED
    notification can promote it. Cancellation may stop a previously accepted
    slew, so retain that history without implying it is still running.
    """
    if identity not in session.participants:
        return "KHÔNG THAM GIA"
    ack = next((a for a in session.acknowledgments if a.source == identity), None)
    status = ack.payload["status"] if ack else None
    if status == "REJECTED":
        return status
    applied = status == "APPLIED" or (status == "ACCEPTED" and identity.node_id in applied_ids)
    if applied:
        return "APPLIED"
    if session.state == SyncRoundState.CANCELLED:
        return "ĐÃ HỦY / ACCEPTED" if status == "ACCEPTED" else "ĐÃ HỦY"
    if status is not None:
        return status
    report = next((r for r in session.reports if r.response.source == identity), None)
    if report is None:
        return ("CHỜ BÁO GIỜ" if session.state == SyncRoundState.COLLECTING
                and now_ns < session.report_deadline_ns else "THIẾU BÁO GIỜ")
    command = next((m for m in session.adjustments if m.target_node_id == identity.node_id), None)
    if command is None:
        return ("CHỜ HIỆU CHỈNH" if session.state in (
            SyncRoundState.COLLECTING, SyncRoundState.COMPUTING) else "KHÔNG HIỆU CHỈNH")
    if session.state in (SyncRoundState.FAILED, SyncRoundState.COMPLETED):
        # A failed session may have closed before its ACK budget elapsed.
        # Reading it later is not proof that timeout caused that failure.
        return "KHÔNG CÓ ACK"
    if session.ack_deadline_ns is not None and now_ns >= session.ack_deadline_ns:
        return "HẾT HẠN ACK"
    return "CHỜ ACK" if session.state == SyncRoundState.WAITING_ACK else "KHÔNG CÓ ACK"


def clock_text(time_ms: Optional[int]) -> str:
    if time_ms is None:
        return "--:--:--.---"
    try:
        return "{}.{:03d}".format(datetime.fromtimestamp(time_ms // 1000).strftime("%H:%M:%S"),
                                  time_ms % 1000)
    except (ValueError, OverflowError, OSError):
        return "Ngoài miền hiển thị"


@dataclass(frozen=True)
class NodeRow:
    node_id: int
    name: str = "Chưa phát hiện"
    ip: str = "--"
    role: str = "--"
    contact: str = "CHƯA BIẾT"
    clock: str = "--:--:--.---"
    heartbeat_rtt: str = "--"
    sync_rtt: str = "--"
    offset: str = "--"
    correction: str = "--"
    ack: str = "--"
    selection: str = "--"
    sample: str = "--"
    connected_workers: str = "--"


def card_text(row: NodeRow) -> str:
    """Use the same measured values and freshness labels as the detail table."""
    lines = [row.role, row.ip, row.contact, row.clock,
             "Ping: {} ms".format(row.heartbeat_rtt),
             "Delta: {} ms".format(row.offset),
             "Chỉnh: {} ms".format(row.correction),
             "ACK: " + row.ack, "Mẫu: " + row.sample]
    if row.role == NodeRole.MASTER.value:
        lines.append("Worker ONLINE: " + row.connected_workers)
    return "\n".join(lines)


def project_rows(node, clock=None, *, local_ip: str = "", local_name: str = "",
                 now_ns: Optional[int] = None) -> Tuple[NodeRow, ...]:
    """Project last measurements; never extrapolate a remote clock or offset.

    Discovery contact does not mean heartbeat-confirmed connectivity. Metrics
    from a retired process or Master tenure cannot be attached to its successor.
    Sample age identifies stale data even when a row remains visible offline.
    """
    if node is None:
        return tuple(NodeRow(i) for i in range(1, 7))
    now_ns = monotonic_ns() if now_ns is None else now_ns
    peers = {p.identity.node_id: p for p in node.peers}
    watched = {p.identity: p for p in node.heartbeat.peers} if node.heartbeat else {}
    sync = node.synchronization
    session = sync.session if sync else None
    if session and session.round_key.leader != node.election.leader:
        session = None
    plan = sync.plan if sync else None
    if not session or not plan or plan.round_key != session.round_key:
        plan = None
    reports = {s.response.source: s for s in session.reports} if session else {}
    samples, reasons = {}, {}
    if plan:
        samples.update((s.identity, s) for s in plan.filter_result.accepted)
        for rejected in plan.filter_result.rejected:
            samples[rejected.sample.identity] = rejected.sample
            reasons[rejected.sample.identity] = rejected.reason.value
    adjustments = {m.target_node_id: m for m in session.adjustments} if session else {}
    rows = []
    for node_id in range(1, 7):
        local = node_id == node.identity.node_id
        peer = peers.get(node_id)
        if not local and peer is None:
            rows.append(NodeRow(node_id))
            continue
        identity = node.identity if local else peer.identity
        watch = watched.get(identity)
        role = node.election.role if local else peer.role
        values = dict(node_id=node_id, name=local_name if local else peer.display_name,
                      ip=local_ip if local else peer.ip, role=role.value)
        values["contact"] = (("LOCAL" if node.running else "ĐÃ DỪNG") if local else
                             watch.status.value if watch else "DISCOVERY/" + peer.status.value)
        if watch and watch.rtt_ms is not None:
            values["heartbeat_rtt"] = "{:.2f}".format(watch.rtt_ms)
        if local and clock:
            values["clock"] = clock_text(clock.time_ms)
        if not local and session:
            values["ack"] = worker_ack_status(session, identity, sync.applied_worker_ids, now_ns)
        report = reports.get(identity)
        if report:
            if not local:
                values["clock"] = clock_text(report.response.payload["sent_time_ms"])
            values["sync_rtt"] = "{:.2f}".format(report.rtt_ns / 1000000)
            age = max(0, now_ns - report.received_monotonic_ns) / 1000000000
            values["sample"] = "#{} / {:.1f}s".format(session.round_key.round_id, age)
        sample = samples.get(identity)
        if sample:
            values["offset"] = "{:+.2f}".format(sample.offset_ms)
            values["selection"] = reasons.get(identity, "GIỮ")
            if not report:
                values["sample"] = "#{} / mốc Master".format(session.round_key.round_id)
        if local and role == NodeRole.MASTER and plan and plan.ready:
            values["correction"] = "{:+d}".format(plan.master_correction_ms)
            values["ack"] = clock.state.value if clock else "--"
        elif report:
            command = adjustments.get(node_id)
            if command:
                values["correction"] = "{:+d}".format(command.payload["correction_ms"])
        if local and sync and sync.worker and sync.worker.round_key.leader == node.election.leader:
            worker = sync.worker
            values["ack"] = worker.state.value
            values["sample"] = "#{} / cục bộ".format(worker.round_key.round_id)
            if worker.correction_ms is not None:
                values["correction"] = "{:+d}".format(worker.correction_ms)
        rows.append(NodeRow(**values))
    status, age_ms, stale = monitoring_view(node, now_ns)
    if status is not None and node.election.role != NodeRole.MASTER:
        for item in status.nodes:
            index = item.identity.node_id - 1
            local = item.identity.node_id == node.identity.node_id
            known = node.identity if local else peers.get(item.identity.node_id)
            known_identity = known if local else known.identity if known else None
            if known_identity is not None and known_identity != item.identity:
                continue
            base = rows[index]
            values = dict(name=local_name if local else item.display_name,
                          ip=local_ip if local else item.ip,
                          role=base.role if local else item.role.value,
                          contact=base.contact if local else
                          ("CŨ/" if stale else "MASTER/") + item.status.value,
                          heartbeat_rtt=("{:.2f}".format(item.heartbeat_rtt_ms)
                                         if item.heartbeat_rtt_ms is not None else "--"))
            sample = item.sample
            if sample:
                values.update(sync_rtt="{:.2f}".format(sample.rtt_ms) if sample.rtt_ms is not None else "--",
                              offset="{:+.2f}".format(sample.offset_ms) if sample.offset_ms is not None else "--",
                              selection="GIỮ" if sample.filter_reason == "KEPT" else sample.filter_reason or "--",
                              sample="#{} / ≈{:.1f}s{}".format(sample.round_id,
                                  (sample.age_ms + age_ms) / 1000, " / CŨ" if stale else ""))
                if not local:
                    values["clock"] = clock_text(sample.clock_time_ms)
            if not local:
                values["correction"] = "{:+d}".format(item.adjustment.correction_ms) if item.adjustment else "--"
                values["ack"] = monitoring_ack(item, status.round)
                if item.role == NodeRole.MASTER and status.master_clock_state:
                    values["ack"] = status.master_clock_state.value
            rows[index] = replace(base, **values)
    leader = node.election.leader
    if leader is not None and node.running:
        master_index = leader.node.node_id - 1
        if node.election.role == NodeRole.MASTER and node.identity == leader.node:
            if node.heartbeat is not None and node.heartbeat.leader == leader:
                count = sum(p.identity in watched and watched[p.identity].status.value == "ONLINE"
                            and watched[p.identity].last_heartbeat_ns is not None
                            for p in node.peers if p.identity != node.identity)
                rows[master_index] = replace(rows[master_index], connected_workers="{}/5".format(count))
        elif status is not None and rows[master_index].role == NodeRole.MASTER.value:
            count = sum(item.role == NodeRole.WORKER and item.status.value == "ONLINE"
                        and (item.identity.node_id not in peers or
                             peers[item.identity.node_id].identity == item.identity)
                        and (item.identity.node_id != node.identity.node_id or item.identity == node.identity)
                        for item in status.nodes)
            rows[master_index] = replace(rows[master_index], connected_workers="{}/5 ({})".format(
                count, "CŨ" if stale else "Master"))
    return tuple(rows)


class NodeTable(ttk.Frame):
    def __init__(self, master):
        super().__init__(master, padding=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        cards = ttk.Frame(self)
        cards.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        self._cards = {}
        for i in range(1, 7):
            cards.columnconfigure(i - 1, weight=1, uniform="node")
            frame = ttk.LabelFrame(cards, text="NODE {}".format(i), padding=8)
            frame.grid(row=0, column=i - 1, sticky="nsew", padx=3)
            label = ttk.Label(frame, text="Chưa phát hiện", justify="left", font=("Segoe UI", 9),
                              wraplength=150)
            label.pack(anchor="w")
            self._cards[i] = (frame, label)
        columns = ("id", "name", "ip", "role", "contact", "ping", "rtt", "offset",
                   "correction", "ack", "filter", "sample")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=7)
        titles = ("ID", "Tên", "IP", "Vai trò", "Liên lạc", "Ping ms", "Sync RTT ms",
                  "Delta ms", "Chỉnh ms", "ACK / Slewing", "Bộ lọc", "Phiên / tuổi mẫu")
        widths = (40, 95, 115, 85, 155, 75, 90, 90, 85, 180, 160, 145)
        for column, title, width in zip(columns, titles, widths):
            self.tree.heading(column, text=title)
            self.tree.column(column, width=width, minwidth=40, anchor="center", stretch=False)
        self.tree.grid(row=1, column=0, sticky="nsew")
        xscroll = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        xscroll.grid(row=2, column=0, sticky="ew")
        self.tree.configure(xscrollcommand=xscroll.set)
        ttk.Label(self, wraplength=1050, text=(
            "Giờ trên card từ xa là mẫu cuối nhận được, không phải đồng hồ trực tiếp. "
            "Delta/RTT đồng bộ là số đo của phiên ghi trong bảng. DISCOVERY chỉ xác nhận liên lạc discovery. "
            "MASTER/ là góc nhìn của Master; CŨ/ là dữ liệu quá 4 giây. Tuổi mẫu ≈ chưa gồm độ trễ truyền. "
            "ACCEPTED = đã nhận hiệu chỉnh; APPLIED = đã hoàn tất slewing, chưa chứng minh hội tụ."
        )).grid(row=3, column=0, sticky="w", pady=(10, 0))
        self.update_rows(project_rows(None))

    def update_rows(self, rows: Tuple[NodeRow, ...]) -> None:
        for row in rows:
            frame, label = self._cards[row.node_id]
            frame.configure(text="{}NODE {}".format("★ " if row.role == "MASTER" else "", row.node_id))
            label.configure(text=card_text(row),
                            foreground="#b45309" if row.role == "MASTER" else "#166534"
                            if row.contact in ("ONLINE", "LOCAL", "MASTER/ONLINE") else "#64748b")
            values = (row.node_id, row.name, row.ip, row.role, row.contact, row.heartbeat_rtt,
                      row.sync_rtt, row.offset, row.correction, row.ack, row.selection, row.sample)
            key = str(row.node_id)
            if self.tree.exists(key):
                self.tree.item(key, values=values)
            else:
                self.tree.insert("", "end", iid=key, values=values)
