"""Bounded convergence history: one real offset measurement per Node/round."""

import math
import tkinter as tk
from collections import deque
from dataclasses import dataclass
from tkinter import ttk

from ..core.models import RoundKey
from .node_table import monitoring_view


COLORS = ("#2563eb", "#16a34a", "#9333ea", "#ea580c", "#0891b2", "#dc2626")


@dataclass(frozen=True)
class ChartPoint:
    time_s: float
    offset_ms: float
    identity: object
    rejected: bool


class ConvergenceHistory:
    def __init__(self, capacity=180):
        if type(capacity) is not int or not 2 <= capacity <= 3600:
            raise ValueError("capacity must be in [2, 3600]")
        self.points = {i: deque(maxlen=capacity) for i in range(1, 7)}
        self.leader = None
        self.last_round = None
        self.origin_s = None
        self._seen = {}
        self._source = None

    def clear(self):
        for series in self.points.values():
            series.clear()
        self.last_round = None
        self.origin_s = None
        self._seen.clear()

    def ingest(self, node):
        leader = node.election.leader if node else None
        if leader != self.leader:
            self.clear()
            self.leader = leader
        source = "cluster" if getattr(node, "cluster", None) is not None else "local"
        if source != self._source:
            self.clear()
            self._source = source
        if source == "cluster":
            status, _, _ = monitoring_view(node)
            if status is None:
                return False
            changed = False
            peers = {p.identity.node_id: p.identity for p in node.peers}
            peers[node.identity.node_id] = node.identity
            for item in status.nodes:
                sample = item.sample
                if sample is None or sample.offset_ms is None:
                    continue
                if item.identity.node_id in peers and peers[item.identity.node_id] != item.identity:
                    continue
                seen = self._seen.get(item.identity.node_id)
                if seen and seen[0] == item.identity and sample.round_id <= seen[1]:
                    continue
                time_s = max(0, status.elapsed_ms - sample.age_ms) / 1000
                self.points[item.identity.node_id].append(ChartPoint(
                    time_s, sample.offset_ms, item.identity,
                    sample.filter_reason not in (None, "KEPT")))
                self._seen[item.identity.node_id] = (item.identity, sample.round_id)
                if self.last_round is None or sample.round_id > self.last_round.round_id:
                    self.last_round = RoundKey(leader, sample.round_id)
                changed = True
            return changed
        sync = node.synchronization if node else None
        plan = sync.plan if sync else None
        session = sync.session if sync else None
        if (not plan or not session or plan.round_key != session.round_key
                or plan.round_key.leader != leader or self.last_round == plan.round_key):
            return False
        if not session.reports:
            return False
        time_s = max(r.received_monotonic_ns for r in session.reports) / 1000000000
        if self.origin_s is None:
            self.origin_s = time_s
        rejected = {r.sample.identity for r in plan.filter_result.rejected}
        samples = list(plan.filter_result.accepted) + [r.sample for r in plan.filter_result.rejected]
        for sample in samples:
            if math.isfinite(sample.offset_ms):
                self.points[sample.identity.node_id].append(ChartPoint(
                    time_s - self.origin_s, sample.offset_ms, sample.identity,
                    sample.identity in rejected))
        self.last_round = plan.round_key
        return True


class ConvergenceChart(ttk.Frame):
    def __init__(self, master):
        super().__init__(master, padding=10)
        self.history = ConvergenceHistory()
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        ttk.Label(self, wraplength=1050, text=(
            "Delta Worker − Master (ms), đo trước hiệu chỉnh mỗi phiên. Mỗi điểm là một mẫu thực; "
            "dấu × là mẫu bị loại. Đổi nhiệm kỳ Master sẽ bắt đầu đồ thị mới. "
            "Worker chưa có số đo toàn cụm sẽ không vẽ dữ liệu thay thế."
        )).grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.canvas = tk.Canvas(self, background="#ffffff", highlightthickness=0, height=260)
        self.canvas.grid(row=1, column=0, sticky="nsew")
        legend = ttk.Frame(self)
        legend.grid(row=2, column=0, sticky="w", pady=6)
        for i, color in enumerate(COLORS, 1):
            ttk.Label(legend, text="● Node {}   ".format(i), foreground=color).pack(side="left")
        self.caption = ttk.Label(self, text="Chưa có số đo đồng bộ.")
        self.caption.grid(row=3, column=0, sticky="w")
        self.canvas.bind("<Configure>", lambda event: self.redraw())

    def update_snapshot(self, node):
        previous = self.history.leader
        previous_source = self.history._source
        changed = self.history.ingest(node)
        if changed or previous != self.history.leader or previous_source != self.history._source:
            self.redraw()
        if self.history.last_round:
            status, age, stale = monitoring_view(node)
            freshness = (" • {}≈{:.1f}s từ lúc nhận".format("DỮ LIỆU CŨ / " if stale else "", age / 1000)
                         if status else " • chưa có bản tin Master hiện hành")
            self.caption.configure(text="Master {} • phiên #{} • tối đa 180 mẫu/Node{}".format(
                self.history.leader.node.node_id, self.history.last_round.round_id, freshness))
        else:
            self.caption.configure(text="Chưa có số đo toàn cụm tại Node này.")

    def redraw(self):
        c = self.canvas
        c.delete("all")
        width, height = c.winfo_width(), c.winfo_height()
        if width < 180 or height < 120:
            return
        left, top, right, bottom = 86, 28, width - 24, height - 42
        points = [p for series in self.history.points.values() for p in series]
        if not points:
            c.create_text(width / 2, height / 2, text="Chờ số đo thực từ Master…", fill="#64748b")
            return
        xmin, xmax = min(p.time_s for p in points), max(p.time_s for p in points)
        xmax = max(xmax, xmin + 1)
        ymin, ymax = min(0, min(p.offset_ms for p in points)), max(0, max(p.offset_ms for p in points))
        margin = max(1, (ymax - ymin) * 0.12)
        ymin, ymax = ymin - margin, ymax + margin
        x = lambda t: left + (t - xmin) / (xmax - xmin) * (right - left)
        y = lambda v: bottom - (v - ymin) / (ymax - ymin) * (bottom - top)
        for i in range(5):
            value = ymin + (ymax - ymin) * i / 4
            yy = y(value)
            c.create_line(left, yy, right, yy, fill="#e2e8f0")
            c.create_text(left - 8, yy, text="{:.1f}".format(value), anchor="e", fill="#64748b")
            t = xmin + (xmax - xmin) * i / 4
            c.create_text(x(t), bottom + 16, text="{:.1f}s".format(t), fill="#64748b")
        c.create_text(left, 12, text="Delta (ms)", anchor="w", fill="#475569")
        c.create_line(left, y(0), right, y(0), fill="#94a3b8", dash=(4, 3))
        for node_id, series in self.history.points.items():
            previous = None
            color = COLORS[node_id - 1]
            for point in series:
                xx, yy = x(point.time_s), y(point.offset_ms)
                if (previous and previous.identity == point.identity
                        and 0 < point.time_s - previous.time_s <= 30):
                    c.create_line(x(previous.time_s), y(previous.offset_ms), xx, yy, fill=color, width=2)
                if point.rejected:
                    c.create_line(xx-4, yy-4, xx+4, yy+4, fill=color, width=2)
                    c.create_line(xx-4, yy+4, xx+4, yy-4, fill=color, width=2)
                else:
                    c.create_oval(xx-3, yy-3, xx+3, yy+3, fill=color, outline=color)
                previous = point
