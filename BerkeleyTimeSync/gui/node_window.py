"""Desktop dashboard; every widget and callback belongs to the Tk main thread."""

import tkinter as tk
import threading
from datetime import datetime
from ipaddress import IPv4Address
from queue import Empty, Full, Queue
from tkinter import messagebox, ttk

from ..core.models import NodeRole
from ..network.interfaces import list_ipv4_adapters
from ..runtime import CommandKind, NodeConfig, NodeRuntime, RuntimeState
from .convergence_chart import ConvergenceChart
from .node_table import NodeTable, clock_text, project_rows, monitoring_view


def monitoring_description(node, now_ns=None):
    """Label telemetry provenance independently of the local Worker state."""
    status, age, stale = monitoring_view(node, now_ns)
    if status is None:
        return "Toàn cụm: chưa có dữ liệu từ Master hiện hành."
    online = sum(n.role == NodeRole.WORKER and n.status.value == "ONLINE" for n in status.nodes)
    text = "Toàn cụm từ Master {} • bản tin #{} • tuổi nhận {:.1f}s • Worker ONLINE: {}".format(
        node.election.leader.node.node_id, status.sequence, age / 1000, online)
    if stale:
        text += " • DỮ LIỆU CŨ"
    if status.round:
        phase = status.round
        text += " • phiên #{} / {}".format(phase.round_id, phase.state.value)
        if phase.reason:
            text += " • Lý do: " + phase.reason
        if phase.missing_report_ids:
            text += " • Thiếu báo giờ: Node " + ", ".join(map(str, phase.missing_report_ids))
        if phase.missing_ack_ids:
            text += " • Thiếu ACK: Node " + ", ".join(map(str, phase.missing_ack_ids))
    for item in status.nodes:
        if item.adjustment and item.adjustment.ack_status == "REJECTED":
            text += " • Node {} từ chối (#{}): {}".format(item.identity.node_id,
                item.adjustment.round_id, item.adjustment.reason)
    return text


def synchronization_description(sync, current_leader):
    """Return phase and diagnostics without treating ACKs as convergence.

    A former Master's retained session must not hide a new Worker round.
    Retained sessions from an earlier tenure are explicitly labelled old.
    """
    if sync is None:
        return "chưa có", "Chưa có phiên đồng bộ."
    session, worker = sync.session, sync.worker
    if session is not None and session.round_key.leader == current_leader:
        progress = "#{} / {} / ACK {}/{}".format(
            session.round_key.round_id, session.state.value,
            len(session.acknowledgments), len(session.adjustments))
        details = []
        if session.reason:
            details.append("Lý do: " + session.reason)
        if session.missing_report_ids:
            details.append("Thiếu báo giờ: Node " + ", ".join(map(str, session.missing_report_ids)))
        if session.missing_ack_ids:
            details.append("Thiếu ACK: Node " + ", ".join(map(str, session.missing_ack_ids)))
        rejected = [a for a in session.acknowledgments if a.payload["status"] == "REJECTED"]
        for ack in rejected:
            details.append("Node {} từ chối: {}".format(ack.source.node_id, ack.payload.get("reason", "--")))
        if not details:
            details.append("ACK xác nhận lệnh hiệu chỉnh; không xác nhận các đồng hồ đã hội tụ.")
        return progress, " • ".join(details)
    if worker is not None and worker.round_key.leader == current_leader:
        return "#{} / {}".format(worker.round_key.round_id, worker.state.value), (
            "Trạng thái đồng bộ cục bộ của Worker; kết quả toàn phiên do Master theo dõi.")
    if session is not None:
        return "phiên cũ #{} / {}".format(session.round_key.round_id, session.state.value), (
            "Master trước: Node {} • {}".format(session.round_key.leader.node.node_id,
                session.reason or "Phiên thuộc nhiệm kỳ trước, không phải phiên hiện hành."))
    if worker is not None:
        return "phiên Worker cũ #{} / {}".format(worker.round_key.round_id, worker.state.value), (
            "Phiên thuộc nhiệm kỳ trước, đang chờ yêu cầu từ Master hiện hành.")
    return "chưa có", "Chưa có phiên đồng bộ."


class NodeWindow:
    def __init__(self, root, initial=None, *, runtime_factory=NodeRuntime):
        self.root = root
        self._factory = runtime_factory
        self.runtime = None
        self._closing = False
        self._after_id = None
        self._log_lines = 0
        self._lost = 0
        self._last_error = None
        self._conflict_targets = {}
        self._conflict_labels = ()
        self._adapters = {}
        self._adapter_results = Queue(maxsize=1)
        self._scanning = False
        root.title("Berkeley Time Sync • Node Dashboard")
        root.geometry("1250x880")
        root.minsize(1100, 760)
        root.configure(background="#f1f5f9")
        style = ttk.Style(root)
        if "clam" in style.theme_names():
            style.theme_use("clam")
        style.configure("TFrame", background="#f1f5f9")
        style.configure("TLabel", background="#f1f5f9", foreground="#0f172a", font=("Segoe UI", 10))
        style.configure("TButton", font=("Segoe UI", 10), padding=(10, 5))
        style.configure("TLabelframe", background="#f1f5f9")
        style.configure("TLabelframe.Label", background="#f1f5f9", font=("Segoe UI", 10, "bold"))
        style.configure("Treeview", rowheight=30, font=("Segoe UI", 9))
        self._build(initial)
        root.protocol("WM_DELETE_WINDOW", self.close)
        self._after_id = root.after(50, self._refresh)
        self._scan_interfaces()

    def _build(self, initial):
        c = initial
        self.node_id = tk.StringVar(value=str(c.node_id if c else 1))
        self.name = tk.StringVar(value=c.display_name if c else "Node 1")
        self.ip = tk.StringVar(value=c.ip if c else "")
        self.bind_host = tk.StringVar(value=c.bind_host if c else "0.0.0.0")
        self.broadcast = tk.StringVar(value=c.broadcast_ip if c else "255.255.255.255")
        self.tcp_port = tk.StringVar(value=str(c.tcp_port if c else 9999))
        self.udp_port = tk.StringVar(value=str(c.udp_port if c else 8888))
        self.discovery_port = tk.StringVar(value=str(c.discovery_port if c else 8888))
        self.auto_sync = tk.BooleanVar(value=c.auto_sync if c else True)
        self.interval = tk.StringVar(value=str(c.sync_interval_s if c else 10))
        self.custom_offset = tk.StringVar(value="5")
        self.conflict = tk.StringVar()
        self.adapter = tk.StringVar(value="Nhập IP thủ công")
        outer = ttk.Frame(self.root, padding=14)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(4, weight=1)
        header = ttk.Frame(outer)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        ttk.Label(header, text="BERKELEY / TIME SYNC", font=("Segoe UI", 19, "bold")).pack(side="left")
        self.role_label = ttk.Label(header, text="CHƯA KHỞI ĐỘNG", font=("Segoe UI", 12, "bold"))
        self.role_label.pack(side="right")
        config = ttk.LabelFrame(outer, text="Cấu hình Node và mạng LAN", padding=10)
        config.grid(row=1, column=0, sticky="ew")
        self._config_widgets = []
        adapter_bar = ttk.Frame(config)
        adapter_bar.grid(row=0, column=0, columnspan=6, sticky="ew", pady=(0, 9))
        ttk.Label(adapter_bar, text="Card mạng IPv4:").pack(side="left", padx=(0, 8))
        self.adapter_box = ttk.Combobox(adapter_bar, textvariable=self.adapter, width=74,
                                       values=("Nhập IP thủ công",), state="readonly")
        self.adapter_box.pack(side="left", padx=(0, 8))
        self.adapter_box.bind("<<ComboboxSelected>>", self._select_interface)
        self.scan_button = ttk.Button(adapter_bar, text="Làm mới card mạng", command=self._scan_interfaces)
        self.scan_button.pack(side="left")
        self.adapter_status = ttk.Label(config, text="Chọn card cùng mạng LAN với các Node khác.", wraplength=1050)
        self.adapter_status.grid(row=1, column=0, columnspan=6, sticky="w", pady=(0, 8))
        self._config_widgets.append(self.adapter_box)
        fields = (("Node ID", self.node_id, 5), ("Tên thành viên", self.name, 19),
                  ("IPv4 card LAN", self.ip, 17), ("Broadcast", self.broadcast, 17))
        for i, (title, var, width) in enumerate(fields):
            ttk.Label(config, text=title).grid(row=2, column=i, sticky="w", padx=(0, 12))
            widget = (ttk.Combobox(config, textvariable=var, values=tuple(range(1, 7)),
                                   state="readonly", width=width) if i == 0 else
                      ttk.Entry(config, textvariable=var, width=width))
            widget.grid(row=3, column=i, sticky="w", padx=(0, 12), pady=(3, 0))
            self._config_widgets.append(widget)
        self.start_button = ttk.Button(config, text="Khởi động Node", command=self._start)
        self.start_button.grid(row=3, column=4, padx=4)
        self.stop_button = ttk.Button(config, text="Dừng Node", command=self._stop, state="disabled")
        self.stop_button.grid(row=3, column=5, padx=4)
        advanced = ttk.Frame(config)
        advanced.grid(row=4, column=0, columnspan=6, sticky="w", pady=(9, 0))
        for title, var in (("Bind IPv4", self.bind_host), ("TCP", self.tcp_port),
                           ("UDP", self.udp_port), ("Cổng discovery", self.discovery_port)):
            ttk.Label(advanced, text=title).pack(side="left", padx=(0, 5))
            widget = ttk.Entry(advanced, textvariable=var, width=16 if title == "Bind IPv4" else 7)
            widget.pack(side="left", padx=(0, 15))
            self._config_widgets.append(widget)
        clock_frame = ttk.Frame(outer, padding=(8, 10))
        clock_frame.grid(row=2, column=0, sticky="ew")
        clock_frame.columnconfigure(1, weight=1)
        self.clock_label = ttk.Label(clock_frame, text="--:--:--.---", font=("Consolas", 38, "bold"))
        self.clock_label.grid(row=0, column=0, rowspan=2, sticky="w", padx=(0, 25))
        self.clock_status = ttk.Label(clock_frame, text="Đồng hồ ảo chưa chạy", font=("Segoe UI", 11))
        self.clock_status.grid(row=0, column=1, sticky="w")
        self.leader_label = ttk.Label(clock_frame, text="Master: -- • phiên: --", wraplength=650)
        self.leader_label.grid(row=1, column=1, sticky="w")
        self.session_detail = ttk.Label(clock_frame, text="Chưa có phiên đồng bộ.",
                                        wraplength=650, justify="left", foreground="#475569")
        self.session_detail.grid(row=2, column=1, sticky="w", pady=(4, 0))
        control = ttk.LabelFrame(outer, text="Điều khiển", padding=8)
        control.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        self.sync_button = ttk.Button(control, text="Đồng bộ ngay", command=lambda: self._submit(CommandKind.SYNC_NOW))
        self.sync_button.grid(row=0, column=0, padx=3)
        self.auto_box = ttk.Checkbutton(control, text="Tự động", variable=self.auto_sync)
        self.auto_box.grid(row=0, column=1, padx=4)
        self.interval_box = ttk.Combobox(control, textvariable=self.interval, values=(5, 10), width=4, state="readonly")
        self.interval_box.grid(row=0, column=2)
        ttk.Label(control, text="giây").grid(row=0, column=3, padx=4)
        self.apply_button = ttk.Button(control, text="Áp dụng chu kỳ", command=self._configure_sync)
        self.apply_button.grid(row=0, column=4, padx=4)
        self.master_button = ttk.Button(control, text="Chỉ định máy này làm Master",
                                       command=lambda: self._submit(CommandKind.FORCE_MASTER, "Selected in Node dashboard"))
        self.master_button.grid(row=0, column=5, padx=4)
        self.pause_button = ttk.Button(control, text="Tạm dừng giờ", command=self._pause)
        self.pause_button.grid(row=2, column=0, padx=4, pady=(8, 0), sticky="w")
        self.reset_button = ttk.Button(control, text="Đặt về giờ hệ thống", command=lambda: self._submit(CommandKind.RESET_CLOCK))
        self.reset_button.grid(row=2, column=1, columnspan=4, padx=4, pady=(8, 0), sticky="w")
        drift = ttk.Frame(control)
        drift.grid(row=1, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Label(drift, text="Tạo lệch mô phỏng:").pack(side="left", padx=4)
        self._drift_buttons = []
        for title, value in (("+5s", 5000), ("-10s", -10000), ("+60s", 60000)):
            button = ttk.Button(drift, text=title, command=lambda v=value: self._submit(CommandKind.INJECT_OFFSET, v))
            button.pack(side="left", padx=3)
            self._drift_buttons.append(button)
        ttk.Entry(drift, textvariable=self.custom_offset, width=8).pack(side="left", padx=(14, 3))
        ttk.Label(drift, text="giây").pack(side="left")
        self.custom_button = ttk.Button(drift, text="Tạo lệch", command=self._inject)
        self.custom_button.pack(side="left", padx=5)
        self.auto_status = ttk.Label(drift, text="")
        self.auto_status.pack(side="left", padx=12)
        notebook = ttk.Notebook(outer)
        notebook.grid(row=4, column=0, sticky="nsew")
        self.table = NodeTable(notebook)
        self.chart = ConvergenceChart(notebook)
        logs = ttk.Frame(notebook, padding=8)
        notebook.add(self.table, text="  Cụm 6 Node  ")
        notebook.add(self.chart, text="  Biểu đồ hội tụ  ")
        notebook.add(logs, text="  Nhật ký  ")
        self.log = tk.Text(logs, background="#0f172a", foreground="#e2e8f0", font=("Consolas", 10),
                           wrap="word", height=10, state="disabled", relief="flat")
        self.log.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(logs, orient="vertical", command=self.log.yview)
        scroll.pack(side="right", fill="y")
        self.log.configure(yscrollcommand=scroll.set)
        conflict_bar = ttk.Frame(outer)
        conflict_bar.grid(row=5, column=0, sticky="ew", pady=(10, 3))
        ttk.Label(conflict_bar, text="Node khởi động lại:").pack(side="left", padx=(0, 6))
        self.conflict_box = ttk.Combobox(conflict_bar, textvariable=self.conflict, width=62, state="readonly")
        self.conflict_box.pack(side="left")
        self.restart_button = ttk.Button(conflict_bar, text="Xác nhận thay thế Node cũ", command=self._accept_restart)
        self.restart_button.pack(side="left", padx=8)
        self.status = ttk.Label(outer, text="Chọn Node ID duy nhất và nhập IPv4 card LAN trước khi khởi động.",
                                wraplength=1150)
        self.status.grid(row=6, column=0, sticky="w", pady=(5, 0))
        self._set_controls(False, None)

    def _network_idle(self):
        return not self._closing and (self.runtime is None or self.runtime.snapshot().state in (
            RuntimeState.STOPPED, RuntimeState.FAILED))

    def _scan_interfaces(self):
        if self._scanning or not self._network_idle():
            return
        self._scanning = True
        self.scan_button.configure(state="disabled")
        self.adapter_status.configure(text="Đang đọc cấu hình card IPv4…")

        def collect():
            try:
                result = (list_ipv4_adapters(), None)
            except Exception as exc:
                result = ((), str(exc))
            self._adapter_results.put_nowait(result)

        try:
            threading.Thread(target=collect, name="BerkeleyInterfaceScan", daemon=True).start()
        except RuntimeError as exc:
            self._scanning = False
            self.adapter_status.configure(text="Không thể đọc card mạng: " + str(exc))

    def _consume_interfaces(self):
        try:
            adapters, error = self._adapter_results.get_nowait()
        except Empty:
            return
        self._scanning = False
        self._adapters = {adapter.label: adapter for adapter in adapters}
        self.adapter_box.configure(values=("Nhập IP thủ công", *self._adapters))
        if self.adapter.get() not in self._adapters:
            self.adapter.set("Nhập IP thủ công")
        if error:
            text = "Không đọc được card mạng; có thể nhập IP/broadcast thủ công. " + error
        else:
            text = "Tìm thấy {} địa chỉ IPv4. Chọn card LAN/Hotspot/mạng máy ảo dùng chung; IP hiện tại được giữ nguyên.".format(len(adapters))
        self.adapter_status.configure(text=text)

    def _select_interface(self, event=None):
        if not self._network_idle():
            return
        adapter = self._adapters.get(self.adapter.get())
        if adapter is None:
            self.adapter_status.configure(text="Nhập IP/broadcast thủ công; bind 0.0.0.0 để nhận broadcast trên Windows.")
            return
        if adapter.broadcast is None:
            self.adapter_status.configure(text="Card /31 hoặc /32 không có broadcast subnet; hãy chọn card LAN khác hoặc cấu hình thủ công.")
            self.adapter.set("Nhập IP thủ công")
            return
        self.ip.set(adapter.ip)
        self.broadcast.set(adapter.broadcast)
        # Windows receives subnet broadcasts reliably on the wildcard bind.
        self.bind_host.set("0.0.0.0")
        self.adapter_status.configure(text="{} • subnet {} • broadcast {}{}".format(
            adapter.name, adapter.network, adapter.broadcast,
            " • loopback chỉ dùng trên máy này" if adapter.is_loopback else ""))

    def _read_config(self):
        node_id = int(self.node_id.get())
        if node_id not in range(1, 7):
            raise ValueError("Node ID phải từ 1 đến 6")
        name = self.name.get().strip()
        if not name or len(name) > 128:
            raise ValueError("Tên phải có từ 1 đến 128 ký tự")
        ip, bind, broadcast = (v.get().strip() for v in (self.ip, self.bind_host, self.broadcast))
        address = IPv4Address(ip)
        if address.is_unspecified or address.is_multicast or ip == "255.255.255.255":
            raise ValueError("Hãy nhập IPv4 unicast của card LAN")
        IPv4Address(bind)
        IPv4Address(broadcast)
        selected = self._adapters.get(self.adapter.get())
        if selected and (ip != selected.ip or broadcast != selected.broadcast):
            raise ValueError("IP/broadcast đã khác card được chọn. Chọn lại card hoặc chuyển sang Nhập IP thủ công.")
        if bind != "0.0.0.0" and bind != ip:
            raise ValueError("Bind IPv4 phải là 0.0.0.0 hoặc trùng IPv4 LAN đã chọn")
        ports = tuple(int(v.get()) for v in (self.tcp_port, self.udp_port, self.discovery_port))
        if any(not 1 <= value <= 65535 for value in ports):
            raise ValueError("Cổng phải từ 1 đến 65535")
        interval = int(self.interval.get())
        if interval not in (5, 10):
            raise ValueError("Chu kỳ phải là 5 hoặc 10 giây")
        return NodeConfig(node_id, name, ip, bind, ports[0], ports[1], broadcast,
                          ports[2], self.auto_sync.get(), interval)

    def _start(self):
        if self._closing:
            return
        if self.runtime and self.runtime.snapshot().state not in (RuntimeState.STOPPED, RuntimeState.FAILED):
            return
        try:
            config = self._read_config()
            runtime = self._factory(config)
            runtime.start()
        except (ValueError, TypeError, RuntimeError, OSError) as exc:
            messagebox.showerror("Không thể khởi động", str(exc), parent=self.root)
            return
        self.runtime = runtime
        self._lost = 0
        self._last_error = None
        self._conflict_targets = {}
        self._conflict_labels = ()
        self.conflict_box.configure(values=())
        self.conflict.set("")
        self.table.update_rows(project_rows(None))
        self.clock_label.configure(text="--:--:--.---")
        self.clock_status.configure(text="Đồng hồ ảo đang khởi động")
        self.leader_label.configure(text="Master: -- • phiên: --")
        self.session_detail.configure(text="Chưa có phiên đồng bộ.")
        self.auto_status.configure(text="")
        self.chart.history.clear()
        self.chart.redraw()
        self.status.configure(text="Đang khởi động và phát hiện các Node…")
        self._update()

    def _stop(self):
        if self.runtime:
            self.runtime.stop()
            self.status.configure(text="Đang dừng Node và đóng các luồng mạng…")
            self._update()

    def _submit(self, kind, *args):
        if not self.runtime or self._closing:
            return
        try:
            token = self.runtime.submit(kind, *args)
            self._append("LỆNH {} [{}] đã vào hàng đợi".format(kind.value, str(token)[:8]))
        except (RuntimeError, TypeError, Full) as exc:
            self.status.configure(text=str(exc))
            self._append("LỖI: " + str(exc))

    def _configure_sync(self):
        try:
            interval = int(self.interval.get())
            if interval not in (5, 10):
                raise ValueError("Chu kỳ phải là 5 hoặc 10 giây")
        except ValueError as exc:
            self.status.configure(text=str(exc))
            return
        self._submit(CommandKind.CONFIGURE_SYNC, self.auto_sync.get(), interval)

    def _inject(self):
        import re
        from decimal import Decimal, DecimalException
        try:
            text = self.custom_offset.get().strip()
            if not re.fullmatch(r"[+-]?\d{1,5}(?:\.\d{1,3})?", text):
                raise ValueError("Nhập số giây thập phân, ví dụ -2.5 hoặc 0.125")
            value = Decimal(text)
            milliseconds = value * 1000
            if not value.is_finite() or not -86400 <= value <= 86400 or milliseconds != milliseconds.to_integral_value():
                raise ValueError("Nhập số giây trong ±86400, tối đa 3 chữ số thập phân")
            self._submit(CommandKind.INJECT_OFFSET, int(milliseconds))
        except (DecimalException, ValueError, OverflowError) as exc:
            self.status.configure(text="Độ lệch không hợp lệ: " + str(exc))

    def _pause(self):
        clock = self.runtime.snapshot().clock if self.runtime else None
        if clock:
            self._submit(CommandKind.RESUME_CLOCK if clock.is_paused else CommandKind.PAUSE_CLOCK)

    def _accept_restart(self):
        target = self._conflict_targets.get(self.conflict.get())
        if target:
            self._submit(CommandKind.ACCEPT_RESTART, target)

    def _append(self, detail):
        # Escape embedded newlines so incoming names/reasons cannot forge log rows.
        detail = detail.replace("\r", "\\r").replace("\n", "\\n")
        at_bottom = self.log.yview()[1] >= 0.99
        self.log.configure(state="normal")
        self.log.insert("end", "{}  {}\n".format(datetime.now().strftime("%H:%M:%S"), detail))
        self._log_lines += 1
        if self._log_lines > 2000:
            count = self._log_lines - 2000
            self.log.delete("1.0", "{}.0".format(count + 1))
            self._log_lines = 2000
        self.log.configure(state="disabled")
        if at_bottom:
            self.log.see("end")

    def _set_controls(self, running, node):
        master = running and node is not None and node.election.role == NodeRole.MASTER
        for widget in (self.apply_button, self.master_button, self.pause_button, self.reset_button,
                       self.custom_button, *self._drift_buttons):
            widget.configure(state="normal" if running and not self._closing else "disabled")
        active = running and not self._closing
        idle = self.runtime is None or self.runtime.snapshot().state in (RuntimeState.STOPPED, RuntimeState.FAILED)
        configurable = not self._closing and (running or idle)
        self.auto_box.configure(state="normal" if configurable else "disabled")
        self.interval_box.configure(state="readonly" if configurable else "disabled")
        self.sync_button.configure(state="normal" if master and not self._closing else "disabled")
        self.restart_button.configure(state="normal" if active and self._conflict_targets else "disabled")

    def _update(self):
        self._consume_interfaces()
        view = self.runtime.snapshot() if self.runtime else None
        node = view.node if view else None
        running = view is not None and view.state == RuntimeState.RUNNING
        busy = view is not None and view.state in (RuntimeState.STARTING, RuntimeState.RUNNING, RuntimeState.STOPPING)
        self.start_button.configure(state="disabled" if busy or self._closing else "normal")
        self.stop_button.configure(state="normal" if busy and view.state != RuntimeState.STOPPING and not self._closing else "disabled")
        self.scan_button.configure(state="disabled" if busy or self._closing or self._scanning else "normal")
        for widget in self._config_widgets:
            widget.configure(state="disabled" if busy or self._closing else "readonly" if isinstance(widget, ttk.Combobox) else "normal")
        if view:
            for event in self.runtime.drain_events():
                prefix = event.notice.kind.value if event.notice else event.kind
                peer = " Node {}".format(event.notice.peer.node_id) if event.notice and event.notice.peer else ""
                command = " [{} / {} / {}]".format(str(event.command_id)[:8], event.command.value,
                                                   "OK" if event.success else "LỖI") if event.command_id else ""
                self._append("{}{}{}: {}".format(prefix, peer, command, event.detail))
                if event.success is False or event.kind == "ERROR":
                    self.status.configure(text=event.detail)
            lost = view.dropped_events + (node.dropped_notices if node else 0)
            if lost > self._lost:
                self._append("CẢNH BÁO: đã mất {} sự kiện/thông báo do bộ đệm đầy".format(lost))
            self._lost = lost
            if view.last_error and view.last_error != self._last_error:
                self.status.configure(text=view.last_error)
                self._last_error = view.last_error
            if view.clock:
                self.clock_label.configure(text=clock_text(view.clock.time_ms))
                self.clock_status.configure(text="{}{} • Đồng bộ Step: chỉnh giờ trực tiếp".format(
                    view.clock.state.value, " / TẠM DỪNG" if view.clock.is_paused else ""))
                self.pause_button.configure(text="Tiếp tục giờ" if view.clock.is_paused else "Tạm dừng giờ")
            if node:
                election = node.election
                self.role_label.configure(text="{} • {}".format(election.role.value, view.state.value),
                                          foreground="#b45309" if election.role == NodeRole.MASTER else "#166534")
                sync = node.synchronization
                progress, detail = synchronization_description(sync, election.leader)
                self.session_detail.configure(text=detail + "\n" + monitoring_description(node))
                self.leader_label.configure(text="Master: {} • {} • phiên {}".format(
                    election.leader.node.node_id if election.leader else "đang bầu chọn", election.mode.value, progress))
                self.auto_status.configure(text="Đang áp dụng: {} / {}s".format(
                    "tự động" if node.auto_sync else "thủ công", node.sync_interval_s))
                self.table.update_rows(project_rows(node, view.clock, local_ip=self.runtime.config.ip,
                                                    local_name=self.runtime.config.display_name))
                self.chart.update_snapshot(node)
                peers = {p.identity.node_id: p for p in node.peers}
                targets = {}
                for candidate in node.conflicts:
                    old = peers.get(candidate.identity.node_id)
                    if old and old.identity != candidate.identity:
                        label = "Node {}: cũ {} → mới {} @ {}".format(candidate.identity.node_id,
                            str(old.identity.boot_id)[:8], str(candidate.identity.boot_id)[:8], candidate.ip)
                        targets[label] = old.identity
                self._conflict_targets = targets
                labels = tuple(targets)
                if labels != self._conflict_labels:
                    self.conflict_box.configure(values=labels)
                    self.conflict.set(labels[0] if labels else "")
                    self._conflict_labels = labels
            else:
                self.role_label.configure(text=view.state.value)
        self._set_controls(running, node)

    def _refresh(self):
        self._after_id = None
        try:
            self._update()
        except Exception as exc:
            # Keep shutdown and error reporting available even if a view fails.
            self.status.configure(text="Lỗi cập nhật giao diện: {}: {}".format(type(exc).__name__, exc))
        if self._closing and (self.runtime is None or self.runtime.wait_stopped(0)):
            self.root.destroy()
            return
        self._after_id = self.root.after(100, self._refresh)

    def close(self):
        self._closing = True
        if self.runtime:
            self.runtime.stop()
        self.status.configure(text="Đang đóng kết nối và chờ luồng Node dừng…")
