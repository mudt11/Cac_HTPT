"""
Giao diện đồ họa (GUI) Máy chủ Thời gian Berkeley (Berkeley Time Server - Master Node).
Lập trình bằng Tkinter / ttk theo chuẩn Desktop App Windows.
Có đồng hồ mô phỏng tích tắc từng giây theo thời gian thực (HH:mm:ss).
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import socket
import datetime
import threading
from typing import Dict, Any

from NetworkCore.server_socket import BerkeleyServerNetwork
from NetworkCore.protocol import minutes_to_time, time_to_minutes


def get_local_lan_ip() -> str:
    """Lấy địa chỉ IP mạng nội bộ (LAN) của máy hiện tại."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


class ServerAppGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Berkeley Time Server - Máy chủ Thời gian (Master Node)")
        self.root.geometry("960x720")
        self.root.minsize(860, 620)

        # Trạng thái đồng hồ mô phỏng Server (Giờ : Phút : Giây)
        now = datetime.datetime.now()
        self.sim_hour = now.hour
        self.sim_minute = now.minute
        self.sim_second = now.second
        self.is_clock_running = True

        # Module mạng Server
        self.server_network: BerkeleyServerNetwork = None
        self.local_ip = get_local_lan_ip()

        self._setup_styles()
        self._build_ui()

        # Bắt đầu vòng lặp tích tắc từng giây của đồng hồ
        self._clock_tick()

        # Tự động khởi động Server
        self._toggle_server()

    @property
    def current_sim_time_minute(self) -> str:
        """Trả về chuỗi HH:mm phục vụ thuật toán Berkeley cấp phút"""
        return f"{self.sim_hour:02d}:{self.sim_minute:02d}"

    @property
    def current_sim_time_full(self) -> str:
        """Trả về chuỗi HH:mm:ss hiển thị trực quan trên giao diện"""
        return f"{self.sim_hour:02d}:{self.sim_minute:02d}:{self.sim_second:02d}"

    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Header.TFrame", background="#1e293b")
        style.configure("Header.TLabel", background="#1e293b", foreground="#f8fafc", font=("Segoe UI", 10, "bold"))
        style.configure("StatusGreen.TLabel", background="#1e293b", foreground="#4ade80", font=("Segoe UI", 10, "bold"))
        style.configure("StatusRed.TLabel", background="#1e293b", foreground="#f87171", font=("Segoe UI", 10, "bold"))

        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        style.configure("Treeview", font=("Segoe UI", 10), rowheight=26)

    def _build_ui(self):
        # 1. THANH TRẠNG THÁI MẠNG (TOP BAR)
        top_frame = ttk.Frame(self.root, style="Header.TFrame", padding=12)
        top_frame.pack(fill=tk.X, side=tk.TOP)

        self.lbl_server_status = ttk.Label(top_frame, text="● Trạng thái: Đang dừng", style="StatusRed.TLabel")
        self.lbl_server_status.pack(side=tk.LEFT, padx=5)

        ttk.Label(top_frame, text=f"|  IP Server (LAN): {self.local_ip}", style="Header.TLabel").pack(side=tk.LEFT, padx=10)

        ttk.Label(top_frame, text="|  Cổng:", style="Header.TLabel").pack(side=tk.LEFT, padx=(10, 2))
        self.entry_port = ttk.Entry(top_frame, width=6, font=("Segoe UI", 10))
        self.entry_port.insert(0, "8888")
        self.entry_port.pack(side=tk.LEFT, padx=2)

        ttk.Label(top_frame, text="|  Giao thức:", style="Header.TLabel").pack(side=tk.LEFT, padx=(10, 2))
        self.combo_proto = ttk.Combobox(top_frame, values=["TCP", "UDP"], width=5, state="readonly", font=("Segoe UI", 10))
        self.combo_proto.set("TCP")
        self.combo_proto.pack(side=tk.LEFT, padx=2)

        self.btn_toggle_server = tk.Button(
            top_frame, text="BẬT SERVER", bg="#10b981", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=10, pady=2, cursor="hand2", command=self._toggle_server
        )
        self.btn_toggle_server.pack(side=tk.RIGHT, padx=5)

        # 2. KHU VỰC ĐỒNG HỒ SERVER & NÚT PHÁT TÍN HIỆU ĐỒNG BỘ
        clock_frame = tk.Frame(self.root, bg="#f1f5f9", pady=12, relief=tk.GROOVE, bd=1)
        clock_frame.pack(fill=tk.X, padx=15, pady=8)

        title_clock = tk.Label(
            clock_frame, text="ĐỒNG HỒ MÁY CHỦ (SERVER TIME - REAL-TIME CLOCK)",
            bg="#f1f5f9", fg="#475569", font=("Segoe UI", 10, "bold")
        )
        title_clock.pack(anchor=tk.CENTER)

        # Hiển thị số đồng hồ to với giây HH:mm:ss
        self.lbl_big_clock = tk.Label(
            clock_frame, text=self.current_sim_time_full, bg="#f1f5f9", fg="#0f172a", font=("Segoe UI", 46, "bold")
        )
        self.lbl_big_clock.pack(anchor=tk.CENTER, pady=2)

        self.lbl_sub_clock = tk.Label(
            clock_frame, text="● Đồng hồ đang tích tắc theo thời gian thực (Giờ : Phút : Giây)",
            bg="#f1f5f9", fg="#16a34a", font=("Segoe UI", 9, "bold")
        )
        self.lbl_sub_clock.pack()

        # Nút điều chỉnh giờ mô phỏng
        ctrl_frame = tk.Frame(clock_frame, bg="#f1f5f9")
        ctrl_frame.pack(pady=8)

        tk.Button(ctrl_frame, text="+1 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(1)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="-1 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(-1)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="+10 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(10)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="-10 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(-10)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="Chỉnh giờ tùy ý", font=("Segoe UI", 9), command=self._set_custom_time).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="Lấy giờ máy tính", font=("Segoe UI", 9), command=self._reset_real_time).pack(side=tk.LEFT, padx=3)

        self.btn_pause_clock = tk.Button(
            ctrl_frame, text="⏸️ Tạm dừng", font=("Segoe UI", 9, "bold"),
            bg="#e2e8f0", command=self._toggle_clock_pause
        )
        self.btn_pause_clock.pack(side=tk.LEFT, padx=6)

        # NÚT ĐỒNG BỘ CHÍNH NỔI BẬT
        self.btn_sync = tk.Button(
            clock_frame, text="⚡ ĐỒNG BỘ TẤT CẢ MÁY TRẠM NGAY (BROADCAST SYNC)",
            bg="#2563eb", fg="white", font=("Segoe UI", 12, "bold"),
            relief=tk.RAISED, padx=25, pady=8, cursor="hand2", command=self._trigger_sync_action
        )
        self.btn_sync.pack(pady=8)

        # 3. BẢNG DANH SÁCH MÁY TRẠM ĐANG KẾT NỐI
        table_label_frame = tk.Frame(self.root, padx=15)
        table_label_frame.pack(fill=tk.X)
        tk.Label(table_label_frame, text="DANH SÁCH MÁY TRẠM TRONG MẠNG LAN (CLIENTS)", font=("Segoe UI", 11, "bold"), fg="#1e293b").pack(side=tk.LEFT, pady=4)
        self.lbl_client_count = tk.Label(table_label_frame, text="(Đang kết nối: 0 máy)", font=("Segoe UI", 10), fg="#64748b")
        self.lbl_client_count.pack(side=tk.LEFT, padx=8)

        table_frame = tk.Frame(self.root, padx=15)
        table_frame.pack(fill=tk.BOTH, expand=True)

        columns = ("id", "ip", "reported_time", "avg_time", "offset", "status")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=5)
        self.tree.heading("id", text="Tên Máy Trạm")
        self.tree.heading("ip", text="Địa chỉ IP")
        self.tree.heading("reported_time", text="Giờ Báo Về")
        self.tree.heading("avg_time", text="Giờ Trung Bình (Avg)")
        self.tree.heading("offset", text="Độ Lệch Cần Chỉnh")
        self.tree.heading("status", text="Trạng Thái")

        self.tree.column("id", width=140, anchor=tk.W)
        self.tree.column("ip", width=130, anchor=tk.CENTER)
        self.tree.column("reported_time", width=110, anchor=tk.CENTER)
        self.tree.column("avg_time", width=140, anchor=tk.CENTER)
        self.tree.column("offset", width=120, anchor=tk.CENTER)
        self.tree.column("status", width=120, anchor=tk.CENTER)

        tree_scroll = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 4. KHU VỰC THÔNG BÁO TRỰC QUAN (ALERT BANNER)
        self.alert_frame = tk.Frame(self.root, bg="#dcfce7", relief=tk.SOLID, bd=1, padx=12, pady=7)
        self.alert_frame.pack(fill=tk.X, padx=15, pady=(6, 4))
        self.lbl_alert = tk.Label(
            self.alert_frame,
            text="Hệ thống sẵn sàng. Nhấn 'ĐỒNG BỘ TẤT CẢ MÁY TRẠM NGAY' khi các máy trạm đã kết nối.",
            bg="#dcfce7", fg="#166534", font=("Segoe UI", 10, "bold")
        )
        self.lbl_alert.pack(anchor=tk.W)

        # 5. KHUNG NHẬT KÝ (LOGS)
        log_frame = tk.Frame(self.root, padx=15, pady=4)
        log_frame.pack(fill=tk.X, side=tk.BOTTOM)
        tk.Label(log_frame, text="Nhật ký hoạt động (Gói tin Socket):", font=("Segoe UI", 9, "bold"), fg="#475569").pack(anchor=tk.W)

        self.txt_log = tk.Text(log_frame, height=4, font=("Consolas", 9), bg="#f8fafc", fg="#334155")
        log_scroll = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.txt_log.yview)
        self.txt_log.configure(yscrollcommand=log_scroll.set)
        self.txt_log.pack(side=tk.LEFT, fill=tk.X, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    # ================= LOGIC ĐỒNG HỒ TÍCH TẮC TỪNG GIÂY =================

    def _clock_tick(self):
        """Vòng lặp tự động tăng 1 giây mỗi 1000ms"""
        if self.is_clock_running:
            self.sim_second += 1
            if self.sim_second >= 60:
                self.sim_second = 0
                self.sim_minute += 1
                if self.sim_minute >= 60:
                    self.sim_minute = 0
                    self.sim_hour = (self.sim_hour + 1) % 24
            self.lbl_big_clock.config(text=self.current_sim_time_full)

        self.root.after(1000, self._clock_tick)

    def _toggle_clock_pause(self):
        self.is_clock_running = not self.is_clock_running
        if self.is_clock_running:
            self.btn_pause_clock.config(text="⏸️ Tạm dừng", bg="#e2e8f0")
            self.lbl_sub_clock.config(text="● Đồng hồ đang tích tắc theo thời gian thực", fg="#16a34a")
            self._log("[ĐỒNG HỒ] Tiếp tục chạy đếm giây.")
        else:
            self.btn_pause_clock.config(text="▶️ Tiếp tục", bg="#fef08a")
            self.lbl_sub_clock.config(text="⏸️ Đồng hồ đang tạm dừng (Đóng băng thời gian)", fg="#d97706")
            self._log("[ĐỒNG HỒ] Đã tạm dừng đồng hồ.")

    def _adjust_sim_time(self, delta_minutes: int):
        mins = (self.sim_hour * 60 + self.sim_minute + delta_minutes) % 1440
        self.sim_hour = mins // 60
        self.sim_minute = mins % 60
        self.lbl_big_clock.config(text=self.current_sim_time_full)
        self._log(f"[ĐỒNG HỒ] Đã chỉnh giờ Server thành {self.current_sim_time_full}")

    def _set_custom_time(self):
        val = simpledialog.askstring("Chỉnh giờ", "Nhập giờ mới cho Server (Định dạng HH:mm hoặc HH:mm:ss):", parent=self.root)
        if val:
            try:
                parts = val.strip().split(":")
                h = int(parts[0])
                m = int(parts[1])
                s = int(parts[2]) if len(parts) >= 3 else 0
                if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59:
                    self.sim_hour = h
                    self.sim_minute = m
                    self.sim_second = s
                    self.lbl_big_clock.config(text=self.current_sim_time_full)
                    self._log(f"[ĐỒNG HỒ] Đã đổi giờ Server thành {self.current_sim_time_full}")
                else:
                    raise ValueError()
            except Exception:
                messagebox.showerror("Lỗi định dạng", "Vui lòng nhập đúng định dạng HH:mm hoặc HH:mm:ss", parent=self.root)

    def _reset_real_time(self):
        now = datetime.datetime.now()
        self.sim_hour = now.hour
        self.sim_minute = now.minute
        self.sim_second = now.second
        self.lbl_big_clock.config(text=self.current_sim_time_full)
        self._log(f"[ĐỒNG HỒ] Đã đồng bộ theo giờ thật của máy tính: {self.current_sim_time_full}")

    # ================= LOGIC MẠNG & ĐIỀU KHIỂN =================

    def _toggle_server(self):
        if self.server_network and self.server_network.is_running:
            self.server_network.stop()
            self.server_network = None
            self.lbl_server_status.config(text="● Trạng thái: Đã dừng", style="StatusRed.TLabel")
            self.btn_toggle_server.config(text="BẬT SERVER", bg="#10b981")
            self.combo_proto.config(state="readonly")
            self.entry_port.config(state="normal")
            self._set_alert("Máy chủ đã dừng lắng nghe.", is_error=True)
        else:
            try:
                port = int(self.entry_port.get().strip())
            except ValueError:
                messagebox.showerror("Lỗi Cổng", "Cổng lắng nghe phải là số nguyên (ví dụ: 8888)", parent=self.root)
                return

            proto = self.combo_proto.get()
            self.server_network = BerkeleyServerNetwork(
                host="0.0.0.0",
                port=port,
                protocol_type=proto,
                on_client_connected=self._on_client_connected,
                on_client_disconnected=self._on_client_disconnected,
                on_time_reported=self._on_time_reported,
                on_sync_complete=self._on_sync_complete,
                on_client_ack=self._on_client_ack,
                on_log=self._log
            )

            success = self.server_network.start()
            if success:
                self.lbl_server_status.config(text=f"● Đang chạy ({proto} Cổng {port})", style="StatusGreen.TLabel")
                self.btn_toggle_server.config(text="TẮT SERVER", bg="#ef4444")
                self.combo_proto.config(state="disabled")
                self.entry_port.config(state="disabled")
                self._set_alert(f"Máy chủ đang hoạt động tại IP {self.local_ip}:{port}. Đang chờ các máy trạm kết nối...", is_error=False)
            else:
                messagebox.showerror("Khởi động thất bại", f"Không thể mở cổng {port}. Vui lòng thử cổng khác hoặc kiểm tra Tường lửa.", parent=self.root)

    def _trigger_sync_action(self):
        if not self.server_network or not self.server_network.is_running:
            messagebox.showwarning("Cảnh báo", "Vui lòng BẬT SERVER trước khi thực hiện đồng bộ!", parent=self.root)
            return

        with self.server_network.clients_lock:
            count = len(self.server_network.connected_clients)

        if count == 0:
            messagebox.showwarning(
                "Chưa có máy trạm",
                "Hiện chưa có máy trạm nào kết nối tới Server!\n"
                f"Vui lòng khởi động các ứng dụng Client và kết nối tới IP: {self.local_ip}:{self.entry_port.get()}",
                parent=self.root
            )
            return

        # Cập nhật trạng thái bảng hiển thị
        for item in self.tree.get_children():
            self.tree.item(item, values=(
                self.tree.item(item, "values")[0],
                self.tree.item(item, "values")[1],
                "Đang lấy...",
                "Đang tính...",
                "...",
                "Đang gửi yêu cầu..."
            ))

        # Gửi mốc giờ cấp phút của Server đi
        sent_count = self.server_network.trigger_sync(self.current_sim_time_minute)
        self._set_alert(f"Đã phát tín hiệu yêu cầu mốc thời gian tới {sent_count} máy trạm. Đang thu thập dữ liệu...", is_error=False)

    def _set_alert(self, message: str, is_error: bool = False):
        def _update():
            if is_error:
                self.alert_frame.config(bg="#fee2e2")
                self.lbl_alert.config(text=message, bg="#fee2e2", fg="#991b1b")
            else:
                self.alert_frame.config(bg="#dcfce7")
                self.lbl_alert.config(text=message, bg="#dcfce7", fg="#166534")
        self.root.after(0, _update)

    def _log(self, text: str):
        def _append():
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            self.txt_log.insert(tk.END, f"[{timestamp}] {text}\n")
            self.txt_log.see(tk.END)
        self.root.after(0, _append)

    # ================= CALLBACKS TỪ LUỒNG MẠNG =================

    def _on_client_connected(self, client_id: str, ip: str):
        def _update():
            found = False
            for item in self.tree.get_children():
                if self.tree.item(item, "values")[0] == client_id:
                    self.tree.item(item, values=(client_id, ip, "-", "-", "-", "Đã kết nối"))
                    found = True
                    break
            if not found:
                self.tree.insert("", tk.END, values=(client_id, ip, "-", "-", "-", "Đã kết nối"))

            total = len(self.tree.get_children())
            self.lbl_client_count.config(text=f"(Đang kết nối: {total} máy)")
            self._set_alert(f"Máy trạm mới tham gia: {client_id} ({ip}). Tổng cộng: {total} máy.")
        self.root.after(0, _update)

    def _on_client_disconnected(self, client_id: str):
        def _update():
            for item in self.tree.get_children():
                if self.tree.item(item, "values")[0] == client_id:
                    self.tree.delete(item)
                    break
            total = len(self.tree.get_children())
            self.lbl_client_count.config(text=f"(Đang kết nối: {total} máy)")
        self.root.after(0, _update)

    def _on_time_reported(self, client_id: str, reported_time: str):
        def _update():
            for item in self.tree.get_children():
                vals = list(self.tree.item(item, "values"))
                if vals[0] == client_id:
                    vals[2] = reported_time
                    vals[5] = "Đã gửi giờ"
                    self.tree.item(item, values=vals)
                    break
        self.root.after(0, _update)

    def _on_sync_complete(self, avg_time: str, results: Dict[str, Any]):
        def _update():
            clients_info = results.get("clients", {})
            for item in self.tree.get_children():
                vals = list(self.tree.item(item, "values"))
                c_id = vals[0]
                if c_id in clients_info:
                    c_data = clients_info[c_id]
                    vals[2] = c_data["reported_time"]
                    vals[3] = avg_time
                    vals[4] = f"{c_data['offset']:+d} phút"
                    vals[5] = "Đang chờ Client xác nhận..."
                    self.tree.item(item, values=vals)

            # Cập nhật giờ của chính Server theo giờ trung bình
            old_server_time = self.current_sim_time_minute
            parts = avg_time.split(":")
            self.sim_hour = int(parts[0])
            self.sim_minute = int(parts[1])
            self.sim_second = 0  # Đưa giây về :00 để bắt đầu chu kỳ mới
            self.lbl_big_clock.config(text=self.current_sim_time_full)

            server_offset = results.get("server_offset", 0)
            success_msg = (
                f"🎉 ĐÃ TÍNH TOÁN XONG GIỜ TRUNG BÌNH!\n\n"
                f"• Giờ trung bình tính toán: {avg_time}:00\n"
                f"• Giờ cũ Server: {old_server_time} (Độ lệch Server: {server_offset:+d} phút)\n"
                f"• Đã gửi kết quả tới {len(clients_info)} máy trạm để xác nhận điều chỉnh."
            )
            self._set_alert(f"TÍNH TOÁN XONG: Giờ trung bình {avg_time}:00 đã gửi tới {len(clients_info)} máy trạm!")
            messagebox.showinfo("Đã phát kết quả (Berkeley Server)", success_msg, parent=self.root)

        self.root.after(0, _update)

    def _on_client_ack(self, client_id: str, ack_status: str):
        def _update():
            status_text = "Đã đồng bộ ✓" if ack_status == "SUCCESS" else "Từ chối điều chỉnh ✗"
            for item in self.tree.get_children():
                vals = list(self.tree.item(item, "values"))
                if vals[0] == client_id:
                    vals[5] = status_text
                    self.tree.item(item, values=vals)
                    break
        self.root.after(0, _update)
