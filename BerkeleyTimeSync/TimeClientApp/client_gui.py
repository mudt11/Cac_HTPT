"""
Giao diện đồ họa (GUI) Máy trạm Thời gian Berkeley (Berkeley Time Client - Workstation Node).
Lập trình bằng Tkinter / ttk theo chuẩn Desktop App Windows.
Có đồng hồ mô phỏng tích tắc từng giây theo thời gian thực (HH:mm:ss).
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import datetime
import random
from typing import Dict, Any

from NetworkCore.client_socket import BerkeleyClientNetwork
from NetworkCore.protocol import minutes_to_time, time_to_minutes, calculate_offset


class ClientAppGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Berkeley Time Client - Máy trạm Thời gian (Workstation Node)")
        self.root.geometry("860x680")
        self.root.minsize(780, 600)

        # Trạng thái đồng hồ mô phỏng của máy trạm (Giờ : Phút : Giây)
        # Mặc định khởi tạo giờ ngẫu nhiên lệch một chút so với giờ thật để dễ test
        now = datetime.datetime.now()
        initial_random_delta = random.randint(-15, 15)
        sim_minutes = (now.hour * 60 + now.minute + initial_random_delta) % 1440
        self.sim_hour = sim_minutes // 60
        self.sim_minute = sim_minutes % 60
        self.sim_second = random.randint(0, 59)
        self.is_clock_running = True

        # Thông tin đồng bộ gần nhất
        self.last_sent_time = "-"
        self.last_avg_received = "-"
        self.last_offset = "-"

        # Module mạng Client
        self.client_network: BerkeleyClientNetwork = None

        self._setup_styles()
        self._build_ui()

        # Bắt đầu vòng lặp tích tắc từng giây của đồng hồ
        self._clock_tick()

    @property
    def current_sim_time_minute(self) -> str:
        """Mốc giờ cấp phút gửi về Server: HH:mm"""
        return f"{self.sim_hour:02d}:{self.sim_minute:02d}"

    @property
    def current_sim_time_full(self) -> str:
        """Chuỗi hiển thị đầy đủ trên giao diện: HH:mm:ss"""
        return f"{self.sim_hour:02d}:{self.sim_minute:02d}:{self.sim_second:02d}"

    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Header.TFrame", background="#1e293b")
        style.configure("Header.TLabel", background="#1e293b", foreground="#f8fafc", font=("Segoe UI", 10, "bold"))

    def _build_ui(self):
        # 1. KHU VỰC CẤU HÌNH KẾT NỐI (CONNECTION SETTINGS)
        conn_frame = tk.LabelFrame(
            self.root, text=" Cấu hình kết nối tới Máy chủ (Connection Settings) ",
            font=("Segoe UI", 10, "bold"), fg="#1e293b", padx=12, pady=10
        )
        conn_frame.pack(fill=tk.X, padx=15, pady=8)

        # Hàng 1: IP Server, Port, Tên máy trạm
        row1 = tk.Frame(conn_frame)
        row1.pack(fill=tk.X, pady=3)

        tk.Label(row1, text="IP Máy chủ (Server IP):", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(0, 4))
        self.entry_server_ip = ttk.Entry(row1, width=15, font=("Segoe UI", 9))
        self.entry_server_ip.insert(0, "127.0.0.1")
        self.entry_server_ip.pack(side=tk.LEFT, padx=(0, 15))

        tk.Label(row1, text="Cổng (Port):", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(0, 4))
        self.entry_port = ttk.Entry(row1, width=6, font=("Segoe UI", 9))
        self.entry_port.insert(0, "8888")
        self.entry_port.pack(side=tk.LEFT, padx=(0, 15))

        tk.Label(row1, text="Tên máy trạm (Client ID):", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(0, 4))
        self.entry_client_id = ttk.Entry(row1, width=12, font=("Segoe UI", 9))
        default_id = f"Client-{random.randint(1, 5):02d}"
        self.entry_client_id.insert(0, default_id)
        self.entry_client_id.pack(side=tk.LEFT, padx=(0, 10))

        # Hàng 2: Giao thức, Nút kết nối, Badge trạng thái
        row2 = tk.Frame(conn_frame)
        row2.pack(fill=tk.X, pady=(5, 2))

        tk.Label(row2, text="Giao thức:", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT, padx=(0, 4))
        self.combo_proto = ttk.Combobox(row2, values=["TCP", "UDP"], width=5, state="readonly", font=("Segoe UI", 9))
        self.combo_proto.set("TCP")
        self.combo_proto.pack(side=tk.LEFT, padx=(0, 15))

        self.btn_connect = tk.Button(
            row2, text="KẾT NỐI TỚI SERVER", bg="#2563eb", fg="white", font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT, padx=12, pady=3, cursor="hand2", command=self._toggle_connection
        )
        self.btn_connect.pack(side=tk.LEFT, padx=(0, 15))

        self.lbl_conn_status = tk.Label(
            row2, text="● Chưa kết nối", font=("Segoe UI", 10, "bold"), fg="#dc2626"
        )
        self.lbl_conn_status.pack(side=tk.LEFT)

        # Hàng 3: Tùy chọn hỏi xác nhận trước khi điều chỉnh giờ
        row3 = tk.Frame(conn_frame)
        row3.pack(fill=tk.X, pady=(5, 0))

        self.var_ask_confirm = tk.BooleanVar(value=True)
        chk_confirm = tk.Checkbutton(
            row3, text="Yêu cầu hỏi xác nhận (Yes/No) trên máy trạm khi nhận kết quả từ Server",
            variable=self.var_ask_confirm, font=("Segoe UI", 9, "bold"), fg="#1e40af", cursor="hand2"
        )
        chk_confirm.pack(side=tk.LEFT)

        # 2. KHU VỰC ĐỒNG HỒ MÔ PHỎNG CỦA MÁY TRẠM
        clock_frame = tk.Frame(self.root, bg="#f8fafc", pady=12, relief=tk.GROOVE, bd=1)
        clock_frame.pack(fill=tk.X, padx=15, pady=6)

        tk.Label(
            clock_frame, text="ĐỒNG HỒ MÁY TRẠM (LOCAL CLIENT CLOCK - REAL-TIME)",
            bg="#f8fafc", fg="#475569", font=("Segoe UI", 10, "bold")
        ).pack(anchor=tk.CENTER)

        self.lbl_big_clock = tk.Label(
            clock_frame, text=self.current_sim_time_full, bg="#f8fafc", fg="#0f172a", font=("Segoe UI", 46, "bold")
        )
        self.lbl_big_clock.pack(anchor=tk.CENTER, pady=2)

        self.lbl_sub_clock = tk.Label(
            clock_frame, text="● Đồng hồ đang tích tắc theo thời gian thực (Giờ : Phút : Giây)",
            bg="#f8fafc", fg="#16a34a", font=("Segoe UI", 9, "bold")
        )
        self.lbl_sub_clock.pack()

        # Nút điều chỉnh giờ mô phỏng
        ctrl_frame = tk.Frame(clock_frame, bg="#f8fafc")
        ctrl_frame.pack(pady=8)

        tk.Button(ctrl_frame, text="+1 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(1)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="-1 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(-1)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="+10 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(10)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="-10 Phút", font=("Segoe UI", 9), command=lambda: self._adjust_sim_time(-10)).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="🎲 Giờ ngẫu nhiên", font=("Segoe UI", 9, "bold"), bg="#fef08a", command=self._randomize_time).pack(side=tk.LEFT, padx=4)
        tk.Button(ctrl_frame, text="Chỉnh giờ tùy ý", font=("Segoe UI", 9), command=self._set_custom_time).pack(side=tk.LEFT, padx=3)
        tk.Button(ctrl_frame, text="Lấy giờ máy tính", font=("Segoe UI", 9), command=self._reset_real_time).pack(side=tk.LEFT, padx=3)

        self.btn_pause_clock = tk.Button(
            ctrl_frame, text="⏸️ Tạm dừng", font=("Segoe UI", 9, "bold"),
            bg="#e2e8f0", command=self._toggle_clock_pause
        )
        self.btn_pause_clock.pack(side=tk.LEFT, padx=4)

        # 3. THẺ TÓM TẮT ĐỒNG BỘ (SYNC SUMMARY CARD)
        summary_frame = tk.LabelFrame(
            self.root, text=" Tóm tắt Kết quả Đồng bộ gần nhất (Sync Summary) ",
            font=("Segoe UI", 10, "bold"), fg="#1e293b", padx=12, pady=8
        )
        summary_frame.pack(fill=tk.X, padx=15, pady=6)

        card_grid = tk.Frame(summary_frame)
        card_grid.pack(fill=tk.X)

        card1 = tk.Frame(card_grid, bg="#f1f5f9", padx=10, pady=6, relief=tk.SOLID, bd=1)
        card1.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        tk.Label(card1, text="Giờ gửi về Server:", bg="#f1f5f9", fg="#475569", font=("Segoe UI", 9)).pack()
        self.lbl_sent_time = tk.Label(card1, text=self.last_sent_time, bg="#f1f5f9", fg="#0f172a", font=("Segoe UI", 15, "bold"))
        self.lbl_sent_time.pack(pady=2)

        card2 = tk.Frame(card_grid, bg="#eff6ff", padx=10, pady=6, relief=tk.SOLID, bd=1)
        card2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        tk.Label(card2, text="Giờ trung bình (Avg) nhận được:", bg="#eff6ff", fg="#1e40af", font=("Segoe UI", 9)).pack()
        self.lbl_avg_time = tk.Label(card2, text=self.last_avg_received, bg="#eff6ff", fg="#1d4ed8", font=("Segoe UI", 15, "bold"))
        self.lbl_avg_time.pack(pady=2)

        card3 = tk.Frame(card_grid, bg="#f0fdf4", padx=10, pady=6, relief=tk.SOLID, bd=1)
        card3.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        tk.Label(card3, text="Độ lệch đã điều chỉnh:", bg="#f0fdf4", fg="#166534", font=("Segoe UI", 9)).pack()
        self.lbl_offset = tk.Label(card3, text=self.last_offset, bg="#f0fdf4", fg="#15803d", font=("Segoe UI", 15, "bold"))
        self.lbl_offset.pack(pady=2)

        # 4. KHU VỰC THÔNG BÁO TRỰC QUAN (ALERT BANNER)
        self.alert_frame = tk.Frame(self.root, bg="#f1f5f9", relief=tk.SOLID, bd=1, padx=12, pady=6)
        self.alert_frame.pack(fill=tk.X, padx=15, pady=4)
        self.lbl_alert = tk.Label(
            self.alert_frame,
            text="Nhập IP Máy chủ và nhấn 'KẾT NỐI TỚI SERVER' để tham gia vào hệ thống phân tán.",
            bg="#f1f5f9", fg="#334155", font=("Segoe UI", 10, "bold")
        )
        self.lbl_alert.pack(anchor=tk.W)

        # 5. KHUNG NHẬT KÝ (LOGS)
        log_frame = tk.Frame(self.root, padx=15, pady=4)
        log_frame.pack(fill=tk.X, side=tk.BOTTOM)
        tk.Label(log_frame, text="Nhật ký gói tin (Client Log):", font=("Segoe UI", 9, "bold"), fg="#475569").pack(anchor=tk.W)

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
        self._log(f"[ĐỒNG HỒ] Đã chỉnh giờ máy trạm thành {self.current_sim_time_full}")

    def _randomize_time(self):
        now = datetime.datetime.now()
        delta = random.randint(-30, 30)
        mins = (now.hour * 60 + now.minute + delta) % 1440
        self.sim_hour = mins // 60
        self.sim_minute = mins % 60
        self.sim_second = random.randint(0, 59)
        self.lbl_big_clock.config(text=self.current_sim_time_full)
        self._log(f"[ĐỒNG HỒ] Đã tạo giờ ngẫu nhiên để thử nghiệm: {self.current_sim_time_full} (Lệch {delta:+d} phút)")

    def _set_custom_time(self):
        val = simpledialog.askstring("Chỉnh giờ", "Nhập giờ mới cho Máy trạm (Định dạng HH:mm hoặc HH:mm:ss):", parent=self.root)
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
                    self._log(f"[ĐỒNG HỒ] Đã đổi giờ máy trạm thành {self.current_sim_time_full}")
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
        self._log(f"[ĐỒNG HỒ] Đã lấy lại giờ thật của hệ điều hành: {self.current_sim_time_full}")

    # ================= LOGIC MẠNG & ĐIỀU KHIỂN =================

    def _toggle_connection(self):
        if self.client_network and self.client_network.is_connected:
            self.client_network.disconnect()
            self.client_network = None
            self._set_disconnected_ui()
        else:
            server_ip = self.entry_server_ip.get().strip()
            if not server_ip:
                messagebox.showerror("Lỗi nhập liệu", "Vui lòng nhập địa chỉ IP của Máy chủ!", parent=self.root)
                return

            try:
                port = int(self.entry_port.get().strip())
            except ValueError:
                messagebox.showerror("Lỗi Cổng", "Cổng phải là số nguyên (ví dụ: 8888)!", parent=self.root)
                return

            client_id = self.entry_client_id.get().strip() or f"Client-{random.randint(1, 99)}"
            proto = self.combo_proto.get()

            self.client_network = BerkeleyClientNetwork(
                server_ip=server_ip,
                server_port=port,
                client_id=client_id,
                protocol_type=proto,
                on_sync_request=self._on_sync_request,
                on_sync_result=self._on_sync_result,
                on_connected=self._on_connected,
                on_disconnected=self._on_disconnected,
                on_log=self._log
            )

            success = self.client_network.connect()
            if not success:
                messagebox.showerror(
                    "Kết nối thất bại",
                    f"Không thể kết nối tới Máy chủ tại {server_ip}:{port}!\n\n"
                    "Kiểm tra:\n"
                    "1. Máy chủ đã bấm 'BẬT SERVER' chưa?\n"
                    "2. Cả 2 máy đã kết nối chung mạng LAN/Wi-Fi chưa?\n"
                    "3. Tường lửa (Firewall) trên máy Server có mở cổng không?",
                    parent=self.root
                )

    def _set_connected_ui(self):
        def _update():
            self.lbl_conn_status.config(text=f"● Đã kết nối ({self.combo_proto.get()})", fg="#16a34a")
            self.btn_connect.config(text="NGẮT KẾT NỐI", bg="#dc2626")
            self.entry_server_ip.config(state="disabled")
            self.entry_port.config(state="disabled")
            self.entry_client_id.config(state="disabled")
            self.combo_proto.config(state="disabled")
            self._set_alert("ĐÃ KẾT NỐI: Đang ở chế độ sẵn sàng. Khi Server phát tín hiệu, máy trạm sẽ tự động gửi giờ.", is_error=False)
        self.root.after(0, _update)

    def _set_disconnected_ui(self):
        def _update():
            self.lbl_conn_status.config(text="● Chưa kết nối", fg="#dc2626")
            self.btn_connect.config(text="KẾT NỐI TỚI SERVER", bg="#2563eb")
            self.entry_server_ip.config(state="normal")
            self.entry_port.config(state="normal")
            self.entry_client_id.config(state="normal")
            self.combo_proto.config(state="readonly")
            self._set_alert("Đã ngắt kết nối với máy chủ.", is_error=True)
        self.root.after(0, _update)

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

    def _on_connected(self):
        self._set_connected_ui()

    def _on_disconnected(self):
        self._set_disconnected_ui()

    def _on_sync_request(self, server_time: str):
        """
        Server gửi SYNC_REQUEST:
        Client tự động lấy mốc giờ hiện tại (HH:mm) gửi về cho Server.
        """
        def _handle():
            sent_time = self.current_sim_time_minute
            self.last_sent_time = f"{sent_time}:{self.sim_second:02d}"
            self.lbl_sent_time.config(text=self.last_sent_time)

            if self.client_network:
                self.client_network.send_time_report(sent_time)

            self._set_alert(f"⚡ ĐÃ NHẬN YÊU CẦU TỪ SERVER: Đã tự động báo mốc giờ '{sent_time}'. Đang chờ kết quả trung bình...")
        self.root.after(0, _handle)

    def _on_sync_result(self, avg_time: str, offset: int, node_times: Dict[str, Any]):
        """
        Server gửi SYNC_RESULT (kèm avg_time):
        Client tự động cập nhật đồng hồ về avg_time:00 và tiếp tục đếm giây.
        """
        def _handle():
            old_time_full = self.current_sim_time_full
            self.last_avg_received = f"{avg_time}:00"
            self.last_offset = f"{offset:+d} phút"

            self.lbl_avg_time.config(text=self.last_avg_received)
            self.lbl_offset.config(text=self.last_offset)

            sign_text = "nhanh thêm" if offset > 0 else ("chậm lại" if offset < 0 else "khớp hoàn toàn")

            # KIỂM TRA CHẾ ĐỘ XÁC NHẬN TỪ NGƯỜI DÙNG
            if self.var_ask_confirm.get():
                confirm_msg = (
                    f"🔔 MÁY CHỦ ĐÃ TÍNH TOÁN XONG GIỜ TRUNG BÌNH:\n\n"
                    f"• Giờ hiện tại của máy trạm: {old_time_full}\n"
                    f"• Giờ trung bình (Avg) nhận từ Server: {avg_time}:00\n"
                    f"• Độ lệch cần điều chỉnh: Chỉnh {sign_text} {abs(offset)} phút ({offset:+d} phút)\n\n"
                    f"👉 BẠN CÓ ĐỒNG Ý ĐIỀU CHỈNH ĐỒNG HỒ MÁY TRẠM THEO GIỜ NÀY KHÔNG?"
                )
                user_agreed = messagebox.askyesno("Xác nhận điều chỉnh thời gian (Berkeley)", confirm_msg, parent=self.root)
            else:
                user_agreed = True

            if user_agreed:
                # CẬP NHẬT ĐỒNG HỒ THEO KẾT QUẢ ĐỒNG BỘ
                parts = avg_time.split(":")
                self.sim_hour = int(parts[0])
                self.sim_minute = int(parts[1])
                self.sim_second = 0  # Đưa giây về :00 để đồng bộ nhịp mới
                self.lbl_big_clock.config(text=self.current_sim_time_full)

                # Gửi xác nhận SUCCESS lại cho Server
                if self.client_network:
                    self.client_network.send_time_ack(avg_time, offset, status="SUCCESS")

                self._set_alert(f"ĐÃ ĐỒNG BỘ: Đồng hồ đã điều chỉnh từ {old_time_full} sang {self.current_sim_time_full} ({offset:+d} phút).")
                messagebox.showinfo(
                    "Đồng bộ thành công",
                    f"🎉 Đồng hồ máy trạm đã được cập nhật thành công thành: {self.current_sim_time_full}",
                    parent=self.root
                )
            else:
                # NGƯỜI DÙNG TỪ CHỐI ĐIỀU CHỈNH
                if self.client_network:
                    self.client_network.send_time_ack(self.current_sim_time_minute, 0, status="REJECTED")

                self._set_alert(f"TỪ CHỐI ĐIỀU CHỈNH: Giữ nguyên giờ hiện tại {self.current_sim_time_full}.", is_error=True)
                messagebox.showwarning(
                    "Đã từ chối điều chỉnh",
                    f"Bạn đã từ chối điều chỉnh giờ.\nĐồng hồ máy trạm vẫn giữ nguyên mốc: {self.current_sim_time_full}.",
                    parent=self.root
                )

        self.root.after(0, _handle)
