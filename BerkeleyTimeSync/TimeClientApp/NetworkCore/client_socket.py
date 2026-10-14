"""
Module quản lý kết nối Socket phía Máy trạm (Client).
Chạy luồng nền (Background thread) để lắng nghe tín hiệu phát từ Server
mà không làm ảnh hưởng hoặc làm treo giao diện người dùng.
"""

import socket
import threading
from typing import Dict, Any, Callable, Optional
from .protocol import (
    serialize_packet,
    parse_packet,
    calculate_offset
)


class BerkeleyClientNetwork:
    """
    Quản lý kết nối Socket Client cho thuật toán Berkeley.
    Nhận tín hiệu đồng bộ từ Server và phản hồi dữ liệu thời gian.
    """

    def __init__(
        self,
        server_ip: str = "127.0.0.1",
        server_port: int = 8888,
        client_id: str = "Client-01",
        protocol_type: str = "TCP",
        on_sync_request: Optional[Callable[[str], None]] = None,
        on_sync_result: Optional[Callable[[str, int, Dict[str, Any]], None]] = None,
        on_connected: Optional[Callable[[], None]] = None,
        on_disconnected: Optional[Callable[[], None]] = None,
        on_log: Optional[Callable[[str], None]] = None
    ):
        self.server_ip = server_ip
        self.server_port = int(server_port)
        self.client_id = client_id
        self.protocol_type = protocol_type.upper()

        # Callbacks cập nhật cho giao diện
        self.on_sync_request = on_sync_request
        self.on_sync_result = on_sync_result
        self.on_connected = on_connected
        self.on_disconnected = on_disconnected
        self.on_log = on_log

        self.client_socket: Optional[socket.socket] = None
        self.is_connected = False
        self.listen_thread: Optional[threading.Thread] = None
        self.last_sent_time: str = ""

    def _log(self, message: str) -> None:
        if self.on_log:
            self.on_log(message)

    def connect(self) -> bool:
        """
        Khởi tạo kết nối tới máy chủ thời gian.
        """
        try:
            if self.protocol_type == "TCP":
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.settimeout(5.0)
                self.client_socket.connect((self.server_ip, self.server_port))
                self.client_socket.settimeout(None)
                self.is_connected = True

                self.listen_thread = threading.Thread(target=self._tcp_listen_loop, daemon=True)
                self.listen_thread.start()

                self._log(f"[KẾT NỐI] Đã kết nối TCP tới Máy chủ {self.server_ip}:{self.server_port}")
                if self.on_connected:
                    self.on_connected()
                return True
            else:
                # Chế độ UDP
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.is_connected = True

                self.listen_thread = threading.Thread(target=self._udp_listen_loop, daemon=True)
                self.listen_thread.start()

                # Gửi gói tin chào hỏi tới Server để đăng ký địa chỉ UDP
                hello_packet = {
                    "type": "HELLO",
                    "client_id": self.client_id,
                    "message": "Xin chao tu May tram UDP"
                }
                self.client_socket.sendto(
                    serialize_packet(hello_packet),
                    (self.server_ip, self.server_port)
                )

                self._log(f"[KẾT NỐI] Đã sẵn sàng giao tiếp UDP với Server {self.server_ip}:{self.server_port}")
                if self.on_connected:
                    self.on_connected()
                return True
        except Exception as e:
            self._log(f"[LỖI KẾT NỐI] Không thể nối tới Server: {e}")
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        """
        Chủ động ngắt kết nối với Server.
        """
        self.is_connected = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except Exception:
                pass
            self.client_socket = None

        self._log("[NGẮT KẾT NỐI] Đã ngắt kết nối với Máy chủ.")
        if self.on_disconnected:
            self.on_disconnected()

    def _tcp_listen_loop(self) -> None:
        """
        Luồng lắng nghe dữ liệu liên tục từ Server qua TCP.
        """
        buffer = ""
        while self.is_connected and self.client_socket:
            try:
                data = self.client_socket.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        packet = parse_packet(line)
                        self._handle_packet(packet)
            except Exception:
                break

        if self.is_connected:
            self.is_connected = False
            self._log("[CẢNH BÁO] Mất kết nối tới máy chủ.")
            if self.on_disconnected:
                self.on_disconnected()

    def _udp_listen_loop(self) -> None:
        """
        Luồng nhận dữ liệu liên tục từ Server qua UDP.
        """
        while self.is_connected and self.client_socket:
            try:
                data, _ = self.client_socket.recvfrom(4096)
                line = data.decode("utf-8").strip()
                if line:
                    packet = parse_packet(line)
                    self._handle_packet(packet)
            except Exception:
                break

    def _handle_packet(self, packet: Dict[str, Any]) -> None:
        """
        Xử lý thông điệp nhận từ Server.
        """
        pkt_type = packet.get("type")

        # 1. Server phát tín hiệu yêu cầu đồng bộ (SYNC_REQUEST)
        if pkt_type == "SYNC_REQUEST":
            server_time = packet.get("server_time", "")
            self._log(f"[NHẬN TÍN HIỆU] Server yêu cầu đồng bộ (Giờ Server: {server_time}).")
            if self.on_sync_request:
                self.on_sync_request(server_time)

        # 2. Server phát trả kết quả thời gian trung bình (SYNC_RESULT)
        elif pkt_type == "SYNC_RESULT":
            avg_time = packet.get("avg_time", "")
            offset = 0
            if self.last_sent_time:
                offset = calculate_offset(self.last_sent_time, avg_time)

            self._log(f"[KẾT QUẢ ĐỒNG BỘ] Giờ trung bình nhận được: {avg_time} | Độ lệch tính toán: {offset:+d} phút.")
            if self.on_sync_result:
                self.on_sync_result(avg_time, offset, packet.get("node_times", {}))

    def send_time_report(self, current_local_time: str) -> bool:
        """
        Gửi mốc thời gian hiện tại của máy trạm về Server khi có yêu cầu.
        """
        if not self.is_connected or not self.client_socket:
            return False

        self.last_sent_time = current_local_time
        packet = {
            "type": "TIME_REPORT",
            "client_id": self.client_id,
            "time": current_local_time
        }
        data = serialize_packet(packet)

        try:
            if self.protocol_type == "TCP":
                self.client_socket.sendall(data)
            else:
                self.client_socket.sendto(data, (self.server_ip, self.server_port))

            self._log(f"[GỬI BÁO GIỜ] Đã gửi mốc giờ '{current_local_time}' về Server.")
            return True
        except Exception as e:
            self._log(f"[LỖI GỬI BÁO GIỜ] {e}")
            return False

    def send_time_ack(self, new_time: str, offset: int, status: str = "SUCCESS") -> bool:
        """
        Gửi xác nhận (Đồng ý SUCCESS hoặc Từ chối REJECTED) về Server.
        """
        if not self.is_connected or not self.client_socket:
            return False

        packet = {
            "type": "TIME_ACK",
            "client_id": self.client_id,
            "old_time": self.last_sent_time,
            "new_time": new_time,
            "offset_minutes": offset,
            "status": status
        }
        data = serialize_packet(packet)
        try:
            if self.protocol_type == "TCP":
                self.client_socket.sendall(data)
            else:
                self.client_socket.sendto(data, (self.server_ip, self.server_port))
            return True
        except Exception:
            return False
