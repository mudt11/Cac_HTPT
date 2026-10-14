"""
Module quản lý kết nối Socket phía Máy chủ (Server).
Hỗ trợ đa luồng (Multi-threading) để phục vụ nhiều máy trạm (Client) cùng lúc.
"""

import socket
import threading
import time
from typing import Dict, Any, Callable, Optional, List
from .protocol import (
    serialize_packet,
    parse_packet,
    calculate_average_time,
    calculate_offset
)


class BerkeleyServerNetwork:
    """
    Quản lý Socket Server cho thuật toán Berkeley.
    Chạy đa luồng, hỗ trợ kết nối đồng thời từ nhiều máy trạm trong mạng LAN.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8888,
        protocol_type: str = "TCP",
        on_client_connected: Optional[Callable[[str, str], None]] = None,
        on_client_disconnected: Optional[Callable[[str], None]] = None,
        on_time_reported: Optional[Callable[[str, str], None]] = None,
        on_sync_complete: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_client_ack: Optional[Callable[[str, str], None]] = None,
        on_log: Optional[Callable[[str], None]] = None
    ):
        self.host = host
        self.port = port
        self.protocol_type = protocol_type.upper()

        # Callbacks cập nhật cho giao diện (GUI)
        self.on_client_connected = on_client_connected
        self.on_client_disconnected = on_client_disconnected
        self.on_time_reported = on_time_reported
        self.on_sync_complete = on_sync_complete
        self.on_client_ack = on_client_ack
        self.on_log = on_log

        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        self.clients_lock = threading.Lock()

        # Lưu trữ các Client đang kết nối:
        # { client_id: {"socket": sock, "address": (ip, port), "reported_time": str, "offset": int, "status": str} }
        self.connected_clients: Dict[str, Dict[str, Any]] = {}
        self.current_server_time: str = "10:00"
        self.current_round_id: int = 0
        self.sync_active: bool = False

    def _log(self, message: str) -> None:
        if self.on_log:
            self.on_log(message)

    def start(self) -> bool:
        """
        Khởi động Server lắng nghe kết nối.
        """
        try:
            if self.protocol_type == "TCP":
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind((self.host, self.port))
                self.server_socket.listen(10)
                self.is_running = True

                listener_thread = threading.Thread(target=self._tcp_listen_loop, daemon=True)
                listener_thread.start()
                self._log(f"[SERVER] Đã khởi động TCP Server tại {self.host}:{self.port}")
                return True
            else:
                # Chế độ UDP
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_socket.bind((self.host, self.port))
                self.is_running = True

                listener_thread = threading.Thread(target=self._udp_listen_loop, daemon=True)
                listener_thread.start()
                self._log(f"[SERVER] Đã khởi động UDP Server tại {self.host}:{self.port}")
                return True
        except Exception as e:
            self._log(f"[LỖI SERVER] Không thể khởi động: {e}")
            self.is_running = False
            return False

    def stop(self) -> None:
        """
        Dừng Server và ngắt tất cả kết nối.
        """
        self.is_running = False
        with self.clients_lock:
            for c_id, info in list(self.connected_clients.items()):
                sock = info.get("socket")
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass
            self.connected_clients.clear()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None

        self._log("[SERVER] Đã dừng máy chủ.")

    def _tcp_listen_loop(self) -> None:
        """
        Vòng lặp chấp nhận kết nối TCP từ các Client mới.
        """
        while self.is_running and self.server_socket:
            try:
                client_sock, client_addr = self.server_socket.accept()
                ip_str = f"{client_addr[0]}:{client_addr[1]}"
                client_thread = threading.Thread(
                    target=self._tcp_handle_client,
                    args=(client_sock, client_addr),
                    daemon=True
                )
                client_thread.start()
            except Exception:
                break

    def _tcp_handle_client(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """
        Luồng riêng xử lý đọc dữ liệu từ một Client TCP cụ thể.
        """
        client_id = f"Client-{client_addr[0]}:{client_addr[1]}"
        ip_str = client_addr[0]

        with self.clients_lock:
            self.connected_clients[client_id] = {
                "socket": client_sock,
                "address": client_addr,
                "reported_time": None,
                "offset": None,
                "status": "Connected"
            }

        self._log(f"[KẾT NỐI] Máy trạm mới tham gia: {client_id} (IP: {ip_str})")
        if self.on_client_connected:
            self.on_client_connected(client_id, ip_str)

        buffer = ""
        while self.is_running:
            try:
                data = client_sock.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        packet = parse_packet(line)
                        self._process_incoming_packet(client_id, packet)
            except Exception:
                break

        # Khi Client ngắt kết nối
        with self.clients_lock:
            if client_id in self.connected_clients:
                del self.connected_clients[client_id]

        self._log(f"[NGẮT KẾT NỐI] {client_id} đã rời khỏi hệ thống.")
        if self.on_client_disconnected:
            self.on_client_disconnected(client_id)
        try:
            client_sock.close()
        except Exception:
            pass

    def _udp_listen_loop(self) -> None:
        """
        Vòng lặp nhận gói tin UDP.
        """
        while self.is_running and self.server_socket:
            try:
                data, addr = self.server_socket.recvfrom(4096)
                line = data.decode("utf-8").strip()
                if line:
                    packet = parse_packet(line)
                    client_id = packet.get("client_id", f"Client-{addr[0]}")
                    with self.clients_lock:
                        if client_id not in self.connected_clients:
                            self.connected_clients[client_id] = {
                                "socket": None,
                                "address": addr,
                                "reported_time": None,
                                "offset": None,
                                "status": "Connected"
                            }
                            if self.on_client_connected:
                                self.on_client_connected(client_id, addr[0])
                    self._process_incoming_packet(client_id, packet)
            except Exception:
                break

    def _process_incoming_packet(self, client_id: str, packet: Dict[str, Any]) -> None:
        """
        Xử lý nội dung gói tin nhận từ Client.
        """
        pkt_type = packet.get("type")

        if pkt_type in ("TIME_REPORT", "HELLO"):
            custom_id = packet.get("client_id")
            with self.clients_lock:
                if client_id in self.connected_clients and custom_id:
                    self.connected_clients[client_id]["custom_id"] = custom_id

        if pkt_type == "TIME_REPORT":
            reported_time = packet.get("time", "")
            display_name = ""
            with self.clients_lock:
                if client_id in self.connected_clients:
                    self.connected_clients[client_id]["reported_time"] = reported_time
                    self.connected_clients[client_id]["status"] = "Reported"
                    display_name = self.connected_clients[client_id].get("custom_id", client_id)

            self._log(f"[BÁO GIỜ] {display_name} đã gửi mốc giờ: {reported_time}")
            if self.on_time_reported:
                self.on_time_reported(client_id, reported_time)

            # Kiểm tra xem tất cả các Client đã gửi về đủ chưa để tự động tính trung bình
            self._check_and_calculate_sync()

        elif pkt_type == "TIME_ACK":
            ack_status = packet.get("status", "SUCCESS")
            display_name = ""
            status_text = "Đã đồng bộ ✓" if ack_status == "SUCCESS" else "Từ chối điều chỉnh ✗"

            with self.clients_lock:
                if client_id in self.connected_clients:
                    display_name = self.connected_clients[client_id].get("custom_id", client_id)
                    self.connected_clients[client_id]["status"] = status_text

            if ack_status == "SUCCESS":
                self._log(f"[XÁC NHẬN] {display_name} ĐÃ ĐỒNG Ý và cập nhật giờ mới ({packet.get('new_time')}).")
            else:
                self._log(f"[XÁC NHẬN] {display_name} ĐÃ TỪ CHỐI điều chỉnh giờ.")

            if self.on_client_ack:
                self.on_client_ack(client_id, ack_status)

    def trigger_sync(self, server_current_time: str) -> int:
        """
        BẮT ĐẦU ĐỒNG BỘ BỊ ĐỘNG:
        Server phát tín hiệu SYNC_REQUEST tới toàn bộ Client đang kết nối.
        Trả về số lượng Client được gửi tín hiệu.
        """
        self.current_server_time = server_current_time
        target_count = 0

        with self.clients_lock:
            self.current_round_id += 1
            self.sync_active = True

            for c_id, info in self.connected_clients.items():
                info["reported_time"] = None
                info["offset"] = None
                info["status"] = "Waiting"

                req_packet = {
                    "type": "SYNC_REQUEST",
                    "round_id": self.current_round_id,
                    "server_time": server_current_time,
                    "message": "Vui lòng gửi mốc thời gian hiện tại"
                }
                data = serialize_packet(req_packet)

                if self.protocol_type == "TCP" and info.get("socket"):
                    try:
                        info["socket"].sendall(data)
                        target_count += 1
                    except Exception as e:
                        self._log(f"[LỖI GỬI] Tới {c_id}: {e}")
                elif self.protocol_type == "UDP" and self.server_socket:
                    try:
                        self.server_socket.sendto(data, info["address"])
                        target_count += 1
                    except Exception as e:
                        self._log(f"[LỖI GỬI UDP] Tới {c_id}: {e}")

        self._log(f"[PHÁT TÍN HIỆU] Đã gửi yêu cầu đồng bộ tới {target_count} máy trạm. Giờ Server: {server_current_time} (Phiên #{self.current_round_id})")
        return target_count

    def _check_and_calculate_sync(self) -> None:
        """
        Kiểm tra nếu tất cả các máy trạm đã gửi đủ giờ,
        tiến hành tính Average và phát trả kết quả lại cho từng máy.
        Đảm bảo chỉ thực thi duy nhất 1 lần cho mỗi phiên đồng bộ (chống Race Condition).
        """
        with self.clients_lock:
            # Nếu phiên đồng bộ đã được xử lý hoặc chưa kích hoạt -> dừng ngay lập tức
            if not self.sync_active:
                return

            if not self.connected_clients:
                return

            all_reported = True
            time_list = [self.current_server_time]
            client_time_map: Dict[str, str] = {"Server": self.current_server_time}

            for c_id, info in self.connected_clients.items():
                r_time = info.get("reported_time")
                if not r_time:
                    all_reported = False
                    break
                time_list.append(r_time)
                client_time_map[c_id] = r_time

            if not all_reported:
                return

            # GOM ĐỦ: LẬP TỨC KHÓA LƯỢT ĐỒNG BỘ ĐỂ CÁC LUỒNG KHÁC KHÔNG CHẠY TRÙNG
            self.sync_active = False

            # Tính giá trị trung bình theo thuật toán Berkeley
            avg_time, _ = calculate_average_time(time_list)
            server_offset = calculate_offset(self.current_server_time, avg_time)

            self._log(f"[TÍNH TOÁN BERKELEY] Gom đủ {len(time_list)} mốc giờ. Giờ trung bình tính ra: {avg_time} (Phiên #{self.current_round_id})")

            # Gửi SYNC_RESULT cho từng Client kèm round_id
            results_summary: Dict[str, Any] = {
                "round_id": self.current_round_id,
                "server_time": self.current_server_time,
                "server_offset": server_offset,
                "avg_time": avg_time,
                "clients": {}
            }

            for c_id, info in self.connected_clients.items():
                c_time = info["reported_time"]
                c_offset = calculate_offset(c_time, avg_time)
                info["offset"] = c_offset
                info["status"] = "Synced"

                results_summary["clients"][c_id] = {
                    "reported_time": c_time,
                    "avg_time": avg_time,
                    "offset": c_offset
                }

                res_packet = {
                    "type": "SYNC_RESULT",
                    "round_id": self.current_round_id,
                    "avg_time": avg_time,
                    "node_times": client_time_map
                }
                data = serialize_packet(res_packet)

                if self.protocol_type == "TCP" and info.get("socket"):
                    try:
                        info["socket"].sendall(data)
                    except Exception:
                        pass
                elif self.protocol_type == "UDP" and self.server_socket:
                    try:
                        self.server_socket.sendto(data, info["address"])
                    except Exception:
                        pass

        if self.on_sync_complete:
            self.on_sync_complete(avg_time, results_summary)
