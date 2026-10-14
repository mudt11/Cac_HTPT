"""
Module giao thức truyền thông chuẩn hóa cho Thuật toán Berkeley.
Được sử dụng cho cả Máy chủ (Server) và Máy trạm (Client).
Độ chính xác: Cấp phút (HH:mm).
"""

import json
from typing import Dict, Any, List, Tuple


def time_to_minutes(time_str: str) -> int:
    """
    Chuyển đổi chuỗi thời gian dạng 'HH:mm' thành tổng số phút trong ngày (0 - 1439).
    Ví dụ: '10:25' -> 10 * 60 + 25 = 625 phút.
    """
    try:
        parts = time_str.strip().split(":")
        hours = int(parts[0])
        minutes = int(parts[1])
        return (hours * 60 + minutes) % 1440
    except Exception as e:
        raise ValueError(f"Định dạng thời gian không hợp lệ (yêu cầu 'HH:mm'): {time_str}") from e


def minutes_to_time(total_minutes: int) -> str:
    """
    Chuyển đổi tổng số phút trong ngày thành chuỗi 'HH:mm'.
    Tự động xử lý quay vòng 24 giờ (modulo 1440).
    Ví dụ: 625 -> '10:25', 1445 -> '00:05'.
    """
    normalized_minutes = total_minutes % 1440
    hours = normalized_minutes // 60
    minutes = normalized_minutes % 60
    return f"{hours:02d}:{minutes:02d}"


def calculate_average_time(time_list: List[str]) -> Tuple[str, List[int]]:
    """
    Tính thời gian trung bình cộng (Average) từ danh sách các mốc thời gian dạng 'HH:mm'.
    Trả về:
        (avg_time_str, list_of_minutes)
    """
    if not time_list:
        raise ValueError("Danh sách mốc thời gian không được rỗng.")

    minutes_list = [time_to_minutes(t) for t in time_list]
    avg_minutes = int(round(sum(minutes_list) / len(minutes_list)))
    return minutes_to_time(avg_minutes), minutes_list


def calculate_offset(local_time_str: str, avg_time_str: str) -> int:
    """
    Tính độ lệch số phút mà máy cần điều chỉnh.
    offset = avg - local
    offset > 0: Cần chỉnh nhanh thêm (tăng giờ)
    offset < 0: Cần chỉnh chậm lại (giảm giờ)
    offset = 0: Không cần chỉnh
    """
    local_min = time_to_minutes(local_time_str)
    avg_min = time_to_minutes(avg_time_str)
    return avg_min - local_min


def serialize_packet(data: Dict[str, Any]) -> bytes:
    """
    Đóng gói dữ liệu dạng Dict thành mảng byte JSON có đuôi '\\n' để gửi qua Socket.
    """
    json_str = json.dumps(data, ensure_ascii=False)
    return (json_str + "\n").encode("utf-8")


def parse_packet(raw_line: str) -> Dict[str, Any]:
    """
    Giải mã dòng dữ liệu JSON nhận được từ Socket.
    """
    return json.loads(raw_line.strip())
