# ĐẶC TẢ GIAO THỨC TRUYỀN THÔNG (BERKELEY PROTOCOL SPECIFICATION)

## 1. Tổng quan
Hệ thống sử dụng cơ chế Socket (hỗ trợ TCP / UDP), truyền dữ liệu dạng văn bản mã hóa UTF-8 theo chuẩn **JSON**, kết thúc mỗi gói tin bằng ký tự xuống dòng `\n` để chống hiện tượng dính gói tin (TCP Packet Framing).

Hệ thống hoạt động theo mô hình **Đồng bộ bị động (Passive Synchronization)**:
- Máy chủ (Server) chủ động gửi tín hiệu yêu cầu đồng bộ.
- Các máy trạm (Client) chỉ gửi mốc thời gian khi nhận được yêu cầu từ Server.
- Độ chính xác: **Cấp phút (`HH:mm`)**.

---

## 2. Các loại thông điệp (Message Types)

### 2.1. `SYNC_REQUEST` (Server -> Client)
* **Ý nghĩa:** Server phát tín hiệu bắt đầu quy trình đồng bộ.
* **Thời điểm gửi:** Khi người điều khiển Server bấm nút **"Đồng bộ ngay (Sync Now)"**.
* **Định dạng:**
```json
{
  "type": "SYNC_REQUEST",
  "server_time": "10:25",
  "message": "Yeu cau bao cao thoi gian hien tai"
}
```

---

### 2.2. `TIME_REPORT` (Client -> Server)
* **Ý nghĩa:** Client nhận được yêu cầu từ Server, lấy mốc thời gian của đồng hồ mô phỏng gửi ngược về.
* **Định dạng:**
```json
{
  "type": "TIME_REPORT",
  "client_id": "Client-01",
  "client_name": "May tram 1",
  "time": "10:20"
}
```

---

### 2.3. `SYNC_RESULT` (Server -> Client)
* **Ý nghĩa:** Sau khi gom đủ mốc thời gian từ Server và 5 Client, Server tính giá trị trung bình cộng (Average) và phát lại kết quả này cho toàn bộ Client.
* **Định dạng:**
```json
{
  "type": "SYNC_RESULT",
  "avg_time": "10:24",
  "total_nodes": 6,
  "node_times": {
    "Server": "10:25",
    "Client-01": "10:20",
    "Client-02": "10:28",
    "Client-03": "10:22",
    "Client-04": "10:30",
    "Client-05": "10:19"
  }
}
```

---

### 2.4. `TIME_ACK` (Client -> Server)
* **Ý nghĩa:** Client nhận được giờ trung bình, tự tính toán độ lệch và cập nhật đồng hồ, sau đó phản hồi lại Server thông báo hoàn tất.
* **Định dạng:**
```json
{
  "type": "TIME_ACK",
  "client_id": "Client-01",
  "old_time": "10:20",
  "new_time": "10:24",
  "offset_minutes": 4,
  "status": "SUCCESS"
}
```

---

## 3. Công thức tính toán thời gian (Chính xác tới phút)

1. **Chuyển đổi giờ sang phút:**
   $$\text{TotalMinutes} = \text{Hour} \times 60 + \text{Minute}$$

2. **Tính giờ trung bình (Average):**
   $$\text{AvgMinutes} = \operatorname{round}\left(\frac{\sum_{i=1}^{N} \text{Minutes}_i}{N}\right)$$

3. **Tính độ lệch thời gian tại Client:**
   $$\Delta = \text{AvgMinutes} - \text{ClientMinutes}$$
   - Nếu $\Delta > 0$: Máy trạm cần chỉnh **nhanh thêm** $\Delta$ phút.
   - Nếu $\Delta < 0$: Máy trạm cần chỉnh **chậm lại** $|\Delta|$ phút.
   - Nếu $\Delta = 0$: Đồng hồ đã khớp hoàn toàn.
