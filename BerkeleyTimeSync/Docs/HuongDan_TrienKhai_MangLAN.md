# CẨM NANG TRIỂN KHAI THỰC TẾ TRÊN 6 MÁY TÍNH (MẠNG LAN)
## ĐỒ ÁN: ĐỒNG BỘ ĐỒNG HỒ VẬT LÝ THEO THUẬT TOÁN BERKELEY

---

## 1. SƠ ĐỒ KẾT NỐI HỆ THỐNG
* **1 Máy chủ (Master Node / Server):** Cầm trịch hệ thống, chạy file `TimeServer.exe`.
* **5 Máy trạm (Workstation Nodes / Clients):** Chạy file `TimeClient.exe`.

```
                  ┌───────────────────────────────┐
                  │    MÁY CHỦ THỜI GIAN (SERVER)  │
                  │        IP: 192.168.x.x        │
                  │          Port: 8888           │
                  └──────────────┬────────────────┘
                                 │ (TCP Socket)
         ┌───────────────┬───────┴───────┬───────────────┬───────────────┐
         │               │               │               │               │
         ▼               ▼               ▼               ▼               ▼
   ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
   │  MÁY TRẠM 1│   │  MÁY TRẠM 2│   │  MÁY TRẠM 3│   │  MÁY TRẠM 4│   │  MÁY TRẠM 5│
   │ (Client 1)│   │ (Client 2)│   │ (Client 3)│   │ (Client 4)│   │ (Client 5)│
   └───────────┘   └───────────┘   └───────────┘   └───────────┘   └───────────┘
```

---

## 2. BÍ QUYẾT KẾT NỐI MẠNG TRONG PHÒNG HỌC (RẤT QUAN TRỌNG)
> ⚠️ **LƯU Ý SỐ 1 KHI DEMO TẠI TRƯỜNG:**
> Mạng Wi-Fi của trường học/công cộng thường kích hoạt tính năng **AP Isolation (Client Isolation)** — tức là chặn không cho các máy tính kết nối trực tiếp với nhau.
> 
> 👉 **GIẢI PHÁP TỐI ƯU:**
> 1. Cho **1 thành viên bật Mobile Hotspot (Điểm phát sóng Wi-Fi di động)** từ điện thoại (hoặc laptop).
> 2. Cả 6 máy laptop của nhóm cùng kết nối vào mạng Wi-Fi Hotspot này.
> 3. **Không tốn dữ liệu mạng di động 4G:** Dữ liệu Socket giữa các máy truyền nội bộ trong mạng LAN nên hoàn toàn không tiêu tốn dung lượng Internet.

---

## 3. THIẾT LẬP TRÊN MÁY 1 (MÁY CHỦ / SERVER)

### Bước 1: Mở cổng Tường lửa (Firewall)
Để máy chủ có thể nhận kết nối từ 5 máy còn lại, máy chủ cần cho phép cổng **8888** đi qua Tường lửa:
* **Cách nhanh nhất:** 
  1. Vào thư mục `D:\code\Các HTPT\BerkeleyTimeSync\TimeServerApp`.
  2. Nhấp chuột phải vào file **`Mo_Cong_Firewall.bat`** $\rightarrow$ Chọn **"Run as administrator"**.
* **Cách thủ công bằng lệnh CMD (Run as Administrator):**
  ```cmd
  netsh advfirewall firewall add rule name="Berkeley Time Server" dir=in action=allow protocol=TCP localport=8888
  ```

### Bước 2: Xem địa chỉ IP của Máy Chủ
* Mở ứng dụng **`TimeServer.exe`**.
* Nhìn góc trên bên trái của giao diện: Sẽ có dòng chữ:
  `IP Server (LAN): 192.168.x.x` (Ví dụ: `192.168.43.105`).
* Đọc địa chỉ IP này cho 5 bạn còn lại để nhập vào máy của họ.

### Bước 3: Đảm bảo Server đang chạy
* Nút trạng thái hiển thị **`● Đang chạy (TCP Cổng 8888)`** màu xanh lá.

---

## 4. THIẾT LẬP TRÊN 5 MÁY TRẠM (CLIENTS)

1. Mỗi bạn mở file **`TimeClient.exe`**.
2. Tại ô **IP Máy chủ (Server IP):** Nhập chính xác IP của máy chủ (ví dụ: `192.168.43.105`).
3. Tại ô **Cổng (Port):** Giữ nguyên `8888`.
4. Tại ô **Tên máy trạm (Client ID):** Mỗi bạn đặt tên riêng để dễ phân biệt (Ví dụ: `May-01 (Thach)`, `May-02 (Nguyen)`, `May-03 (Nam)...`).
5. Bấm nút **"KẾT NỐI TỚI SERVER"**.
   * Khi trạng thái chuyển sang **`● Đã kết nối (TCP)`** màu xanh lá là thành công!

---

## 5. KỊCH BẢN BIỂU DIỄN (DEMO) TRƯỚC GIẢNG VIÊN

* **Bước 1 (Chuẩn bị độ lệch):** 
  Trước khi đồng bộ, mỗi bạn Client bấm nút **`🎲 Giờ ngẫu nhiên`** một lần. Đồng hồ mỗi máy sẽ nhảy sang các mốc giờ khác nhau (ví dụ: máy 10:18, máy 10:29, máy 10:22...).
* **Bước 2 (Kiểm tra trên Server):**
  Người quản lý Server nhìn vào bảng danh sách: Đã thấy đủ 5 máy trạm kết nối.
* **Bước 3 (Thực hiện Đồng bộ):**
  Người điều khiển Server bấm nút màu xanh to:
  👉 **`⚡ ĐỒNG BỘ TẤT CẢ MÁY TRẠM NGAY (BROADCAST SYNC)`**
* **Bước 4 (Quan sát kết quả):**
  1. Server thu thập giờ của toàn bộ máy $\rightarrow$ tính giờ trung bình $Avg$.
  2. Bảng Server hiển thị đầy đủ: Giờ báo về, Giờ Avg, Độ lệch của từng máy.
  3. Cả 5 máy Client cùng nhảy đồng hồ về chung 1 mốc giờ trung bình, kim giây tiếp tục tích tắc đồng bộ cùng nhau.
  4. Cửa sổ `MessageBox` thông báo thành công bật lên trên tất cả các máy!

---

## 6. XỬ LÝ SỰ CỐ THƯỜNG GẶP (TROUBLESHOOTING)

| Hiện tượng | Nguyên nhân | Cách khắc phục |
| :--- | :--- | :--- |
| **Bấm kết nối báo lỗi "Không thể kết nối tới Máy chủ"** | 1. Sai IP máy Server<br>2. Chưa mở cổng Firewall máy Server<br>3. Khác mạng Wi-Fi | 1. Kiểm tra lại IP Server gõ đúng chưa.<br>2. Chạy file `Mo_Cong_Firewall.bat` với quyền Admin trên máy Server.<br>3. Bật Mobile Hotspot phát chung cho 6 máy. |
| **Máy Server chỉ thấy 3 hoặc 4 máy trạm** | 1 máy trạm bị ngắt mạng hoặc chưa bấm Kết nối | Kiểm tra trạng thái máy trạm chưa kết nối, bấm lại "KẾT NỐI TỚI SERVER". |

---

## 7. ĐÓNG LẠI CỔNG FIREWALL SAU KHI DEMO XONG
Sau khi hoàn thành buổi báo cáo, bạn có thể chạy file **`Dong_Cong_Firewall.bat`** trên máy Server (Run as administrator) để gỡ bỏ rule mở port cho máy tính về trạng thái ban đầu.
