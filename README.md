# Đồ Án Các Hệ Thống Phân Tán (C-c_HTPT)
## Đề tài: Đồng bộ Đồng hồ Vật lý theo Thuật toán Berkeley (Berkeley Time Sync)

Dự án mô phỏng và triển khai thuật toán đồng bộ thời gian Berkeley qua mạng nội bộ (LAN) bằng giao thức TCP Socket với giao diện trực quan (Tkinter GUI).

---

## 📁 Cấu trúc dự án

```text
Các HTPT/
├── BerkeleyTimeSync/
│   ├── TimeServerApp/            # Ứng dụng Máy chủ (Master Node)
│   │   ├── NetworkCore/          # Xử lý Socket & tính toán thuật toán Berkeley
│   │   ├── server_gui.py         # Giao diện đồ họa máy chủ (Tkinter)
│   │   └── main.py               # File chạy chính của Server
│   │
│   ├── TimeClientApp/            # Ứng dụng Máy trạm (Workstation Node)
│   │   ├── NetworkCore/          # Xử lý Socket gửi nhận thời gian
│   │   ├── client_gui.py         # Giao diện đồ họa máy trạm (Tkinter)
│   │   └── main.py               # File chạy chính của Client
│   │
│   ├── Docs/                     # Tài liệu thiết kế & cẩm nang triển khai LAN
│   │   ├── HuongDan_TrienKhai_MangLAN.md
│   │   ├── Protocol_Specification.md
│   │   └── TongQuan_PhanCong_DoAn.docx
│   │
│   ├── Mo_Cong_Firewall.bat      # Script mở port 8888 Windows Firewall (Chạy quyền Admin)
│   └── Dong_Cong_Firewall.bat    # Script đóng port sau khi thử nghiệm
│
├── .gitignore
└── README.md
```

---

## 🛠️ Yêu cầu môi trường

- **Hệ điều hành:** Windows 10/11.
- **Ngôn ngữ:** Python 3.8 trở lên.
- **Thư viện ngoài:** **KHÔNG CẦN CÀI ĐẶT THÊM** (Dự án sử dụng 100% thư viện chuẩn tích hợp sẵn trong Python: `tkinter`, `socket`, `threading`, `json`, `datetime`).

---

## 🚀 Hướng dẫn chạy nhanh (Quick Start)

### Cách 1: Thử nghiệm nhanh trên cùng 1 máy tính (Localhost)

1. **Khởi chạy Máy chủ (Server):**
   Mở terminal / PowerShell:
   ```powershell
   cd "BerkeleyTimeSync/TimeServerApp"
   python main.py
   ```
   *Giao diện Server sẽ xuất hiện và mặc định lắng nghe tại cổng `8888`.*

2. **Khởi chạy Máy trạm (Client):**
   Mở một cửa sổ terminal khác:
   ```powershell
   cd "BerkeleyTimeSync/TimeClientApp"
   python main.py
   ```
   *Nhập IP: `127.0.0.1` | Port: `8888` -> Bấm nút **Kết nối**.*  
   *(Bạn có thể mở nhiều cửa sổ Client cùng lúc để mô phỏng nhóm nhiều máy).*

3. **Thực hiện đồng bộ:**
   - Trên giao diện Client, bấm **"Gửi giờ hiện tại"** hoặc chỉnh sai lệch giờ để thử nghiệm.
   - Trên giao diện Server, bấm **"Thực hiện Đồng bộ Berkeley"**. Server sẽ tính toán giờ trung bình loại trừ ngoại lai và gửi điều chỉnh về cho các máy trạm.

---

### Cách 2: Triển khai trên nhiều máy qua mạng LAN (Phòng học / Demo)

1. **Kết nối mạng chung:** Tất cả các máy tính cùng kết nối vào 1 mạng Wi-Fi (khuyên dùng 1 máy bật **Mobile Hotspot** để tránh bị chặn kết nối do Wi-Fi trường bật tính năng AP Isolation).
2. **Cấu hình trên Máy Chủ:**
   - Nhấp chuột phải vào `BerkeleyTimeSync/Mo_Cong_Firewall.bat` -> Chọn **"Run as administrator"** (để mở cổng 8888 trên Windows Defender Firewall).
   - Chạy `python main.py` trong thư mục `TimeServerApp`.
   - Xem địa chỉ IP hiển thị trên góc giao diện Server (Ví dụ: `192.168.43.105`).
3. **Cấu hình trên các Máy Trạm:**
   - Chạy `python main.py` trong thư mục `TimeClientApp`.
   - Nhập IP của Máy Chủ và bấm **"Kết nối"**.
4. *Chi tiết cách triển khai xem thêm tại: `BerkeleyTimeSync/Docs/HuongDan_TrienKhai_MangLAN.md`.*

---

## 📦 Hướng dẫn tự đóng gói file `.exe` (Tùy chọn)

Nếu thành viên trong nhóm muốn tự build ứng dụng ra file thực thi `.exe` độc lập để chạy không cần mở code:

1. Cài đặt PyInstaller:
   ```powershell
   pip install pyinstaller
   ```
2. Đóng gói Server:
   ```powershell
   cd "BerkeleyTimeSync/TimeServerApp"
   pyinstaller --noconsole --onefile --name "TimeServer" main.py
   ```
   *File `.exe` sẽ được tạo trong thư mục `dist/`.*

3. Đóng gói Client:
   ```powershell
   cd "BerkeleyTimeSync/TimeClientApp"
   pyinstaller --noconsole --onefile --name "TimeClient" main.py
   ```
   *File `.exe` sẽ được tạo trong thư mục `dist/`.*
