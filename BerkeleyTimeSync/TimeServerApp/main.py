"""
Điểm khởi chạy chính của Ứng dụng Máy chủ Thời gian (Berkeley Time Server).
Chạy lệnh: python main.py
"""

import sys
import os
import tkinter as tk

# Thêm đường dẫn thư mục hiện tại vào sys.path để import dễ dàng
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from server_gui import ServerAppGUI


def main():
    root = tk.Tk()
    app = ServerAppGUI(root)
    # Xử lý sự kiện đóng cửa sổ an toàn
    def on_closing():
        if app.server_network:
            app.server_network.stop()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
