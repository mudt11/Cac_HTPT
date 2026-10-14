"""
Điểm khởi chạy chính của Ứng dụng Máy trạm Thời gian (Berkeley Time Client).
Chạy lệnh: python main.py
Dành cho 5 máy thành viên kết nối tới Máy chủ.
"""

import sys
import os
import tkinter as tk

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from client_gui import ClientAppGUI


def main():
    root = tk.Tk()
    app = ClientAppGUI(root)

    def on_closing():
        if app.client_network and app.client_network.is_connected:
            app.client_network.disconnect()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
