"""Launch the shared Node dashboard: python -m BerkeleyTimeSync.main."""

import argparse
import sys
from pathlib import Path

# Also support python BerkeleyTimeSync/main.py and PyInstaller's script entry.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from BerkeleyTimeSync.gui import NodeWindow
from BerkeleyTimeSync.runtime import NodeConfig


def main():
    parser = argparse.ArgumentParser(description="Berkeley Time Sync Node Dashboard")
    parser.add_argument("--node-id", type=int, choices=range(1, 7), default=1)
    parser.add_argument("--name", default=None)
    parser.add_argument("--ip", default="", help="IPv4 of the LAN interface; editable in the dashboard")
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--broadcast", default="255.255.255.255")
    parser.add_argument("--tcp-port", type=int, default=9999)
    parser.add_argument("--udp-port", type=int, default=8888)
    parser.add_argument("--discovery-port", type=int, default=8888)
    args = parser.parse_args()
    config = NodeConfig(args.node_id, args.name or "Node {}".format(args.node_id),
                        args.ip, args.bind_host, args.tcp_port, args.udp_port,
                        args.broadcast, args.discovery_port)
    import tkinter as tk
    root = tk.Tk()
    NodeWindow(root, config)
    root.mainloop()


if __name__ == "__main__":
    main()
