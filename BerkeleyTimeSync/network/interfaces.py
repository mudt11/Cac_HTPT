"""Read local Windows IPv4 configuration without probing Internet routes.

Uses built-in NetTCPIP read-only cmdlets and structured JSON, independent of
localized ipconfig output. Enumeration runs with a timeout in a hidden process.
Call it outside the Tk thread. It never changes interfaces, routes or firewall.
"""

import json
import math
import os
import subprocess
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv4Interface
from pathlib import Path
from typing import Optional, Tuple


class InterfaceDiscoveryError(RuntimeError):
    """Local adapter enumeration failed; manual configuration remains available."""


@dataclass(frozen=True)
class IPv4Adapter:
    index: int
    name: str
    ip: str
    prefix_length: int

    def __post_init__(self):
        if type(self.index) is not int or self.index < 1:
            raise ValueError("Invalid interface index")
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("Invalid interface name")
        if type(self.prefix_length) is not int or not 0 <= self.prefix_length <= 32:
            raise ValueError("Invalid IPv4 prefix length")
        address = IPv4Address(self.ip)
        if address.is_unspecified or address.is_multicast or int(address) == 0xffffffff:
            raise ValueError("An adapter requires a usable unicast IPv4 address")

    @property
    def network(self):
        return IPv4Interface("{}/{}".format(self.ip, self.prefix_length)).network

    @property
    def broadcast(self) -> Optional[str]:
        # /31 and /32 are point-to-point/host routes, not broadcast LANs.
        return str(self.network.broadcast_address) if self.prefix_length <= 30 else None

    @property
    def is_loopback(self) -> bool:
        return IPv4Address(self.ip).is_loopback

    @property
    def label(self) -> str:
        name = self.name.replace("\r", " ").replace("\n", " ")
        return "{} [{}] • {}/{}{}".format(name, self.index, self.ip, self.prefix_length,
                                         " • loopback" if self.is_loopback else "")


def parse_adapters_json(text: str) -> Tuple[IPv4Adapter, ...]:
    """Validate the fixed enumeration result, preserving multiple IPv4s/card."""
    try:
        data = json.loads(text.lstrip("\ufeff"))
        if not isinstance(data, list) or len(data) > 4096:
            raise ValueError("Expected a bounded JSON array")
        adapters = {}
        for item in data:
            if not isinstance(item, dict) or set(item) != {"index", "name", "ip", "prefix"}:
                raise ValueError("Invalid interface fields")
            adapter = IPv4Adapter(item["index"], item["name"], item["ip"], item["prefix"])
            adapters[(adapter.index, adapter.ip, adapter.prefix_length)] = adapter
        return tuple(sorted(adapters.values(), key=lambda a: (
            a.is_loopback, a.index, int(IPv4Address(a.ip)), a.prefix_length)))
    except (ValueError, TypeError, RecursionError) as exc:
        raise InterfaceDiscoveryError("Invalid local adapter data: {}".format(exc)) from exc


_ENUMERATION_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
try {
    $connected = @(Get-NetIPInterface -AddressFamily IPv4 |
        Where-Object { $_.ConnectionState -eq 'Connected' } |
        Select-Object -ExpandProperty InterfaceIndex)
    $addresses = @(Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.AddressState -eq 'Preferred' -and $connected -contains $_.InterfaceIndex })
    $items = @($addresses | ForEach-Object {
        [PSCustomObject]@{
            index = [int]$_.InterfaceIndex
            name = [string]$_.InterfaceAlias
            ip = [string]$_.IPAddress
            prefix = [int]$_.PrefixLength
        }
    })
    ConvertTo-Json -InputObject $items -Compress -Depth 3
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
"""


def list_ipv4_adapters(*, timeout: float = 10.0) -> Tuple[IPv4Adapter, ...]:
    """List connected interfaces with Preferred IPv4 addresses on Windows.

    No card is assumed to be the LAN: VPN, VM and Hotspot interfaces may all
    appear. The operator chooses the card used by the other cluster members.
    Non-Windows systems retain the GUI's manual entry mode.
    """
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 30:
        raise ValueError("timeout must be in (0, 30] seconds")
    if os.name != "nt":
        raise InterfaceDiscoveryError("Automatic adapter selection requires Windows; enter IPv4 manually")
    executable = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    try:
        result = subprocess.run(
            [str(executable), "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", _ENUMERATION_SCRIPT],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout, creationflags=subprocess.CREATE_NO_WINDOW, check=False,
        )
        if result.returncode:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise InterfaceDiscoveryError("Cannot read local interfaces: {}".format(detail[:600] or result.returncode))
        if len(result.stdout) > 1048576:
            raise InterfaceDiscoveryError("Local interface result exceeds the size limit")
        return parse_adapters_json(result.stdout.decode("utf-8-sig"))
    except subprocess.TimeoutExpired as exc:
        raise InterfaceDiscoveryError("Local adapter lookup timed out after {} seconds".format(timeout)) from exc
    except (OSError, UnicodeError) as exc:
        raise InterfaceDiscoveryError("Cannot enumerate IPv4 adapters: {}".format(exc)) from exc
