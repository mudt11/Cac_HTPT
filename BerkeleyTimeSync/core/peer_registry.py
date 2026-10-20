"""Thread-safe membership snapshots based on locally observed contact times.

The registry stores remote peers only. It performs no network operations,
authentication or elections. Callers must validate messages and their sources
before recording contact; advertised status and remote timestamps are ignored.
"""

from dataclasses import replace
from threading import RLock
from time import monotonic_ns
from typing import Callable, Dict, Optional, Set, Tuple
from uuid import UUID

from .models import NodeIdentity, NodeRole, PeerInfo, PeerStatus


class NodeIdConflictError(ValueError):
    """Another process has advertised a node ID already known locally."""

    def __init__(self, current: NodeIdentity, incoming: NodeIdentity) -> None:
        self.current_identity = current
        self.incoming_identity = incoming
        super().__init__(
            "Node ID {} is already assigned to another process".format(incoming.node_id)
        )


class StalePeerError(ValueError):
    """A contact or replacement refers to an obsolete process incarnation."""


def _integer(value: int, name: str, minimum: int, maximum: Optional[int] = None) -> None:
    if type(value) is not int:
        raise TypeError("{} must be an integer".format(name))
    if value < minimum or (maximum is not None and value > maximum):
        raise ValueError("{} is outside the allowed range".format(name))


def _identity(value: NodeIdentity) -> None:
    if not isinstance(value, NodeIdentity):
        raise TypeError("Expected a NodeIdentity")


class PeerRegistry:
    """Track at most five remote nodes of a six-node cluster.

    Defaults mark a silent peer SUSPECT after 4 seconds (two ping periods),
    a Worker OFFLINE after 6 seconds, and a Master OFFLINE after 8 seconds.
    These are local failure suspicions, not proof that a machine has crashed.
    A Master retains its last observed role when it goes offline.

    get and snapshot calculate current availability without consuming status
    changes. refresh persists timeout transitions for the controller's timer.
    No background thread is started by the registry itself.
    """

    def __init__(
        self, local_identity: NodeIdentity, *, suspect_after_ms: int = 4000,
        worker_timeout_ms: int = 6000, master_timeout_ms: int = 8000,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        _identity(local_identity)
        for name, value in (
            ("suspect_after_ms", suspect_after_ms),
            ("worker_timeout_ms", worker_timeout_ms),
            ("master_timeout_ms", master_timeout_ms),
        ):
            _integer(value, name, 1)
        if not suspect_after_ms < worker_timeout_ms <= master_timeout_ms:
            raise ValueError(
                "Timeouts must satisfy suspect_after_ms < worker_timeout_ms <= master_timeout_ms"
            )
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self._local_identity = local_identity
        self._lock = RLock()
        self._monotonic_source = monotonic_source
        self._last_now_ns = monotonic_source()
        _integer(self._last_now_ns, "monotonic reading", 0)
        self._suspect_ns = suspect_after_ms * 1000000
        self._worker_timeout_ns = worker_timeout_ms * 1000000
        self._master_timeout_ns = master_timeout_ms * 1000000
        self._peers: Dict[int, PeerInfo] = {}
        self._retired_boot_ids: Dict[int, Set[UUID]] = {}

    @property
    def local_identity(self) -> NodeIdentity:
        return self._local_identity

    def __len__(self) -> int:
        """Number of known remote peers, including offline entries."""
        with self._lock:
            return len(self._peers)

    def _now(self) -> int:
        """Read a monotonic timestamp while the registry lock is held."""
        now = self._monotonic_source()
        _integer(now, "monotonic reading", 0)
        if now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    def _view(self, peer: PeerInfo, now: int) -> PeerInfo:
        elapsed = now - peer.last_seen_monotonic_ns
        timeout = (
            self._master_timeout_ns if peer.role == NodeRole.MASTER
            else self._worker_timeout_ns
        )
        if elapsed >= timeout:
            status = PeerStatus.OFFLINE
        elif elapsed >= self._suspect_ns:
            status = PeerStatus.SUSPECT
        else:
            status = PeerStatus.ONLINE
        return peer if peer.status == status else replace(peer, status=status)

    def _current(self, identity: NodeIdentity) -> PeerInfo:
        peer = self._peers.get(identity.node_id)
        if peer is None:
            raise KeyError("Peer is not registered")
        if peer.identity != identity:
            raise StalePeerError("Peer identity does not match the registered process")
        return peer

    def observe(self, peer: PeerInfo) -> Optional[PeerInfo]:
        """Record a validated first-hand advertisement and its current metadata.

        A looped-back advertisement from this process returns None. A different
        boot_id never replaces an existing entry automatically, even when that
        entry has timed out. Use replace_offline after validating a restart.
        """
        if not isinstance(peer, PeerInfo):
            raise TypeError("Expected a PeerInfo")
        with self._lock:
            identity = peer.identity
            if identity.node_id == self._local_identity.node_id:
                if identity == self._local_identity:
                    return None
                raise NodeIdConflictError(self._local_identity, identity)
            if identity.boot_id in self._retired_boot_ids.get(identity.node_id, set()):
                raise StalePeerError("Advertisement belongs to a retired process")
            current = self._peers.get(identity.node_id)
            if current is not None and current.identity != identity:
                raise NodeIdConflictError(current.identity, identity)
            observed = replace(
                peer, status=PeerStatus.ONLINE, last_seen_monotonic_ns=self._now()
            )
            self._peers[identity.node_id] = observed
            return observed

    def mark_seen(self, identity: NodeIdentity) -> PeerInfo:
        """Record validated contact without replacing discovery metadata or role."""
        _identity(identity)
        with self._lock:
            current = self._current(identity)
            observed = replace(
                current, status=PeerStatus.ONLINE, last_seen_monotonic_ns=self._now()
            )
            self._peers[identity.node_id] = observed
            return observed

    def replace_offline(self, expected: NodeIdentity, replacement: PeerInfo) -> PeerInfo:
        """Explicitly accept a validated new incarnation of an expired peer.

        Timeout alone cannot distinguish restart from a duplicate ID. The
        controller must validate the replacement first. expected acts as a
        compare-and-swap guard against a delayed restart handler overwriting
        a newer incarnation. Retired boot IDs remain rejected for this run.
        """
        _identity(expected)
        if not isinstance(replacement, PeerInfo):
            raise TypeError("Expected a PeerInfo replacement")
        if replacement.identity.node_id != expected.node_id:
            raise ValueError("Replacement must use the same node ID")
        if replacement.identity == expected:
            raise ValueError("Replacement must have a different boot_id")
        with self._lock:
            current = self._current(expected)
            now = self._now()
            if self._view(current, now).status != PeerStatus.OFFLINE:
                raise ValueError("A live or suspect peer cannot be replaced")
            retired = self._retired_boot_ids.get(expected.node_id, set())
            if replacement.identity.boot_id in retired:
                raise StalePeerError("Replacement belongs to a retired process")
            observed = replace(
                replacement, status=PeerStatus.ONLINE, last_seen_monotonic_ns=now
            )
            self._retired_boot_ids.setdefault(expected.node_id, set()).add(expected.boot_id)
            self._peers[expected.node_id] = observed
            return observed

    def update_role(self, identity: NodeIdentity, role: NodeRole) -> PeerInfo:
        """Apply a validated role decision without treating it as new contact.

        This does not choose or enforce a unique Master. That decision belongs
        to the election/controller layer, which may update multiple entries.
        """
        _identity(identity)
        if not isinstance(role, NodeRole):
            raise TypeError("role must be a NodeRole")
        with self._lock:
            current = self._current(identity)
            updated = self._view(replace(current, role=role), self._now())
            self._peers[identity.node_id] = updated
            return updated

    def get(self, node_id: int) -> Optional[PeerInfo]:
        """Return an immutable, current view, or None for an unknown/local ID."""
        _integer(node_id, "node_id", 1, 6)
        with self._lock:
            now = self._now()
            peer = self._peers.get(node_id)
            return self._view(peer, now) if peer is not None else None

    def snapshot(self) -> Tuple[PeerInfo, ...]:
        """Return all peers ordered by node ID and evaluated at one local instant."""
        with self._lock:
            now = self._now()
            return tuple(self._view(self._peers[k], now) for k in sorted(self._peers))

    def refresh(self) -> Tuple[PeerInfo, ...]:
        """Persist elapsed-time status changes and return only changed peers."""
        with self._lock:
            now = self._now()
            changed = []
            for node_id in sorted(self._peers):
                previous = self._peers[node_id]
                current = self._view(previous, now)
                if current.status != previous.status:
                    self._peers[node_id] = current
                    changed.append(current)
            return tuple(changed)
