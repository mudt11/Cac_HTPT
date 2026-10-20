"""Bully election state machine, independent of sockets, GUI and heartbeat.

Classic priority rule: probe every known higher ID (even marked OFFLINE),
promote if none answers, otherwise wait for a coordinator and retry on timeout.
Discovery must supply the membership view; a recovered higher node must start
an election. Timely delivery and eventual stable membership are required for
convergence. This is not a quorum protocol and cannot prevent partition leaders.

Freshness extension using the EXISTING optional COORDINATOR.reply_to field:
an unsolicited announcement triggers a probe, never immediate adoption. The
winner confirms its current tenure against that probe's message ID. This avoids
ordering random UUID epochs, and allows a falsely suspected incumbent to prove
it is still Master without manufacturing a new tenure for every query.

Calls belong to one controller thread. Returned outbound messages must be
dispatched promptly over TCP; deadlines include dispatch time. The controller
applies role/leader changes to heartbeat/discovery and cancels old sync sessions.
This module only reads PeerRegistry. Manual mode gives an operator-selected
Master priority over automatic elections until its heartbeat times out. The
controller must call leader_timed_out only after validated heartbeat failure;
discovery contact and ordinary start calls cannot release a manual selection.
FORCE_MASTER announces its sender as Master, so the local force_master action
is invoked on the selected node. Remote GUI command routing is a controller
concern. Operator commands must be serialized; concurrent manual selections
have no global ordering in this wire protocol and are not a consensus scheme.
"""

from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Callable, Dict, Optional, Tuple
from uuid import UUID, uuid4

from .models import LeaderIdentity, NodeIdentity, NodeRole
from .peer_registry import PeerRegistry
from .protocol import Message, MessageType, ProtocolError


class ElectionState(str, Enum):
    IDLE = "IDLE"
    WAITING_OK = "WAITING_OK"
    WAITING_COORDINATOR = "WAITING_COORDINATOR"
    FOLLOWER = "FOLLOWER"
    LEADER = "LEADER"


class ElectionMode(str, Enum):
    AUTOMATIC = "AUTOMATIC"
    MANUAL = "MANUAL"


@dataclass(frozen=True)
class ElectionSnapshot:
    state: ElectionState
    role: NodeRole
    leader: Optional[LeaderIdentity]
    election_id: Optional[UUID]
    deadline_ns: Optional[int]
    higher_nodes: Tuple[NodeIdentity, ...]
    responders: Tuple[NodeIdentity, ...]
    mode: ElectionMode = ElectionMode.AUTOMATIC
    manual_reason: Optional[str] = None


@dataclass(frozen=True)
class ElectionUpdate:
    outbound: Tuple[Message, ...]
    snapshot: ElectionSnapshot
    leader_changed: bool
    detail: str


def _duration(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 60000:
        raise ValueError("{} must be in [1, 60000] milliseconds".format(name))
    return value * 1000000


class BullyElection:
    """One local election participant with IDs 1..6 and pinned process IDs.

    Construct once per application boot. start is used after discovery settles,
    recovery, or heartbeat timeout. Repeated start during a live attempt and
    duplicate packets never reset deadlines. tick alone advances expired phases;
    snapshot does not silently promote or consume any outbound action.

    The leader is None while campaigning so the controller stops old sync work.
    A new local promotion creates a fresh LeaderIdentity; answering an election
    as an incumbent retains the existing tenure. A coordinator's election_id
    identifies its OWN election, not necessarily this node's attempt.
    """

    def __init__(
        self, registry: PeerRegistry, *, ok_timeout_ms: int = 2000,
        coordinator_timeout_ms: int = 6000, max_event_age_ms: int = 1000,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not isinstance(registry, PeerRegistry):
            raise TypeError("registry must be PeerRegistry")
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        self._ok_ns = _duration(ok_timeout_ms, "ok_timeout_ms")
        self._coordinator_ns = _duration(coordinator_timeout_ms, "coordinator_timeout_ms")
        self._event_age_ns = _duration(max_event_age_ms, "max_event_age_ms")
        self._registry, self._local = registry, registry.local_identity
        self._monotonic_source = monotonic_source
        self._last_now_ns = None  # type: Optional[int]
        self._state = ElectionState.IDLE
        self._leader = None  # type: Optional[LeaderIdentity]
        self._mode = ElectionMode.AUTOMATIC
        self._manual_reason = None  # type: Optional[str]
        self._election_id = None  # type: Optional[UUID]
        self._leader_election_id = None  # type: Optional[UUID]
        self._deadline_ns = None  # type: Optional[int]
        self._started_ns = None  # type: Optional[int]
        self._requests = {}  # type: Dict[int, Message]
        self._responders = {}  # type: Dict[int, NodeIdentity]
        self._pinned = {}  # type: Dict[int, NodeIdentity]
        self._seen = {}  # type: Dict[Tuple[NodeIdentity, UUID], Tuple[Message, Optional[Message]]]
        self._retired = {}  # type: Dict[LeaderIdentity, None]
        self._now()

    def _now(self) -> int:
        now = self._monotonic_source()
        if type(now) is not int or now < 0:
            raise ValueError("monotonic_source must return nonnegative integer nanoseconds")
        if self._last_now_ns is not None and now < self._last_now_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_now_ns = now
        return now

    @property
    def _active(self) -> bool:
        return self._state in (ElectionState.WAITING_OK, ElectionState.WAITING_COORDINATOR)

    def snapshot(self) -> ElectionSnapshot:
        self._now()
        role = (NodeRole.CANDIDATE if self._active else
                NodeRole.MASTER if self._state == ElectionState.LEADER else
                NodeRole.WORKER if self._state == ElectionState.FOLLOWER else NodeRole.UNASSIGNED)
        return ElectionSnapshot(
            self._state, role, self._leader, self._election_id, self._deadline_ns,
            tuple(self._pinned[key] for key in sorted(self._pinned)),
            tuple(self._responders[key] for key in sorted(self._responders)),
            self._mode, self._manual_reason)

    def _result(
        self, previous: Optional[LeaderIdentity], outbound: Tuple[Message, ...], detail: str,
    ) -> ElectionUpdate:
        return ElectionUpdate(outbound, self.snapshot(), previous != self._leader, detail)

    def _clear_leader(self) -> None:
        if self._leader is not None:
            if len(self._retired) >= 256:
                self._retired.pop(next(iter(self._retired)))
            self._retired[self._leader] = None
        self._leader = None
        self._leader_election_id = None
        self._mode = ElectionMode.AUTOMATIC
        self._manual_reason = None

    def _announcement(self, target: int, reply_to: Optional[UUID] = None) -> Message:
        if self._mode == ElectionMode.MANUAL:
            return Message(
                MessageType.FORCE_MASTER, self._local, {"reason": self._manual_reason},
                target_node_id=target, leader=self._leader, reply_to=reply_to)
        return Message(
            MessageType.COORDINATOR, self._local,
            {"election_id": str(self._leader_election_id)},
            target_node_id=target, leader=self._leader, reply_to=reply_to)

    def _promote(self) -> Tuple[Message, ...]:
        self._leader = LeaderIdentity(self._local)
        self._leader_election_id = self._election_id
        self._state = ElectionState.LEADER
        self._deadline_ns = None
        self._requests.clear()
        self._responders.clear()
        self._pinned.clear()
        return tuple(self._announcement(peer.identity.node_id) for peer in self._registry.snapshot())

    def _begin(self, now: int) -> Tuple[Message, ...]:
        self._clear_leader()
        self._election_id = uuid4()
        self._started_ns = now
        self._responders.clear()
        self._pinned = {peer.identity.node_id: peer.identity for peer in self._registry.snapshot()
                        if peer.identity.node_id > self._local.node_id}
        self._requests = {
            node_id: Message(MessageType.ELECTION, self._local,
                             {"election_id": str(self._election_id)}, target_node_id=node_id)
            for node_id in sorted(self._pinned)
        }
        self._state = ElectionState.WAITING_OK
        self._deadline_ns = now + self._ok_ns
        return tuple(self._requests.values()) if self._requests else self._promote()

    def start(self, *, expected_leader: Optional[LeaderIdentity] = None) -> ElectionUpdate:
        """Campaign once, preserving manual mode even if expected_leader matches.

        The heartbeat controller uses leader_timed_out to explicitly release a
        manual selection. Ordinary startup/recovery events cannot override it.
        """
        if expected_leader is not None and not isinstance(expected_leader, LeaderIdentity):
            raise TypeError("expected_leader must be LeaderIdentity or None")
        now, previous = self._now(), self._leader
        if expected_leader is not None and previous != expected_leader:
            return self._result(previous, (), "Stale timeout for another leader")
        if self._mode == ElectionMode.MANUAL:
            return self._result(previous, (), "Manual Master has priority; await heartbeat failure")
        if self._active:
            return self._result(previous, (), "Election already active; call tick for deadlines")
        return self._result(previous, self._begin(now), "Election started")

    def force_master(self, reason: str) -> ElectionUpdate:
        """Operator action on THIS node; announce a fresh manual tenure.

        A repeated action on the current local manual Master resends its claim
        without changing its tenure/reason or resetting heartbeat deadlines.
        Validation completes before any state change. This is a trusted local
        operator action, not a request to force an arbitrary remote node ID.
        """
        previous = self._leader
        proposed = LeaderIdentity(self._local)
        Message(MessageType.FORCE_MASTER, self._local, {"reason": reason}, leader=proposed)
        self._now()
        if self._mode == ElectionMode.MANUAL and self._state == ElectionState.LEADER:
            outbound = tuple(self._announcement(peer.identity.node_id)
                             for peer in self._registry.snapshot())
            return self._result(previous, outbound, "Current manual Master reannounced")
        self._install_manual(proposed, reason, local=True)
        outbound = tuple(self._announcement(peer.identity.node_id)
                         for peer in self._registry.snapshot())
        return self._result(previous, outbound, "Local node selected as manual Master")

    def _install_manual(self, leader: LeaderIdentity, reason: str, *, local: bool) -> None:
        self._clear_leader()
        self._leader = leader
        self._retired.pop(leader, None)
        self._mode = ElectionMode.MANUAL
        self._manual_reason = reason
        self._state = ElectionState.LEADER if local else ElectionState.FOLLOWER
        self._election_id = None
        self._started_ns = None
        self._deadline_ns = None
        self._requests.clear()
        self._responders.clear()
        self._pinned.clear()

    def leader_timed_out(self, expected_leader: LeaderIdentity) -> ElectionUpdate:
        """Apply a validated remote-Master heartbeat timeout, then run Bully.

        This pure state machine does not independently measure heartbeats. The
        caller must check HeartbeatSnapshot.master_timed_out first. Comparing
        the full tenure prevents an old timeout cancelling a newer selection.
        A node cannot declare its own heartbeat lost through this method.
        """
        if not isinstance(expected_leader, LeaderIdentity):
            raise TypeError("expected_leader must be LeaderIdentity")
        now, previous = self._now(), self._leader
        if previous != expected_leader:
            return self._result(previous, (), "Stale timeout for another leader")
        if previous.node == self._local:
            return self._result(previous, (), "Cannot expire the local Master's own heartbeat")
        return self._result(previous, self._begin(now), "Master heartbeat timed out; automatic election started")

    def tick(self) -> ElectionUpdate:
        now, previous = self._now(), self._leader
        if not self._active or now < self._deadline_ns:
            return self._result(previous, (), "No election deadline reached")
        # A restarted/new higher node must be probed under its own boot ID.
        current = {peer.identity.node_id: peer.identity for peer in self._registry.snapshot()
                   if peer.identity.node_id > self._local.node_id}
        if current != self._pinned:
            return self._result(previous, self._begin(now), "Higher membership changed; retry")
        if self._state == ElectionState.WAITING_OK:
            return self._result(previous, self._promote(), "No higher response; local node won")
        return self._result(previous, self._begin(now), "Coordinator timeout; election restarted")

    def _remember(self, message: Message, reply: Optional[Message] = None) -> None:
        if len(self._seen) >= 256:
            self._seen.pop(next(iter(self._seen)))
        self._seen[(message.source, message.message_id)] = (message, reply)

    def handle(
        self, message: Message, *, received_monotonic_ns: Optional[int] = None,
    ) -> ElectionUpdate:
        """Process only election messages; other types leave state untouched.

        The TCP/controller layer validates the actual endpoint/connection before
        routing here. Boot IDs are checked again against current membership.
        Late OKs cannot reopen or extend a phase. A proved COORDINATOR may end
        an overdue phase before tick, but it must reference that phase's request.
        """
        if not isinstance(message, Message):
            raise TypeError("Expected a Message")
        now, previous = self._now(), self._leader
        if message.type not in (
            MessageType.ELECTION, MessageType.OK, MessageType.COORDINATOR, MessageType.FORCE_MASTER,
        ):
            return self._result(previous, (), "Message belongs to another handler")
        received = now if received_monotonic_ns is None else received_monotonic_ns
        if type(received) is not int or not 0 <= received <= now:
            raise ProtocolError("Invalid local election receive timestamp")
        if now - received > self._event_age_ns:
            return self._result(previous, (), "Stale election event")
        if message.target_node_id not in (None, self._local.node_id):
            return self._result(previous, (), "Message targets another node")
        peer = self._registry.get(message.source.node_id)
        if peer is None or peer.identity != message.source:
            return self._result(previous, (), "Unknown or obsolete source identity")
        cached = self._seen.get((message.source, message.message_id))
        if cached is not None and cached[0] != message:
            raise ProtocolError("Election message ID reused with different contents")
        if message.type == MessageType.FORCE_MASTER:
            if cached is not None:
                return self._result(previous, (), "Duplicate manual command; state unchanged")
            if message.leader in self._retired:
                return self._result(previous, (), "Retired manual tenure ignored")
            if self._mode == ElectionMode.MANUAL and self._leader == message.leader:
                if message.payload["reason"] != self._manual_reason:
                    raise ProtocolError("Manual tenure reused with another reason")
                self._remember(message)
                return self._result(previous, (), "Current manual Master confirmed; state unchanged")
            self._install_manual(message.leader, message.payload["reason"], local=False)
            self._remember(message)
            return self._result(previous, (), "Operator-selected manual Master adopted")
        if self._mode == ElectionMode.MANUAL:
            # Never replay an old cached automatic OK while manual mode holds.
            # The manual Master can teach a joining node about its selection.
            outbound = ()
            if self._state == ElectionState.LEADER and message.type in (
                MessageType.ELECTION, MessageType.COORDINATOR,
            ):
                outbound = (self._announcement(message.source.node_id, message.message_id),)
            self._remember(message)
            return self._result(previous, outbound, "Automatic election suppressed by manual Master")
        if cached is not None:
            outbound = (cached[1],) if cached[1] is not None else ()
            if cached[1] is not None and self._state == ElectionState.LEADER:
                outbound += (self._announcement(message.source.node_id, message.message_id),)
            return self._result(previous, outbound, "Duplicate message; state unchanged")
        if message.type == MessageType.ELECTION:
            if message.source.node_id >= self._local.node_id:
                return self._result(previous, (), "ELECTION must come from a lower ID")
            reply = Message(
                MessageType.OK, self._local, message.payload,
                target_node_id=message.source.node_id, reply_to=message.message_id)
            self._remember(message, reply)
            if self._state == ElectionState.LEADER:
                outbound = (reply, self._announcement(message.source.node_id, message.message_id))
            elif self._active:
                outbound = (reply,)
            else:
                outbound = (reply,) + self._begin(now)
            return self._result(previous, outbound, "Lower election answered")
        if message.type == MessageType.OK:
            request = self._requests.get(message.source.node_id)
            if (not self._active or now >= self._deadline_ns or request is None
                    or received < self._started_ns
                    or message.source != self._pinned.get(message.source.node_id)
                    or message.reply_to != request.message_id
                    or message.payload["election_id"] != str(self._election_id)):
                return self._result(previous, (), "Stale or unrelated OK")
            self._remember(message)
            self._responders[message.source.node_id] = message.source
            if self._state == ElectionState.WAITING_OK:
                self._state = ElectionState.WAITING_COORDINATOR
                self._deadline_ns = now + self._coordinator_ns
            return self._result(previous, (), "Higher node answered; awaiting coordinator")
        # COORDINATOR election_id is the winner's own attempt, not ours.
        if message.source.node_id < self._local.node_id:
            self._remember(message)
            if self._state == ElectionState.LEADER:
                outbound = (self._announcement(message.source.node_id),)
            else:
                outbound = () if self._active else self._begin(now)
            return self._result(previous, outbound, "Lower coordinator challenged by Bully priority")
        if self._leader == message.leader:
            if message.payload["election_id"] != str(self._leader_election_id):
                raise ProtocolError("Coordinator tenure reused with another election ID")
            self._remember(message)
            return self._result(previous, (), "Current coordinator confirmed; state unchanged")
        request = self._requests.get(message.source.node_id)
        proven = (self._active and request is not None
                  and message.source == self._pinned.get(message.source.node_id)
                  and message.reply_to == request.message_id
                  and received >= self._started_ns)
        if not proven:
            if message.leader in self._retired:
                return self._result(previous, (), "Unproved retired coordinator ignored")
            outbound = () if self._active else self._begin(now)
            return self._result(previous, outbound, "Probe unsolicited coordinator before adopting")
        if any(node_id > message.source.node_id for node_id in self._responders):
            return self._result(previous, (), "A higher responding candidate is still pending")
        self._clear_leader()
        self._leader = message.leader
        self._leader_election_id = UUID(message.payload["election_id"])
        self._retired.pop(self._leader, None)
        self._state = ElectionState.FOLLOWER
        self._deadline_ns = None
        self._requests.clear()
        self._responders.clear()
        self._pinned.clear()
        self._remember(message)
        return self._result(previous, (), "Correlated higher coordinator adopted")
