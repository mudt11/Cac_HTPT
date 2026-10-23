"""Validate measurement quality without rejecting clocks for large offsets.

All timely Worker measurements and the Master contribute to Berkeley's mean.
There is no offset threshold, cluster selection or tied-cluster rejection.
"""

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Dict, Iterable, Optional, Tuple

from .models import NodeIdentity, RoundKey
from .protocol import MAX_INTEGER


class FilterStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    INSUFFICIENT_SAMPLES = "INSUFFICIENT_SAMPLES"
    AMBIGUOUS_CLUSTER = "AMBIGUOUS_CLUSTER"


class RejectionReason(str, Enum):
    HIGH_RTT = "HIGH_RTT"
    OUTSIDE_CLUSTER = "OUTSIDE_CLUSTER"
    INSUFFICIENT_CONSENSUS = "INSUFFICIENT_CONSENSUS"
    AMBIGUOUS_CONSENSUS = "AMBIGUOUS_CONSENSUS"


def _number(value: float, name: str, *, nonnegative: bool = False) -> float:
    if type(value) not in (int, float):
        raise TypeError("{} must be an integer or float".format(name))
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError("{} is outside the numeric range".format(name)) from exc
    if not isfinite(result) or abs(value) > MAX_INTEGER:
        raise ValueError("{} must be finite and within the protocol numeric range".format(name))
    if nonnegative and result < 0:
        raise ValueError("{} must not be negative".format(name))
    return result


@dataclass(frozen=True)
class OffsetSample:
    """One estimated offset from the Master at a common reference instant.

    offset_ms is Worker time minus Master time, after delay estimation by
    the Berkeley engine. Fractional milliseconds are retained. rtt_ms is the
    nonnegative request/reply duration measured locally by that engine.
    """

    identity: NodeIdentity
    round_key: RoundKey
    offset_ms: float
    rtt_ms: float

    def __post_init__(self) -> None:
        if not isinstance(self.identity, NodeIdentity):
            raise TypeError("identity must be a NodeIdentity")
        if not isinstance(self.round_key, RoundKey):
            raise TypeError("round_key must be a RoundKey")
        object.__setattr__(self, "offset_ms", _number(self.offset_ms, "offset_ms"))
        object.__setattr__(self, "rtt_ms", _number(self.rtt_ms, "rtt_ms", nonnegative=True))


@dataclass(frozen=True)
class RejectedSample:
    sample: OffsetSample
    reason: RejectionReason


@dataclass(frozen=True)
class FilterResult:
    """A complete partition of supplied Workers plus the implicit Master.

    No samples are accepted when selection is insufficient or ambiguous.
    Exclusion from averaging is not an instruction to skip clock correction:
    the engine decides which excluded clocks have usable correction estimates.
    """

    round_key: RoundKey
    status: FilterStatus
    accepted: Tuple[OffsetSample, ...]
    rejected: Tuple[RejectedSample, ...]

    @property
    def can_average(self) -> bool:
        return self.status == FilterStatus.ACCEPTED

    @property
    def master_included(self) -> bool:
        return any(s.identity == self.round_key.leader.node for s in self.accepted)

    @property
    def spread_ms(self) -> Optional[float]:
        if not self.accepted:
            return None
        offsets = [sample.offset_ms for sample in self.accepted]
        return max(offsets) - min(offsets)


def filter_outliers(
    round_key: RoundKey, samples: Iterable[OffsetSample], *,
    max_rtt_ms: float = 500.0,
    min_cluster_size: int = 2,
) -> FilterResult:
    """Accept every timely sample, regardless of its clock offset.

    samples contains only Worker estimates; the Master is added once with
    offset 0 and RTT 0. Repeated node IDs or mixed rounds are input errors,
    never additional votes. Size includes the Master when it belongs to the
    accepted set. The minimum prevents averaging the Master alone when no
    usable Worker report exists. RTT remains a measurement quality check.
    Results are ordered by node ID and are independent of input order.
    """
    if not isinstance(round_key, RoundKey):
        raise TypeError("round_key must be a RoundKey")
    max_rtt = _number(max_rtt_ms, "max_rtt_ms", nonnegative=True)
    if type(min_cluster_size) is not int:
        raise TypeError("min_cluster_size must be an integer")
    if not 2 <= min_cluster_size <= 6:
        raise ValueError("min_cluster_size must be between 2 and 6")
    workers = tuple(samples)
    if len(workers) > 5:
        raise ValueError("A six-node cluster has at most five Worker estimates")
    master = OffsetSample(round_key.leader.node, round_key, 0.0, 0.0)
    all_samples: Dict[int, OffsetSample] = {master.identity.node_id: master}
    for sample in workers:
        if not isinstance(sample, OffsetSample):
            raise TypeError("Each Worker estimate must be an OffsetSample")
        if sample.round_key != round_key:
            raise ValueError("Cannot mix estimates from different rounds or Master tenures")
        if sample.identity.node_id in all_samples:
            raise ValueError("A node ID may contribute only one sample; Master is implicit")
        all_samples[sample.identity.node_id] = sample

    reasons: Dict[int, RejectionReason] = {}
    eligible = []
    for sample in all_samples.values():
        if sample.rtt_ms > max_rtt:
            reasons[sample.identity.node_id] = RejectionReason.HIGH_RTT
        else:
            eligible.append(sample)
    if len(eligible) < min_cluster_size:
        status = FilterStatus.INSUFFICIENT_SAMPLES
        accepted: Tuple[OffsetSample, ...] = ()
        failure_reason = RejectionReason.INSUFFICIENT_CONSENSUS
    else:
        status = FilterStatus.ACCEPTED
        accepted = tuple(sorted(eligible, key=lambda s: s.identity.node_id))
        failure_reason = RejectionReason.INSUFFICIENT_CONSENSUS

    kept_ids = {sample.identity.node_id for sample in accepted}
    rejected = tuple(
        RejectedSample(all_samples[node_id], reasons.get(node_id, failure_reason))
        for node_id in sorted(all_samples) if node_id not in kept_ids
    )
    return FilterResult(round_key, status, accepted, rejected)
