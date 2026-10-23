"""Delay-aware offset estimation and Berkeley correction planning.

Four clock timestamps estimate Worker-minus-Master offset. Measurement uses
the standard midpoint relation also described in RFC 5905 section 8; group
selection and relative corrections follow Berkeley/TEMPO. Symmetric paths
and approximately stable offsets over one polling round remain assumptions.

This module is pure computation. It never sends packets or changes a clock.
The controller must register a plan with SyncSession before applying the
Master correction and dispatching Worker commands, so expired plans cannot
be applied merely because an earlier snapshot was still COMPUTING.
"""

from dataclasses import dataclass
from fractions import Fraction
from typing import Dict, Iterable, Optional, Tuple

from .models import RoundKey, SyncRoundState
from .outlier_filter import (
    FilterResult, OffsetSample, RejectionReason, filter_outliers,
)
from .protocol import MAX_INTEGER, Message, MessageType, ProtocolError, validate_context
from .sync_session import ReportSample, SessionSnapshot, SessionStateError


def _integer(value: int, name: str, minimum: int = 0, maximum: Optional[int] = None) -> None:
    if type(value) is not int:
        raise TypeError("{} must be an integer".format(name))
    if value < minimum or (maximum is not None and value > maximum):
        raise ValueError("{} is outside the allowed range".format(name))


@dataclass(frozen=True)
class ClockMeasurement:
    """A correlated session report plus the Master clock reading at reception.

    Capture master_received_time_ms when the response arrives, not later
    during averaging. It cannot be inferred from elapsed monotonic time when
    the Master is slewing. Worker receive/send timestamps come from the report.
    The additional timestamp stays local; no wire protocol change is needed.
    """

    report: ReportSample
    master_received_time_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.report, ReportSample):
            raise TypeError("report must be a ReportSample")
        request, response = self.report.request, self.report.response
        if not isinstance(request, Message) or request.type != MessageType.SYNC_REQUEST:
            raise ProtocolError("Measurement requires a SYNC_REQUEST")
        if not isinstance(response, Message) or response.type != MessageType.TIME_REPORT:
            raise ProtocolError("Measurement requires a TIME_REPORT")
        validate_context(response, recipient_id=request.source.node_id,
                         expected_round=request.round_key)
        if response.source.node_id != request.target_node_id or response.reply_to != request.message_id:
            raise ProtocolError("Measurement report does not answer its poll")
        _integer(self.master_received_time_ms, "master_received_time_ms", 0, MAX_INTEGER)
        _integer(self.report.sent_monotonic_ns, "sent_monotonic_ns")
        _integer(self.report.received_monotonic_ns, "received_monotonic_ns")
        duration_ns = self.report.rtt_ns
        if not 0 <= duration_ns <= MAX_INTEGER * 1000000:
            raise ValueError("Measurement has a negative or unrepresentable RTT")
        master_elapsed = self.master_received_time_ms - request.payload["master_time_ms"]
        if master_elapsed < 0:
            raise ValueError("Master clock moved backwards during the measurement")
        worker_elapsed = response.payload["sent_time_ms"] - response.payload["received_time_ms"]
        # VirtualClock's slew rate is below 100 percent, so clock intervals
        # advance at less than twice real elapsed time. Allow 1 ms quantization.
        upper_bound = Fraction(2 * duration_ns, 1000000) + 1
        if master_elapsed > upper_bound or worker_elapsed > upper_bound:
            raise ValueError("Clock interval is inconsistent with RTT; a clock may have stepped")


def _offset(measurement: ClockMeasurement) -> Fraction:
    report = measurement.report
    t1 = report.request.payload["master_time_ms"]
    t2 = report.response.payload["received_time_ms"]
    t3 = report.response.payload["sent_time_ms"]
    t4 = measurement.master_received_time_ms
    # Subtract integer timestamps before any floating-point conversion.
    return Fraction((t2 - t1) + (t3 - t4), 2)


def estimate_offset(measurement: ClockMeasurement) -> OffsetSample:
    """Estimate Worker-minus-Master offset while retaining fractional ms.

    offset = ((Worker_receive - Master_send) +
              (Worker_send - Master_receive)) / 2.
    Using both Worker timestamps accounts for processing time instead of
    treating that entire interval as network transit. rtt_ms remains the
    full locally observed request/reply duration for the filter's quality gate.
    Clock rate changes and asymmetric paths can still bias the estimate.
    """
    if not isinstance(measurement, ClockMeasurement):
        raise TypeError("Expected a ClockMeasurement")
    response = measurement.report.response
    return OffsetSample(
        response.source, response.round_key, float(_offset(measurement)),
        measurement.report.rtt_ns / 1000000,
    )


def _round_ms(value: Fraction) -> int:
    """Round to nearest millisecond, with exact half ties away from zero."""
    magnitude = abs(value)
    rounded = (2 * magnitude.numerator + magnitude.denominator) // (2 * magnitude.denominator)
    result = -rounded if value < 0 else rounded
    _integer(result, "correction_ms", -MAX_INTEGER, MAX_INTEGER)
    return result


def _validate_master_timeline(measurements: Iterable[ClockMeasurement]) -> None:
    """Reject a Master clock step between polls as well as within a poll."""
    readings = []
    for measurement in measurements:
        report = measurement.report
        readings.append((report.sent_monotonic_ns, report.request.payload["master_time_ms"]))
        readings.append((report.received_monotonic_ns, measurement.master_received_time_ms))
    readings.sort()
    for previous, current in zip(readings, readings[1:]):
        elapsed_ns = current[0] - previous[0]
        clock_elapsed_ms = current[1] - previous[1]
        if clock_elapsed_ms < 0 or clock_elapsed_ms > Fraction(2 * elapsed_ns, 1000000) + 1:
            raise ValueError("Master clock timeline is inconsistent across Worker polls")


@dataclass(frozen=True)
class BerkeleyPlan:
    """Immutable proposed corrections; readiness does not authorize dispatch.

    mean_offset_ms is diagnostic. Integer corrections are calculated from
    exact rational offsets, not the rounded diagnostic value. Per-node rounding
    may leave up to 1 ms spread even with otherwise perfect measurements.
    """

    round_key: RoundKey
    filter_result: FilterResult
    mean_offset_ms: Optional[float]
    master_correction_ms: Optional[int]
    adjustments: Tuple[Message, ...]
    omitted_worker_ids: Tuple[int, ...]

    @property
    def ready(self) -> bool:
        return self.filter_result.can_average and self.master_correction_ms is not None


def build_plan(
    snapshot: SessionSnapshot, measurements: Iterable[ClockMeasurement], *,
    max_rtt_ms: float = 500.0,
    min_cluster_size: int = 2,
) -> BerkeleyPlan:
    """Build relative corrections for a COMPUTING snapshot without side effects.

    Every report needs an accompanying Master reception timestamp. Slow
    measurements are neither averaged nor corrected. Every timely report is
    averaged with the Master, regardless of the size of its clock offset.

    No consensus returns a non-ready plan with no corrections. Missing Worker
    reports remain omitted, allowing the session to finish PARTIAL. Callers
    must revalidate the live session before committing this plan.
    """
    if not isinstance(snapshot, SessionSnapshot):
        raise TypeError("snapshot must be a SessionSnapshot")
    if snapshot.state != SyncRoundState.COMPUTING:
        raise SessionStateError("Berkeley planning requires a COMPUTING round")
    if snapshot.adjustments or snapshot.acknowledgments:
        raise ValueError("A computation snapshot cannot already contain corrections or ACKs")
    key = snapshot.round_key
    if not isinstance(key, RoundKey):
        raise TypeError("Snapshot has no valid round key")
    members = {p.node_id: p for p in snapshot.participants}
    if len(members) != len(snapshot.participants) or not 2 <= len(members) <= 6:
        raise ValueError("Snapshot must contain 2 to 6 distinct node IDs")
    if members.get(key.leader.node.node_id) != key.leader.node:
        raise ValueError("Snapshot does not contain its exact Master identity")
    workers = {k: p for k, p in members.items() if p != key.leader.node}
    requests = {m.target_node_id: m for m in snapshot.requests}
    reports: Dict[int, ReportSample] = {}
    for report in snapshot.reports:
        if not isinstance(report, ReportSample):
            raise TypeError("Snapshot reports must be ReportSample objects")
        source = report.response.source
        if source.node_id in reports or workers.get(source.node_id) != source:
            raise ProtocolError("Report does not match a unique pinned Worker")
        validate_context(report.response, expected_round=key)
        if requests.get(source.node_id) != report.request:
            raise ProtocolError("Report poll does not match the session's registered request")
        if set(report.request.payload["participant_ids"]) != set(members):
            raise ProtocolError("Poll members differ from the session snapshot")
        reports[source.node_id] = report

    by_id: Dict[int, ClockMeasurement] = {}
    for measurement in measurements:
        if not isinstance(measurement, ClockMeasurement):
            raise TypeError("Every measurement must be a ClockMeasurement")
        node_id = measurement.report.response.source.node_id
        if node_id in by_id or reports.get(node_id) != measurement.report:
            raise ValueError("Measurement does not match a unique report in the snapshot")
        by_id[node_id] = measurement
    if set(by_id) != set(reports):
        raise ValueError("Every report requires its captured Master reception time")
    _validate_master_timeline(by_id.values())

    estimates = tuple(estimate_offset(by_id[k]) for k in sorted(by_id))
    selection = filter_outliers(
        key, estimates, max_rtt_ms=max_rtt_ms,
        min_cluster_size=min_cluster_size,
    )
    if not selection.can_average:
        return BerkeleyPlan(key, selection, None, None, (), tuple(sorted(workers)))

    offsets = {key.leader.node.node_id: Fraction(0)}
    offsets.update((k, _offset(m)) for k, m in by_id.items())
    mean = sum((offsets[s.identity.node_id] for s in selection.accepted), Fraction(0))
    mean /= len(selection.accepted)
    master_correction = _round_ms(mean)
    slow_ids = {item.sample.identity.node_id for item in selection.rejected
                if item.reason == RejectionReason.HIGH_RTT}
    commands = tuple(
        Message(
            type=MessageType.SYNC_ADJUST, source=key.leader.node,
            payload={"correction_ms": _round_ms(mean - offsets[node_id])},
            target_node_id=node_id, leader=key.leader, round_id=key.round_id,
            reply_to=by_id[node_id].report.response.message_id,
        )
        for node_id in sorted(by_id) if node_id not in slow_ids
    )
    targeted = {command.target_node_id for command in commands}
    return BerkeleyPlan(
        key, selection, float(mean), master_correction, commands,
        tuple(sorted(set(workers) - targeted)),
    )
