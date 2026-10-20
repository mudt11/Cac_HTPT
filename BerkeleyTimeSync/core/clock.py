"""Thread-safe virtual clock driven by elapsed monotonic time.

Synchronization uses schedule_correction to change the clock's rate without
jumping backwards. set_time_ms and inject_offset are explicit simulator
controls: they may jump the clock and cancel any pending correction. No
operation changes the operating system clock or starts a background thread.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from threading import RLock
from time import monotonic_ns, time_ns
from typing import Callable, Optional, Tuple

from .models import ClockState


_NS_PER_MS = 1000000
_PPM_SCALE = 1000000
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _integer(value: int, name: str, minimum: Optional[int] = None) -> None:
    if type(value) is not int:
        raise TypeError("{} must be an integer".format(name))
    if minimum is not None and value < minimum:
        raise ValueError("{} must be at least {}".format(name, minimum))


def _slew_rate(value: int) -> None:
    _integer(value, "slew_rate_ppm", 1)
    if value >= _PPM_SCALE:
        raise ValueError("slew_rate_ppm must be less than 1000000")


@dataclass(frozen=True)
class ClockSnapshot:
    """One consistent reading; monotonic_ns is local and must not go on the wire."""

    time_ms: int
    monotonic_ns: int
    remaining_correction_ns: int
    state: ClockState
    is_paused: bool

    @property
    def remaining_correction_ms(self) -> float:
        return self.remaining_correction_ns / _NS_PER_MS


class VirtualClock:
    """Virtual Unix time with integer nanosecond accounting and millisecond output.

    A 100000 ppm default changes the simulated rate by at most 10 percent,
    so a 1000 ms correction takes 10 seconds of running time. Read frequency
    does not determine clock progress. The injectable monotonic source permits
    deterministic tests without sleeping.
    """

    def __init__(
        self, initial_time_ms: Optional[int] = None, *,
        slew_rate_ppm: int = 100000,
        monotonic_source: Callable[[], int] = monotonic_ns,
    ) -> None:
        _slew_rate(slew_rate_ppm)
        if not callable(monotonic_source):
            raise TypeError("monotonic_source must be callable")
        if initial_time_ms is not None:
            _integer(initial_time_ms, "initial_time_ms", 0)
        self._lock = RLock()
        self._monotonic_source = monotonic_source
        self._anchor_real_ns = monotonic_source()
        _integer(self._anchor_real_ns, "monotonic reading", 0)
        self._last_observed_ns = self._anchor_real_ns
        self._anchor_virtual_ns = (
            time_ns() if initial_time_ms is None else initial_time_ms * _NS_PER_MS
        )
        self._remaining_ns = 0
        self._slew_rate_ppm = slew_rate_ppm
        self._paused = False

    def _observe(self) -> Tuple[int, int, int]:
        """Compute a reading under the lock without rebasing the clock."""
        real_ns = self._monotonic_source()
        _integer(real_ns, "monotonic reading", 0)
        if real_ns < self._last_observed_ns:
            raise ValueError("The monotonic source moved backwards")
        self._last_observed_ns = real_ns
        elapsed_ns = 0 if self._paused else real_ns - self._anchor_real_ns
        capacity_ns = elapsed_ns * self._slew_rate_ppm // _PPM_SCALE
        applied_ns = min(abs(self._remaining_ns), capacity_ns)
        if self._remaining_ns < 0:
            applied_ns = -applied_ns
        virtual_ns = self._anchor_virtual_ns + elapsed_ns + applied_ns
        return real_ns, virtual_ns, self._remaining_ns - applied_ns

    def _rebase(self, real_ns: int, virtual_ns: int, remaining_ns: int) -> None:
        self._anchor_real_ns = real_ns
        self._anchor_virtual_ns = virtual_ns
        self._remaining_ns = remaining_ns

    def snapshot(self) -> ClockSnapshot:
        with self._lock:
            real_ns, virtual_ns, remaining_ns = self._observe()
            return ClockSnapshot(
                time_ms=virtual_ns // _NS_PER_MS,
                monotonic_ns=real_ns,
                remaining_correction_ns=remaining_ns,
                state=ClockState.SLEWING if remaining_ns else ClockState.NORMAL,
                is_paused=self._paused,
            )

    def now_ms(self) -> int:
        return self.snapshot().time_ms

    @property
    def state(self) -> ClockState:
        return self.snapshot().state

    @property
    def is_paused(self) -> bool:
        with self._lock:
            return self._paused

    @property
    def slew_rate_ppm(self) -> int:
        with self._lock:
            return self._slew_rate_ppm

    def schedule_correction(self, correction_ms: int) -> None:
        """Replace the remaining correction with a new relative measurement.

        A new Berkeley round measures the clock's current error. Adding its
        correction to the old pending amount would count that error twice.
        Zero cancels a pending slew without jumping the current clock value.
        """
        _integer(correction_ms, "correction_ms")
        with self._lock:
            real_ns, virtual_ns, _ = self._observe()
            self._rebase(real_ns, virtual_ns, correction_ms * _NS_PER_MS)

    def set_slew_rate_ppm(self, value: int) -> None:
        _slew_rate(value)
        with self._lock:
            self._rebase(*self._observe())
            self._slew_rate_ppm = value

    def pause(self) -> None:
        """Freeze virtual time and slew progress for a simulator demonstration."""
        with self._lock:
            if not self._paused:
                self._rebase(*self._observe())
                self._paused = True

    def resume(self) -> None:
        """Resume from the frozen reading without adding time spent paused."""
        with self._lock:
            if self._paused:
                self._rebase(*self._observe())
                self._paused = False

    def set_time_ms(self, value: int) -> None:
        """Explicitly set simulator time and cancel slewing; may move backwards."""
        _integer(value, "time_ms", 0)
        with self._lock:
            real_ns, _, _ = self._observe()
            self._rebase(real_ns, value * _NS_PER_MS, 0)

    def inject_offset(self, delta_ms: int) -> None:
        """Inject a signed demo fault, preserving submillisecond time precision."""
        _integer(delta_ms, "delta_ms")
        with self._lock:
            real_ns, virtual_ns, _ = self._observe()
            new_ns = virtual_ns + delta_ms * _NS_PER_MS
            if new_ns < 0:
                raise ValueError("Injected offset would precede the Unix epoch")
            self._rebase(real_ns, new_ns, 0)

    def reset_to_system_time(self) -> None:
        """Reset only this simulator to the current OS time and cancel slewing."""
        with self._lock:
            real_ns, _, _ = self._observe()
            self._rebase(real_ns, time_ns(), 0)

    def format_time(self, tz: Optional[tzinfo] = None) -> str:
        """Display HH:mm:ss.SSS in the supplied zone, or the local zone by default.

        Day information is retained in now_ms; only this display wraps at
        midnight. A timestamp outside datetime's supported range raises an
        explicit ValueError rather than silently wrapping its date.
        """
        if tz is not None and not isinstance(tz, tzinfo):
            raise TypeError("tz must be a timezone or None")
        try:
            instant = (_UNIX_EPOCH + timedelta(milliseconds=self.now_ms())).astimezone(tz)
        except (OverflowError, OSError) as exc:
            raise ValueError("Clock time is outside the supported display range") from exc
        return "{}.{:03d}".format(instant.strftime("%H:%M:%S"), instant.microsecond // 1000)
