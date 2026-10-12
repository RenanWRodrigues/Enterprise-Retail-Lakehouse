"""Compare alert events with real failures: one-to-one matching, confusion matrix, latency.

Event level (does an alert warn of a failure?):

- a failure's **detection window** is ``[degradation_start - before, failure_timestamp + after]``
  (the degradation the failure produces, with a tolerance); an alert event *covers* a failure
  when its days intersect that window, on the same equipment;
- matching is **one-to-one and greedy in time**: failures in order of ``failure_timestamp``, each
  takes the earliest unmatched alert event that covers it. One alert cannot count for two
  failures, nor one failure for two alerts;
- TP = matched failures; FN = unmatched failures; FP = unmatched alert events. There is no
  natural "true negative event": TN and the false positive rate are computed at day level.

Day level (equipment-day granularity, over the days with a detector decision):

- a day is **positive** when it intersects a failure's detection window; **predicted** when the
  equipment alerts that day. TP/FP/TN/FN, and FPR = FP / (FP + TN).

Every ratio with a zero denominator is ``None`` (undefined), never 0 or 1.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from iot_lakehouse.evaluation.detector import AlertEvent


@dataclass(frozen=True, slots=True, order=True)
class FailureWindow:
    equipment_id: str
    failure_timestamp: datetime
    degradation_start: datetime
    failure_id: str


@dataclass(frozen=True, slots=True)
class Tolerance:
    before: timedelta = timedelta(0)
    after: timedelta = timedelta(days=1)


def _start(day: date) -> datetime:
    return datetime.combine(day, time(), tzinfo=UTC)


def covers(event: AlertEvent, failure: FailureWindow, tolerance: Tolerance) -> bool:
    if event.equipment_id != failure.equipment_id:
        return False
    window_start = failure.degradation_start - tolerance.before
    window_end = failure.failure_timestamp + tolerance.after
    return (
        _start(event.first_day) <= window_end
        and _start(event.last_day + timedelta(days=1)) > window_start
    )


@dataclass(frozen=True, slots=True)
class Match:
    failure: FailureWindow
    event: AlertEvent

    @property
    def lead_time_hours(self) -> float:
        """Hours of warning before the failure (negative: the alert came after it)."""
        return (
            self.failure.failure_timestamp - _start(self.event.first_day)
        ).total_seconds() / 3600

    @property
    def delay_from_degradation_hours(self) -> float:
        """Hours from the start of the degradation to the first alert day."""
        return (
            _start(self.event.first_day) - self.failure.degradation_start
        ).total_seconds() / 3600


@dataclass(frozen=True, slots=True)
class EventOutcome:
    matches: tuple[Match, ...]
    missed: tuple[FailureWindow, ...]  # FN
    false_alarms: tuple[AlertEvent, ...]  # FP


def match_events(
    events: Iterable[AlertEvent], failures: Iterable[FailureWindow], tolerance: Tolerance
) -> EventOutcome:
    """One-to-one greedy matching in time order (deterministic for the same inputs)."""
    pending = sorted(events, key=lambda e: (e.first_day, e.equipment_id, e.last_day))
    matches, missed = [], []
    for failure in sorted(
        failures, key=lambda f: (f.failure_timestamp, f.equipment_id, f.failure_id)
    ):
        found = next((e for e in pending if covers(e, failure, tolerance)), None)
        if found is None:
            missed.append(failure)
        else:
            pending.remove(found)
            matches.append(Match(failure, found))
    return EventOutcome(tuple(matches), tuple(missed), tuple(pending))


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None:
        return None
    return _ratio(2 * precision * recall, precision + recall)


@dataclass(frozen=True, slots=True)
class Confusion:
    tp: int
    fp: int
    tn: int
    fn: int

    @property
    def precision(self) -> float | None:
        return _ratio(self.tp, self.tp + self.fp)

    @property
    def recall(self) -> float | None:
        return _ratio(self.tp, self.tp + self.fn)

    @property
    def f1(self) -> float | None:
        return _f1(self.precision, self.recall)

    @property
    def false_positive_rate(self) -> float | None:
        return _ratio(self.fp, self.fp + self.tn)


def event_confusion(outcome: EventOutcome) -> Confusion:
    """TN is 0 at event level (no negative events exist); use the day level for FPR."""
    return Confusion(len(outcome.matches), len(outcome.false_alarms), 0, len(outcome.missed))


def day_confusion(
    decisions: Sequence[tuple[str, date, bool]],
    failures: Iterable[FailureWindow],
    tolerance: Tolerance,
) -> Confusion:
    """Equipment-day confusion matrix over the days with a detector decision."""
    windows = [(f.equipment_id, f.degradation_start - tolerance.before,
                f.failure_timestamp + tolerance.after) for f in failures]  # fmt: skip

    def positive(equipment_id: str, day: date) -> bool:
        start, end = _start(day), _start(day + timedelta(days=1))
        return any(e == equipment_id and s < end and stop >= start for e, s, stop in windows)

    counts = {"tp": 0, "fp": 0, "tn": 0, "fn": 0}
    for equipment_id, day, predicted in decisions:
        actual = positive(equipment_id, day)
        counts[("t" if predicted == actual else "f") + ("p" if predicted else "n")] += 1
    return Confusion(**counts)


def latency_summary(matches: Sequence[Match]) -> dict[str, float | None]:
    """Median and minimum warning (hours before the failure) of the matched failures."""
    leads = sorted(m.lead_time_hours for m in matches)
    if not leads:
        return {"median_lead_time_hours": None, "min_lead_time_hours": None}
    middle = len(leads) // 2
    median = leads[middle] if len(leads) % 2 else (leads[middle - 1] + leads[middle]) / 2
    return {"median_lead_time_hours": round(median, 2), "min_lead_time_hours": round(leads[0], 2)}
