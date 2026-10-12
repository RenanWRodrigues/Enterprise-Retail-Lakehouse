"""Matching alert events to failures, confusion matrices and latency (pure Python)."""

from datetime import UTC, date, datetime, timedelta

import pytest

from iot_lakehouse.evaluation.detector import AlertEvent, alert_events
from iot_lakehouse.evaluation.matching import (
    Confusion,
    FailureWindow,
    Tolerance,
    day_confusion,
    event_confusion,
    latency_summary,
    match_events,
)

TOL = Tolerance(before=timedelta(0), after=timedelta(days=1))


def _failure(fid: str, equipment: str, fail_day: int, degrade_days: int = 3) -> FailureWindow:
    at = datetime(2025, 1, fail_day, 12, tzinfo=UTC)
    return FailureWindow(equipment, at, at - timedelta(days=degrade_days), fid)


def _event(equipment: str, first: int, last: int) -> AlertEvent:
    return AlertEvent(equipment, date(2025, 1, first), date(2025, 1, last))


def test_an_alert_during_the_degradation_is_a_detection() -> None:
    outcome = match_events([_event("E1", 8, 9)], [_failure("F1", "E1", 10)], TOL)

    [match] = outcome.matches
    assert (outcome.missed, outcome.false_alarms) == ((), ())
    assert match.lead_time_hours == 60.0  # 2025-01-08 00:00 -> 2025-01-10 12:00
    assert match.delay_from_degradation_hours == 12.0  # degradation 7 12:00 -> alert 8 00:00


def test_an_alert_far_from_any_failure_is_a_false_positive() -> None:
    outcome = match_events([_event("E1", 1, 1)], [_failure("F1", "E1", 20)], TOL)

    assert event_confusion(outcome) == Confusion(tp=0, fp=1, tn=0, fn=1)


def test_an_alert_on_another_equipment_does_not_detect() -> None:
    outcome = match_events([_event("E2", 9, 9)], [_failure("F1", "E1", 10)], TOL)

    assert (len(outcome.missed), len(outcome.false_alarms)) == (1, 1)


def test_one_alert_cannot_detect_two_overlapping_failures() -> None:
    failures = [_failure("F1", "E1", 10), _failure("F2", "E1", 11)]  # windows overlap

    outcome = match_events([_event("E1", 9, 11)], failures, TOL)

    assert [m.failure.failure_id for m in outcome.matches] == ["F1"]  # the earliest failure
    assert [f.failure_id for f in outcome.missed] == ["F2"]


def test_two_alerts_cannot_both_detect_one_failure() -> None:
    outcome = match_events(
        [_event("E1", 8, 8), _event("E1", 10, 10)], [_failure("F1", "E1", 10)], TOL
    )

    assert [m.event.first_day.day for m in outcome.matches] == [8]  # the earliest alert
    assert [e.first_day.day for e in outcome.false_alarms] == [10]


def test_the_tolerance_after_the_failure_is_respected() -> None:
    late = _event("E1", 12, 12)  # failure at 10 12:00; window ends 11 12:00

    assert match_events([late], [_failure("F1", "E1", 10)], TOL).matches == ()
    assert match_events(
        [late], [_failure("F1", "E1", 10)], Tolerance(after=timedelta(days=2))
    ).matches


def test_empty_ground_truth_leaves_recall_undefined() -> None:
    outcome = match_events([_event("E1", 1, 2)], [], TOL)
    confusion = event_confusion(outcome)

    assert confusion == Confusion(tp=0, fp=1, tn=0, fn=0)
    assert (confusion.precision, confusion.recall, confusion.f1) == (0.0, None, None)


def test_no_prediction_leaves_precision_undefined() -> None:
    confusion = event_confusion(match_events([], [_failure("F1", "E1", 10)], TOL))

    assert (confusion.precision, confusion.recall, confusion.f1) == (None, 0.0, None)


def test_nothing_at_all_leaves_every_ratio_undefined() -> None:
    confusion = event_confusion(match_events([], [], TOL))

    assert (confusion.precision, confusion.recall, confusion.f1, confusion.false_positive_rate) == (
        None, None, None, None,
    )  # fmt: skip


def test_all_days_predicted_positive_at_day_level() -> None:
    decisions = [("E1", date(2025, 1, d), True) for d in range(1, 15)]

    confusion = day_confusion(decisions, [_failure("F1", "E1", 10)], TOL)

    # positive days: 7 (degradation start, 12:00) to 11 (failure + 1 day, 12:00) = 5 days
    assert confusion == Confusion(tp=5, fp=9, tn=0, fn=0)
    assert (confusion.recall, confusion.false_positive_rate) == (1.0, 1.0)
    assert confusion.precision == pytest.approx(5 / 14)


def test_day_level_counts_every_quadrant() -> None:
    decisions = [
        ("E1", date(2025, 1, 8), True), ("E1", date(2025, 1, 9), False),
        ("E1", date(2025, 1, 2), True), ("E1", date(2025, 1, 3), False),
    ]  # fmt: skip

    assert day_confusion(decisions, [_failure("F1", "E1", 10)], TOL) == Confusion(1, 1, 1, 1)


def test_consecutive_alert_days_form_one_event() -> None:
    days = [("E1", date(2025, 1, d), d in (3, 4, 5, 8)) for d in range(1, 10)] + [
        ("E2", date(2025, 1, 4), True)
    ]

    assert alert_events(days) == [_event("E1", 3, 5), _event("E1", 8, 8), _event("E2", 4, 4)]


def test_results_do_not_depend_on_input_order() -> None:
    events = [_event("E1", 9, 9), _event("E2", 2, 3), _event("E1", 1, 1)]
    failures = [_failure("F2", "E2", 4), _failure("F1", "E1", 10)]

    first = match_events(events, failures, TOL)
    again = match_events(list(reversed(events)), list(reversed(failures)), TOL)

    assert first == again
    assert latency_summary(first.matches) == latency_summary(again.matches)
