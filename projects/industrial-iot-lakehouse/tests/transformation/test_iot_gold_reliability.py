"""MTBF, MTTR and availability against values computed by hand (docs/GOLD.md, "Fórmulas").

Period: 2025-01-01 00:00 to 2025-01-03 00:00 UTC (2 days = 172 800 s). One equipment per case.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from iot_lakehouse.gold.intervals import daily_stop_seconds, merge_intervals, stop_intervals
from iot_lakehouse.gold.reliability import (
    daily_reliability,
    equipment_days,
    failure_facts,
    period_reliability,
)

START, END = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)
H = 3600
DAY = 24 * H


def _at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, minute, tzinfo=UTC)


FAILURES = [  # failure_id, equipment_id, failure_timestamp
    ("F1", "E1", _at(1, 10)), ("F2", "E1", _at(1, 23)),
    ("F3", "E3", _at(1, 6)), ("F4", "E3", _at(1, 8)),
    ("F5", "E4", _at(2, 20)),
    ("F6", "E5", _at(1, 12)),
    ("F7", "E6", _at(1, 5)),
]  # fmt: skip
ORDERS = [  # order_id, equipment_id, order_type, related_failure_id, started_at, completed_at
    ("M1", "E1", "corrective", "F1", _at(1, 10, 30), _at(1, 12)),
    ("M2", "E1", "corrective", "F2", _at(1, 23, 30), _at(2, 1)),  # crosses midnight
    ("M3", "E1", "preventive", None, _at(2, 8), _at(2, 10)),
    ("M4", "E3", "corrective", "F3", _at(1, 6, 30), _at(1, 9)),  # overlaps F4's stop
    ("M5", "E3", "corrective", "F4", _at(1, 8, 30), _at(1, 10)),
    ("M6", "E4", "corrective", "F5", _at(2, 20, 30), None),  # repair still open
    ("M7", "E5", "preventive", None, _at(1, 10), _at(1, 14)),
    ("M8", "E5", "corrective", "F6", _at(1, 12, 10), _at(1, 13)),  # inside a planned stop
    ("M9", "E8", "preventive", None, _at(1, 0), _at(3, 0)),  # planned the whole period
]  # fmt: skip
EQUIPMENT = [(e, None) for e in ("E1", "E2", "E3", "E4", "E5", "E6", "E8")] + [
    ("E7", date(2025, 1, 2))  # installed on the second day
]


def _failures(spark: SparkSession) -> DataFrame:
    rows: list[tuple[Any, ...]] = [(f, e, t, "mode", "major", "file", "b1") for f, e, t in FAILURES]
    return spark.createDataFrame(
        rows,
        "failure_id string, equipment_id string, failure_timestamp timestamp, failure_mode string, "
        "severity string, _file_id string, _batch_id string",
    )


def _orders(spark: SparkSession) -> DataFrame:
    rows = [(o, e, kind, rel, s, s, c) for o, e, kind, rel, s, c in ORDERS]
    return spark.createDataFrame(
        rows,
        "maintenance_order_id string, equipment_id string, order_type string, "
        "related_failure_id string, opened_at timestamp, started_at timestamp, "
        "completed_at timestamp",
    )


@dataclass(frozen=True)
class Outcome:
    period: dict[str, Row]
    daily: dict[tuple[str, date], Row]
    facts: dict[str, Row]


@pytest.fixture(scope="module")
def outcome(spark: SparkSession) -> Outcome:
    failures, orders = _failures(spark), _orders(spark)
    equipment = spark.createDataFrame(EQUIPMENT, "equipment_id string, installed_at date")
    stops = daily_stop_seconds(
        merge_intervals(stop_intervals(failures, orders, period_start=START, period_end=END))
    )
    facts = failure_facts(failures, orders)
    daily = daily_reliability(
        equipment_days(spark, equipment, START.date(), END.date()), stops, facts
    )
    period = period_reliability(daily, START, END)
    return Outcome(
        period={r.equipment_id: r for r in period.collect()},
        daily={(r.equipment_id, r.day): r for r in daily.collect()},
        facts={r.failure_id: r for r in facts.collect()},
    )


def _period(row: Row) -> tuple[Any, ...]:
    return (
        row.observed_seconds, row.planned_seconds, row.scheduled_seconds, row.unplanned_seconds,
        row.operating_seconds, row.failures, row.repairs, row.mttr_seconds, row.mtbf_seconds,
    )  # fmt: skip


def test_two_failures_one_crossing_midnight_and_a_planned_stop(outcome: Outcome) -> None:
    row = outcome.period["E1"]

    assert _period(row) == (
        2 * DAY,
        2 * H,
        2 * DAY - 2 * H,
        4 * H,
        2 * DAY - 6 * H,
        2,
        2,
        2 * H,
        21 * H,
    )
    assert row.availability == pytest.approx((2 * DAY - 6 * H) / (2 * DAY - 2 * H))


def test_the_failure_crossing_midnight_is_split_between_the_days(outcome: Outcome) -> None:
    first, second = outcome.daily[("E1", date(2025, 1, 1))], outcome.daily[("E1", date(2025, 1, 2))]

    assert (first.unplanned_seconds, first.failures, first.availability) == (3 * H, 2, 0.875)
    assert (second.unplanned_seconds, second.planned_seconds, second.failures) == (1 * H, 2 * H, 0)
    assert second.availability == pytest.approx((DAY - 3 * H) / (DAY - 2 * H))
    assert second.mtbf_seconds is None and second.mttr_seconds is None  # no failure that day


def test_equipment_without_failures_is_fully_available_with_no_mtbf(outcome: Outcome) -> None:
    row = outcome.period["E2"]

    assert (row.availability, row.failures, row.mtbf_seconds, row.mttr_seconds) == (
        1.0,
        0,
        None,
        None,
    )


def test_overlapping_failures_stop_the_equipment_once(outcome: Outcome) -> None:
    row = outcome.period["E3"]

    assert row.unplanned_seconds == 4 * H  # 06:00-10:00, not 3 h + 2 h
    assert (row.failures, row.repairs, row.mttr_seconds) == (2, 2, 2.5 * H)  # each repair counts
    assert row.mtbf_seconds == (2 * DAY - 4 * H) / 2


def test_an_open_repair_stops_until_the_period_end_and_stays_out_of_mttr(outcome: Outcome) -> None:
    row = outcome.period["E4"]

    assert (row.unplanned_seconds, row.failures, row.repairs, row.mttr_seconds) == (
        4 * H,
        1,
        0,
        None,
    )
    assert outcome.facts["F5"].repair_status == "in_progress"
    assert outcome.facts["F5"].repair_seconds is None


def test_a_failure_inside_a_planned_stop_is_not_unplanned_downtime(outcome: Outcome) -> None:
    row = outcome.period["E5"]

    assert (row.planned_seconds, row.unplanned_seconds, row.availability) == (4 * H, 0, 1.0)
    assert (row.failures, row.mttr_seconds) == (1, 1 * H)  # still a failure, still a repair


def test_a_failure_without_repair_order_counts_but_has_no_known_stop(outcome: Outcome) -> None:
    row = outcome.period["E6"]

    assert (row.failures, row.repairs, row.unplanned_seconds) == (1, 0, 0)
    assert outcome.facts["F7"].repair_status == "missing"


def test_equipment_is_observed_from_its_installation(outcome: Outcome) -> None:
    assert outcome.period["E7"].observed_seconds == DAY
    assert ("E7", date(2025, 1, 1)) not in outcome.daily


def test_zero_scheduled_time_gives_no_availability(outcome: Outcome) -> None:
    row = outcome.period["E8"]

    assert (row.scheduled_seconds, row.availability, row.operating_seconds) == (0, None, 0)


def test_a_known_hand_computation_of_mtbf_mttr_and_availability(outcome: Outcome) -> None:
    """E1 by hand: 48 h observed - 2 h planned = 46 h scheduled; - 4 h unplanned = 42 h operating;
    availability 42/46; MTBF 42 h / 2 failures = 21 h; MTTR (2 h + 2 h) / 2 repairs = 2 h."""
    row = outcome.period["E1"]

    assert row.operating_seconds / H == 42 and row.scheduled_seconds / H == 46
    assert (row.mtbf_seconds / H, row.mttr_seconds / H) == (21, 2)
    assert round(row.availability, 6) == round(42 / 46, 6)
