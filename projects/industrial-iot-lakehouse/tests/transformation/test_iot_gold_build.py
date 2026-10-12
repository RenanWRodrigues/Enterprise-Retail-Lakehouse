"""IoT Gold build: gate, unknown members, empty windows, late events, rebuilds and queries.

Small Silver frames (2 equipment, 2 days) with known results. No Delta needed.
"""

from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from iot_lakehouse.gold.build import Period, build_gold
from iot_lakehouse.gold.schemas import GRAIN, UNKNOWN_DATE_KEY, UNKNOWN_EQUIPMENT
from iot_lakehouse.quality.suites import iot_gold_suite
from lakehouse_common.quality import Status, run_suite

PERIOD = Period(date(2025, 1, 1), date(2025, 1, 3))
AT = datetime(2026, 10, 9, tzinfo=UTC)
SQL_DIR = Path("projects/industrial-iot-lakehouse/sql/gold")
H = 3600


def _at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, minute, tzinfo=UTC)


EQUIPMENT = [
    ("E1", "pump", "P1", "Acme", "S1", "Plant 1", "SP", None, 55.0),
    ("E2", "compressor", "C1", "Acme", "S1", "Plant 1", "SP", None, 90.0),
]
SENSORS = [("SEN-1", "E1", "temperature"), ("SEN-2", "E2", "temperature")]
FAILURES = [
    ("F1", "E1", _at(1, 10)),
    ("F9", "E9", _at(1, 11)),  # an equipment the CMMS does not know
    ("F0", "E1", datetime(2024, 12, 31, 23, tzinfo=UTC)),  # before the period
]
ORDERS = [("M1", "E1", "corrective", "F1", _at(1, 10), _at(1, 12))]
READINGS = [  # only day 1: day 2 of both equipment has no readings
    ("R1", "SEN-1", _at(1, 10, 5), 20.0, True, False),
    ("R2", "SEN-1", _at(1, 10, 40), 30.0, False, True),
    ("R3", "SEN-2", _at(1, 11, 0), 50.0, False, False),
]
QUARANTINE = [("SEN-1", "2025-01-01T09:00:00Z", ["physically_impossible_value"])]


def _silver(spark: SparkSession, failures: list[Any] = FAILURES) -> dict[str, DataFrame]:
    lineage = "_file_id string, _batch_id string"
    return {
        "equipment": spark.createDataFrame(
            EQUIPMENT,
            "equipment_id string, equipment_type string, model string, manufacturer string, "
            "site_id string, site_name string, state string, installed_at date, "
            "rated_power_kw double",
        ),
        "sensors": spark.createDataFrame(
            SENSORS, "sensor_id string, equipment_id string, sensor_type string"
        ),
        "failure_events": spark.createDataFrame(
            [(f, e, t, "bearing", "major", "file", "b1") for f, e, t in failures],
            f"failure_id string, equipment_id string, failure_timestamp timestamp, "
            f"failure_mode string, severity string, {lineage}",
        ),
        "maintenance_orders": spark.createDataFrame(
            [(o, e, k, r, s, s, c, "completed", "file", "b1") for o, e, k, r, s, c in ORDERS],
            "maintenance_order_id string, equipment_id string, order_type string, "
            "related_failure_id string, opened_at timestamp, started_at timestamp, "
            f"completed_at timestamp, status string, {lineage}",
        ),
        "readings": spark.createDataFrame(
            [(r, s, t, v, "degC", late, ooo, False) for r, s, t, v, late, ooo in READINGS],
            "reading_id string, sensor_id string, event_timestamp timestamp, reading_value double, "
            "unit string, is_late boolean, arrived_out_of_order boolean, unit_converted boolean",
        ),
        "readings_quarantine": spark.createDataFrame(
            [(s, t, AT, r) for s, t, r in QUARANTINE],
            "sensor_id string, event_timestamp string, bronze_ingestion_timestamp timestamp, "
            "_dq_failed_checks array<string>",
        ),
    }


@pytest.fixture(scope="module")
def gold(spark: SparkSession) -> dict[str, DataFrame]:
    return build_gold(_silver(spark), PERIOD)


def test_a_valid_gold_passes_every_gate_check(gold: dict[str, DataFrame]) -> None:
    results = run_suite(iot_gold_suite(gold), execution_timestamp=AT)

    assert {r.check_name for r in results if r.status is not Status.PASS} == set()
    assert {r.table_name for r in results} == set(GRAIN)


def test_unknown_equipment_and_out_of_period_events_use_the_unknown_members(
    gold: dict[str, DataFrame],
) -> None:
    facts = {r.failure_id: r for r in gold["fact_equipment_failures"].collect()}

    assert (facts["F9"].equipment_id, facts["F9"].date_key) == (UNKNOWN_EQUIPMENT, 20250101)
    assert (facts["F0"].equipment_id, facts["F0"].date_key) == ("E1", UNKNOWN_DATE_KEY)


def test_a_day_without_readings_has_a_row_with_zero_readings(gold: dict[str, DataFrame]) -> None:
    daily = {(r.equipment_id, r.date_key): r for r in gold["agg_equipment_daily"].collect()}

    assert daily[("E1", 20250102)].readings == 0
    assert (daily[("E1", 20250101)].readings, daily[("E1", 20250101)].quarantined_readings) == (
        2,
        1,
    )
    assert daily[("E1", 20250101)].impossible_values == 1


def test_hourly_aggregates_of_the_valid_readings(gold: dict[str, DataFrame]) -> None:
    hourly = {
        (r.equipment_id, r.hour_start.hour): r for r in gold["agg_equipment_hourly"].collect()
    }
    e1 = [r for (e, _), r in hourly.items() if e == "E1"]

    assert [(r.readings, r.avg_value, r.min_value, r.max_value) for r in e1] == [
        (2, 25.0, 20.0, 30.0)
    ]
    assert (e1[0].late_readings, e1[0].out_of_order_readings) == (1, 1)


def test_a_late_failure_changes_the_aggregates_of_its_day(spark: SparkSession) -> None:
    late = [*FAILURES, ("F-LATE", "E2", _at(2, 6))]  # reaches the Silver after a first build

    before = build_gold(_silver(spark), PERIOD)["agg_equipment_daily"]
    after = build_gold(_silver(spark, late), PERIOD)["agg_equipment_daily"]

    key = "equipment_id = 'E2' AND date_key = 20250102"
    [old], [new] = before.where(key).collect(), after.where(key).collect()
    assert (old.failures, new.failures) == (0, 1)
    assert (old.mtbf_seconds, new.mtbf_seconds) == (None, float(24 * H))  # no repair: no stop


def test_a_rebuild_is_identical(spark: SparkSession, gold: dict[str, DataFrame]) -> None:
    again = build_gold(_silver(spark), PERIOD)

    for name, frame in gold.items():
        assert frame.exceptAll(again[name]).isEmpty() and again[name].exceptAll(frame).isEmpty(), (
            name
        )


def test_the_gate_stops_a_duplicated_grain(gold: dict[str, DataFrame]) -> None:
    broken = {**gold, "agg_equipment_daily": gold["agg_equipment_daily"].unionByName(
        gold["agg_equipment_daily"].limit(1))}  # fmt: skip

    results = run_suite(iot_gold_suite(broken), execution_timestamp=AT)

    assert {r.check_name for r in results if r.status is Status.CRITICAL} == {
        "grain__agg_equipment_daily"
    }


@pytest.fixture(scope="module")
def views(spark: SparkSession, gold: dict[str, DataFrame]) -> Iterator[None]:
    for name, frame in gold.items():
        frame.createOrReplaceTempView(name)
    yield
    for name in gold:
        spark.catalog.dropTempView(name)


def _query(spark: SparkSession, repo_root: Path, name: str) -> list[Any]:
    sql = (repo_root / SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    return spark.sql(sql).collect()


@pytest.mark.usefixtures("views")
def test_the_queries_recompute_ratios_from_the_sums(spark: SparkSession, repo_root: Path) -> None:
    by_type = {
        r.equipment_type: r for r in _query(spark, repo_root, "reliability_by_equipment_type")
    }
    trend = {r.date: r for r in _query(spark, repo_root, "availability_trend_daily")}
    anomalies = _query(spark, repo_root, "anomaly_trend_daily")

    assert (by_type["pump"].failures, by_type["pump"].mttr_hours) == (1, 2.0)
    assert by_type["pump"].availability == round((48 - 2) / 48, 4)
    assert trend[date(2025, 1, 1)].availability == round((48 - 2) / 48, 4)  # 2 equipment x 24 h
    assert sum(r.quarantined_readings for r in anomalies) == 1
