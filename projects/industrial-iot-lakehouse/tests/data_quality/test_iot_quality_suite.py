"""IoT quality suite: nonexistent sensor, invalid timestamp, impossible values, duplicates."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from iot_lakehouse.quality import READINGS_SCHEMA, deduplicate_readings, iot_readings_suite
from iot_lakehouse.quality.suites import contract_schema
from lakehouse_common.quality import CheckResult, Status, run_suite

AT = datetime(2026, 10, 6, 12, tzinfo=UTC)
T0 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
ARRIVED = T0 + timedelta(seconds=10)


def _reading(reading_id: str, sensor_id: str, value: float | None, **extra: Any) -> dict[str, Any]:
    return {
        "reading_id": reading_id,
        "sensor_id": sensor_id,
        "event_timestamp": T0,
        "ingestion_timestamp": ARRIVED,
        "reading_value": value,
        "unit": "degC",
    } | extra


READINGS = [
    _reading("S1-1", "S1", 80.0),
    _reading("S1-1", "S1", 80.0, ingestion_timestamp=ARRIVED + timedelta(seconds=60)),  # resend
    _reading("S1-2", "S1", 81.0, event_timestamp=T0 - timedelta(hours=30)),  # late: valid
    _reading("S1-3", "S1", 999.0, event_timestamp=T0 + timedelta(minutes=1)),  # above physical max
    _reading("S1-4", "S1", float("nan"), event_timestamp=T0 + timedelta(minutes=2)),  # NaN
    _reading("S1-5", "S1", 70.0, event_timestamp=T0 + timedelta(minutes=3), unit="degF"),  # unit
    _reading("S1-6", "S1", 70.0, event_timestamp=datetime(1970, 1, 1, tzinfo=UTC)),  # clock reset
    _reading("S1-7", "S1", 70.0, event_timestamp=ARRIVED + timedelta(hours=2)),  # from the future
    _reading("S1-8", "S1", 70.0, event_timestamp=None),  # missing timestamp
    _reading("S1-9", "S1", 72.0, event_timestamp=T0 + timedelta(minutes=4)),
    _reading("S1-X", "S1", 72.5, event_timestamp=T0 + timedelta(minutes=4)),  # same instant, new id
    _reading("S9-1", "S9", 50.0, event_timestamp=T0 + timedelta(minutes=5)),  # unknown sensor
]


def _frame(spark: SparkSession, dataset: str, rows: list[dict[str, Any]]) -> DataFrame:
    """Contract columns and types, but nullable: the checks must see the NULLs they catch."""
    contract = contract_schema(dataset)
    schema = StructType([StructField(f.name, f.dataType, True) for f in contract.fields])
    return spark.createDataFrame(
        [tuple(r.get(n) for n in schema.fieldNames()) for r in rows], schema
    )


@pytest.fixture(scope="module")
def suite_input(spark: SparkSession) -> tuple[DataFrame, DataFrame, int]:
    sensors = _frame(
        spark,
        "sensors",
        [
            {
                "sensor_id": "S1",
                "equipment_id": "E1",
                "sensor_type": "temperature",
                "unit": "degC",
                "physical_min": -40.0,
                "physical_max": 200.0,
                "sampling_interval_seconds": 60,
            }
        ],
    )
    raw = _frame(spark, "sensor_readings", READINGS)
    deduplicated = deduplicate_readings(raw)
    return deduplicated, sensors, raw.count() - deduplicated.count()


@pytest.fixture(scope="module")
def results(suite_input: tuple[DataFrame, DataFrame, int]) -> dict[str, CheckResult]:
    readings, sensors, _ = suite_input
    return {
        r.check_name: r
        for r in run_suite(iot_readings_suite(readings, sensors), execution_timestamp=AT)
    }


def test_resends_are_removed_before_the_checks(
    suite_input: tuple[DataFrame, DataFrame, int], results: dict[str, CheckResult]
) -> None:
    assert suite_input[2] == 1  # duplicate_rows
    assert results["duplicate__reading_id"].status is Status.PASS  # the gate proves it


@pytest.mark.parametrize(
    ("check", "status", "failed"),
    [
        ("unknown_sensor", Status.WARNING, 1),
        ("unknown_sensor_systemic", Status.CRITICAL, 1),  # 1 of 11 readings > 5%
        ("invalid_event_timestamp", Status.ERROR, 2),  # 1970 and two hours in the future
        ("missing_event_timestamp", Status.ERROR, 1),
        ("physically_impossible_value", Status.ERROR, 2),  # 999 degC and NaN
        ("unit_mismatch", Status.ERROR, 1),
        ("conflicting_readings", Status.ERROR, 2),  # two ids, same sensor and instant
    ],
)
def test_requested_defects_are_caught(
    results: dict[str, CheckResult], check: str, status: Status, failed: int
) -> None:
    assert (results[check].status, results[check].failed_rows) == (status, failed)


def test_valid_rows_raise_nothing(results: dict[str, CheckResult]) -> None:
    for name in ("schema__sensor_readings", "missing_sensor", "missing_value"):
        assert results[name].status is Status.PASS, name


def test_late_readings_are_not_invalid(spark: SparkSession) -> None:
    late = _frame(spark, "sensor_readings", [READINGS[2]])
    sensors = _frame(
        spark,
        "sensors",
        [{"sensor_id": "S1", "unit": "degC", "physical_min": -40.0, "physical_max": 200.0}],
    )
    results = run_suite(iot_readings_suite(late, sensors), execution_timestamp=AT)
    assert {r.status for r in results} == {Status.PASS}


def test_schema_change_is_critical(spark: SparkSession) -> None:
    readings = _frame(spark, "sensor_readings", READINGS[:1]).withColumnRenamed(
        "reading_value", "value"
    )
    sensors = _frame(spark, "sensors", [{"sensor_id": "S1", "unit": "degC"}])
    [result] = run_suite(iot_readings_suite(readings, sensors), execution_timestamp=AT)
    assert result.status is Status.CRITICAL
    assert result.details == "missing: reading_value; unexpected: value"
    assert READINGS_SCHEMA.fieldNames()[0] == "reading_id"
