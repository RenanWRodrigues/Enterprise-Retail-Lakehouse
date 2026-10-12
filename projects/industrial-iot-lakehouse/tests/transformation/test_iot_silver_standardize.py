"""IoT Silver transformations: every Bronze row ends valid, quarantined or a duplicate.

One batch holds every case (its own reading_id or sensor each), standardized once per module:
each Spark job costs seconds locally, the assertions stay one test per rule.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from iot_lakehouse.bronze.ingest import bronze_schema
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.silver.standardize import (
    SilverBatch,
    SilverHistory,
    build_silver,
    sensor_reference,
)

AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
LATE_AFTER = 3600
T0, T5, T6, T7, T10 = (f"2025-01-03T10:{m:02d}:00Z" for m in (0, 5, 6, 7, 10))
SENSORS = [("SEN-T", "degC", "-40", "200"), ("SEN-OO", "degC", "-40", "200")]


def _bronze_reading(
    reading_id: str, value: str = "20.0", *, sensor: str = "SEN-T", event: str = T0,
    arrived: str = T5, unit: str = "degC", file: str = "f1",
) -> dict[str, Any]:  # fmt: skip
    return {
        "reading_id": reading_id, "sensor_id": sensor, "event_timestamp": event,
        "ingestion_timestamp": arrived, "reading_value": value, "unit": unit,
        "_ingestion_timestamp": AT, "_source_file": f"/landing/{file}.json.gz", "_batch_id": "b1",
        "_ingestion_date": AT.date(), "_file_id": file, "_source_system": "historian",
    }  # fmt: skip


CASES: list[dict[str, Any]] = [
    _bronze_reading("R-OK"),
    _bronze_reading("R-DUP"),
    _bronze_reading("R-DUP", arrived=T6, file="f2"),  # identical resend, later
    _bronze_reading("R-CON", "20.0"),
    _bronze_reading("R-CON", "21.0", arrived=T6, file="f2"),  # same id, other value, later
    _bronze_reading("R-LATE", event="2025-01-03T07:00:00Z", arrived=T10),  # 3h10 late
    _bronze_reading("R-OO-A", sensor="SEN-OO", event=T5, arrived=T6),
    _bronze_reading("R-OO-B", sensor="SEN-OO", event=T0, arrived=T7),  # older event, later
    _bronze_reading("R-TS-TEXT", event="03/01/2025 10:00"),
    _bronze_reading("R-TS-FUTURE", event="2025-01-03T11:00:00Z"),  # after arrival + 5 min
    _bronze_reading("R-TS-OLD", event="1999-12-31T23:00:00Z"),
    _bronze_reading("R-UNIT", unit="psi"),  # pressure unit on a temperature sensor
    _bronze_reading("R-CONV", "212", unit="degF"),  # 100 degC
    _bronze_reading("R-CONV-HIGH", "500", unit="degF"),  # 260 degC: impossible once converted
    _bronze_reading("R-HIGH", "250"),
    _bronze_reading("R-MAX", "200"),
    _bronze_reading("R-MIN", "-40"),
    _bronze_reading("R-NAN", "abc"),
    _bronze_reading("", "20.0"),
    _bronze_reading("R-UNKNOWN", sensor="SEN-NONE"),
]


def _frame(spark: SparkSession, dataset: str, rows: list[dict[str, Any]]) -> DataFrame:
    schema = bronze_schema(IOT_SOURCE_CONTRACTS[dataset])
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in rows], schema
    )


def _sensors(spark: SparkSession) -> DataFrame:
    rows = [
        {"sensor_id": s, "equipment_id": "EQ-1", "sensor_type": "temperature", "unit": u,
         "physical_min": lo, "physical_max": hi, "sampling_interval_seconds": "300",
         "_ingestion_timestamp": AT, "_source_file": "s.csv", "_batch_id": "b1",
         "_ingestion_date": AT.date(), "_file_id": "fs", "_source_system": "cmms"}
        for s, u, lo, hi in SENSORS
    ]  # fmt: skip
    return sensor_reference(_frame(spark, "sensors", rows))


def _build(spark: SparkSession, rows: list[dict[str, Any]], history: SilverHistory) -> SilverBatch:
    return build_silver(
        _frame(spark, "sensor_readings", rows), _sensors(spark), history,
        late_after_seconds=LATE_AFTER, execution_timestamp=AT,
    )  # fmt: skip


@dataclass(frozen=True)
class Outcome:
    valid: dict[str, Row]
    quarantine: dict[str, list[str]]  # reading_id (as delivered) -> reasons
    duplicates: list[str]
    bronze_rows: int


@pytest.fixture(scope="module")
def outcome(spark: SparkSession) -> Outcome:
    batch = _build(spark, CASES, SilverHistory())
    return Outcome(
        valid={r.reading_id: r for r in batch.valid.collect()},
        quarantine={r.reading_id: list(r._dq_failed_checks) for r in batch.quarantine.collect()},
        duplicates=[r.reading_id for r in batch.duplicates.collect()],
        bronze_rows=len(CASES),
    )


def test_every_bronze_row_is_classified_exactly_once(outcome: Outcome) -> None:
    accounted = len(outcome.valid) + len(outcome.quarantine) + len(outcome.duplicates)

    assert accounted == outcome.bronze_rows


def test_an_identical_resend_is_a_duplicate_of_the_first_arrival(outcome: Outcome) -> None:
    assert outcome.duplicates == ["R-DUP"]
    assert outcome.valid["R-DUP"]._file_id == "f1"  # first arrival kept


def test_a_conflicting_resend_is_quarantined_and_the_first_arrival_wins(outcome: Outcome) -> None:
    assert outcome.quarantine["R-CON"] == ["conflicting_resend"]
    assert outcome.valid["R-CON"].reading_value == 20.0


def test_a_late_reading_is_kept_and_flagged(outcome: Outcome) -> None:
    late = outcome.valid["R-LATE"]

    assert (late.is_late, late.lateness_seconds) == (True, 3 * 3600 + 600)
    assert not outcome.valid["R-OK"].is_late


def test_an_out_of_order_reading_is_kept_and_flagged(outcome: Outcome) -> None:
    assert outcome.valid["R-OO-B"].arrived_out_of_order
    assert not outcome.valid["R-OO-A"].arrived_out_of_order


def _local(moment: datetime) -> datetime:
    """PySpark returns TIMESTAMP values as naive datetimes in the local time zone."""
    return moment.astimezone().replace(tzinfo=None)


def test_event_and_ingestion_times_are_the_delivered_instants(outcome: Outcome) -> None:
    row = outcome.valid["R-OK"]

    assert (row.event_timestamp, row.ingestion_timestamp) == (
        _local(datetime(2025, 1, 3, 10, 0, tzinfo=UTC)),
        _local(datetime(2025, 1, 3, 10, 5, tzinfo=UTC)),
    )
    assert row.lateness_seconds == 300


@pytest.mark.parametrize(
    ("reading_id", "reason"),
    [
        ("R-TS-TEXT", "unparseable_event_timestamp"),
        ("R-TS-FUTURE", "invalid_event_timestamp"),
        ("R-TS-OLD", "invalid_event_timestamp"),
        ("R-UNIT", "unit_mismatch"),
        ("R-HIGH", "physically_impossible_value"),
        ("R-CONV-HIGH", "physically_impossible_value"),
        ("R-NAN", "unparseable_value"),
        ("", "missing_reading_id"),
    ],
)
def test_invalid_readings_are_quarantined_with_the_reason(
    outcome: Outcome, reading_id: str, reason: str
) -> None:
    assert reason in outcome.quarantine[reading_id]
    assert reading_id not in outcome.valid


def test_a_convertible_unit_is_converted_and_the_delivered_one_kept(outcome: Outcome) -> None:
    row = outcome.valid["R-CONV"]

    assert (row.reading_value, row.unit, row.source_value, row.source_unit, row.unit_converted) == (
        pytest.approx(100.0), "degC", 212.0, "degF", True,
    )  # fmt: skip


def test_values_at_the_physical_limits_are_valid(outcome: Outcome) -> None:
    assert (outcome.valid["R-MAX"].reading_value, outcome.valid["R-MIN"].reading_value) == (
        200.0, -40.0,
    )  # fmt: skip


def test_a_reading_of_an_unknown_sensor_is_kept_and_flagged(outcome: Outcome) -> None:
    assert outcome.valid["R-UNKNOWN"].unknown_sensor
    assert not outcome.valid["R-OK"].unknown_sensor


def test_an_empty_batch_classifies_nothing(spark: SparkSession) -> None:
    batch = _build(spark, [], SilverHistory())

    assert (batch.valid.count(), batch.quarantine.count(), batch.duplicates.count()) == (0, 0, 0)


def test_resends_of_published_readings_never_replace_them(spark: SparkSession) -> None:
    first = _build(spark, [_bronze_reading("R-PUB")], SilverHistory())
    published = first.valid.select("reading_id", "_content_hash")
    latest = (
        first.valid.groupBy("sensor_id")
        .agg({"event_timestamp": "max"})
        .withColumnRenamed("max(event_timestamp)", "_stored_event")
    )
    resends = [
        _bronze_reading("R-PUB", arrived=T7, file="f9"),  # identical
        _bronze_reading("R-PUB", "25.0", arrived=T7, file="f9"),  # conflicting
        _bronze_reading("R-OLDER", event="2025-01-03T09:00:00Z", arrived=T10, file="f9"),
    ]

    batch = _build(spark, resends, SilverHistory(published, latest))

    assert [r.reading_id for r in batch.duplicates.collect()] == ["R-PUB"]
    assert [list(r._dq_failed_checks) for r in batch.quarantine.collect()] == [
        ["conflicting_resend"]
    ]
    [older] = batch.valid.collect()
    assert (older.reading_id, older.arrived_out_of_order) == ("R-OLDER", True)  # vs stored 10:00
