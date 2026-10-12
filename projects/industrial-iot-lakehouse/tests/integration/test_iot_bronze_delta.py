"""IoT Bronze on Delta: valid and broken files, reruns, late files, crash and retry.

Small hand-written landing files only (no generator run). Needs Delta: test container (Linux).
"""

import gzip
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from iot_lakehouse.bronze import pipeline as bronze_pipeline
from iot_lakehouse.bronze.ingest import BronzeTarget, DatasetOutcome, record_decisions
from iot_lakehouse.bronze.pipeline import run_bronze
from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus
from lakehouse_common.quality import Status

Kit = Any  # tests/conftest.py::LandingKit
AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
DAY, OLD_DAY = "2025-01-04", "2025-01-03"
SENSOR_ROW = {
    "sensor_id": "SEN-1", "equipment_id": "EQ-1", "sensor_type": "temperature", "unit": "C",
    "physical_min": "-40", "physical_max": "150", "sampling_interval_seconds": "300",
}  # fmt: skip


def _valid_landing(root: Path, kit: Kit) -> None:
    kit.write_csv(root, "equipment", "b1", [kit.equipment_row])
    kit.write_csv(root, "sensors", "b1", [SENSOR_ROW])
    kit.write_readings(
        root, "b1", OLD_DAY, "part-0", [kit.reading("R1", 12.5), kit.reading("R2", "x")]
    )


def _run(
    spark: SparkSession, root: Path, ns: str, at: datetime = AT
) -> tuple[list[DatasetOutcome], InMemoryObservabilitySink]:
    sink = InMemoryObservabilitySink()
    outcomes = run_bronze(
        spark, landing_root=root, target=BronzeTarget(ns), ingestion_timestamp=at, sink=sink
    )
    return outcomes, sink


def _by_dataset(outcomes: list[DatasetOutcome]) -> dict[str, DatasetOutcome]:
    return {o.dataset: o for o in outcomes}


def test_valid_files_land_with_their_metadata_and_broken_files_are_quarantined(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    kit, ns = landing_kit, "iot_bronze_mixed"
    _valid_landing(tmp_path, kit)
    failures = kit.write_csv(tmp_path, "failure_events", "b1", [])  # header only: 0 rows, valid
    with failures.open("a", encoding="utf-8") as handle:  # ...then a line with an extra field
        handle.write("F-1,EQ-1,bearing,2025-01-03T10:00:00Z,high,EXTRA\n")
    kit.write_csv(tmp_path, "maintenance_orders", "b1", [], header=["order_id", "equipment_id"])
    kit.readings_path(tmp_path, "b1", DAY, "zero-bytes").write_bytes(b"")
    kit.write_readings(tmp_path, "b1", DAY, "no-records", [])
    kit.write_readings(
        tmp_path, "b1", DAY, "malformed", [kit.reading("R3"), '{"reading_id": "R4",']
    )
    kit.write_readings(tmp_path, "b1", DAY, "new-key", [kit.reading("R5", quality_flag="G")])
    payload = gzip.compress(b'{"reading_id": "R6"}\n' * 50)
    kit.readings_path(tmp_path, "b1", DAY, "truncated").write_bytes(payload[: len(payload) // 2])

    outcomes, sink = _run(delta_spark, tmp_path, ns)

    by = _by_dataset(outcomes)
    readings = by["sensor_readings"]
    assert {d.file.path.name: (d.status, d.reason) for d in readings.decisions} == {
        "part-0.json.gz": ("ingested", None),
        "no-records.json.gz": ("ingested", None),
        "zero-bytes.json.gz": ("quarantined", "empty_file"),
        "malformed.json.gz": ("quarantined", "malformed_records"),
        "new-key.json.gz": ("quarantined", "schema_drift"),
        "truncated.json.gz": ("quarantined", "corrupt_gzip"),
    }
    assert [d.reason for d in by["failure_events"].decisions] == ["malformed_records"]
    assert [d.reason for d in by["maintenance_orders"].decisions] == ["schema_drift"]
    # metadata and text preserved exactly
    rows = delta_spark.table(f"{ns}.bronze_sensor_readings").orderBy("reading_id").collect()
    [part0] = [d.file for d in readings.decisions if d.file.path.name == "part-0.json.gz"]
    assert [(r.reading_id, r.reading_value) for r in rows] == [("R1", "12.5"), ("R2", "x")]
    assert {(r._file_id, r._batch_id, r._source_system, r._ingestion_date) for r in rows} == {
        (part0.file_id, "b1", "historian", AT.date())
    }
    assert all(r._source_file.endswith("part-0.json.gz") for r in rows)
    # observability: rows reconcile, each broken file raises one ERROR, nothing FAILED
    runs = {r.table.rsplit(".", 1)[1]: r for r in sink.runs}
    run = runs["bronze_sensor_readings"]
    assert (run.status, run.input_rows, run.output_rows, run.rejected_rows) == (
        RunStatus.SUCCEEDED_WITH_ALERTS, 5, 2, 3,  # read: part-0 2, malformed 2, new-key 1
    )  # fmt: skip
    checks = {c.check_name: c for c in run.quality}
    assert (checks["reconciliation"].status, checks["file_problems"].failed_rows) == (
        Status.PASS,
        4,
    )
    files = delta_spark.table(f"{ns}.bronze_files").groupBy("status").count().collect()
    assert {r.status: r["count"] for r in files} == {"ingested": 4, "quarantined": 6}


def test_a_rerun_changes_nothing_and_late_or_rewritten_files_are_handled(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    kit, ns = landing_kit, "iot_bronze_rerun"
    _valid_landing(tmp_path, kit)
    _run(delta_spark, tmp_path, ns)

    again, sink = _run(delta_spark, tmp_path, ns, AT + timedelta(hours=1))
    assert [(o.files_skipped, o.decisions) for o in again if o.files_found] == [
        (1, []),
        (1, []),
        (1, []),
    ]
    assert {r.status for r in sink.runs} == {RunStatus.SUCCEEDED}  # no repeated alert
    assert delta_spark.table(f"{ns}.bronze_sensor_readings").count() == 2  # no duplicates

    kit.write_readings(tmp_path, "b1", OLD_DAY, "part-late", [kit.reading("R9")])  # old partition
    kit.write_readings(tmp_path, "b1", OLD_DAY, "part-0", [kit.reading("R1", 99.0)])  # rewritten
    late, _ = _run(delta_spark, tmp_path, ns, AT + timedelta(hours=2))
    decisions = {
        d.file.path.name: (d.status, d.reason)
        for d in _by_dataset(late)["sensor_readings"].decisions
    }
    assert decisions == {
        "part-late.json.gz": ("ingested", None),
        "part-0.json.gz": ("quarantined", "changed_after_ingestion"),
    }
    assert delta_spark.table(f"{ns}.bronze_sensor_readings").count() == 3


def test_a_crash_after_the_append_is_repaired_without_duplicates(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit, monkeypatch: pytest.MonkeyPatch
) -> None:
    kit, ns = landing_kit, "iot_bronze_crash"
    _valid_landing(tmp_path, kit)
    calls: list[str] = []

    def crash_on_readings(
        spark: SparkSession, target: BronzeTarget, decisions: list[Any], **kw: Any
    ) -> None:
        calls.append(decisions[0].file.dataset if decisions else "")
        if (
            decisions
            and decisions[0].file.dataset == "sensor_readings"
            and calls.count("sensor_readings") == 1
        ):
            raise RuntimeError("injected crash after the append")
        record_decisions(spark, target, decisions, **kw)

    monkeypatch.setattr(bronze_pipeline, "record_decisions", crash_on_readings)
    with pytest.raises(RuntimeError, match="injected crash"):
        _run(delta_spark, tmp_path, ns)
    assert delta_spark.table(f"{ns}.bronze_sensor_readings").count() == 2  # appended, not recorded

    retry, sink = _run(delta_spark, tmp_path, ns, AT + timedelta(hours=1))

    [repair] = _by_dataset(retry)["sensor_readings"].decisions
    assert (repair.status, repair.rows) == ("ingested", 2)  # recorded from the table
    assert delta_spark.table(f"{ns}.bronze_sensor_readings").count() == 2  # not appended again
    assert {r.status for r in sink.runs} == {RunStatus.SUCCEEDED}
