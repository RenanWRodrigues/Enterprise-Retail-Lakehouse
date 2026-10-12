"""IoT Silver on Delta: incremental runs without loss, late files, resends, retry after failure.

Small hand-written landing files through the real Bronze. Needs Delta: test container (Linux).
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from iot_lakehouse.bronze.ingest import BronzeTarget
from iot_lakehouse.bronze.pipeline import run_bronze
from iot_lakehouse.silver import pipeline as silver_pipeline
from iot_lakehouse.silver.pipeline import SilverSettings, SilverTarget, run_silver
from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus

Kit = Any  # tests/conftest.py::LandingKit
AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
SENSOR = {
    "sensor_id": "SEN-1", "equipment_id": "EQ-1", "sensor_type": "temperature", "unit": "degC",
    "physical_min": "-40", "physical_max": "200", "sampling_interval_seconds": "300",
}  # fmt: skip


def _reading(
    kit: Kit, reading_id: str, value: float = 20.0, event: str = "10:00", arrived: str = "10:05"
) -> dict[str, Any]:
    row: dict[str, Any] = kit.reading(
        reading_id, value, unit="degC",
        event_timestamp=f"2025-01-03T{event}:00Z", ingestion_timestamp=f"2025-01-03T{arrived}:00Z",
    )  # fmt: skip
    return row


class Lake:
    def __init__(self, spark: SparkSession, root: Path, prefix: str) -> None:
        self.spark, self.root = spark, root
        self.settings = SilverSettings(
            BronzeTarget(f"{prefix}_bronze"), SilverTarget(f"{prefix}_silver"), 3600
        )
        self.clock = AT

    def run(self) -> tuple[list[str], InMemoryObservabilitySink]:
        self.clock += timedelta(hours=1)
        run_bronze(self.spark, landing_root=self.root, target=self.settings.bronze,
                   ingestion_timestamp=self.clock, sink=InMemoryObservabilitySink())  # fmt: skip
        sink = InMemoryObservabilitySink()
        files = run_silver(
            self.spark, settings=self.settings, ingestion_timestamp=self.clock, sink=sink
        )
        return files, sink

    def rows(self, table: str) -> int:
        return self.spark.table(getattr(self.settings.target, table)).count()


def test_incremental_runs_classify_every_reading_without_loss(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit
) -> None:
    kit, lake = landing_kit, Lake(delta_spark, tmp_path, "iot_silver_inc")
    kit.write_csv(tmp_path, "sensors", "b1", [SENSOR])
    kit.write_readings(
        tmp_path, "b1", "2025-01-03", "a", [_reading(kit, "R1"), _reading(kit, "R2", 999.0)]
    )
    kit.write_readings(
        tmp_path, "b1", "2025-01-03", "b", [_reading(kit, "R1", arrived="10:06")]
    )  # resend

    files, sink = lake.run()
    [run] = sink.runs
    assert len(files) == 2
    assert run.status is RunStatus.SUCCEEDED_WITH_ALERTS  # R2 quarantined: reported once
    assert (run.input_rows, run.output_rows, run.rejected_rows, run.duplicate_rows) == (3, 1, 1, 1)

    idle, idle_sink = lake.run()  # nothing new
    assert idle == [] and [r.status for r in idle_sink.runs] == [RunStatus.SUCCEEDED]

    # a late file in the old partition: a late reading, an older event, a conflicting resend
    late_file = [
        _reading(kit, "R3", event="07:00", arrived="10:30"),
        _reading(kit, "R4", event="09:55", arrived="10:31"),  # older than R1, 36 min: not late
        _reading(kit, "R1", 21.5, arrived="10:32"),
    ]
    kit.write_readings(tmp_path, "b1", "2025-01-03", "c", late_file)
    files, sink = lake.run()

    [run] = sink.runs
    assert (run.input_rows, run.output_rows, run.rejected_rows) == (3, 2, 1)
    stored = {r.reading_id: r for r in delta_spark.table(lake.settings.target.readings).collect()}
    assert sorted(stored) == ["R1", "R3", "R4"]
    assert stored["R1"].reading_value == 20.0
    assert (stored["R3"].is_late, stored["R4"].arrived_out_of_order) == (True, True)
    reasons = {
        tuple(r._dq_failed_checks)
        for r in delta_spark.table(lake.settings.target.quarantine).collect()
    }
    assert reasons == {("physically_impossible_value",), ("conflicting_resend",)}
    metrics = {
        (r.run_id, r.metric): r.value
        for r in delta_spark.table(lake.settings.target.metrics).collect()
    }
    last = max(run_id for run_id, _ in metrics)
    wanted = ("bronze_rows", "valid", "quarantine", "duplicate", "late", "out_of_order")
    assert {m: metrics[(last, m)] for m in wanted} == {
        "bronze_rows": 3, "valid": 2, "quarantine": 1, "duplicate": 0, "late": 1, "out_of_order": 2,
    }  # fmt: skip


def test_a_retry_after_a_failure_while_publishing_duplicates_nothing(
    delta_spark: SparkSession, tmp_path: Path, landing_kit: Kit, monkeypatch: pytest.MonkeyPatch
) -> None:
    kit, lake = landing_kit, Lake(delta_spark, tmp_path, "iot_silver_retry")
    kit.write_csv(tmp_path, "sensors", "b1", [SENSOR])
    kit.write_readings(
        tmp_path, "b1", "2025-01-03", "a", [_reading(kit, "R1"), _reading(kit, "R2", 999.0)]
    )
    merge = silver_pipeline._merge_insert_only

    def crash_before_marking_files(frame: Any, table: str, condition: str) -> None:
        if table.endswith("silver_processed_files"):
            raise RuntimeError("injected failure after the Silver and quarantine MERGEs")
        merge(frame, table, condition)

    monkeypatch.setattr(silver_pipeline, "_merge_insert_only", crash_before_marking_files)
    with pytest.raises(RuntimeError, match="injected failure"):
        lake.run()
    monkeypatch.setattr(silver_pipeline, "_merge_insert_only", merge)
    assert (lake.rows("readings"), lake.rows("quarantine")) == (1, 1)  # published, not marked

    files, sink = lake.run()  # the same file again

    [run] = sink.runs
    # R2 was quarantined (and alerted) by the failed attempt: counted, not alerted again
    assert len(files) == 1 and run.status is RunStatus.SUCCEEDED
    assert (run.output_rows, run.duplicate_rows, run.rejected_rows) == (0, 1, 1)  # R1 already in
    assert (lake.rows("readings"), lake.rows("quarantine"), lake.rows("processed")) == (1, 1, 1)
