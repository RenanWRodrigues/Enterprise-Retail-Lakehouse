"""Landing zone + ground truth written by Spark (needs a Linux container: Spark file writes)."""

import csv
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.synthetic import IotGenerationConfig, MasterData, write_landing
from lakehouse_common.observability import InMemoryObservabilitySink


def test_landing_and_ground_truth_are_written(
    delta_spark: SparkSession,  # Linux/Delta-capable session: Spark can write files here
    iot_config: IotGenerationConfig,
    iot_master: MasterData,
    iot_readings: DataFrame,
    tmp_path: Path,
) -> None:
    runs = InMemoryObservabilitySink()
    summary = write_landing(delta_spark, iot_config, iot_master, tmp_path, sink=runs)

    [run] = runs.runs
    assert (run.project, run.layer, run.status) == ("iot", "landing", "SUCCEEDED")
    assert run.output_rows == sum(summary.values())

    readings_dir = tmp_path / "historian" / "sensor_readings" / f"batch_id={iot_config.batch_id}"
    partitions = sorted(p.name for p in readings_dir.iterdir() if p.is_dir())
    assert partitions and all(p.startswith("ingestion_date=") for p in partitions)
    landed = delta_spark.read.json(str(readings_dir))
    assert landed.count() == iot_readings.count() == summary["sensor_readings"]
    assert set(landed.columns) == {
        *IOT_SOURCE_CONTRACTS["sensor_readings"].column_names,
        "ingestion_date",
    }

    # Only the id of late readings comes from the labels: they also carry sensor_id and
    # event_timestamp, which made the reference ambiguous (regression, first Linux run).
    late_ids = (
        delta_spark.read.parquet(str(tmp_path / "_ground_truth" / "sensor_reading_labels"))
        .where("is_late")
        .select("reading_id")
    )
    late_dates = landed.join(late_ids, "reading_id").where(
        F.to_date("event_timestamp") < F.col("ingestion_date")
    )
    assert late_dates.count() > 0, "late events must land in later ingestion partitions"

    for dataset in ("equipment", "sensors", "failure_events", "maintenance_orders"):
        path = tmp_path / "cmms" / dataset / f"{dataset}_{iot_config.batch_id}.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            header = next(csv.reader(handle))
        assert header == list(IOT_SOURCE_CONTRACTS[dataset].column_names)


@pytest.mark.parametrize("name", ["failure_degradation", "drift_episodes", "offline_periods"])
def test_episode_ground_truth_files(
    delta_spark: SparkSession,
    iot_config: IotGenerationConfig,
    iot_master: MasterData,
    tmp_path: Path,
    name: str,
) -> None:
    write_landing(delta_spark, iot_config, iot_master, tmp_path, sink=InMemoryObservabilitySink())
    assert (tmp_path / "_ground_truth" / f"{name}.csv").is_file()
