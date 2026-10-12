"""Write the IoT landing zone (sources) and the ground truth used to validate detections.

<root>/cmms/<dataset>/<dataset>_<batch>.csv                    (Python, small)
<root>/historian/sensor_readings/batch_id=<batch>/ingestion_date=<d>/part-*.json.gz (Spark)
<root>/_ground_truth/{failure_degradation,drift_episodes,offline_periods}.csv
<root>/_ground_truth/sensor_reading_labels/ (Spark, Parquet)
"""

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from pyspark.sql import SparkSession

from iot_lakehouse import PROJECT
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.quality.suites import READINGS_SCHEMA
from iot_lakehouse.synthetic.config import IotGenerationConfig
from iot_lakehouse.synthetic.master_data import MasterData, _iso
from iot_lakehouse.synthetic.readings import LABEL_COLUMNS, READING_COLUMNS, build_readings
from lakehouse_common.observability import Layer, ObservabilitySink, track_run

PIPELINE_NAME: Final = "iot_landing"
CMMS_DATASETS: Final = ("equipment", "sensors", "failure_events", "maintenance_orders")
TIMESTAMP_FORMAT: Final = "yyyy-MM-dd'T'HH:mm:ssXXX"  # rendered in UTC -> "...Z"


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str | None]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _ground_truth_rows(master: MasterData) -> dict[str, list[dict[str, str | None]]]:
    return {
        "failure_degradation": [
            {
                "failure_id": d.failure_id,
                "equipment_id": d.equipment_id,
                "sensor_type": str(d.sensor_type),
                "degradation_start": _iso(d.start),
                "failure_timestamp": _iso(d.end),
                "relative_change": str(d.relative_change),
            }
            for d in master.degradation_episodes
        ],
        "drift_episodes": [
            {
                "drift_episode_id": d.drift_episode_id,
                "sensor_id": d.sensor_id,
                "equipment_id": d.equipment_id,
                "start": _iso(d.start),
                "end": _iso(d.end),
                "relative_change": str(d.relative_change),
            }
            for d in master.drift_episodes
        ],
        "offline_periods": [
            {
                "offline_id": p.offline_id,
                "equipment_id": p.equipment_id,
                "sensor_id": p.sensor_id,
                "start": _iso(p.start),
                "end": _iso(p.end),
                "reason": p.reason,
                "reference_id": p.reference_id,
            }
            for p in master.offline_periods
        ],
    }


def write_landing(
    spark: SparkSession,
    config: IotGenerationConfig,
    master: MasterData,
    root: Path | str,
    *,
    sink: ObservabilitySink,
) -> dict[str, int]:
    """Write sources + ground truth under ``root`` (local path or Volume). Returns row counts.

    Observed as one ``pipeline_runs`` row (layer ``landing``, table = the landing root).
    """
    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}",
        project=PROJECT,
        layer=Layer.LANDING,
        table=Path(root).as_posix(),
        sink=sink,
    ) as step:
        counts = _write_landing(spark, config, master, root)
        step.record_counts(output_rows=sum(counts.values()), rejected_rows=0)
    return counts


def _write_landing(
    spark: SparkSession, config: IotGenerationConfig, master: MasterData, root: Path | str
) -> dict[str, int]:
    base = Path(root)
    counts: dict[str, int] = {}
    for dataset in CMMS_DATASETS:
        rows = master.source_rows(dataset)
        _write_csv(
            base / "cmms" / dataset / f"{dataset}_{config.batch_id}.csv",
            list(IOT_SOURCE_CONTRACTS[dataset].column_names),
            rows,
        )
        counts[dataset] = len(rows)
    for name, rows in _ground_truth_rows(master).items():
        columns = list(rows[0]) if rows else ["empty"]
        _write_csv(base / "_ground_truth" / f"{name}.csv", columns, rows)

    readings = build_readings(spark, config, master)
    readings_path = f"{base.as_posix()}/historian/sensor_readings/batch_id={config.batch_id}"
    (
        readings.select(*READING_COLUMNS, "ingestion_date")
        .write.mode("overwrite")
        .partitionBy("ingestion_date")
        .option("compression", "gzip")
        .option("timeZone", "UTC")
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .json(readings_path)
    )
    (
        readings.select("reading_id", "sensor_id", "event_timestamp", *LABEL_COLUMNS)
        .write.mode("overwrite")
        .parquet(f"{base.as_posix()}/_ground_truth/sensor_reading_labels")
    )
    # Explicit schema: inferring it would read every landed reading once more just to count.
    counts["sensor_readings"] = spark.read.schema(READINGS_SCHEMA).json(readings_path).count()
    return counts
