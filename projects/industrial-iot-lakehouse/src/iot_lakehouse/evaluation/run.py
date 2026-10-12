"""Reproducible evaluation: generate, standardize, detect, calibrate, evaluate, report.

1. the generator (fixed seed) produces readings and, separately, the ground truth;
2. the readings go through the real Silver standardization (``build_silver``, pure) without
   any label column, then the detector;
3. equipment is split in two (alternating within each equipment type, by id): the threshold is
   **calibrated** on one half (best event F1) and **evaluated** on the other, so the reported
   numbers never come from the data the threshold was chosen on;
4. the report records the parameters, the seed, a dataset fingerprint and every result.

Only per-sensor daily means and per-equipment daily decisions reach the driver (bounded by
sensors x days); readings never do.
"""

import tomllib
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from iot_lakehouse.evaluation.detector import (
    DetectorParams,
    alert_events,
    daily_means,
    equipment_alert_days,
    sensor_day_scores,
)
from iot_lakehouse.evaluation.matching import (
    Confusion,
    FailureWindow,
    Tolerance,
    day_confusion,
    event_confusion,
    latency_summary,
    match_events,
)
from iot_lakehouse.silver.standardize import SilverHistory, build_silver
from iot_lakehouse.synthetic import IotGenerationConfig, MasterData, generate_master_data
from iot_lakehouse.synthetic.physics import PHYSICAL_LIMITS
from iot_lakehouse.synthetic.readings import READING_COLUMNS, build_readings, readings_checksum

ISO: Final = "yyyy-MM-dd'T'HH:mm:ss'Z'"
LATE_AFTER_SECONDS: Final = 3600


@dataclass(frozen=True)
class EvaluationConfig:
    generation: IotGenerationConfig
    thresholds: tuple[float, ...]
    baseline_days: int
    min_history_days: int
    tolerance: Tolerance = field(default_factory=Tolerance)


def load_evaluation_config(path: Path) -> EvaluationConfig:
    with path.open("rb") as handle:
        doc = tomllib.load(handle)
    gen = dict(doc["generation"])
    gen["start"] = datetime.combine(gen["start"], datetime.min.time(), tzinfo=UTC)
    detector, matching = doc["detector"], doc["matching"]
    return EvaluationConfig(
        IotGenerationConfig(**gen),
        tuple(float(t) for t in detector["thresholds"]),
        int(detector["baseline_days"]),
        int(detector["min_history_days"]),
        Tolerance(
            timedelta(hours=matching["tolerance_before_hours"]),
            timedelta(hours=matching["tolerance_after_hours"]),
        ),
    )


def _as_bronze(readings: DataFrame) -> DataFrame:
    """Readings as the Bronze stores them: contract columns as text, ingestion metadata."""
    text = {c: F.col(c).cast("string") for c in READING_COLUMNS}
    text["event_timestamp"] = F.date_format("event_timestamp", ISO)
    text["ingestion_timestamp"] = F.date_format("ingestion_timestamp", ISO)
    return readings.select(
        *(text[c].alias(c) for c in READING_COLUMNS),
        F.current_timestamp().alias("_ingestion_timestamp"),
        F.lit("generated").alias("_source_file"),
        F.lit("evaluation").alias("_batch_id"),
        F.lit("generated").alias("_file_id"),
    )


def _sensor_reference(spark: SparkSession, master: MasterData) -> DataFrame:
    """Unit and physical limits of each sensor, from its type (as the generator defines them)."""
    rows = []
    for sensor in master.sensors:
        limit = PHYSICAL_LIMITS[sensor.sensor_type]
        rows.append((sensor.sensor_id, limit.unit, limit.minimum, limit.maximum))
    return spark.createDataFrame(
        rows, "sensor_id string, sensor_unit string, physical_min double, physical_max double"
    )


def valid_readings(
    spark: SparkSession, config: IotGenerationConfig, master: MasterData
) -> DataFrame:
    """Generated readings through the Silver standardization: label columns never enter."""
    bronze = _as_bronze(build_readings(spark, config, master).select(*READING_COLUMNS))
    batch = build_silver(
        bronze, _sensor_reference(spark, master), SilverHistory(),
        late_after_seconds=LATE_AFTER_SECONDS, execution_timestamp=datetime.now(UTC),
    )  # fmt: skip
    return batch.valid.select("sensor_id", "event_timestamp", "reading_value")


def failure_windows(master: MasterData) -> list[FailureWindow]:
    return [
        FailureWindow(f.equipment_id, f.failure_timestamp, f.degradation_start, f.failure_id)
        for f in master.failures
    ]


def split_equipment(master: MasterData) -> tuple[set[str], set[str]]:
    """(calibration, evaluation): alternating within each equipment type, by id."""
    ordered = sorted(master.equipment, key=lambda e: (str(e.equipment_type), e.equipment_id))
    by_type: dict[str, list[str]] = {}
    for equipment in ordered:
        by_type.setdefault(str(equipment.equipment_type), []).append(equipment.equipment_id)
    calibration = {eid for ids in by_type.values() for eid in ids[0::2]}
    return calibration, {e.equipment_id for e in master.equipment} - calibration


def _decisions(
    means: DataFrame, sensors: DataFrame, params: DetectorParams
) -> list[tuple[Any, ...]]:
    days = equipment_alert_days(sensor_day_scores(means, params), sensors)
    return sorted((r.equipment_id, r.day, bool(r.alert)) for r in days.collect())


def _summary(confusion: Confusion) -> dict[str, Any]:
    return {
        **asdict(confusion),
        "precision": confusion.precision,
        "recall": confusion.recall,
        "f1": confusion.f1,
    }


def _metrics(
    decisions: list[tuple[Any, ...]], failures: list[FailureWindow], tolerance: Tolerance
) -> dict[str, Any]:
    outcome = match_events(alert_events(decisions), failures, tolerance)
    events, days = event_confusion(outcome), day_confusion(decisions, failures, tolerance)
    return {
        "failures": len(failures),
        "alert_events": len(outcome.matches) + len(outcome.false_alarms),
        "event": _summary(events),
        "day": {**_summary(days), "false_positive_rate": days.false_positive_rate},
        "latency": latency_summary(outcome.matches),
    }  # fmt: skip


def _subset(rows: list[tuple[Any, ...]], equipment: set[str]) -> list[tuple[Any, ...]]:
    return [r for r in rows if r[0] in equipment]


def evaluate(spark: SparkSession, config: EvaluationConfig) -> dict[str, Any]:
    """Calibrate on one half of the equipment, evaluate on the other; the full report."""
    master = generate_master_data(config.generation)
    local = daily_means(
        valid_readings(spark, config.generation, master)
    ).collect()  # sensors x days
    means = spark.createDataFrame(local, "sensor_id string, day date, mean double")
    sensors = spark.createDataFrame(
        [(s.sensor_id, s.equipment_id) for s in master.sensors],
        "sensor_id string, equipment_id string",
    )
    calibration, evaluation = split_equipment(master)
    failures = failure_windows(master)
    sweep = []
    for threshold in config.thresholds:
        params = DetectorParams(threshold, config.baseline_days, config.min_history_days)
        decisions = _decisions(means, sensors, params)
        cal = [f for f in failures if f.equipment_id in calibration]
        sweep.append(
            (threshold, decisions, _metrics(_subset(decisions, calibration), cal, config.tolerance))
        )
    chosen, decisions, _ = max(sweep, key=lambda s: ((s[2]["event"]["f1"] or -1.0), s[0]))
    held_out = [f for f in failures if f.equipment_id in evaluation]
    checksum = readings_checksum(build_readings(spark, config.generation, master))
    return {
        "parameters": {
            "generation": {k: str(v) for k, v in asdict(config.generation).items()},
            "thresholds": list(config.thresholds), "chosen_threshold": chosen,
            "baseline_days": config.baseline_days, "min_history_days": config.min_history_days,
            "tolerance_before_hours": config.tolerance.before.total_seconds() / 3600,
            "tolerance_after_hours": config.tolerance.after.total_seconds() / 3600,
        },
        "dataset": {
            "readings_checksum": checksum,
            "equipment": len(master.equipment),
            "sensors": len(master.sensors),
            "failures": len(failures),
            "drift_episodes": len(master.drift_episodes),
            "sensor_days": len(local),
            "calibration_equipment": sorted(calibration),
            "evaluation_equipment": sorted(evaluation),
        },
        "calibration_sweep": [{"threshold": t, **m} for t, _, m in sweep],
        "evaluation": _metrics(_subset(decisions, evaluation), held_out, config.tolerance),
    }  # fmt: skip
