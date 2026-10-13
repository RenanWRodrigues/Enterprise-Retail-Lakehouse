"""Characterization of ``build_readings``: a refactor must not change any column of any row.

Recorded from the code before ``build_readings`` was split into named steps (2026-10-13): the
exact output schema, the row count and an order-independent checksum of **every** column
(contract, labels and internal), on the tiny test configuration.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GOLDEN_COLUMNS = [
    "reading_id",
    "sensor_id",
    "event_timestamp",
    "ingestion_timestamp",
    "reading_value",
    "unit",
    "copy_index",
    "is_duplicate",
    "is_spike",
    "is_impossible",
    "is_late",
    "is_out_of_order",
    "failure_id",
    "drift_episode_id",
    "equipment_id",
    "sensor_type",
    "physical_min",
    "physical_max",
    "noise_sd",
    "baseline_value",
    "drift_offset",
    "drift_progress",
    "degradation_change",
    "degradation_progress",
    "clean_value",
    "residual_sd",
    "ingestion_date",
]
GOLDEN_ROWS = 12206
GOLDEN_CHECKSUM = 524327017263533592840


def _fingerprint(readings: DataFrame) -> tuple[list[str], int, int]:
    hashed = F.xxhash64(*(F.col(c).cast("string") for c in readings.columns)).cast("decimal(38,0)")
    row = readings.agg(F.sum(hashed).alias("checksum"), F.count(F.lit(1)).alias("rows")).first()
    assert row is not None
    return readings.columns, int(row["rows"]), int(row["checksum"])


def test_the_readings_are_identical(iot_readings: DataFrame) -> None:
    assert _fingerprint(iot_readings) == (GOLDEN_COLUMNS, GOLDEN_ROWS, GOLDEN_CHECKSUM)
