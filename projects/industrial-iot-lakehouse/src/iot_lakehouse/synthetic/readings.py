"""Sensor readings generated with native Spark functions (no Python UDFs, no driver lists).

Grid = ``spark.range(steps)`` x sensors (broadcast). Every random draw is
``xxhash64(seed, salt, sensor_id, step)`` mapped to (0, 1): results do not depend on
partitioning, so the same configuration always produces the same readings.

reading = baseline (nominal + daily cycle) + Gaussian noise + calibration drift
          + progressive degradation before a failure, then spikes / impossible values;
offline periods remove readings; resends duplicate them; delays shape arrival order.
"""

import math
from datetime import date, datetime
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.synthetic.config import SECONDS_PER_DAY, IotGenerationConfig
from iot_lakehouse.synthetic.master_data import MasterData
from iot_lakehouse.synthetic.physics import OPERATING_PROFILES, PHYSICAL_LIMITS, SPIKE_SIGMAS

READING_COLUMNS: Final = IOT_SOURCE_CONTRACTS["sensor_readings"].column_names
LABEL_COLUMNS: Final = (
    "copy_index",
    "is_duplicate",
    "is_spike",
    "is_impossible",
    "is_late",
    "is_out_of_order",
    "failure_id",
    "drift_episode_id",
)
INTERNAL_COLUMNS: Final = (
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
)

_MODULUS: Final = 1_000_000_007
SITE_UTC_OFFSET_SECONDS: Final = -3 * 3600  # Brazilian plants: daily load cycle in local time
HOURS_PER_DAY: Final = 24.0
SECONDS_PER_HOUR: Final = 3600
VALUE_DECIMALS: Final = 3
MAX_ON_TIME_DELAY_SECONDS: Final = 30
MAX_LATE_EXTRA_SECONDS: Final = 47 * 3600
RESEND_DELAY_SECONDS: Final = (5, 120)
HALF: Final = 0.5
_EPOCH: Final = date(1970, 1, 1)


def uniform(seed: int, salt: str, *keys: Column) -> Column:
    """Deterministic pseudo-random number in (0, 1)."""
    hashed = F.xxhash64(F.lit(seed), F.lit(salt), *keys)
    return (F.pmod(hashed, F.lit(_MODULUS)).cast(DoubleType()) + 0.5) / _MODULUS


def gaussian(seed: int, salt: str, *keys: Column) -> Column:
    """Standard normal via Box-Muller over two independent uniforms."""
    u1, u2 = uniform(seed, f"{salt}:1", *keys), uniform(seed, f"{salt}:2", *keys)
    return F.sqrt(-2.0 * F.log(u1)) * F.cos(2.0 * math.pi * u2)


def _sensors(spark: SparkSession, master: MasterData) -> DataFrame:
    rows = []
    for sensor in master.sensors:
        profile = OPERATING_PROFILES[sensor.equipment_type][sensor.sensor_type]
        limit = PHYSICAL_LIMITS[sensor.sensor_type]
        rows.append(
            (
                sensor.sensor_id,
                sensor.equipment_id,
                str(sensor.sensor_type),
                limit.unit,
                limit.minimum,
                limit.maximum,
                profile.nominal,
                profile.noise_sd,
                profile.diurnal_amplitude,
            )
        )
    schema = (
        "sensor_id string, equipment_id string, sensor_type string, unit string, "
        "physical_min double, physical_max double, nominal double, noise_sd double, "
        "diurnal_amplitude double"
    )
    return spark.createDataFrame(rows, schema)


def _epoch(value: datetime) -> int:
    return int(value.timestamp())


_INTERVAL_SCHEMA: Final = StructType(
    [
        StructField("_key", StringType(), False),
        StructField("_sub_key", StringType(), True),
        StructField("_start", LongType(), False),
        StructField("_end", LongType(), False),
        StructField("_ref", StringType(), True),
        StructField("_change", DoubleType(), True),
    ]
)


def _intervals(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    return F.broadcast(spark.createDataFrame(rows, _INTERVAL_SCHEMA))


def _drop_offline(grid: DataFrame, spark: SparkSession, master: MasterData) -> DataFrame:
    periods = _intervals(
        spark,
        [
            (p.equipment_id, p.sensor_id, _epoch(p.start), _epoch(p.end), p.reason, None)
            for p in master.offline_periods
        ],
    )
    inside = (
        (F.col("equipment_id") == F.col("_key"))
        & (F.col("_sub_key").isNull() | (F.col("_sub_key") == F.col("sensor_id")))
        & (F.col("epoch") >= F.col("_start"))
        & (F.col("epoch") < F.col("_end"))
    )
    return grid.join(periods, inside, "left_anti")


def _with_interval(  # noqa: PLR0913, PLR0917 - column names of one interval join
    grid: DataFrame,
    intervals: DataFrame,
    match: Column,
    ref_alias: str,
    change_alias: str,
    progress_alias: str,
) -> DataFrame:
    """Left join to at most one active interval; adds its id, change and progress (0..1)."""
    within = match & (F.col("epoch") >= F.col("_start")) & (F.col("epoch") < F.col("_end"))
    progress = (F.col("epoch") - F.col("_start")) / (F.col("_end") - F.col("_start"))
    return (
        grid.join(intervals, within, "left")
        .withColumn(progress_alias, progress)
        .withColumnRenamed("_ref", ref_alias)
        .withColumnRenamed("_change", change_alias)
        .drop("_key", "_sub_key", "_start", "_end")
    )


def _keys() -> tuple[Column, Column]:
    """The per-reading key of every random draw: (sensor, step)."""
    return F.col("sensor_id"), F.col("step")


def _grid(spark: SparkSession, config: IotGenerationConfig, master: MasterData) -> DataFrame:
    """One row per (sampling step, sensor) while the sensor's equipment is online."""
    grid = (
        spark.range(config.steps)
        .withColumnRenamed("id", "step")
        .crossJoin(F.broadcast(_sensors(spark, master)))
        .withColumn(
            "epoch",
            F.lit(int(config.start.timestamp())) + F.col("step") * config.sampling_interval_seconds,
        )
    )
    return _drop_offline(grid, spark, master)


def _with_episodes(spark: SparkSession, grid: DataFrame, master: MasterData) -> DataFrame:
    """The active degradation (per equipment and sensor type) and drift (per sensor) episodes."""
    degradations = _intervals(
        spark,
        [
            (
                d.equipment_id,
                str(d.sensor_type),
                _epoch(d.start),
                _epoch(d.end),
                d.failure_id,
                d.relative_change,
            )
            for d in master.degradation_episodes
        ],
    )
    grid = _with_interval(
        grid,
        degradations,
        (F.col("equipment_id") == F.col("_key")) & (F.col("sensor_type") == F.col("_sub_key")),
        "failure_id",
        "degradation_change",
        "degradation_progress",
    )
    drifts = _intervals(
        spark,
        [
            (
                d.sensor_id,
                None,
                _epoch(d.start),
                _epoch(d.end),
                d.drift_episode_id,
                d.relative_change,
            )
            for d in master.drift_episodes
        ],
    )
    return _with_interval(
        grid,
        drifts,
        F.col("sensor_id") == F.col("_key"),
        "drift_episode_id",
        "drift_change",
        "drift_progress",
    )


def _with_values(grid: DataFrame, config: IotGenerationConfig) -> DataFrame:
    """The physical signal: diurnal baseline, degradation, drift, noise, then spikes and
    impossible values (labelled)."""
    seed, keys = config.seed, _keys()
    local_hour = (
        F.pmod(F.col("epoch") + SITE_UTC_OFFSET_SECONDS, F.lit(SECONDS_PER_DAY)) / SECONDS_PER_HOUR
    )
    phase = uniform(seed, "phase", F.col("sensor_id")) * HOURS_PER_DAY
    baseline = F.col("nominal") * (
        1 + F.col("diurnal_amplitude") * F.sin(2 * math.pi * (local_hour + phase) / HOURS_PER_DAY)
    )
    degradation_offset = F.coalesce(
        F.col("nominal") * F.col("degradation_change") * F.pow(F.col("degradation_progress"), 2),
        F.lit(0.0),
    )
    drift_offset = F.coalesce(
        F.col("nominal") * F.col("drift_change") * F.col("drift_progress"), F.lit(0.0)
    )
    noise = gaussian(seed, "noise", *keys)

    spike_size = F.col("noise_sd") * (
        SPIKE_SIGMAS[0] + (SPIKE_SIGMAS[1] - SPIKE_SIGMAS[0]) * uniform(seed, "spike_size", *keys)
    )
    impossible_value = F.when(
        uniform(seed, "impossible_side", *keys) < HALF, F.col("physical_max") * 2 + 1
    ).otherwise(F.col("physical_min") - (F.col("physical_max") - F.col("physical_min")) - 1)

    return (
        grid.withColumn("baseline_value", baseline)
        .withColumn("drift_offset", drift_offset)
        .withColumn("residual_sd", noise)
        .withColumn(
            "clean_value",
            F.col("baseline_value")
            + degradation_offset
            + F.col("drift_offset")
            + F.col("noise_sd") * noise,
        )
        .withColumn("is_spike", uniform(seed, "spike", *keys) < config.spike_rate)
        .withColumn("is_impossible", uniform(seed, "impossible", *keys) < config.impossible_rate)
        .withColumn(
            "reading_value",
            F.round(
                F.when(F.col("is_impossible"), impossible_value)
                .when(F.col("is_spike"), F.col("clean_value") + spike_size)  # upward transient
                .otherwise(F.col("clean_value")),
                VALUE_DECIMALS,
            ),
        )
    )


def _with_arrival(readings: DataFrame, config: IotGenerationConfig) -> DataFrame:
    """When each reading reaches the platform: on time, out of order or late, and resent
    copies (labelled), plus ids and the event/ingestion timestamps."""
    seed, keys, interval = config.seed, _keys(), config.sampling_interval_seconds
    threshold = config.lateness_threshold_minutes * 60
    late = uniform(seed, "late", *keys) < config.late_rate
    out_of_order = ~late & (uniform(seed, "out_of_order", *keys) < config.out_of_order_rate)
    delay_draw = uniform(seed, "delay", *keys)
    on_time_window = min(MAX_ON_TIME_DELAY_SECONDS, interval - 1)
    delay = (
        F.when(late, threshold + 1 + F.floor(delay_draw * MAX_LATE_EXTRA_SECONDS))
        .when(out_of_order, interval + 1 + F.floor(delay_draw * (threshold - interval - 1)))
        .otherwise(1 + F.floor(delay_draw * on_time_window))
    )
    resend = uniform(seed, "duplicate", *keys) < config.duplicate_rate
    resend_delay = RESEND_DELAY_SECONDS[0] + F.floor(
        uniform(seed, "resend_delay", *keys) * (RESEND_DELAY_SECONDS[1] - RESEND_DELAY_SECONDS[0])
    )
    return (
        readings.withColumn("is_late", late)
        .withColumn("is_out_of_order", out_of_order)
        .withColumn(
            "copy_index",
            F.explode(F.when(resend, F.array(F.lit(0), F.lit(1))).otherwise(F.array(F.lit(0)))),
        )
        .withColumn("is_duplicate", F.col("copy_index") == 1)
        .withColumn(
            "arrival_epoch",
            F.col("epoch") + delay + F.when(F.col("is_duplicate"), resend_delay).otherwise(0),
        )
        .withColumn(
            "reading_id",
            F.concat(F.col("sensor_id"), F.lit("-"), F.lpad(F.col("step").cast("string"), 7, "0")),
        )
        .withColumn("event_timestamp", F.timestamp_seconds(F.col("epoch")))
        .withColumn("ingestion_timestamp", F.timestamp_seconds(F.col("arrival_epoch")))
        .withColumn(
            "ingestion_date",
            F.date_add(
                F.lit(_EPOCH), F.floor(F.col("arrival_epoch") / SECONDS_PER_DAY).cast("int")
            ),
        )
        .withColumn("degradation_change", F.coalesce(F.col("degradation_change"), F.lit(0.0)))
    )


def build_readings(
    spark: SparkSession, config: IotGenerationConfig, master: MasterData
) -> DataFrame:
    """All readings with ground-truth labels and internal columns (lazy DataFrame).

    Steps: the (step, sensor) grid of online equipment, its degradation and drift episodes, the
    measured values, then the arrival behaviour (late, out of order, resent).
    """
    grid = _with_episodes(spark, _grid(spark, config, master), master)
    readings = _with_arrival(_with_values(grid, config), config)
    return readings.select(*READING_COLUMNS, *LABEL_COLUMNS, *INTERNAL_COLUMNS)


def readings_checksum(readings: DataFrame) -> int:
    """Order-independent fingerprint of the contract and label columns (one aggregation)."""
    hashed = F.xxhash64(*READING_COLUMNS, *LABEL_COLUMNS).cast("decimal(38,0)")
    row = readings.agg(F.sum(hashed).alias("checksum"), F.count(F.lit(1)).alias("rows")).first()
    assert row is not None  # noqa: S101 - a global aggregation always returns one row
    return int(row["checksum"]) * 1_000_003 + int(row["rows"])
