"""Sensor readings generated with Spark: determinism and each simulated behaviour.

Each test runs ONE aggregation over ~20k rows (no collect of raw readings).
"""

from datetime import timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from iot_lakehouse.synthetic import (
    READING_COLUMNS,
    IotGenerationConfig,
    MasterData,
    build_readings,
    readings_checksum,
)

HALF = 0.5
MIN_READINGS_PER_HALF = 10


def _no_effect() -> Column:
    """Rows without any injected effect (built lazily: needs an active SparkContext)."""
    return (
        ~F.col("is_spike")
        & ~F.col("is_impossible")
        & F.col("failure_id").isNull()
        & F.col("drift_episode_id").isNull()
    )


def _single(frame: DataFrame) -> dict[str, object]:
    row = frame.first()
    assert row is not None
    return row.asDict()


def test_generation_is_deterministic(
    spark: SparkSession,
    iot_config: IotGenerationConfig,
    iot_master: MasterData,
    iot_readings: DataFrame,
) -> None:
    again = build_readings(spark, iot_config, iot_master)
    other_seed = build_readings(spark, iot_config.with_seed(iot_config.seed + 1), iot_master)

    assert readings_checksum(again) == readings_checksum(iot_readings)
    assert readings_checksum(other_seed) != readings_checksum(iot_readings)


def test_readings_follow_the_contract_and_the_grid(
    iot_readings: DataFrame, iot_config: IotGenerationConfig, iot_master: MasterData
) -> None:
    assert list(iot_readings.select(*READING_COLUMNS).columns) == list(READING_COLUMNS)
    steps = iot_config.days * 86_400 // iot_config.sampling_interval_seconds
    stats = _single(
        iot_readings.agg(
            F.count_distinct("reading_id").alias("ids"),
            F.min("event_timestamp").alias("first"),
            F.max("event_timestamp").alias("last"),
        )
    )
    assert 0 < stats["ids"] < len(iot_master.sensors) * steps  # type: ignore[operator]  # offline gaps


def test_normal_behaviour_is_noise_around_the_baseline(iot_readings: DataFrame) -> None:
    normal = iot_readings.where(_no_effect())
    by_type = (
        normal.groupBy("sensor_type")
        .agg(
            (F.stddev("residual_sd")).alias("z_sd"),
            F.avg("residual_sd").alias("z_mean"),
            F.max(F.abs("residual_sd")).alias("z_max"),
            F.count(F.lit(1)).alias("n"),
        )
        .where(
            (F.abs(F.col("z_mean")) > 0.2)
            | (F.abs(F.col("z_sd") - 1) > 0.25)
            | (F.col("z_max") > 6)
        )
    )
    assert by_type.isEmpty(), by_type.collect()


def test_values_respect_physical_limits_unless_impossible(iot_readings: DataFrame) -> None:
    outside = (F.col("reading_value") < F.col("physical_min")) | (
        F.col("reading_value") > F.col("physical_max")
    )
    stats = _single(
        iot_readings.agg(
            F.sum(F.when(outside & ~F.col("is_impossible"), 1).otherwise(0)).alias("unexpected"),
            F.sum(F.when(~outside & F.col("is_impossible"), 1).otherwise(0)).alias("missed"),
            F.sum(F.col("is_impossible").cast("int")).alias("impossible"),
        )
    )
    assert stats["unexpected"] == 0
    assert stats["missed"] == 0
    assert stats["impossible"] > 0  # type: ignore[operator]


def test_spikes_are_large_and_labelled(iot_readings: DataFrame) -> None:
    spikes = iot_readings.where(F.col("is_spike") & ~F.col("is_impossible"))
    stats = _single(
        spikes.agg(
            F.count(F.lit(1)).alias("n"),
            F.min(F.abs(F.col("reading_value") - F.col("clean_value")) / F.col("noise_sd")).alias(
                "min_sigmas"
            ),
        )
    )
    assert stats["n"] > 0  # type: ignore[operator]
    assert stats["min_sigmas"] >= 5.99  # type: ignore[operator]


def test_progressive_degradation_precedes_every_failure(
    iot_readings: DataFrame, iot_master: MasterData
) -> None:
    degrading = iot_readings.where(F.col("failure_id").isNotNull())
    progress_bucket = F.when(F.col("degradation_progress") < 1 / 3, "early").when(
        F.col("degradation_progress") >= 2 / 3, "late"
    )
    shift = (F.col("clean_value") - F.col("baseline_value") - F.col("drift_offset")) / F.col(
        "baseline_value"
    )
    in_direction = shift * F.signum(F.col("degradation_change"))
    per_episode = (
        degrading.withColumn("bucket", progress_bucket)
        .groupBy("failure_id", "sensor_type")
        .pivot("bucket", ["early", "late"])
        .agg(F.avg(in_direction))
    )
    violations = per_episode.where(
        F.col("late").isNull() | (F.col("late") <= F.coalesce(F.col("early"), F.lit(0.0)))
    )
    linked = {r["failure_id"] for r in degrading.select("failure_id").distinct().collect()}

    assert linked, "degradation must be visible in the window"
    assert linked <= {f.failure_id for f in iot_master.failures}
    assert violations.isEmpty(), violations.collect()


def test_drift_grows_over_the_episode(iot_readings: DataFrame) -> None:
    """Signed deviation in the drift direction: noise averages out, the drift does not."""
    drifting = iot_readings.where(
        F.col("drift_episode_id").isNotNull() & F.col("failure_id").isNull()
    )
    half = F.when(F.col("drift_progress") < HALF, "first").otherwise("second")
    signed = (F.col("clean_value") - F.col("baseline_value")) / F.col("baseline_value")
    per_half = (
        drifting.withColumn("half", half)
        .groupBy("drift_episode_id", "half")
        .agg(
            F.avg(signed * F.signum(F.col("drift_offset"))).alias("shift"),
            F.count(F.lit(1)).alias("n"),
        )
        .where(F.col("n") >= MIN_READINGS_PER_HALF)
    )
    per_episode = (
        per_half.groupBy("drift_episode_id")
        .pivot("half", ["first", "second"])
        .agg(F.first("shift"))
    )
    comparable = per_episode.where(F.col("first").isNotNull() & F.col("second").isNotNull())

    assert not comparable.isEmpty(), "at least one drift episode must be observable"
    assert comparable.where(F.col("second") <= F.col("first")).isEmpty(), comparable.collect()


def test_no_readings_during_offline_periods(
    spark: SparkSession, iot_readings: DataFrame, iot_master: MasterData
) -> None:
    periods = spark.createDataFrame(
        [(p.equipment_id, p.sensor_id, p.start, p.end) for p in iot_master.offline_periods],
        "equipment_id string, offline_sensor string, start timestamp, end timestamp",
    )
    inside = iot_readings.join(
        F.broadcast(periods),
        (iot_readings["equipment_id"] == periods["equipment_id"])
        & (
            periods["offline_sensor"].isNull()
            | (periods["offline_sensor"] == iot_readings["sensor_id"])
        )
        & (iot_readings["event_timestamp"] >= periods["start"])
        & (iot_readings["event_timestamp"] < periods["end"]),
    )
    assert inside.isEmpty()


def test_duplicates_are_exact_resends(iot_readings: DataFrame) -> None:
    per_id = iot_readings.groupBy("reading_id").agg(
        F.count(F.lit(1)).alias("copies"),
        F.count_distinct("reading_value", "event_timestamp").alias("distinct_payloads"),
        F.sum(F.col("is_duplicate").cast("int")).alias("labelled"),
    )
    stats = _single(
        per_id.agg(
            F.sum(F.when(F.col("copies") == 2, 1).otherwise(0)).alias("resent"),
            F.max("copies").alias("max_copies"),
            F.max("distinct_payloads").alias("max_payloads"),
            F.sum(F.when(F.col("labelled") != F.col("copies") - 1, 1).otherwise(0)).alias(
                "mislabelled"
            ),
        )
    )
    assert stats["resent"] > 0  # type: ignore[operator]
    assert (stats["max_copies"], stats["max_payloads"], stats["mislabelled"]) == (2, 1, 0)


def test_arrival_delays_match_late_and_out_of_order_labels(
    iot_readings: DataFrame, iot_config: IotGenerationConfig
) -> None:
    delay = F.unix_timestamp("ingestion_timestamp") - F.unix_timestamp("event_timestamp")
    interval = iot_config.sampling_interval_seconds
    threshold = iot_config.lateness_threshold_minutes * 60
    originals = iot_readings.where(~F.col("is_duplicate"))
    stats = _single(
        originals.agg(
            F.sum(F.col("is_late").cast("int")).alias("late"),
            F.sum(F.col("is_out_of_order").cast("int")).alias("out_of_order"),
            F.sum(F.when(F.col("is_late") & (delay <= threshold), 1).otherwise(0)).alias(
                "bad_late"
            ),
            F.sum(
                F.when(
                    F.col("is_out_of_order") & ((delay <= interval) | (delay > threshold)), 1
                ).otherwise(0)
            ).alias("bad_ooo"),
            F.sum(
                F.when(
                    ~F.col("is_late")
                    & ~F.col("is_out_of_order")
                    & ((delay < 0) | (delay >= interval)),
                    1,
                ).otherwise(0)
            ).alias("bad_on_time"),
        )
    )
    assert stats["late"] > 0 and stats["out_of_order"] > 0  # type: ignore[operator]
    assert (stats["bad_late"], stats["bad_ooo"], stats["bad_on_time"]) == (0, 0, 0)
    assert timedelta(seconds=threshold) > timedelta(seconds=interval)
