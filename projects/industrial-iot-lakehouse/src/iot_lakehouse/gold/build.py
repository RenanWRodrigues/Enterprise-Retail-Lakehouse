"""Build every IoT Gold table from the Silver (full, deterministic rebuild over the period).

A late reading, failure or order that reaches the Silver changes the windows it belongs to on
the next build: every table is recomputed from the Silver, so no published window keeps a stale
value (docs/GOLD.md, "Dados tardios").
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from iot_lakehouse.gold.intervals import daily_stop_seconds, merge_intervals, stop_intervals
from iot_lakehouse.gold.reliability import (
    LINEAGE,
    daily_reliability,
    equipment_days,
    failure_facts,
    period_reliability,
)
from iot_lakehouse.gold.schemas import UNKNOWN_DATE_KEY, UNKNOWN_EQUIPMENT
from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS

SILVER_INPUTS: Final = (
    "equipment",
    "sensors",
    "failure_events",
    "maintenance_orders",
    "readings",
    "readings_quarantine",
)
IMPOSSIBLE: Final = "physically_impossible_value"


@dataclass(frozen=True, slots=True)
class Period:
    """Observation period ``[start, end)`` in whole UTC days."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("the period must end after it starts")

    @property
    def start_at(self) -> datetime:
        return datetime.combine(self.start, time(), tzinfo=UTC)

    @property
    def end_at(self) -> datetime:
        return datetime.combine(self.end, time(), tzinfo=UTC)


def _date_key(day: Column, period: Period) -> Column:
    key = (F.year(day) * 10_000 + F.month(day) * 100 + F.dayofmonth(day)).cast("int")
    inside = (day >= F.lit(period.start)) & (day < F.lit(period.end))
    return F.when(inside, key).otherwise(F.lit(UNKNOWN_DATE_KEY))


def _known(column: str, known: DataFrame) -> Column:
    """``column`` if it is a known equipment (joined as ``_known``), else the unknown member."""
    return F.when(F.col("_known").isNotNull(), F.col(column)).otherwise(F.lit(UNKNOWN_EQUIPMENT))


def _with_equipment(frame: DataFrame, equipment: DataFrame) -> DataFrame:
    known = equipment.select("equipment_id", F.lit(True).alias("_known"))
    joined = frame.join(F.broadcast(known), "equipment_id", "left")  # N:1
    return joined.withColumn("equipment_id", _known("equipment_id", known)).drop("_known")


def dim_equipment(equipment: DataFrame) -> DataFrame:
    columns = list(IOT_SOURCE_CONTRACTS["equipment"].column_names)
    unknown = equipment.sparkSession.createDataFrame([(UNKNOWN_EQUIPMENT,)], "equipment_id string")
    return equipment.select(*columns).unionByName(unknown, allowMissingColumns=True)


def dim_date(spark: SparkSession, period: Period) -> DataFrame:
    days = spark.range((period.end - period.start).days).select(
        F.date_add(F.lit(period.start), F.col("id").cast("int")).alias("date")
    )
    known = days.select(
        _date_key(F.col("date"), period).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        (((F.dayofweek("date") + 5) % 7) + 1).alias("day_of_week"),  # ISO: 1 = Monday
        F.weekofyear("date").alias("week_of_year"),
    )
    unknown = spark.createDataFrame([(UNKNOWN_DATE_KEY,)], "date_key int")
    return known.unionByName(unknown, allowMissingColumns=True)


def fact_failures(facts: DataFrame, equipment: DataFrame, period: Period) -> DataFrame:
    with_key = facts.withColumn("date_key", _date_key(F.col("failure_day"), period))
    return _with_equipment(with_key, equipment).drop("failure_day")


def _seconds(start: str, end: str) -> Column:
    return (F.unix_timestamp(end) - F.unix_timestamp(start)).cast("long")


def fact_maintenance(orders: DataFrame, equipment: DataFrame, period: Period) -> DataFrame:
    shaped = orders.select(
        "maintenance_order_id",
        "equipment_id",
        _date_key(F.to_date("opened_at"), period).alias("date_key"),
        "order_type",
        (F.col("order_type") == "preventive").alias("is_planned"),
        "related_failure_id",
        "opened_at",
        "started_at",
        "completed_at",
        "status",
        _seconds("opened_at", "started_at").alias("response_seconds"),
        _seconds("started_at", "completed_at").alias("duration_seconds"),  # NULL while open
        *LINEAGE,
    )
    return _with_equipment(shaped, equipment)


def _in_period(readings: DataFrame, period: Period) -> DataFrame:
    event = F.col("event_timestamp")
    return readings.where((event >= F.lit(period.start_at)) & (event < F.lit(period.end_at)))


def _sensor_equipment(frame: DataFrame, sensors: DataFrame, equipment: DataFrame) -> DataFrame:
    """Equipment and sensor type of each row (unknown sensor -> unknown equipment)."""
    attributes = sensors.select("sensor_id", "equipment_id", "sensor_type")
    joined = frame.join(F.broadcast(attributes), "sensor_id", "left")  # N:1
    with_type = joined.withColumn("sensor_type", F.coalesce("sensor_type", F.lit("unknown")))
    return _with_equipment(with_type, equipment)


def agg_hourly(
    readings: DataFrame, sensors: DataFrame, equipment: DataFrame, period: Period
) -> DataFrame:
    """Valid readings per equipment, UTC hour and sensor type (hours without readings: no row)."""
    rows = _sensor_equipment(_in_period(readings, period), sensors, equipment)
    hour = F.date_trunc("hour", "event_timestamp")
    return (
        rows.groupBy("equipment_id", hour.alias("hour_start"), "sensor_type")
        .agg(
            F.first("unit").alias("unit"),
            F.count(F.lit(1)).alias("readings"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
            F.stddev_samp("reading_value").alias("stddev_value"),
            F.sum(F.col("is_late").cast("long")).alias("late_readings"),
            F.sum(F.col("arrived_out_of_order").cast("long")).alias("out_of_order_readings"),
            F.sum(F.col("unit_converted").cast("long")).alias("converted_readings"),
        )
        .withColumn("date_key", _date_key(F.to_date("hour_start"), period))
    )


def _daily_reading_quality(
    readings: DataFrame,
    quarantine: DataFrame,
    sensors: DataFrame,
    equipment: DataFrame,
    period: Period,
) -> DataFrame:
    """Per equipment and day: valid, late, out-of-order, quarantined and impossible readings.

    A quarantined reading is dated by its event when the text parses, else by its Bronze arrival.
    """
    valid = _sensor_equipment(_in_period(readings, period), sensors, equipment).select(
        "equipment_id", F.to_date("event_timestamp").alias("day"),
        F.lit(1).alias("readings"), F.col("is_late").cast("int").alias("late_readings"),
        F.col("arrived_out_of_order").cast("int").alias("out_of_order_readings"),
        F.lit(0).alias("quarantined_readings"), F.lit(0).alias("impossible_values"),
    )  # fmt: skip
    rejected_day = F.coalesce(
        F.to_date(F.trim("event_timestamp").try_cast("timestamp")),
        F.to_date("bronze_ingestion_timestamp"),
    )
    rejected = _sensor_equipment(
        quarantine.withColumn("sensor_id", F.trim("sensor_id")), sensors, equipment
    ).select(
        "equipment_id", rejected_day.alias("day"), F.lit(0).alias("readings"),
        F.lit(0).alias("late_readings"), F.lit(0).alias("out_of_order_readings"),
        F.lit(1).alias("quarantined_readings"),
        F.array_contains("_dq_failed_checks", IMPOSSIBLE).cast("int").alias("impossible_values"),
    )  # fmt: skip
    counts = (
        "readings",
        "late_readings",
        "out_of_order_readings",
        "quarantined_readings",
        "impossible_values",
    )
    return (
        valid.unionByName(rejected)
        .groupBy("equipment_id", "day")
        .agg(*(F.sum(c).cast("long").alias(c) for c in counts))
    )


def build_gold(silver: Mapping[str, DataFrame], period: Period) -> dict[str, DataFrame]:
    """Every Gold table from the Silver inputs (``SILVER_INPUTS``)."""
    missing = sorted(set(SILVER_INPUTS) - set(silver))
    if missing:
        raise ValueError(f"missing Silver inputs: {missing}")
    equipment, orders = silver["equipment"], silver["maintenance_orders"]
    spark = equipment.sparkSession
    facts = failure_facts(silver["failure_events"], orders)
    stops = daily_stop_seconds(
        merge_intervals(stop_intervals(silver["failure_events"], orders,
                                       period_start=period.start_at, period_end=period.end_at))
    )  # fmt: skip
    days = equipment_days(spark, equipment, period.start, period.end)
    daily = daily_reliability(days, stops, facts)
    quality = _daily_reading_quality(
        silver["readings"], silver["readings_quarantine"], silver["sensors"], equipment, period
    )
    zero = F.lit(0).cast("long")
    counts = [c for c in quality.columns if c not in ("equipment_id", "day")]
    daily_full = daily.join(quality, ["equipment_id", "day"], "left").select(
        "equipment_id",
        _date_key(F.col("day"), period).alias("date_key"),
        F.col("day").alias("date"),
        *(c for c in daily.columns if c not in ("equipment_id", "day")),
        *(F.coalesce(F.col(c), zero).alias(c) for c in counts),  # a day without readings: 0
    )
    return {
        "dim_equipment": dim_equipment(equipment),
        "dim_date": dim_date(spark, period),
        "fact_equipment_failures": fact_failures(facts, equipment, period),
        "fact_maintenance": fact_maintenance(orders, equipment, period),
        "agg_equipment_hourly": agg_hourly(
            silver["readings"], silver["sensors"], equipment, period
        ),
        "agg_equipment_daily": daily_full,
        "agg_equipment_reliability": period_reliability(daily, period.start_at, period.end_at),
    }
