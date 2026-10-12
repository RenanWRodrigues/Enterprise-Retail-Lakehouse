"""Reliability indicators per equipment and day, and per equipment over the period.

Formulas (docs/GOLD.md, "Fórmulas"); every time is in seconds, days are UTC:

- observed   = the day, if the equipment was installed on or before it (else no row);
- planned    = merged preventive stops in the day;
- scheduled  = observed - planned;
- unplanned  = merged failure stops in the day, minus the part inside planned stops;
- operating  = scheduled - unplanned;
- availability = operating / scheduled                (NULL when scheduled = 0);
- failures   = failures whose ``failure_timestamp`` falls in the day (with or without repair);
- repairs    = completed repairs of those failures; repair time = completed_at - failure_timestamp;
- MTTR = repair time / repairs                         (NULL when repairs = 0);
- MTBF = operating / failures                          (NULL when failures = 0: no failure observed,
  reported as such, never as zero or infinity).

Period values are recomputed from the daily sums, never averaged from daily ratios.
"""

from datetime import date, datetime
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from iot_lakehouse.gold.intervals import SECONDS_PER_DAY

COMPLETED, IN_PROGRESS, MISSING = "completed", "in_progress", "missing"
LINEAGE: Final = ("_file_id", "_batch_id")


def equipment_days(spark: SparkSession, equipment: DataFrame, start: date, end: date) -> DataFrame:
    """(equipment_id, day) for every day of ``[start, end)`` on which the equipment existed."""
    days = spark.range((end - start).days).select(
        F.date_add(F.lit(start), F.col("id").cast("int")).alias("day")
    )
    grid = equipment.select("equipment_id", "installed_at").crossJoin(days)  # E x D, both small
    return grid.where(
        F.col("installed_at").isNull() | (F.col("day") >= F.col("installed_at"))
    ).select("equipment_id", "day")


def _seconds(start: Column, end: Column) -> Column:
    return (F.unix_timestamp(end) - F.unix_timestamp(start)).cast("long")


def failure_facts(failures: DataFrame, maintenance: DataFrame) -> DataFrame:
    """One row per failure: its repair (earliest corrective order), status and repair time."""
    corrective = maintenance.where(F.col("order_type") == "corrective").select(
        F.col("related_failure_id").alias("failure_id"), "maintenance_order_id", "completed_at"
    )
    first = corrective.groupBy("failure_id").agg(
        F.min("maintenance_order_id").alias("repair_order_id"),
        F.min("completed_at").alias("repair_completed_at"),
        F.count(F.lit(1)).alias("repair_orders"),
    )
    joined = failures.join(first, "failure_id", "left")  # N:1 (aggregated per failure)
    status = (
        F.when(F.col("repair_order_id").isNull(), F.lit(MISSING))
        .when(F.col("repair_completed_at").isNull(), F.lit(IN_PROGRESS))
        .otherwise(F.lit(COMPLETED))
    )
    return joined.select(
        "failure_id",
        "equipment_id",
        "failure_timestamp",
        F.to_date("failure_timestamp").alias("failure_day"),
        "failure_mode",
        "severity",
        "repair_order_id",
        "repair_completed_at",
        status.alias("repair_status"),
        F.when(
            status == COMPLETED, _seconds(F.col("failure_timestamp"), F.col("repair_completed_at"))
        ).alias("repair_seconds"),
        *LINEAGE,
    )


def _ratio(numerator: Column, denominator: Column) -> Column:
    return F.when(denominator > 0, numerator / denominator)


def _indicators(frame: DataFrame) -> DataFrame:
    scheduled = F.col("observed_seconds") - F.col("planned_seconds")
    with_scheduled = frame.withColumn("scheduled_seconds", scheduled)
    operating = F.col("scheduled_seconds") - F.col("unplanned_seconds")
    return with_scheduled.withColumn("operating_seconds", operating).select(
        "*",
        _ratio(F.col("operating_seconds"), F.col("scheduled_seconds")).alias("availability"),
        _ratio(F.col("operating_seconds"), F.col("failures")).alias("mtbf_seconds"),
        _ratio(F.col("repair_seconds"), F.col("repairs")).alias("mttr_seconds"),
    )


def daily_reliability(days: DataFrame, stops: DataFrame, failures: DataFrame) -> DataFrame:
    """One row per (equipment, day) of ``days``; days without stops or failures included."""
    per_day = failures.groupBy("equipment_id", F.col("failure_day").alias("day")).agg(
        F.count(F.lit(1)).alias("failures"),
        F.sum(F.when(F.col("repair_status") == COMPLETED, 1).otherwise(0)).alias("repairs"),
        F.sum("repair_seconds").alias("repair_seconds"),
    )
    zero = F.lit(0).cast("long")
    joined = days.join(stops, ["equipment_id", "day"], "left").join(
        per_day, ["equipment_id", "day"], "left"
    )  # both N:1 on (equipment, day)
    base = joined.select(
        "equipment_id",
        "day",
        F.lit(SECONDS_PER_DAY).cast("long").alias("observed_seconds"),
        F.coalesce("planned_seconds", zero).alias("planned_seconds"),
        F.coalesce("unplanned_seconds", zero).alias("unplanned_seconds"),
        F.coalesce("failures", zero).alias("failures"),
        F.coalesce("repairs", zero).alias("repairs"),
        F.coalesce("repair_seconds", zero).alias("repair_seconds"),
    )
    return _indicators(base)


def period_reliability(daily: DataFrame, start: datetime, end: datetime) -> DataFrame:
    """One row per equipment over the period: sums of the daily seconds and counts, ratios
    recomputed from the sums."""
    sums = daily.groupBy("equipment_id").agg(
        *(F.sum(c).alias(c) for c in (
            "observed_seconds", "planned_seconds", "unplanned_seconds", "failures", "repairs",
            "repair_seconds",
        ))
    )  # fmt: skip
    return _indicators(sums.drop("scheduled_seconds")).select(
        F.lit(start).alias("period_start"), F.lit(end).alias("period_end"), "*"
    )
