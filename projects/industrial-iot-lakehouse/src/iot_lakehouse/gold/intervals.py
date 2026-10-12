"""Downtime intervals: normalized, merged, split by day. Pure ``DataFrame -> DataFrame``.

Definitions (docs/GOLD.md, "Fórmulas"):

- **unplanned** stop: ``[failure_timestamp, completed_at)`` of the failure's corrective order
  (the CMMS records only the failure instant; the repair order closes it). An order still open
  (``completed_at`` NULL) stops at the period end: the stop is real but its end is unknown
  (censored). A failure without corrective order has no known stop: no interval;
- **planned** stop: ``[started_at, completed_at)`` of a preventive order (open: period end).
  Planned downtime is never a failure;
- intervals are clipped to the observation period and merged per equipment and kind, so
  overlapping stops count once; the unplanned time inside a planned stop counts as planned.

Days are UTC calendar days ``[00:00, 24:00)``.
"""

from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

PLANNED, UNPLANNED = "planned", "unplanned"
INTERVAL_COLUMNS: Final = ("equipment_id", "kind", "start", "end")
SECONDS_PER_DAY: Final = 86_400


def _clip(frame: DataFrame, start: datetime, end: datetime) -> DataFrame:
    clipped = frame.select(
        "equipment_id",
        "kind",
        F.greatest(F.col("start"), F.lit(start)).alias("start"),
        F.least(F.coalesce(F.col("end"), F.lit(end)), F.lit(end)).alias("end"),
    )
    return clipped.where(F.col("end") > F.col("start"))


def stop_intervals(
    failures: DataFrame, maintenance: DataFrame, *, period_start: datetime, period_end: datetime
) -> DataFrame:
    """Planned and unplanned stops of every equipment, clipped to the period (not merged)."""
    corrective = maintenance.where(F.col("order_type") == "corrective").select(
        F.col("related_failure_id").alias("failure_id"), "completed_at"
    )
    # one repair per failure: the earliest completion (an open order has NULL: last)
    repairs = corrective.groupBy("failure_id").agg(
        F.min("completed_at").alias("completed_at"), F.count(F.lit(1)).alias("_orders")
    )
    unplanned = failures.join(repairs, "failure_id", "inner").select(
        "equipment_id",
        F.lit(UNPLANNED).alias("kind"),
        F.col("failure_timestamp").alias("start"),
        F.col("completed_at").alias("end"),
    )
    planned = maintenance.where(F.col("order_type") == "preventive").select(
        "equipment_id",
        F.lit(PLANNED).alias("kind"),
        F.col("started_at").alias("start"),
        F.col("completed_at").alias("end"),
    )
    return _clip(unplanned.unionByName(planned), period_start, period_end)


def merge_intervals(intervals: DataFrame) -> DataFrame:
    """Union of overlapping or touching intervals per (equipment, kind): gaps and islands."""
    order = Window.partitionBy("equipment_id", "kind").orderBy("start", "end")
    previous_end = F.max("end").over(order.rowsBetween(Window.unboundedPreceding, -1))
    islands = intervals.withColumn(
        "_new", F.when(previous_end.isNull() | (F.col("start") > previous_end), 1).otherwise(0)
    ).withColumn("_island", F.sum("_new").over(order))
    return (
        islands.groupBy("equipment_id", "kind", "_island")
        .agg(F.min("start").alias("start"), F.max("end").alias("end"))
        .select(*INTERVAL_COLUMNS)
    )


def planned_overlap(merged: DataFrame) -> DataFrame:
    """The parts of unplanned stops inside planned stops (kind ``overlap``).

    Both sides are merged (disjoint within each kind), so pairwise intersections never overlap
    each other and their lengths add up exactly.
    """
    planned = merged.where(F.col("kind") == PLANNED).select(
        "equipment_id", F.col("start").alias("p_start"), F.col("end").alias("p_end")
    )
    unplanned = merged.where(F.col("kind") == UNPLANNED)
    pairs = unplanned.join(planned, "equipment_id").select(
        "equipment_id",
        F.lit("overlap").alias("kind"),
        F.greatest("start", "p_start").alias("start"),
        F.least("end", "p_end").alias("end"),
    )
    return pairs.where(F.col("end") > F.col("start"))


def _seconds(start: Column, end: Column) -> Column:
    return (F.unix_timestamp(end) - F.unix_timestamp(start)).cast("long")


def by_day(intervals: DataFrame) -> DataFrame:
    """Each interval cut at UTC midnights: (equipment_id, kind, day, seconds)."""
    days = F.sequence(
        F.to_date("start"), F.to_date(F.col("end") - F.expr("INTERVAL 1 MICROSECOND"))
    )
    exploded = intervals.select(
        "equipment_id", "kind", "start", "end", F.explode(days).alias("day")
    )
    day_start = F.to_timestamp(F.col("day"))
    day_end = F.to_timestamp(F.date_add(F.col("day"), 1))
    piece = _seconds(F.greatest("start", day_start), F.least("end", day_end))
    return exploded.select("equipment_id", "kind", "day", piece.alias("seconds"))


def daily_stop_seconds(merged: DataFrame) -> DataFrame:
    """(equipment_id, day, planned_seconds, unplanned_seconds): unplanned net of planned."""
    pieces = by_day(merged.unionByName(planned_overlap(merged)))
    totals = (
        pieces.groupBy("equipment_id", "day")
        .pivot("kind", [PLANNED, UNPLANNED, "overlap"])
        .sum("seconds")
    )
    zero = F.lit(0).cast("long")
    return totals.select(
        "equipment_id",
        "day",
        F.coalesce(F.col(PLANNED), zero).alias("planned_seconds"),
        (F.coalesce(F.col(UNPLANNED), zero) - F.coalesce(F.col("overlap"), zero)).alias(
            "unplanned_seconds"
        ),
    )
