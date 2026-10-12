"""Resampling to the monthly grain — the only way series of different frequencies meet.

Rules (docs/GOLD.md §2). Every output has exactly one row per ``reference_month``:

- daily level (Selic target, exchange rates, reserves): last observation of the month;
- daily average (USD/BRL): mean of the month's observations;
- daily rate (Selic % a.d.): compounded over the month's business days;
- monthly, quarterly and rolling-quarter series: attached to the month in which their period
  *ends* (Silver ``period_end``). Nothing is interpolated, repeated or spread across months:
  a quarterly value appears only in the quarter's last month.
"""

from collections.abc import Sequence
from functools import reduce
from typing import Final

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MONTH: Final = "reference_month"
PERCENT: Final = 100


def series(silver: DataFrame, indicator_id: str) -> DataFrame:
    """One indicator from Silver (unique per reference_date by the Silver key)."""
    return silver.filter(F.col("indicator_id") == F.lit(indicator_id)).select(
        "reference_date", "period_end", "value"
    )


def _month_of(column: str) -> Column:
    return F.trunc(F.col(column), "month").alias(MONTH)


def monthly_last(daily: DataFrame, name: str) -> DataFrame:
    """Daily level -> the month's last observation (rates in force, balances, closing prices)."""
    return daily.groupBy(_month_of("reference_date")).agg(
        F.max_by("value", "reference_date").alias(name)
    )


def monthly_mean(daily: DataFrame, name: str) -> DataFrame:
    """Daily price -> the mean of the month's observations."""
    return daily.groupBy(_month_of("reference_date")).agg(F.avg("value").alias(name))


def compound_pct(rate_pct: Column) -> Column:
    """Aggregate: (prod(1 + r/100) - 1) * 100, via exp(sum(log1p)) — Spark has no product()."""
    return (F.exp(F.sum(F.log1p(rate_pct / PERCENT))) - 1) * PERCENT


def monthly_compound(daily_rate: DataFrame, name: str, days_name: str) -> DataFrame:
    return daily_rate.groupBy(_month_of("reference_date")).agg(
        compound_pct(F.col("value")).alias(name), F.count("value").alias(days_name)
    )


def at_period_end_month(frame: DataFrame, name: str) -> DataFrame:
    """Monthly, quarterly and rolling-quarter values: the month in which the period ends."""
    return frame.select(_month_of("period_end"), F.col("value").alias(name))


def month_spine(frames: Sequence[DataFrame]) -> DataFrame:
    """Every calendar month between the first and the last month present in ``frames``."""
    months = reduce(DataFrame.unionByName, (frame.select(MONTH) for frame in frames))
    bounds = months.agg(F.min(MONTH).alias("first"), F.max(MONTH).alias("last"))
    return bounds.select(
        F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 MONTH"))).alias(MONTH)
    )


def join_on_month(spine: DataFrame, frames: Sequence[DataFrame]) -> DataFrame:
    """Left joins on reference_month; each frame has one row per month (1:1, no fan-out)."""
    return reduce(lambda panel, frame: panel.join(frame, MONTH, "left"), frames, spine)
