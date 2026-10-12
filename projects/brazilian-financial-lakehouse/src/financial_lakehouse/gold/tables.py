"""Gold tables from Silver (pure ``DataFrame -> DataFrame``).

Daily tables join only daily series on ``reference_date``; everything else meets on the monthly
spine after resampling (``resample.py``). Monthly-derived metrics are computed once, on the
monthly panel, so the same metric has the same value in every table that shows it.
"""

from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from financial_lakehouse.gold.metrics import (
    annualized_daily_pct,
    compounded_pct,
    difference,
    moving_average,
    pct_change,
    real_interest_rate,
)
from financial_lakehouse.gold.resample import (
    MONTH,
    at_period_end_month,
    join_on_month,
    month_spine,
    monthly_compound,
    monthly_last,
    monthly_mean,
    series,
)
from financial_lakehouse.silver.standardize import DAILY

TWELVE_MONTHS, QUARTER_MONTHS, YEAR_OF_QUARTERS = 12, 3, 4
QUARTER_END_MONTHS: Final = (3, 6, 9, 12)


def _daily_change(frame: DataFrame, value: str, change: Column) -> DataFrame:
    """Change versus the previous *available* observation of the same series."""
    return frame.select("reference_date", F.col("value").alias(value), change)


def build_interest_rates(silver: DataFrame) -> DataFrame:
    """Grain: one row per date with a Selic target (calendar days) or daily rate (business days)."""
    by_date = Window.orderBy("reference_date")
    target = series(silver, "selic_target").select(
        "reference_date",
        F.col("value").alias("selic_target_pct_year"),
        (F.col("value") - F.lag("value").over(by_date)).alias("selic_target_change_pp"),
    )
    daily = series(silver, "selic_daily").select(
        "reference_date",
        F.col("value").alias("selic_daily_pct_day"),
        annualized_daily_pct(F.col("value")).alias("selic_daily_annualized_pct_year"),
    )
    # 1:1 on reference_date (both unique per date in Silver); full: target exists on weekends.
    return target.join(daily, "reference_date", "full").withColumn(
        "is_copom_change", F.col("selic_target_change_pp") != 0
    )


def build_exchange_rates(silver: DataFrame) -> DataFrame:
    """Grain: one row per date with USD (SGS 1), EUR (PTAX closing) or reserves; daily changes."""
    by_date = Window.orderBy("reference_date")

    def priced(indicator_id: str, name: str) -> DataFrame:
        change = (F.try_divide(F.col("value"), F.lag("value").over(by_date)) - 1) * 100
        return _daily_change(series(silver, indicator_id), name, change.alias(f"{name}_change_pct"))

    reserves = series(silver, "international_reserves").select(
        "reference_date", F.col("value").alias("international_reserves_usd_mn")
    )
    # 1:1 joins on reference_date: each side is one daily series, unique per date.
    joined = (
        priced("usd_brl_sell", "usd_brl")
        .join(priced("eur_brl_ptax", "eur_brl"), "reference_date", "full")
        .join(reserves, "reference_date", "full")
    )
    return joined.withColumn("eur_usd_implied", F.try_divide(F.col("eur_brl"), F.col("usd_brl")))


def monthly_panel(silver: DataFrame) -> DataFrame:
    """One row per calendar month with every series resampled by the documented rule."""
    frames = [
        monthly_last(series(silver, "selic_target"), "selic_target_pct_year"),
        monthly_compound(series(silver, "selic_daily"), "selic_month_pct", "selic_business_days"),
        at_period_end_month(series(silver, "ipca_monthly"), "ipca_pct"),
        at_period_end_month(series(silver, "igpm_monthly"), "igpm_pct"),
        monthly_last(series(silver, "usd_brl_sell"), "usd_brl"),
        monthly_mean(series(silver, "usd_brl_sell"), "usd_brl_average"),
        monthly_last(series(silver, "eur_brl_ptax"), "eur_brl"),
        monthly_last(series(silver, "international_reserves"), "international_reserves_usd_mn"),
        at_period_end_month(series(silver, "monetary_base"), "monetary_base"),
        at_period_end_month(series(silver, "credit_total"), "credit_total"),
        at_period_end_month(series(silver, "unemployment_rate"), "unemployment_rate_pct"),
        at_period_end_month(series(silver, "gdp_growth_yoy"), "gdp_growth_yoy_pct"),
    ]
    latest_daily = silver.filter(F.col("frequency") == DAILY).agg(
        F.max("reference_date").alias("_as_of")
    )
    month = F.col(MONTH)
    partial = (month == F.trunc("_as_of", "month")) & (F.col("_as_of") < F.last_day(month))
    return (
        join_on_month(month_spine(frames), frames)
        .crossJoin(latest_daily)  # single row
        .withColumn("is_partial_month", F.coalesce(partial, F.lit(False)))
        .drop("_as_of")
    )


def enrich_monthly(panel: DataFrame) -> DataFrame:
    """Derived monthly metrics (docs/GOLD.md §3) on the consecutive monthly spine."""
    selic_12m = F.when(~F.col("is_partial_month"), compounded_pct("selic_month_pct", TWELVE_MONTHS))
    gdp_window = Window.orderBy(MONTH).rowsBetween(-(TWELVE_MONTHS - QUARTER_MONTHS), 0)
    gdp_ma = F.when(
        F.count("gdp_growth_yoy_pct").over(gdp_window) == YEAR_OF_QUARTERS,
        F.avg("gdp_growth_yoy_pct").over(gdp_window),
    )
    return panel.select(
        "*",
        difference("selic_target_pct_year").alias("selic_change"),
        selic_12m.alias("selic_12m_pct"),
        compounded_pct("ipca_pct", TWELVE_MONTHS).alias("inflation_12m"),
        moving_average("ipca_pct", 3).alias("ipca_moving_average_3m"),
        moving_average("ipca_pct", TWELVE_MONTHS).alias("ipca_moving_average_12m"),
        compounded_pct("igpm_pct", TWELVE_MONTHS).alias("igpm_12m"),
        pct_change("usd_brl").alias("usd_brl_change"),
        moving_average("usd_brl", 3).alias("usd_brl_moving_average_3m"),
        moving_average("usd_brl", TWELVE_MONTHS).alias("usd_brl_moving_average_12m"),
        difference("unemployment_rate_pct", QUARTER_MONTHS).alias("unemployment_change_3m_pp"),
        difference("unemployment_rate_pct", TWELVE_MONTHS).alias("unemployment_change_12m_pp"),
        gdp_ma.alias("gdp_growth_yoy_moving_average_4q"),
        pct_change("credit_total", TWELVE_MONTHS).alias("credit_total_yoy_pct"),
        pct_change("monetary_base", TWELVE_MONTHS).alias("monetary_base_yoy_pct"),
    ).withColumn(
        "real_interest_rate", real_interest_rate(F.col("selic_12m_pct"), F.col("inflation_12m"))
    )


def build_inflation(monthly: DataFrame) -> DataFrame:
    """Grain: months with an IPCA or IGP-M value, metrics from the monthly panel."""
    return monthly.filter(F.col("ipca_pct").isNotNull() | F.col("igpm_pct").isNotNull())


def build_labor_market(monthly: DataFrame) -> DataFrame:
    """Grain: one PNAD rolling quarter, keyed by its last month, with its explicit period."""
    month = F.col(MONTH)
    return monthly.filter(F.col("unemployment_rate_pct").isNotNull()).select(
        "*",
        F.add_months(month, -(QUARTER_MONTHS - 1)).alias("period_start"),
        F.last_day(month).alias("period_end"),
    )


def build_economic_activity(monthly: DataFrame) -> DataFrame:
    """Grain: one quarter (quarter-end month of the panel): GDP, credit and base at quarter end."""
    month = F.col(MONTH)
    has_data = (
        F.col("gdp_growth_yoy_pct").isNotNull()
        | F.col("credit_total").isNotNull()
        | F.col("monetary_base").isNotNull()
    )
    quarter_start = F.add_months(month, -(QUARTER_MONTHS - 1))
    label = F.format_string("%d-Q%d", F.year(month), F.quarter(month))
    return monthly.filter(F.month(month).isin(*QUARTER_END_MONTHS) & has_data).select(
        "*", quarter_start.alias("reference_quarter"), label.alias("quarter")
    )
