"""Analytical SQL (sql/gold/*.sql) executed on the Gold tables of the synthetic Silver."""

from datetime import date
from decimal import Decimal
from math import prod
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

SQL_DIR = Path("projects/brazilian-financial-lakehouse/sql/gold")
IPCA = [0.42, 0.83, 0.16, 0.38, 0.46, 0.21, 0.38, -0.02, 0.44, 0.56, 0.39, 0.52]
TOLERANCE = 1e-4

pytestmark = pytest.mark.usefixtures("gold_views")


def _run(spark: SparkSession, repo_root: Path, name: str) -> list[Row]:
    sql = (repo_root / SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    return spark.sql(sql).collect()


def _close(actual: Decimal | float | None, expected: float) -> bool:
    return actual is not None and abs(float(actual) - expected) <= TOLERANCE


@pytest.mark.parametrize(
    ("name", "grain"),
    [
        ("selic_vs_inflation", "reference_month"),
        ("selic_vs_exchange", "reference_month"),
        ("inflation_over_time", "reference_year"),
        ("exchange_over_time", "reference_year"),
        ("economic_activity", "reference_quarter"),
        ("labor_market", "reference_month"),
    ],
)
def test_every_query_runs_and_returns_one_row_per_grain(
    spark: SparkSession, repo_root: Path, name: str, grain: str
) -> None:
    rows = _run(spark, repo_root, name)
    keys = [row[grain] for row in rows]
    assert keys and len(keys) == len(set(keys))
    assert keys == sorted(keys)


def test_selic_vs_inflation_hides_correlation_without_a_full_window(
    spark: SparkSession, repo_root: Path
) -> None:
    rows = {r.reference_month: r for r in _run(spark, repo_root, "selic_vs_inflation")}
    assert date(2026, 2, 1) not in rows  # partial month excluded
    assert all(r.selic_inflation_corr_24m is None for r in rows.values())  # < 24 paired months
    december = rows[date(2024, 12, 1)]
    assert (
        december.selic_minus_inflation_pp == december.selic_target_pct_year - december.inflation_12m
    )
    assert december.inflation_12m_one_year_later == rows[date(2025, 12, 1)].inflation_12m


def test_inflation_over_time_accumulates_only_complete_years(
    spark: SparkSession, repo_root: Path
) -> None:
    years = {r.reference_year: r for r in _run(spark, repo_root, "inflation_over_time")}
    expected = (prod(1 + x / 100 for x in IPCA) - 1) * 100
    assert _close(years[2024].ipca_year_pct, expected)
    assert _close(years[2025].ipca_year_pct, expected)
    assert years[2026].ipca_year_pct is None  # only January published
    assert years[2024].ipca_deflation_months == 1


def test_exchange_over_time_uses_the_last_quote_of_each_year(
    spark: SparkSession, repo_root: Path
) -> None:
    years = {r.reference_year: r for r in _run(spark, repo_root, "exchange_over_time")}
    assert years[2024].usd_brl_year_end == Decimal("5.581000")  # 2024-12-31
    assert years[2025].usd_brl_year_end == Decimal("6.181000")  # 2025-12-31
    assert _close(years[2025].usd_brl_year_change_pct, (6.181 / 5.581 - 1) * 100)
    assert years[2024].usd_brl_year_change_pct is None


def test_economic_activity_deflates_credit_with_quarter_end_inflation(
    spark: SparkSession, repo_root: Path
) -> None:
    quarters = {r.reference_quarter: r for r in _run(spark, repo_root, "economic_activity")}
    q = quarters[date(2025, 4, 1)]
    nominal, inflation = float(q.credit_total_yoy_pct), float(q.inflation_12m)
    assert _close(
        q.credit_total_real_yoy_pct_approx, ((1 + nominal / 100) / (1 + inflation / 100) - 1) * 100
    )


def test_labor_market_direction_and_range(spark: SparkSession, repo_root: Path) -> None:
    months = {r.reference_month: r for r in _run(spark, repo_root, "labor_market")}
    january = months[date(2025, 1, 1)]
    assert january.year_over_year_direction == "down_vs_last_year"
    assert (january.lowest_rate_12m_pct, january.highest_rate_12m_pct) == (
        Decimal("6.6"),
        Decimal("7.7"),
    )
    assert months[date(2024, 1, 1)].year_over_year_direction is None
