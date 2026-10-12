"""Rolling correlations in the SELIC queries: exact Pearson, full window only, NULL when undefined.

Regression: with Spark 4 ANSI mode, ``CORR`` raised DIVIDE_BY_ZERO when the Selic stayed
constant for the whole window (common in real data); the queries now return NULL instead.
"""

from collections.abc import Iterator
from datetime import date
from itertools import pairwise
from pathlib import Path
from statistics import correlation

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from financial_lakehouse.gold import GOLD_SCHEMAS

SQL_DIR = Path("projects/brazilian-financial-lakehouse/sql/gold")
MONTHS = [date(2020 + i // 12, i % 12 + 1, 1) for i in range(30)]
CORRELATION_COLUMNS = {
    "selic_vs_inflation": "selic_inflation_corr_24m",
    "selic_vs_exchange": "selic_change_usd_change_corr_24m",
}


def _macro_view(spark: SparkSession, selic: list[float], inflation: list[float]) -> None:
    """The macro table's columns (DOUBLE instead of DECIMAL for brevity); others are NULL."""
    fields = GOLD_SCHEMAS["gold_brazil_macro_monthly"].fields
    values = {
        "reference_month": MONTHS,
        "is_partial_month": [False] * len(MONTHS),
        "selic_target_pct_year": selic,
        "inflation_12m": inflation,
        "selic_change": [None, *(b - a for a, b in pairwise(selic))],
        "usd_brl_change": inflation,
    }
    empty = [None] * len(MONTHS)
    rows = list(zip(*(values.get(f.name, empty) for f in fields), strict=True))
    ddl = ", ".join(
        f"{f.name} {'double' if isinstance(f.dataType, DecimalType) else f.dataType.simpleString()}"
        for f in fields
    )
    spark.createDataFrame(rows, ddl).createOrReplaceTempView("gold_brazil_macro_monthly")


@pytest.fixture
def macro_view(spark: SparkSession) -> Iterator[None]:
    yield
    spark.catalog.dropTempView("gold_brazil_macro_monthly")


def _query(spark: SparkSession, repo_root: Path, name: str) -> dict[date, float | None]:
    sql = (repo_root / SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    column = CORRELATION_COLUMNS[name]
    return {r.reference_month: r[column] for r in spark.sql(sql).collect()}


@pytest.mark.usefixtures("macro_view")
def test_constant_selic_gives_null_correlation_instead_of_an_error(
    spark: SparkSession, repo_root: Path
) -> None:
    _macro_view(spark, [10.5] * 30, [4.0 + 0.1 * i for i in range(30)])
    for name in CORRELATION_COLUMNS:
        assert set(_query(spark, repo_root, name).values()) == {None}


@pytest.mark.usefixtures("macro_view")
def test_correlation_is_pearson_over_the_last_24_months(
    spark: SparkSession, repo_root: Path
) -> None:
    selic = [10.0 + (i % 5) * 0.25 for i in range(30)]
    inflation = [4.0 + ((i * 7) % 11) * 0.1 for i in range(30)]
    _macro_view(spark, selic, inflation)

    result = _query(spark, repo_root, "selic_vs_inflation")

    assert all(result[m] is None for m in MONTHS[:23])  # fewer than 24 paired months
    for end in range(23, 30):
        expected = correlation(selic[end - 23 : end + 1], inflation[end - 23 : end + 1])
        assert abs(float(result[MONTHS[end]]) - expected) <= 1e-4  # type: ignore[arg-type]
