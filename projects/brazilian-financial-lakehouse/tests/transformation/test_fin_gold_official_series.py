"""Gold temporal transformations against the series the BCB and IBGE publish themselves.

The synthetic tests (``test_fin_gold.py``) check the code against Python oracles written from
the same reading of the rules; a wrong convention would pass both. Here the oracle is the
official aggregate:

- ``selic_month_pct`` (daily -> month, compounded), input SGS 11: SGS 4390, Selic accumulated
  in the month (2 dp);
- ``usd_brl_average`` (daily -> month, mean), input SGS 1: SGS 3698, monthly mean (4 dp);
- ``usd_brl`` (daily -> month, last), input SGS 1: SGS 3696, end of period (4 dp);
- ``inflation_12m`` (monthly -> 12 months), input IBGE 1737 v63: IBGE 1737 v2265 (2 dp);
- ``selic_daily_annualized_pct_year``, input SGS 11: SGS 1178, annualized base 252 (2 dp).
- ``interest_rates`` with the Selic target, input SGS 432: every calendar day kept as
  published, 0.10 pp above SGS 1178 on business days, Copom steps flagged.

SGS metadata is not exposed by the API: 3698 and 3696 were identified because their values equal
the mean and the last value of SGS 1 in every month.

Fixtures in ``fixtures/official_*.json`` are the raw API responses captured on 2026-10-07
(2024-2025; IPCA from 2023 for the first 12-month window). Our values are rounded half-up to the
published precision before comparing. No network.
"""

import json
from calendar import monthrange
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from financial_lakehouse.catalog import indicator
from financial_lakehouse.gold.tables import build_interest_rates, enrich_monthly, monthly_panel
from financial_lakehouse.silver import SILVER_SCHEMA

FIXTURES = Path(__file__).parent / "fixtures"
CAPTURED = datetime(2026, 10, 7, 13, tzinfo=UTC)
CENTS, BASIS = Decimal("0.01"), Decimal("0.0001")
MONTHS_2024_2025 = [date(2024 + i // 12, i % 12 + 1, 1) for i in range(24)]


def _sgs(code: int) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = json.loads(
        (FIXTURES / f"official_sgs_{code}.json").read_text("utf-8")
    )
    return rows


def _sgs_date(text: str) -> date:
    day, month, year = text.split("/")
    return date(int(year), int(month), int(day))


def _ibge(variable: str) -> dict[date, Decimal]:
    payload = json.loads((FIXTURES / "official_ibge_1737_63_2265.json").read_text("utf-8"))
    [entry] = [v for v in payload if v["id"] == variable]
    series: dict[str, str] = entry["resultados"][0]["series"][0]["serie"]
    return {date(int(p[:4]), int(p[4:]), 1): Decimal(v) for p, v in series.items()}


def _monthly(code: int) -> dict[date, Decimal]:
    return {_sgs_date(r["data"]): Decimal(r["valor"]) for r in _sgs(code)}


def _silver_row(indicator_id: str, day: date, value: Decimal) -> tuple[Any, ...]:
    item = indicator(indicator_id)
    last_day = day.replace(day=monthrange(day.year, day.month)[1])
    period_end = day if item.frequency == "daily" else last_day  # daily or monthly only here
    row = {
        "indicator_id": indicator_id,
        "indicator_name": item.name,
        "reference_date": day,
        "value": value,
        "unit": item.unit,
        "frequency": item.frequency,
        "source": str(item.source),
        "updated_at": CAPTURED,
        "period_start": day,
        "period_end": period_end,
        "series_id": indicator_id,
        "revision_count": 1,
        "record_key": f"{indicator_id}-{day}",
        "bronze_ingestion_timestamp": CAPTURED,
    }
    return tuple(row[name] for name in SILVER_SCHEMA.fieldNames())


@pytest.fixture(scope="module")
def official_silver(spark: SparkSession) -> Iterator[DataFrame]:
    rows = [
        *(_silver_row("selic_daily", _sgs_date(r["data"]), Decimal(r["valor"])) for r in _sgs(11)),
        *(
            _silver_row("selic_target", _sgs_date(r["data"]), Decimal(r["valor"]))
            for r in _sgs(432)
        ),
        *(_silver_row("usd_brl_sell", _sgs_date(r["data"]), Decimal(r["valor"])) for r in _sgs(1)),
        *(_silver_row("ipca_monthly", month, value) for month, value in _ibge("63").items()),
    ]
    frame = spark.createDataFrame(rows, SILVER_SCHEMA).cache()
    yield frame
    frame.unpersist()


@pytest.fixture(scope="module")
def monthly(official_silver: DataFrame) -> dict[date, Any]:
    columns = ("reference_month", "selic_month_pct", "usd_brl", "usd_brl_average", "inflation_12m")
    panel = enrich_monthly(monthly_panel(official_silver)).select(*columns)
    return {row.reference_month: row for row in panel.collect()}


def _rounded(value: object, precision: Decimal) -> Decimal:
    return Decimal(str(value)).quantize(precision, ROUND_HALF_UP)


@pytest.mark.parametrize(
    ("column", "code", "precision"),
    [
        ("selic_month_pct", 4390, CENTS),  # compounded over the month's business days
        ("usd_brl_average", 3698, BASIS),  # mean of the month's quotes
        ("usd_brl", 3696, BASIS),  # last quote of the month
    ],
)
def test_daily_to_monthly_matches_the_official_monthly_series(
    monthly: dict[date, Any], column: str, code: int, precision: Decimal
) -> None:
    official = _monthly(code)
    assert sorted(official) == MONTHS_2024_2025
    mismatches = {
        month: (_rounded(getattr(monthly[month], column), precision), expected)
        for month, expected in official.items()
        if _rounded(getattr(monthly[month], column), precision) != expected
    }
    assert mismatches == {}


def test_inflation_12m_matches_the_ipca_accumulated_in_12_months(
    monthly: dict[date, Any],
) -> None:
    official = {m: v for m, v in _ibge("2265").items() if m.year >= 2024}  # 2023: warm-up
    assert sorted(official) == MONTHS_2024_2025
    mismatches = {
        month: (_rounded(monthly[month].inflation_12m, CENTS), expected)
        for month, expected in official.items()
        if _rounded(monthly[month].inflation_12m, CENTS) != expected
    }
    assert mismatches == {}
    assert monthly[date(2023, 11, 1)].inflation_12m is None  # 11 months: no partial window


def test_daily_selic_annualization_matches_sgs_1178(official_silver: DataFrame) -> None:
    rates = {
        row.reference_date: row.selic_daily_annualized_pct_year
        for row in build_interest_rates(official_silver)
        .filter("reference_date >= '2025-01-01'")
        .collect()
    }
    official = {_sgs_date(r["data"]): Decimal(r["valor"]) for r in _sgs(1178)}
    assert len(official) == 252
    mismatches = {
        day: (_rounded(rates[day], CENTS), expected)
        for day, expected in official.items()
        if _rounded(rates[day], CENTS) != expected
    }
    assert mismatches == {}


def test_selic_target_is_kept_for_every_calendar_day_without_filling(
    official_silver: DataFrame,
) -> None:
    """GOLD.md §"meta Selic nos fins de semana": SGS 432 itself publishes every calendar day."""
    rows = build_interest_rates(official_silver).filter("year(reference_date) IN (2024, 2025)")
    target_days = rows.filter("selic_target_pct_year IS NOT NULL")
    weekends = target_days.filter("dayofweek(reference_date) IN (1, 7)")

    assert target_days.count() == len(_sgs(432)) == 731
    assert weekends.filter("selic_daily_pct_day IS NOT NULL").count() == 0  # SGS 11: business days
    calendar = [date(2024, 1, 1) + timedelta(days=i) for i in range(731)]
    assert weekends.count() == sum(day.isoweekday() > 5 for day in calendar)  # all from source


def test_target_minus_effective_selic_is_the_published_spread(
    official_silver: DataFrame,
) -> None:
    """Three official series on one row: target (432) minus our annualized SGS 11 equals the
    0.10 pp the captured responses show between 432 and 1178 on all 252 business days of 2025,
    so the full join on ``reference_date`` lines the series up (no shifted day at a Copom step)."""
    rows = {
        row.reference_date: row
        for row in build_interest_rates(official_silver)
        .filter("reference_date >= '2025-01-01'")
        .collect()
    }
    effective = {_sgs_date(r["data"]) for r in _sgs(1178)}
    spreads = {
        _rounded(rows[day].selic_target_pct_year, CENTS)
        - _rounded(rows[day].selic_daily_annualized_pct_year, CENTS)
        for day in effective
    }
    assert spreads == {Decimal("0.10")}


def test_copom_changes_are_flagged_on_the_days_the_target_moved(
    official_silver: DataFrame,
) -> None:
    published = _sgs(432)
    moved = {
        _sgs_date(today["data"]): Decimal(today["valor"]) - Decimal(before["valor"])
        for before, today in pairwise(published)
        if today["valor"] != before["valor"]
    }
    flagged = {
        row.reference_date: _rounded(row.selic_target_change_pp, CENTS)
        for row in build_interest_rates(official_silver).filter("is_copom_change").collect()
    }

    assert flagged == moved
    assert len(moved) == 10  # 2024-2025: 3 cuts, then 7 hikes up to 15.00
    assert {day.isoweekday() for day in moved} == {4}  # effective the day after the meeting
