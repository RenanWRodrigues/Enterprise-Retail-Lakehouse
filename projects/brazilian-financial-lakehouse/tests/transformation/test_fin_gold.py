"""Gold: resampling rules, derived metrics and grain, against plain-Python oracles.

Oracles are computed from the synthetic Silver rows (conftest) with ``Decimal``/``float``
arithmetic, independently of the Spark expressions under test.
"""

from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from itertools import pairwise
from math import prod

import pytest
from pyspark.sql import DataFrame, Row

from financial_lakehouse.gold import GOLD_SCHEMAS
from financial_lakehouse.gold.tables import enrich_monthly, monthly_panel

TOLERANCE = 1e-5  # Gold stores DECIMAL(16, 6)
MONTHS = [date(2024 + i // 12, i % 12 + 1, 1) for i in range(26)]  # 2024-01 .. 2026-02
SilverRows = list[dict[str, object]]


def _series(rows: SilverRows, indicator_id: str) -> dict[date, Decimal]:
    return {r["reference_date"]: r["value"] for r in rows if r["indicator_id"] == indicator_id}  # type: ignore[misc]


def _in_month(points: dict[date, Decimal], month: date) -> list[tuple[date, Decimal]]:
    return sorted(
        (d, v) for d, v in points.items() if (d.year, d.month) == (month.year, month.month)
    )


def _compound(rates: Iterable[float]) -> float:
    return (prod(1 + r / 100 for r in rates) - 1) * 100


def _close(actual: Decimal | None, expected: float | None) -> bool:
    if actual is None or expected is None:
        return actual is None and expected is None
    return abs(float(actual) - expected) <= TOLERANCE


def _shift(month: date, months: int) -> date:
    index = month.year * 12 + month.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def _trailing(points: dict[date, float], month: date, size: int) -> list[float] | None:
    window = [points.get(_shift(month, -k)) for k in range(size)]
    return None if any(v is None for v in window) else [v for v in window if v is not None]


@pytest.fixture(scope="module")
def macro(gold_rows: dict[str, list[Row]]) -> dict[date, Row]:
    return {r.reference_month: r for r in gold_rows["gold_brazil_macro_monthly"]}


# --- contract and grain -----------------------------------------------------------------------


def test_every_table_matches_its_contract(gold_tables: dict[str, DataFrame]) -> None:
    for name, schema in GOLD_SCHEMAS.items():
        assert [(f.name, f.dataType) for f in gold_tables[name].schema.fields] == [
            (f.name, f.dataType) for f in schema.fields
        ], name


@pytest.mark.parametrize(
    ("table", "grain"),
    [
        ("gold_interest_rates", "reference_date"),
        ("gold_exchange_rates", "reference_date"),
        ("gold_inflation", "reference_month"),
        ("gold_economic_activity", "reference_quarter"),
        ("gold_labor_market", "reference_month"),
        ("gold_brazil_macro_monthly", "reference_month"),
    ],
)
def test_grain_is_unique(gold_rows: dict[str, list[Row]], table: str, grain: str) -> None:
    keys = [row[grain] for row in gold_rows[table]]
    assert keys and len(keys) == len(set(keys))


def test_macro_has_every_calendar_month_once(macro: dict[date, Row]) -> None:
    assert sorted(macro) == MONTHS  # consecutive spine: windows of N rows are N months


# --- resampling rules (docs/GOLD.md §2) -------------------------------------------------------


def test_daily_levels_use_the_last_observation_of_the_month(
    macro: dict[date, Row], silver_rows: SilverRows
) -> None:
    target, usd = _series(silver_rows, "selic_target"), _series(silver_rows, "usd_brl_sell")
    for month in MONTHS:
        assert macro[month].selic_target_pct_year == _in_month(target, month)[-1][1]
        assert macro[month].usd_brl == _in_month(usd, month)[-1][1]


def test_usd_average_is_the_mean_of_the_month(
    macro: dict[date, Row], silver_rows: SilverRows
) -> None:
    usd = _series(silver_rows, "usd_brl_sell")
    for month in MONTHS:
        values = [float(v) for _, v in _in_month(usd, month)]
        assert _close(macro[month].usd_brl_average, sum(values) / len(values))


def test_daily_selic_is_compounded_over_the_month(
    macro: dict[date, Row], silver_rows: SilverRows
) -> None:
    daily = _series(silver_rows, "selic_daily")
    for month in MONTHS:
        days = _in_month(daily, month)
        assert macro[month].selic_business_days == len(days)
        assert _close(macro[month].selic_month_pct, _compound(float(v) for _, v in days))


def test_quarterly_gdp_appears_only_in_the_quarter_end_month(
    macro: dict[date, Row], silver_rows: SilverRows
) -> None:
    gdp = _series(silver_rows, "gdp_growth_yoy")
    expected = {_shift(quarter, 2): value for quarter, value in gdp.items()}
    actual = {m: r.gdp_growth_yoy_pct for m, r in macro.items() if r.gdp_growth_yoy_pct is not None}
    assert actual == expected  # no repetition, interpolation or fan-out to other months


def test_rolling_quarter_is_attached_to_its_last_month(
    macro: dict[date, Row], gold_rows: dict[str, list[Row]], silver_rows: SilverRows
) -> None:
    rate = _series(silver_rows, "unemployment_rate")
    assert {m: r.unemployment_rate_pct for m, r in macro.items() if r.unemployment_rate_pct} == rate
    labor = {r.reference_month: r for r in gold_rows["gold_labor_market"]}
    assert (labor[date(2025, 3, 1)].period_start, labor[date(2025, 3, 1)].period_end) == (
        date(2025, 1, 1),
        date(2025, 3, 31),
    )


def test_only_the_month_in_progress_is_partial(macro: dict[date, Row]) -> None:
    assert [m for m, r in macro.items() if r.is_partial_month] == [date(2026, 2, 1)]


# --- derived metrics (docs/GOLD.md §3) --------------------------------------------------------


def test_inflation_12m_compounds_twelve_consecutive_months(
    macro: dict[date, Row], silver_rows: SilverRows
) -> None:
    ipca = {m: float(v) for m, v in _series(silver_rows, "ipca_monthly").items()}
    for month in MONTHS:
        window = _trailing(ipca, month, 12)
        assert _close(macro[month].inflation_12m, _compound(window) if window else None), month
    assert macro[date(2024, 11, 1)].inflation_12m is None  # only 11 months available
    assert macro[date(2024, 12, 1)].inflation_12m is not None


def test_igpm_12m(macro: dict[date, Row], silver_rows: SilverRows) -> None:
    igpm = {m: float(v) for m, v in _series(silver_rows, "igpm_monthly").items()}
    for month in MONTHS:
        window = _trailing(igpm, month, 12)
        assert _close(macro[month].igpm_12m, _compound(window) if window else None), month


@pytest.mark.parametrize("size", [3, 12])
def test_moving_averages_need_a_full_window(
    macro: dict[date, Row], silver_rows: SilverRows, size: int
) -> None:
    ipca = {m: float(v) for m, v in _series(silver_rows, "ipca_monthly").items()}
    usd = {m: float(r.usd_brl) for m, r in macro.items()}
    for month in MONTHS:
        row = macro[month]
        ipca_window, usd_window = _trailing(ipca, month, size), _trailing(usd, month, size)
        assert _close(
            row[f"ipca_moving_average_{size}m"], sum(ipca_window) / size if ipca_window else None
        )
        assert _close(
            row[f"usd_brl_moving_average_{size}m"], sum(usd_window) / size if usd_window else None
        )


def test_selic_change_is_month_end_difference_in_pp(macro: dict[date, Row]) -> None:
    assert macro[MONTHS[0]].selic_change is None
    for previous, month in pairwise(MONTHS):
        expected = macro[month].selic_target_pct_year - macro[previous].selic_target_pct_year
        assert macro[month].selic_change == expected
    assert macro[date(2024, 5, 1)].selic_change == Decimal("-1.25")  # 11.75 -> 10.50


def test_usd_brl_change_is_month_end_percent_change(macro: dict[date, Row]) -> None:
    for previous, month in pairwise(MONTHS):
        expected = (float(macro[month].usd_brl) / float(macro[previous].usd_brl) - 1) * 100
        assert _close(macro[month].usd_brl_change, expected)


def test_selic_12m_and_real_interest_rate(macro: dict[date, Row]) -> None:
    selic_month = {m: float(r.selic_month_pct) for m, r in macro.items()}
    checked = 0
    for month in MONTHS:
        row = macro[month]
        window = _trailing(selic_month, month, 12)
        expected = _compound(window) if window and not row.is_partial_month else None
        assert _close(row.selic_12m_pct, expected), month
        if row.selic_12m_pct is not None and row.inflation_12m is not None:
            nominal, inflation = float(row.selic_12m_pct), float(row.inflation_12m)
            fisher = ((1 + nominal / 100) / (1 + inflation / 100) - 1) * 100
            assert _close(row.real_interest_rate, fisher)
            checked += 1
        else:
            assert row.real_interest_rate is None
    assert checked == 14  # 2024-12 .. 2026-01
    assert macro[date(2026, 2, 1)].selic_12m_pct is None  # partial month: not a full window


def test_labor_market_changes_compare_non_overlapping_quarters(
    gold_rows: dict[str, list[Row]],
) -> None:
    labor = {r.reference_month: r for r in gold_rows["gold_labor_market"]}
    assert labor[date(2024, 3, 1)].unemployment_change_3m_pp is None
    assert labor[date(2024, 4, 1)].unemployment_change_3m_pp == Decimal("-0.3")  # vs Jan (3 back)
    assert labor[date(2025, 1, 1)].unemployment_change_12m_pp == Decimal("-1.2")
    assert labor[date(2024, 12, 1)].unemployment_change_12m_pp is None


def test_economic_activity_is_quarterly_with_quarter_end_balances(
    gold_rows: dict[str, list[Row]], silver_rows: SilverRows
) -> None:
    quarters = {r.reference_quarter: r for r in gold_rows["gold_economic_activity"]}
    credit = _series(silver_rows, "credit_total")
    assert sorted(quarters) == [_shift(date(2024, 1, 1), 3 * i) for i in range(8)]  # 2024Q1..2025Q4
    q = quarters[date(2025, 4, 1)]
    assert (q.quarter, q.credit_total) == ("2025-Q2", credit[date(2025, 6, 1)])
    yoy = (float(credit[date(2025, 6, 1)]) / float(credit[date(2024, 6, 1)]) - 1) * 100
    assert _close(q.credit_total_yoy_pct, yoy)
    assert quarters[date(2024, 10, 1)].gdp_growth_yoy_moving_average_4q is not None
    assert _close(
        quarters[date(2024, 10, 1)].gdp_growth_yoy_moving_average_4q, (2.5 + 2.8 + 3.1 + 3.4) / 4
    )
    assert quarters[date(2024, 7, 1)].gdp_growth_yoy_moving_average_4q is None
    assert quarters[date(2025, 10, 1)].gdp_growth_yoy_pct is None  # not published yet


def test_metrics_are_identical_across_tables(
    macro: dict[date, Row], gold_rows: dict[str, list[Row]]
) -> None:
    for row in gold_rows["gold_inflation"]:
        assert row.inflation_12m == macro[row.reference_month].inflation_12m


# --- daily tables -----------------------------------------------------------------------------


def test_interest_rates_daily(gold_rows: dict[str, list[Row]], silver_rows: SilverRows) -> None:
    rows = {r.reference_date: r for r in gold_rows["gold_interest_rates"]}
    daily = _series(silver_rows, "selic_daily")
    day = date(2025, 3, 3)
    assert _close(
        rows[day].selic_daily_annualized_pct_year, ((1 + float(daily[day]) / 100) ** 252 - 1) * 100
    )
    assert sorted(d for d, r in rows.items() if r.is_copom_change) == [
        date(2024, 5, 9),
        date(2025, 1, 30),
    ]
    saturday = date(2025, 3, 1)
    assert (rows[saturday].selic_target_pct_year, rows[saturday].selic_daily_pct_day) == (
        Decimal("13.25"),
        None,
    )  # target is in force every calendar day; the daily rate exists only on business days


def test_exchange_rates_change_versus_previous_business_day(
    gold_rows: dict[str, list[Row]], silver_rows: SilverRows
) -> None:
    rows = {r.reference_date: r for r in gold_rows["gold_exchange_rates"]}
    usd = _series(silver_rows, "usd_brl_sell")
    monday, friday = date(2025, 3, 3), date(2025, 2, 28)
    assert _close(
        rows[monday].usd_brl_change_pct, (float(usd[monday]) / float(usd[friday]) - 1) * 100
    )
    assert _close(
        rows[monday].eur_usd_implied, float(rows[monday].eur_brl) / float(rows[monday].usd_brl)
    )
    assert date(2025, 3, 1) not in rows  # no exchange data on a Saturday


# --- gaps -------------------------------------------------------------------------------------


def test_a_missing_month_nulls_every_window_that_contains_it(
    silver_frame: DataFrame,
) -> None:
    gap = date(2024, 6, 1)
    without_june = silver_frame.filter(
        ~((silver_frame.indicator_id == "ipca_monthly") & (silver_frame.reference_date == gap))
    )
    rows = {
        r.reference_month: r
        for r in enrich_monthly(monthly_panel(without_june))
        .select("reference_month", "inflation_12m", "ipca_moving_average_3m")
        .collect()
    }
    assert all(rows[_shift(gap, k)].inflation_12m is None for k in range(12))
    assert rows[_shift(gap, 12)].inflation_12m is not None
    assert rows[_shift(gap, 2)].ipca_moving_average_3m is None
    assert rows[_shift(gap, 3)].ipca_moving_average_3m is not None
