"""Derived metrics as Spark column expressions (definitions in docs/GOLD.md §3).

Windows run over a *consecutive* monthly spine, so "N rows" means "N months". A trailing metric
is NULL unless all N months are present: no partial windows presented as full ones.
"""

from typing import Final

from pyspark.sql import Column, Window, WindowSpec
from pyspark.sql import functions as F

from financial_lakehouse.gold.resample import MONTH, PERCENT

BUSINESS_DAYS_PER_YEAR: Final = 252  # BCB convention for annualizing the Selic


def _trailing(periods: int, order: str) -> WindowSpec:
    return Window.orderBy(order).rowsBetween(-(periods - 1), Window.currentRow)


def compounded_pct(column: str, periods: int, order: str = MONTH) -> Column:
    """Accumulated rate over the trailing ``periods``: (prod(1 + r/100) - 1) * 100."""
    window = _trailing(periods, order)
    growth = F.exp(F.sum(F.log1p(F.col(column) / PERCENT)).over(window)) - 1
    return F.when(F.count(column).over(window) == periods, growth * PERCENT)


def moving_average(column: str, periods: int, order: str = MONTH) -> Column:
    """Mean of the trailing ``periods`` rows; NULL unless all of them are present."""
    window = _trailing(periods, order)
    return F.when(F.count(column).over(window) == periods, F.avg(column).over(window))


def difference(column: str, lag: int = 1, order: str = MONTH) -> Column:
    """Change in the column's own unit (p.p. for rates) versus ``lag`` rows earlier."""
    return F.col(column) - F.lag(column, lag).over(Window.orderBy(order))


def pct_change(column: str, lag: int = 1, order: str = MONTH) -> Column:
    """Percent change versus ``lag`` rows earlier (NULL when the earlier value is NULL or 0)."""
    previous = F.lag(column, lag).over(Window.orderBy(order))
    return (F.try_divide(F.col(column), previous) - 1) * PERCENT


def annualized_daily_pct(daily_pct: Column) -> Column:
    """((1 + r/100) ^ 252 - 1) * 100 — the BCB annualization of a daily rate."""
    return (F.pow(1 + daily_pct / PERCENT, BUSINESS_DAYS_PER_YEAR) - 1) * PERCENT


def real_interest_rate(nominal_pct: Column, inflation_pct: Column) -> Column:
    """Fisher: ((1 + i/100) / (1 + pi/100) - 1) * 100. Ex-post when both are realized rates."""
    return (F.try_divide(1 + nominal_pct / PERCENT, 1 + inflation_pct / PERCENT) - 1) * PERCENT
