"""Gold output contract: columns, types and the grain of every table (docs/GOLD.md §1).

Units are in the column names: ``_pct`` (percent), ``_pp`` (percentage points),
``_pct_year`` (% a.a.), ``_pct_day`` (% a.d.). Exchange rates are R$ per unit of currency.
"""

from typing import Final

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from financial_lakehouse.silver.schemas import VALUE_TYPE

RATE: Final = DecimalType(16, 6)  # percentages and exchange rates
AMOUNT: Final = VALUE_TYPE  # balances (reserves, monetary base, credit)
DT, STR, BOOL, INT = DateType(), StringType(), BooleanType(), IntegerType()


def _schema(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, kind, True) for name, kind in fields])


GOLD_SCHEMAS: Final[dict[str, StructType]] = {
    "gold_interest_rates": _schema(
        ("reference_date", DT),
        ("selic_target_pct_year", RATE),
        ("selic_target_change_pp", RATE),
        ("is_copom_change", BOOL),
        ("selic_daily_pct_day", RATE),
        ("selic_daily_annualized_pct_year", RATE),
    ),
    "gold_exchange_rates": _schema(
        ("reference_date", DT),
        ("usd_brl", RATE),
        ("usd_brl_change_pct", RATE),
        ("eur_brl", RATE),
        ("eur_brl_change_pct", RATE),
        ("eur_usd_implied", RATE),
        ("international_reserves_usd_mn", AMOUNT),
    ),
    "gold_inflation": _schema(
        ("reference_month", DT),
        ("ipca_pct", RATE),
        ("inflation_12m", RATE),
        ("ipca_moving_average_3m", RATE),
        ("ipca_moving_average_12m", RATE),
        ("igpm_pct", RATE),
        ("igpm_12m", RATE),
    ),
    "gold_economic_activity": _schema(
        ("reference_quarter", DT),
        ("quarter", STR),
        ("gdp_growth_yoy_pct", RATE),
        ("gdp_growth_yoy_moving_average_4q", RATE),
        ("credit_total", AMOUNT),
        ("credit_total_yoy_pct", RATE),
        ("monetary_base", AMOUNT),
        ("monetary_base_yoy_pct", RATE),
    ),
    "gold_labor_market": _schema(
        ("reference_month", DT),
        ("period_start", DT),
        ("period_end", DT),
        ("unemployment_rate_pct", RATE),
        ("unemployment_change_3m_pp", RATE),
        ("unemployment_change_12m_pp", RATE),
    ),
    "gold_brazil_macro_monthly": _schema(
        ("reference_month", DT),
        ("is_partial_month", BOOL),
        ("selic_target_pct_year", RATE),
        ("selic_change", RATE),
        ("selic_month_pct", RATE),
        ("selic_business_days", INT),
        ("selic_12m_pct", RATE),
        ("ipca_pct", RATE),
        ("inflation_12m", RATE),
        ("ipca_moving_average_3m", RATE),
        ("ipca_moving_average_12m", RATE),
        ("igpm_pct", RATE),
        ("igpm_12m", RATE),
        ("real_interest_rate", RATE),
        ("usd_brl", RATE),
        ("usd_brl_average", RATE),
        ("usd_brl_change", RATE),
        ("usd_brl_moving_average_3m", RATE),
        ("usd_brl_moving_average_12m", RATE),
        ("eur_brl", RATE),
        ("international_reserves_usd_mn", AMOUNT),
        ("monetary_base", AMOUNT),
        ("credit_total", AMOUNT),
        ("unemployment_rate_pct", RATE),
        ("gdp_growth_yoy_pct", RATE),
    ),
}

TABLE_COMMENTS: Final[dict[str, str]] = {
    "gold_interest_rates": "Grain: one row per calendar date with a Selic target or daily rate.",
    "gold_exchange_rates": "Grain: one row per date with USD, EUR (PTAX close) or reserves.",
    "gold_inflation": "Grain: one row per month (IPCA and IGP-M monthly rates).",
    "gold_economic_activity": "Grain: one row per quarter (GDP; credit and base at quarter end).",
    "gold_labor_market": "Grain: one row per PNAD rolling quarter, keyed by its last month.",
    "gold_brazil_macro_monthly": (
        "Grain: one row per calendar month; resampling rules in docs/GOLD.md section 2."
    ),
}
