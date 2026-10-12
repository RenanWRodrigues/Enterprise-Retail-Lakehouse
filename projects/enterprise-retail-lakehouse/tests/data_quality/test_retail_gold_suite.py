"""Gold publication gate: every contract violation is CRITICAL, unknown members are accepted.

A minimal hand-built star (one member per dimension plus its unknown member) keeps each case to
a single changed value. It avoids the 9999-12-31 SCD2 dates, so it also runs on Windows.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, DateType, DecimalType, StringType, TimestampType

from lakehouse_common.quality import CheckResult, QualityDecision, Status, decide, run_suite
from retail_lakehouse.gold.keys import UNKNOWN_KEY
from retail_lakehouse.gold.schemas import FOREIGN_KEYS, GOLD_SCHEMAS, GRAIN, NATURAL_KEYS
from retail_lakehouse.quality.suites import retail_gold_suite

AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
DAY_KEY = 20250105
CUSTOMER, PRODUCT, STORE, SUPPLIER = 1, 10, 20, 100
MISSING_KEY = 999_999

# One valid row per table (overrides on top of type defaults). Every fact references the members
# above; the shipment is not delivered yet: unknown delivery key (-1) and no delivery date.
_ROWS: dict[str, list[dict[str, Any]]] = {
    "dim_date": [{"date_key": DAY_KEY}],
    "dim_customer": [{"customer_sk": CUSTOMER, "customer_id": "C1"}],
    "dim_product": [{"product_sk": PRODUCT, "product_id": "P1", "supplier_sk": SUPPLIER}],
    "dim_store": [{"store_sk": STORE, "store_id": "S1"}],
    "dim_supplier": [{"supplier_sk": SUPPLIER, "supplier_id": "F1"}],
    "fact_sales": [
        {"order_id": "O1", "line_number": n, "date_key": DAY_KEY, "customer_sk": CUSTOMER,
         "product_sk": PRODUCT, "store_sk": STORE, "supplier_sk": SUPPLIER}
        for n in (1, 2)
    ],
    "fact_payments": [
        {"payment_id": "PAY1", "date_key": DAY_KEY, "customer_sk": CUSTOMER, "store_sk": STORE}
    ],
    "fact_shipments": [
        {"shipment_id": "SH1", "ship_date_key": DAY_KEY, "delivery_date_key": UNKNOWN_KEY,
         "delivery_date": None,
         "customer_sk": CUSTOMER, "store_sk": STORE}
    ],
    "fact_inventory_movements": [
        {"movement_id": "M1", "date_key": DAY_KEY, "store_sk": STORE, "product_sk": PRODUCT,
         "supplier_sk": SUPPLIER}
    ],
}  # fmt: skip
# The unknown member of each dimension, as dimensions.with_unknown_member builds it.
_UNKNOWN: dict[str, dict[str, Any]] = {
    "dim_date": {"date_key": UNKNOWN_KEY, "date": None},
    "dim_customer": {"customer_sk": UNKNOWN_KEY, "customer_id": None},
    "dim_product": {"product_sk": UNKNOWN_KEY, "product_id": None, "supplier_sk": UNKNOWN_KEY},
    "dim_store": {"store_sk": UNKNOWN_KEY, "store_id": None},
    "dim_supplier": {"supplier_sk": UNKNOWN_KEY, "supplier_id": None},
}


def _default(data_type: object) -> object:
    if isinstance(data_type, StringType):
        return "x"
    if isinstance(data_type, BooleanType):
        return False
    if isinstance(data_type, DecimalType):
        return Decimal("1.00")
    if isinstance(data_type, TimestampType):
        return AT
    if isinstance(data_type, DateType):
        return date(2025, 1, 5)
    return 0


def _frame(spark: SparkSession, name: str, rows: list[dict[str, Any]]) -> DataFrame:
    fields = GOLD_SCHEMAS[name].fields
    return spark.createDataFrame(
        [tuple(row.get(f.name, _default(f.dataType)) for f in fields) for row in rows],
        GOLD_SCHEMAS[name],
    )


def _gold(
    spark: SparkSession, changes: dict[str, list[dict[str, Any]]] | None = None
) -> dict[str, DataFrame]:
    """The valid star; ``changes`` replaces the rows (unknown members excluded) of some tables."""
    rows = {**_ROWS, **(changes or {})}
    return {
        name: _frame(spark, name, rows[name] + ([_UNKNOWN[name]] if name in _UNKNOWN else []))
        for name in GOLD_SCHEMAS
    }


def _with(name: str, **overrides: Any) -> dict[str, list[dict[str, Any]]]:
    """``name`` with its first row changed by ``overrides``."""
    first, *rest = _ROWS[name]
    return {name: [{**first, **overrides}, *rest]}


def _results(gold: dict[str, DataFrame], only: str | None = None) -> list[CheckResult]:
    """The whole gate, or only the check set of ``only`` (the one table a case changed: the
    others are the valid star, covered by the first test; one Spark action instead of nine)."""
    check_sets = [cs for cs in retail_gold_suite(gold) if only in (None, cs.table_name)]
    return run_suite(check_sets, execution_timestamp=AT)


def _failed(results: list[CheckResult], status: Status) -> set[tuple[str, str]]:
    return {(r.table_name, r.check_name) for r in results if r.status is status}


def test_a_valid_star_passes_every_check(spark: SparkSession) -> None:
    results = _results(_gold(spark))

    assert decide(results) is QualityDecision.CONTINUE
    assert {r.table_name for r in results} == set(GOLD_SCHEMAS)


def test_a_duplicated_order_line_is_critical(spark: SparkSession) -> None:
    duplicated = {"fact_sales": [_ROWS["fact_sales"][0], *_ROWS["fact_sales"]]}

    results = _results(_gold(spark, duplicated), "fact_sales")

    assert decide(results) is QualityDecision.FAIL
    assert _failed(results, Status.CRITICAL) == {("fact_sales", "grain__fact_sales")}


def test_a_sale_dated_outside_dim_date_is_critical(spark: SparkSession) -> None:
    results = _results(_gold(spark, _with("fact_sales", date_key=20991231)), "fact_sales")

    assert _failed(results, Status.CRITICAL) == {("fact_sales", "fk__date_key__dim_date")}


RELATIONSHIPS = [(table, fk) for table, fks in FOREIGN_KEYS.items() for fk in fks]


@pytest.mark.parametrize(
    ("table", "fk"), RELATIONSHIPS, ids=[f"{t}.{fk.column}" for t, fk in RELATIONSHIPS]
)
def test_every_dangling_foreign_key_is_critical(spark: SparkSession, table: str, fk: Any) -> None:
    results = _results(_gold(spark, _with(table, **{fk.column: MISSING_KEY})), table)

    assert _failed(results, Status.CRITICAL) == {(table, f"fk__{fk.column}__{fk.dimension}")}


REQUIRED = [
    (table, column)
    for table in GOLD_SCHEMAS
    for column in dict.fromkeys((*GRAIN[table], *(fk.column for fk in FOREIGN_KEYS.get(table, ()))))
]


@pytest.mark.parametrize(("table", "column"), REQUIRED, ids=[f"{t}.{c}" for t, c in REQUIRED])
def test_every_null_key_is_critical(spark: SparkSession, table: str, column: str) -> None:
    results = _results(_gold(spark, _with(table, **{column: None})), table)

    assert (table, f"not_null__{column}") in _failed(results, Status.CRITICAL)


@pytest.mark.parametrize("dimension", sorted(NATURAL_KEYS))
def test_a_member_without_natural_key_is_critical(spark: SparkSession, dimension: str) -> None:
    changed = _gold(spark, _with(dimension, **{NATURAL_KEYS[dimension]: None}))
    results = _results(changed, dimension)

    assert _failed(results, Status.CRITICAL) == {
        (dimension, f"natural_key__{NATURAL_KEYS[dimension]}")
    }


def test_the_unknown_member_is_accepted_and_reported(spark: SparkSession) -> None:
    orphan_sale = _with("fact_sales", customer_sk=UNKNOWN_KEY, supplier_sk=UNKNOWN_KEY)

    results = _results(_gold(spark, orphan_sale), "fact_sales")

    assert _failed(results, Status.CRITICAL) == set()
    # late customer: reported; unknown supplier (nullable in Silver): a normal state
    assert _failed(results, Status.WARNING) == {("fact_sales", "unknown_member__customer_sk")}
    assert decide(results) is QualityDecision.ALERT


def test_an_unknown_member_missing_from_its_dimension_is_critical(spark: SparkSession) -> None:
    gold = _gold(spark, _with("fact_sales", store_sk=UNKNOWN_KEY))
    gold["dim_store"] = gold["dim_store"].where(f"store_sk <> {UNKNOWN_KEY}")

    results = _results(gold, "fact_sales")

    assert ("fact_sales", "fk__store_sk__dim_store") in _failed(results, Status.CRITICAL)


def test_a_duplicated_surrogate_key_is_critical(spark: SparkSession) -> None:
    twice = {"dim_store": [*_ROWS["dim_store"], {"store_sk": STORE, "store_id": "S2"}]}

    results = _results(_gold(spark, twice), "dim_store")

    assert ("dim_store", "grain__dim_store") in _failed(results, Status.CRITICAL)


def test_a_changed_schema_is_critical_and_stops_the_suite(spark: SparkSession) -> None:
    gold = _gold(spark)
    gold["dim_date"] = gold["dim_date"].drop("is_black_friday")

    results = _results(gold)

    assert _failed(results, Status.CRITICAL) == {("dim_date", "schema__dim_date")}
    assert {r.table_name for r in results} == {"dim_date"}  # nothing else is evaluated


def test_a_missing_table_is_refused(spark: SparkSession) -> None:
    gold = _gold(spark)
    del gold["fact_payments"]

    with pytest.raises(ValueError, match="fact_payments"):
        retail_gold_suite(gold)
