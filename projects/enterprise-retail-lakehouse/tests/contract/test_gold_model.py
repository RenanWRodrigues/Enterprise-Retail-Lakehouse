"""Gold model contract: schemas, grain, uniqueness, surrogate keys, referential integrity."""

import operator
from datetime import date
from functools import reduce

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from retail_lakehouse.gold import GOLD_SCHEMAS, GRAIN, UNKNOWN_KEY, build_gold

DIMENSIONS = ("dim_customer", "dim_product", "dim_store", "dim_supplier")
FACT_SOURCES = {
    "fact_sales": "order_items",
    "fact_payments": "payments",
    "fact_shipments": "shipments",
    "fact_inventory_movements": "inventory_movements",
}
FOREIGN_KEYS = [
    ("fact_sales", "date_key", "dim_date", "date_key"),
    ("fact_sales", "customer_sk", "dim_customer", "customer_sk"),
    ("fact_sales", "product_sk", "dim_product", "product_sk"),
    ("fact_sales", "store_sk", "dim_store", "store_sk"),
    ("fact_sales", "supplier_sk", "dim_supplier", "supplier_sk"),
    ("fact_payments", "date_key", "dim_date", "date_key"),
    ("fact_payments", "customer_sk", "dim_customer", "customer_sk"),
    ("fact_payments", "store_sk", "dim_store", "store_sk"),
    ("fact_shipments", "ship_date_key", "dim_date", "date_key"),
    ("fact_shipments", "delivery_date_key", "dim_date", "date_key"),
    ("fact_shipments", "customer_sk", "dim_customer", "customer_sk"),
    ("fact_shipments", "store_sk", "dim_store", "store_sk"),
    ("fact_inventory_movements", "date_key", "dim_date", "date_key"),
    ("fact_inventory_movements", "store_sk", "dim_store", "store_sk"),
    ("fact_inventory_movements", "product_sk", "dim_product", "product_sk"),
    ("fact_inventory_movements", "supplier_sk", "dim_supplier", "supplier_sk"),
    ("dim_product", "supplier_sk", "dim_supplier", "supplier_sk"),
]
SK_COLUMNS = {
    "dim_customer": "customer_sk",
    "dim_product": "product_sk",
    "dim_store": "store_sk",
    "dim_supplier": "supplier_sk",
    "dim_date": "date_key",
}


@pytest.mark.parametrize("table", sorted(GOLD_SCHEMAS))
def test_schema_matches_the_gold_contract(gold_tables: dict[str, DataFrame], table: str) -> None:
    actual = [(f.name, f.dataType) for f in gold_tables[table].schema]
    assert actual == [(f.name, f.dataType) for f in GOLD_SCHEMAS[table]]


@pytest.mark.parametrize("table", sorted(GRAIN))
def test_grain_is_unique(gold_tables: dict[str, DataFrame], table: str) -> None:
    columns = list(GRAIN[table])
    [row] = (
        gold_tables[table]
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.count_distinct(*columns).alias("distinct_grain"),
            # Regression: F.greatest needs >= 2 columns, so every one-column grain errored on Linux
            # (the test is skipped on Windows, and CI had not run yet).
            F.sum(
                F.when(reduce(operator.or_, [F.col(c).isNull() for c in columns]), 1).otherwise(0)
            ).alias("null_grain"),
        )
        .collect()
    )
    assert row["rows"] == row["distinct_grain"]
    assert row["null_grain"] == 0


@pytest.mark.parametrize(("fact", "source"), sorted(FACT_SOURCES.items()))
def test_facts_keep_exactly_one_row_per_source_row(
    gold_tables: dict[str, DataFrame], silver_tables: dict[str, DataFrame], fact: str, source: str
) -> None:
    """No join may multiply (double count) or drop rows."""
    assert gold_tables[fact].count() == silver_tables[source].count()


@pytest.mark.parametrize("dimension", sorted(SK_COLUMNS))
def test_each_dimension_has_exactly_one_unknown_member(
    gold_tables: dict[str, DataFrame], dimension: str
) -> None:
    sk = SK_COLUMNS[dimension]
    assert gold_tables[dimension].where(F.col(sk) == UNKNOWN_KEY).count() == 1


@pytest.mark.parametrize("dimension", DIMENSIONS)
def test_natural_keys_are_unique_among_current_rows(
    gold_tables: dict[str, DataFrame], dimension: str
) -> None:
    natural_key = SK_COLUMNS[dimension].replace("_sk", "_id")
    current = gold_tables[dimension].where(F.col(SK_COLUMNS[dimension]) != UNKNOWN_KEY)
    if dimension == "dim_customer":
        current = current.where("is_current")
    duplicated = current.groupBy(natural_key).count().where("count > 1")
    assert duplicated.isEmpty()


def test_surrogate_keys_are_stable_across_rebuilds(
    silver_tables: dict[str, DataFrame],
    gold_tables: dict[str, DataFrame],
    gold_calendar: tuple[date, date],
) -> None:
    start, end = gold_calendar
    rebuilt = build_gold(silver_tables, calendar_start=start, calendar_end=end)
    for dimension in DIMENSIONS:
        sk = SK_COLUMNS[dimension]
        natural_key = sk.replace("_sk", "_id")
        assertDataFrameEqual(
            rebuilt[dimension].select(sk, natural_key),
            gold_tables[dimension].select(sk, natural_key),
        )


@pytest.mark.parametrize(("fact", "fk", "dimension", "key"), FOREIGN_KEYS, ids=str)
def test_referential_integrity(
    gold_tables: dict[str, DataFrame], fact: str, fk: str, dimension: str, key: str
) -> None:
    orphans = gold_tables[fact].join(
        gold_tables[dimension].select(F.col(key).alias(fk)), fk, "left_anti"
    )
    assert orphans.isEmpty(), f"{fact}.{fk} has values missing from {dimension}.{key}"


def test_clean_sources_never_fall_back_to_the_unknown_member(
    gold_tables: dict[str, DataFrame],
) -> None:
    for fact, fk, _, _ in FOREIGN_KEYS:
        if fk == "delivery_date_key":  # in-transit shipments legitimately have no delivery
            continue
        rows = gold_tables[fact]
        if fact in SK_COLUMNS:  # a dimension's own unknown member points to unknown by design
            rows = rows.where(F.col(SK_COLUMNS[fact]) != UNKNOWN_KEY)
        assert rows.where(F.col(fk) == UNKNOWN_KEY).isEmpty(), f"{fact}.{fk}"


def test_in_transit_shipments_point_to_the_unknown_delivery_date(
    gold_tables: dict[str, DataFrame],
) -> None:
    shipments = gold_tables["fact_shipments"]
    unknown = shipments.where(F.col("delivery_date_key") == UNKNOWN_KEY)
    assert unknown.where(F.col("shipment_status") != "in_transit").isEmpty()


def test_customer_versions_never_overlap_and_one_is_current(
    gold_tables: dict[str, DataFrame],
) -> None:
    dim = gold_tables["dim_customer"].where(F.col("customer_sk") != UNKNOWN_KEY)
    a, b = dim.alias("a"), dim.alias("b")
    overlaps = a.join(
        b,
        (F.col("a.customer_id") == F.col("b.customer_id"))
        & (F.col("a.customer_sk") != F.col("b.customer_sk"))
        & (F.col("a.valid_from") < F.col("b.valid_to"))
        & (F.col("b.valid_from") < F.col("a.valid_to")),
    )
    assert overlaps.isEmpty()
    assert (
        dim.groupBy("customer_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
        .where("n != 1")
        .isEmpty()
    )


def test_dim_date_is_a_contiguous_calendar(
    gold_tables: dict[str, DataFrame], gold_calendar: tuple[date, date]
) -> None:
    calendar_start, calendar_end = gold_calendar
    days = gold_tables["dim_date"].where(F.col("date_key") != UNKNOWN_KEY)
    [row] = days.agg(F.min("date"), F.max("date"), F.count(F.lit(1))).collect()
    assert (row[0], row[1]) == (calendar_start, calendar_end)
    assert row[2] == (calendar_end - calendar_start).days + 1


def test_black_friday_flags(gold_tables: dict[str, DataFrame]) -> None:
    flagged = {r["date"] for r in gold_tables["dim_date"].where("is_black_friday").collect()}
    assert flagged == {date(2024, 11, 29), date(2025, 11, 28)}
