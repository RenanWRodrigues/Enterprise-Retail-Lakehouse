"""SCD Type 2 through the real Silver and Gold functions, fed by a full and an incremental batch.

The checks run in Spark and return counts: current versions end on 9999-12-31, which PySpark
cannot turn into a Python datetime on Windows.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from retail_lakehouse.gold.dimensions import build_dim_customer, customer_birth_dates
from retail_lakehouse.gold.facts import build_fact_sales
from retail_lakehouse.silver.reference import STATE_REGIONS
from retail_lakehouse.silver.schemas import silver_table_schema
from retail_lakehouse.silver.standardize import customer_versions
from retail_lakehouse.synthetic import GenerationConfig, MemorySink
from retail_lakehouse.synthetic.incremental import (
    BatchPlan,
    IncrementalPlan,
    base_state,
    generate_incremental_batch,
)

BASE = GenerationConfig(
    seed=7, start_date=date(2024, 1, 1), end_date=date(2024, 3, 31),
    customers=200, products=60, stores=4, suppliers=10, orders=150,
)  # fmt: skip
PLAN = IncrementalPlan(BASE, (BatchPlan("b2", date(2024, 4, 10), customer_change_rate=0.05),))
FULL_AT = datetime(2024, 4, 1, 6, tzinfo=UTC)
INCREMENT_AT = datetime(2024, 4, 11, 6, tzinfo=UTC)  # the batch is ingested after its window
SCHEMA = silver_table_schema("customers")
OBSERVED = StructType([f for f in SCHEMA.fields if f.name != "effective_from"])  # set by Silver


def _observed(
    spark: SparkSession, rows: list[dict[str, Any]], at: datetime, batch: str
) -> DataFrame:
    """CRM rows as the Silver types them, with the batch lineage (no version start yet)."""
    typed = [
        (r["customer_id"], r["city"], r["state"], STATE_REGIONS[r["state"]], r["segment"],
         date.fromisoformat(r["birth_date"]), datetime.fromisoformat(r["registered_at"]),
         at, f"{batch}.csv", batch)
        for r in rows
    ]  # fmt: skip
    return spark.createDataFrame(typed, OBSERVED)


@pytest.fixture(scope="module")
def batches(spark: SparkSession) -> dict[str, Any]:
    full_rows = list(base_state(BASE).customers.values())
    sink = MemorySink()
    summary = generate_incremental_batch(PLAN, "b2", sink)
    empty = spark.createDataFrame([], SCHEMA)
    first = customer_versions(_observed(spark, full_rows, FULL_AT, "initial"), empty)
    second = customer_versions(_observed(spark, sink.rows["customers"], INCREMENT_AT, "b2"), first)
    stored = first.unionByName(second)
    replayed = customer_versions(
        _observed(spark, sink.rows["customers"], INCREMENT_AT, "b2"), stored
    )
    changed = sorted({c.entity_id for c in summary.changes})
    return {"stored": stored, "replayed": replayed, "changed": changed,
            "dim": build_dim_customer(stored), "customers": len(full_rows)}  # fmt: skip


def test_each_changed_customer_gets_exactly_one_new_version(batches: dict[str, Any]) -> None:
    versions = {
        r.customer_id: r["count"]
        for r in batches["stored"].groupBy("customer_id").count().collect()
    }

    assert len(versions) == batches["customers"]
    assert sorted(c for c, n in versions.items() if n == 2) == batches["changed"]
    assert set(versions.values()) == {1, 2}


def test_reprocessing_the_same_batch_adds_no_version(batches: dict[str, Any]) -> None:
    assert batches["replayed"].count() == 0


def test_one_current_version_per_customer(batches: dict[str, Any]) -> None:
    dim = batches["dim"].where(F.col("customer_sk") != -1)
    currents = dim.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("n"))

    assert {r.n for r in currents.collect()} == {1}


def test_the_previous_version_closes_where_the_new_one_opens(batches: dict[str, Any]) -> None:
    dim = batches["dim"].where(F.col("customer_sk") != -1)
    by_start = F.lead("valid_from").over(Window.partitionBy("customer_id").orderBy("valid_from"))
    gaps = dim.withColumn("_next", by_start).where(
        F.col("_next").isNotNull() & (F.col("valid_to") != F.col("_next"))
    )
    reopened = dim.where(F.col("customer_id").isin(batches["changed"]) & F.col("is_current")).where(
        F.col("valid_from") != F.lit(INCREMENT_AT)
    )

    assert gaps.count() == 0  # closed exactly at the next version's start
    assert reopened.count() == 0  # the new version opens at the batch that revealed it


def test_version_intervals_never_overlap(batches: dict[str, Any]) -> None:
    dim = batches["dim"].where(F.col("customer_sk") != -1)
    a, b = dim.alias("a"), dim.alias("b")
    overlaps = a.join(
        b,
        (F.col("a.customer_id") == F.col("b.customer_id"))
        & (F.col("a.customer_sk") < F.col("b.customer_sk"))
        & (F.col("a.valid_from") < F.col("b.valid_to"))
        & (F.col("b.valid_from") < F.col("a.valid_to")),
    )

    assert overlaps.count() == 0


def test_sales_take_the_version_valid_at_their_business_time(
    spark: SparkSession, batches: dict[str, Any]
) -> None:
    customer = batches["changed"][0]
    before, after = datetime(2024, 4, 5, 15, tzinfo=UTC), datetime(2024, 4, 20, 15, tzinfo=UTC)
    orders = spark.createDataFrame(
        [("O-BEFORE", customer, "S1", "store", "delivered", before),
         ("O-AFTER", customer, "S1", "store", "delivered", after)],
        "order_id string, customer_id string, store_id string, channel string, "
        "order_status string, order_timestamp timestamp",
    )  # fmt: skip
    items = spark.createDataFrame(
        [(o, 1, "P1", 1, Decimal("10.00"), Decimal("0.00")) for o in ("O-BEFORE", "O-AFTER")],
        "order_id string, line_number int, product_id string, quantity int, "
        "unit_price decimal(12,2), discount_amount decimal(12,2)",
    )
    products = spark.createDataFrame(
        [(1, "P1", 2)], "product_sk long, product_id string, supplier_sk long"
    )
    stores = spark.createDataFrame([(3, "S1")], "store_sk long, store_id string")

    births = customer_birth_dates(batches["stored"])
    sales = build_fact_sales(orders, items, batches["dim"], products, stores, birth_dates=births)

    dim = batches["dim"].select("customer_sk", "valid_from", "valid_to")
    joined = sales.join(orders.select("order_id", "order_timestamp"), "order_id").join(
        dim, "customer_sk"
    )
    wrong = joined.where(
        ~(
            (F.col("valid_from") <= F.col("order_timestamp"))
            & (F.col("order_timestamp") < F.col("valid_to"))
        )
    )
    sks = {r.order_id: r.customer_sk for r in sales.select("order_id", "customer_sk").collect()}
    assert wrong.count() == 0 and joined.count() == 2
    assert sks["O-BEFORE"] != sks["O-AFTER"]  # the change splits the customer's history
