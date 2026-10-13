"""Surrogate keys, business dates, SCD Type 2 lookups and the SCD2 double-counting trap."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_lakehouse.gold import (
    UNKNOWN_KEY,
    build_dim_customer,
    build_dim_product,
    build_dim_store,
    build_dim_supplier,
    build_fact_sales,
    business_date_key,
    surrogate_key,
)
from retail_lakehouse.gold.dimensions import customer_birth_dates
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

T1 = datetime(2024, 1, 1, tzinfo=UTC)
T2 = datetime(2024, 6, 1, tzinfo=UTC)  # C1 becomes premium and moves to Recife


def _df(spark: SparkSession, entity: str, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_SCHEMAS[entity])


def _customers(spark: SparkSession) -> DataFrame:
    return _df(
        spark,
        "customers",
        [
            ("C1", "Natal", "RN", "Nordeste", "standard", date(1990, 5, 1), T1, T1),
            (
                "C1",
                "Natal",
                "RN",
                "Nordeste",
                "standard",
                date(1990, 5, 1),
                T1,
                datetime(2024, 3, 1, tzinfo=UTC),
            ),  # unchanged snapshot
            ("C1", "Recife", "PE", "Nordeste", "premium", date(1990, 5, 1), T1, T2),
            ("C2", "Curitiba", "PR", "Sul", "standard", None, T1, T1),
        ],
    )


def test_business_date_key_uses_brasilia_local_date(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            (datetime(2024, 3, 5, 2, 30, tzinfo=UTC),),
            (datetime(2024, 3, 5, 3, 0, tzinfo=UTC),),
            (None,),
        ],
        "ts timestamp",
    )
    keys = [r["k"] for r in events.select(business_date_key(F.col("ts")).alias("k")).collect()]
    assert keys == [20240304, 20240305, None]  # 02:30 UTC = 23:30 of the previous day in Brasília


def test_surrogate_keys_are_deterministic_and_scoped_by_dimension(spark: SparkSession) -> None:
    row = (
        spark.createDataFrame([("X1",)], "natural_key string")
        .select(
            surrogate_key("customer", F.col("natural_key")).alias("a"),
            surrogate_key("customer", F.col("natural_key")).alias("b"),
            surrogate_key("product", F.col("natural_key")).alias("c"),
        )
        .first()
    )
    assert row is not None
    assert row["a"] == row["b"] != row["c"]
    assert UNKNOWN_KEY not in (row["a"], row["c"])


def test_scd2_opens_a_version_only_when_tracked_attributes_change(spark: SparkSession) -> None:
    dim = build_dim_customer(_customers(spark)).where(F.col("customer_id") == "C1")

    # valid_to is compared in Spark: the open end (9999-12-31) cannot become a Python datetime
    # on Windows (OSError 22 from the local-time C API).
    versions = (
        dim.orderBy("valid_from")
        .select(
            "segment",
            "city",
            "is_current",
            "customer_sk",
            (F.lead("valid_from").over(Window.orderBy("valid_from")) == F.col("valid_to")).alias(
                "contiguous"
            ),
            F.year("valid_to").alias("valid_to_year"),
        )
        .collect()
    )

    assert [(v["segment"], v["city"], v["is_current"]) for v in versions] == [
        ("standard", "Natal", False),
        ("premium", "Recife", True),
    ]
    assert versions[0]["contiguous"] is True  # contiguous, no overlap
    assert versions[0]["customer_sk"] != versions[1]["customer_sk"]
    assert versions[1]["valid_to_year"] == 9999


@pytest.mark.parametrize(
    ("birth", "expected"),
    [
        (date(1999, 6, 2), "18-24"),
        (date(1999, 6, 1), "25-34"),
        (date(1958, 1, 1), "65+"),
        (None, "unknown"),
    ],
)
def test_age_band_is_computed_at_the_start_of_the_version(
    spark: SparkSession, birth: date | None, expected: str
) -> None:
    start = datetime(2024, 6, 1, 12, tzinfo=UTC)
    history = _df(
        spark, "customers", [("C9", "Natal", "RN", "Nordeste", "standard", birth, start, start)]
    )

    [row] = (
        build_dim_customer(history).where(F.col("customer_id") == "C9").select("age_band").collect()
    )

    assert row["age_band"] == expected


def _sales_inputs(spark: SparkSession) -> dict[str, DataFrame]:
    customers = build_dim_customer(_customers(spark))
    suppliers = build_dim_supplier(_df(spark, "suppliers", [("S1", "Sup", "Natal", "RN", 5)]))
    products = build_dim_product(
        _df(
            spark,
            "products",
            [("P1", "Prod", "CAT-2", "S1", "Brand", Decimal("10.00"), Decimal("5.00"), True)],
        ),
        _df(spark, "categories", [("CAT-1", "Dept", None), ("CAT-2", "Leaf", "CAT-1")]),
        suppliers,
    )
    stores = build_dim_store(
        _df(
            spark,
            "stores",
            [("ST1", "Loja", "physical", "Natal", "RN", "Nordeste", date(2020, 1, 1))],
        )
    )
    orders = _df(
        spark,
        "orders",
        [
            (
                "O1",
                "C1",
                "ST1",
                "store",
                "delivered",
                datetime(2024, 2, 1, tzinfo=UTC),
                Decimal("0.00"),
            ),
            (
                "O2",
                "C1",
                "ST1",
                "store",
                "delivered",
                datetime(2024, 7, 1, tzinfo=UTC),
                Decimal("0.00"),
            ),
            (
                "O3",
                "C404",
                "ST1",
                "store",
                "delivered",
                datetime(2024, 7, 1, tzinfo=UTC),
                Decimal("0.00"),
            ),
        ],
    )
    items = _df(
        spark,
        "order_items",
        [
            ("O1", 1, "P1", 1, Decimal("10.00"), Decimal("0.00")),
            ("O2", 1, "P1", 2, Decimal("10.00"), Decimal("0.00")),
            ("O3", 1, "P1", 1, Decimal("10.00"), Decimal("0.00")),
        ],
    )
    return {
        "orders": orders,
        "items": items,
        "customers": customers,
        "products": products,
        "stores": stores,
    }


def test_facts_use_the_customer_version_valid_at_the_event(spark: SparkSession) -> None:
    inputs = _sales_inputs(spark)
    sales = build_fact_sales(
        inputs["orders"],
        inputs["items"],
        inputs["customers"],
        inputs["products"],
        inputs["stores"],
        birth_dates=customer_birth_dates(_customers(spark)),
    )
    versions = {
        r["segment"]: r["customer_sk"]
        for r in inputs["customers"]
        .where(F.col("customer_id") == "C1")
        .select("segment", "customer_sk")
        .collect()
    }

    keys = {
        r["order_id"]: r["customer_sk"] for r in sales.select("order_id", "customer_sk").collect()
    }

    assert keys == {"O1": versions["standard"], "O2": versions["premium"], "O3": UNKNOWN_KEY}


BEFORE_REGISTRATION = datetime(2023, 12, 1, tzinfo=UTC)  # C1 registers at T1
EVENT_TIMES = {
    "O-BEFORE": ("C1", BEFORE_REGISTRATION),
    "O-AT-START": ("C1", T1),  # exactly valid_from of the first version
    "O-FIRST": ("C1", datetime(2024, 2, 1, tzinfo=UTC)),
    "O-SECOND": ("C1", datetime(2024, 7, 1, tzinfo=UTC)),  # after the change at T2
    "O-UNKNOWN": ("C404", datetime(2024, 7, 1, tzinfo=UTC)),  # customer not in Silver
}


def _sales_at_event_times(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """fact_sales for one line per EVENT_TIMES order, and the dim_customer it was built with."""
    inputs = _sales_inputs(spark)
    orders = _df(
        spark,
        "orders",
        [
            (order_id, customer, "ST1", "store", "delivered", at, Decimal("0.00"))
            for order_id, (customer, at) in EVENT_TIMES.items()
        ],
    )
    items = _df(
        spark,
        "order_items",
        [(order_id, 1, "P1", 1, Decimal("10.00"), Decimal("0.00")) for order_id in EVENT_TIMES],
    )
    sales = build_fact_sales(
        orders,
        items,
        inputs["customers"],
        inputs["products"],
        inputs["stores"],
        birth_dates=customer_birth_dates(_customers(spark)),
    )
    return sales, inputs["customers"]


def test_event_before_the_first_version_keeps_the_customer_identity(spark: SparkSession) -> None:
    """Regression: an order placed before registration lost its customer (customer_sk = -1, no
    other key). The version stays unknown, since no attributes are known for that instant, but
    the durable key still identifies the customer."""
    sales, dim = _sales_at_event_times(spark)
    durable = {
        r["customer_id"]: r["customer_durable_key"]
        for r in dim.select("customer_id", "customer_durable_key").distinct().collect()
    }

    keys = {
        r["order_id"]: r["customer_durable_key"]
        for r in sales.select("order_id", "customer_durable_key").collect()
    }

    assert keys == {
        "O-BEFORE": durable["C1"],
        "O-AT-START": durable["C1"],
        "O-FIRST": durable["C1"],
        "O-SECOND": durable["C1"],
        "O-UNKNOWN": UNKNOWN_KEY,
    }


def test_customer_version_is_point_in_time_and_never_a_later_one(spark: SparkSession) -> None:
    sales, dim = _sales_at_event_times(spark)
    versions = {
        r["segment"]: r["customer_sk"]
        for r in dim.where(F.col("customer_id") == "C1").select("segment", "customer_sk").collect()
    }

    keys = {
        r["order_id"]: r["customer_sk"] for r in sales.select("order_id", "customer_sk").collect()
    }

    assert keys == {
        "O-BEFORE": UNKNOWN_KEY,  # no version valid yet: the first one is not stretched back
        "O-AT-START": versions["standard"],  # valid_from is inclusive
        "O-FIRST": versions["standard"],
        "O-SECOND": versions["premium"],
        "O-UNKNOWN": UNKNOWN_KEY,
    }


def test_durable_key_is_shared_by_versions_and_unique_per_customer(spark: SparkSession) -> None:
    dim = build_dim_customer(_customers(spark))

    rows = dim.select("customer_id", "customer_sk", "customer_durable_key").collect()
    by_customer: dict[str | None, set[int]] = {}
    for row in rows:
        by_customer.setdefault(row["customer_id"], set()).add(row["customer_durable_key"])

    assert len(rows) == 4  # C1: 2 versions, C2: 1, unknown member
    assert len({r["customer_sk"] for r in rows}) == len(rows)  # surrogate keys stay unique
    assert all(len(keys) == 1 for keys in by_customer.values())
    assert by_customer[None] == {UNKNOWN_KEY}
    assert len({next(iter(keys)) for keys in by_customer.values()}) == len(by_customer)


def test_joining_by_natural_key_would_double_count_scd2_customers(spark: SparkSession) -> None:
    """The trap BUSINESS_RULES.md warns about: always join facts to dim_customer by sk."""
    inputs = _sales_inputs(spark)
    sales = build_fact_sales(
        inputs["orders"],
        inputs["items"],
        inputs["customers"],
        inputs["products"],
        inputs["stores"],
        birth_dates=customer_birth_dates(_customers(spark)),
    )
    dim = inputs["customers"]
    total = sales.agg(F.sum("net_amount")).first()[0]  # type: ignore[index]

    by_sk = sales.join(dim, "customer_sk", "left").agg(F.sum("net_amount")).first()[0]  # type: ignore[index]
    by_natural_key = (
        sales.join(dim.select("customer_sk", "customer_id"), "customer_sk")
        .select("net_amount", "customer_id")
        .join(dim.select("customer_id", "segment"), "customer_id")  # N:M with SCD2 versions
        .agg(F.sum("net_amount"))
        .first()[0]  # type: ignore[index]
    )

    assert by_sk == total == Decimal("40.00")
    assert by_natural_key > total
