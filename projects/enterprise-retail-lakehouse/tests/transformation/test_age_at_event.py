"""Age band at the event date (fact_sales.customer_age_band) vs the historized dim attribute.

dim_customer.age_band is historized: the band at the start of the version. The analytical band
is derived per order: the customer's age on the order's business date. Same age rule and bands.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.gold import (
    UNKNOWN_KEY,
    build_dim_customer,
    build_dim_product,
    build_dim_store,
    build_dim_supplier,
    build_fact_sales,
)
from retail_lakehouse.gold.dimensions import age_band_at, age_in_years, customer_birth_dates
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

REGISTERED = datetime(2010, 1, 10, tzinfo=UTC)
MOVED = datetime(2015, 5, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("birth", "on", "age"),
    [
        (date(1990, 3, 1), date(2024, 6, 10), 34),  # birthday already passed this year
        (date(1990, 9, 1), date(2024, 6, 10), 33),  # birthday still to come
        (date(1990, 6, 10), date(2024, 6, 10), 34),  # on the birthday
        (date(2000, 2, 29), date(2018, 2, 28), 17),  # leap-day birth: birthday is March 1st
        (date(2000, 2, 29), date(2018, 3, 1), 18),
        (date(2000, 2, 29), date(2024, 2, 29), 24),  # a leap year has the real birthday
        (None, date(2024, 6, 10), None),  # unknown birth date: no invented age
    ],
)
def test_age_respects_month_and_day(
    spark: SparkSession, birth: date | None, on: date, age: int | None
) -> None:
    frame = spark.createDataFrame([(birth, on)], "birth date, on date")

    [row] = frame.select(age_in_years(F.col("birth"), F.col("on")).alias("age")).collect()

    assert row["age"] == age


@pytest.mark.parametrize(
    ("birth", "band"),
    [
        (date(2006, 6, 11), "unknown"),
        (date(2006, 6, 10), "18-24"),
        (date(1959, 6, 10), "65+"),
        (None, "unknown"),
    ],
)
def test_one_band_rule(spark: SparkSession, birth: date | None, band: str) -> None:
    frame = spark.createDataFrame([(birth, date(2024, 6, 10))], "birth date, on date")

    [row] = frame.select(age_band_at(F.col("birth"), F.col("on")).alias("band")).collect()

    assert row["band"] == band  # under 18 is invalid data: unknown, as in dim_customer


def _df(spark: SparkSession, entity: str, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_SCHEMAS[entity])


def _sales(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    customers = _df(
        spark,
        "customers",
        [
            # C1 born 1990-06-10, two versions (moved city in 2015)
            (
                "C1",
                "Natal",
                "RN",
                "Nordeste",
                "standard",
                date(1990, 6, 10),
                REGISTERED,
                REGISTERED,
            ),
            ("C1", "Recife", "PE", "Nordeste", "standard", date(1990, 6, 10), REGISTERED, MOVED),
            ("C2", "Natal", "RN", "Nordeste", "standard", None, REGISTERED, REGISTERED),  # no birth
        ],
    )
    dim_customer = build_dim_customer(customers)
    suppliers = build_dim_supplier(_df(spark, "suppliers", [("S1", "Sup", "Natal", "RN", 5)]))
    products = build_dim_product(
        _df(
            spark,
            "products",
            [("P1", "Prod", "CAT-1", "S1", "B", Decimal("10.00"), Decimal("5.00"), True)],
        ),
        _df(spark, "categories", [("CAT-1", "Leaf", None)]),
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
                "O-2009",
                "C1",
                "ST1",
                "store",
                "delivered",
                datetime(2009, 6, 9, 15, tzinfo=UTC),
                Decimal("0"),
            ),  # before registration, age 18
            (
                "O-2012",
                "C1",
                "ST1",
                "store",
                "delivered",
                datetime(2012, 3, 1, 15, tzinfo=UTC),
                Decimal("0"),
            ),  # first version, age 21
            (
                "O-2024",
                "C1",
                "ST1",
                "store",
                "delivered",
                datetime(2024, 6, 10, 15, tzinfo=UTC),
                Decimal("0"),
            ),  # second version, birthday: 34
            (
                "O-NOBIRTH",
                "C2",
                "ST1",
                "store",
                "delivered",
                datetime(2024, 6, 10, 15, tzinfo=UTC),
                Decimal("0"),
            ),
        ],
    )
    items = _df(
        spark,
        "order_items",
        [
            (o, 1, "P1", 1, Decimal("10.00"), Decimal("0.00"))
            for o in ("O-2009", "O-2012", "O-2024", "O-NOBIRTH")
        ],
    )
    sales = build_fact_sales(
        orders, items, dim_customer, products, stores, birth_dates=customer_birth_dates(customers)
    )
    return sales, dim_customer


def test_the_band_is_the_age_on_the_order_date_not_at_the_version_start(
    spark: SparkSession,
) -> None:
    sales, dim = _sales(spark)

    bands = {
        r["order_id"]: r["customer_age_band"]
        for r in sales.select("order_id", "customer_age_band").collect()
    }
    historized = {
        r["valid_from_year"]: r["age_band"]
        for r in dim.where(F.col("customer_id") == "C1")
        .select(F.year("valid_from").alias("valid_from_year"), "age_band")
        .collect()
    }

    assert bands == {
        "O-2009": "18-24",
        "O-2012": "18-24",
        "O-2024": "25-34",
        "O-NOBIRTH": "unknown",
    }
    # the moving city opens a second version; both started when C1 was 19 and 24: frozen 18-24,
    # while the 2024 order (age 34, in the 2015 version) is analysed as 25-34
    assert historized == {2010: "18-24", 2015: "18-24"}


def test_no_row_is_multiplied_by_scd2_versions(spark: SparkSession) -> None:
    sales, _ = _sales(spark)

    assert sales.count() == 4
    assert sales.select("order_id").distinct().count() == 4


def test_an_order_before_registration_still_gets_an_age(spark: SparkSession) -> None:
    sales, _ = _sales(spark)

    [row] = (
        sales.where(F.col("order_id") == "O-2009")
        .select("customer_sk", "customer_age_band")
        .collect()
    )

    assert (row["customer_sk"], row["customer_age_band"]) == (UNKNOWN_KEY, "18-24")


def test_the_latest_known_birth_date_is_used(spark: SparkSession) -> None:
    """A birth date corrected later applies to every order: it is a fact about the person."""
    customers = _df(
        spark,
        "customers",
        [
            ("C9", "Natal", "RN", "Nordeste", "standard", date(1990, 1, 1), REGISTERED, REGISTERED),
            ("C9", "Natal", "RN", "Nordeste", "standard", date(1980, 1, 1), REGISTERED, MOVED),
        ],
    )

    rows = customer_birth_dates(customers).collect()

    assert [(r["customer_id"], r["birth_date"]) for r in rows] == [("C9", date(1980, 1, 1))]
