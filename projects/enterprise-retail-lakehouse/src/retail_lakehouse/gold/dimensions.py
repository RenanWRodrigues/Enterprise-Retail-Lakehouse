"""Gold dimensions. Every dimension carries an unknown member (surrogate key -1)."""

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from retail_lakehouse.gold.keys import UNKNOWN_KEY, business_date, date_key_of, surrogate_key

UNKNOWN_LABEL: Final = "Desconhecido"
UNKNOWN_DATE_LABEL: Final = "UNKNOWN_DATE"  # year_month of date_key = -1: never a real month
FAR_FUTURE: Final = datetime(9999, 12, 31, tzinfo=UTC)
FAR_PAST: Final = datetime(1900, 1, 1, tzinfo=UTC)
CUSTOMER_TRACKED_ATTRIBUTES: Final = ("segment", "city", "state", "region")  # SCD Type 2
CUSTOMER_IDENTITY: Final = "customer_identity"  # hash scope of customer_durable_key
MONTH_NAMES: Final = (
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
)  # fmt: skip
DAY_NAMES: Final = (
    "segunda-feira", "terça-feira", "quarta-feira", "quinta-feira",
    "sexta-feira", "sábado", "domingo",
)  # fmt: skip
ADULT_AGE: Final = 18
AGE_BANDS: Final = ((25, "18-24"), (35, "25-34"), (45, "35-44"), (55, "45-54"), (65, "55-64"))
OLDEST_BAND: Final = "65+"
UNKNOWN_AGE_BAND: Final = "unknown"
NOVEMBER, FRIDAY, SATURDAY = 11, 5, 6
BLACK_FRIDAY_DAYS: Final = (23, 29)  # the Friday after the 4th Thursday falls in this range


def with_unknown_member(dim: DataFrame, overrides: Mapping[str, object]) -> DataFrame:
    """Append the unknown-member row: overrides, then ``Desconhecido`` for text, else NULL."""
    columns = []
    for field in dim.schema.fields:
        if field.name in overrides:
            value = F.lit(overrides[field.name])
        elif isinstance(field.dataType, StringType):
            value = F.lit(UNKNOWN_LABEL)
        else:
            value = F.lit(None)
        columns.append(value.cast(field.dataType).alias(field.name))
    return dim.unionByName(dim.sparkSession.range(1).select(*columns))


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per day in [start, end] (Brazilian calendar labels), plus ``date_key = -1``.

    ``year_month`` (``yyyy-MM``) is the reporting period. The unknown member has
    ``UNKNOWN_DATE``; its year and month stay NULL (not January, not month zero).
    """
    day = F.col("date")
    iso_weekday = ((F.dayofweek(day) + 5) % 7) + 1  # 1 = Monday ... 7 = Sunday
    days = spark.range((end - start).days + 1).select(
        F.date_add(F.lit(start), F.col("id").cast("int")).alias("date")
    )
    dim = days.select(
        date_key_of(day).alias("date_key"),
        day,
        F.year(day).alias("year"),
        F.quarter(day).alias("quarter"),
        F.month(day).alias("month"),
        F.date_format(day, "yyyy-MM").alias("year_month"),
        F.element_at(F.array(*map(F.lit, MONTH_NAMES)), F.month(day)).alias("month_name"),
        F.dayofmonth(day).alias("day"),
        iso_weekday.alias("day_of_week"),
        F.element_at(F.array(*map(F.lit, DAY_NAMES)), iso_weekday).alias("day_name"),
        F.weekofyear(day).alias("week_of_year"),
        (iso_weekday >= SATURDAY).alias("is_weekend"),
        (
            (F.month(day) == NOVEMBER)
            & (iso_weekday == FRIDAY)
            & F.dayofmonth(day).between(*BLACK_FRIDAY_DAYS)
        ).alias("is_black_friday"),
    )
    return with_unknown_member(dim, {"date_key": UNKNOWN_KEY, "year_month": UNKNOWN_DATE_LABEL})


def age_in_years(birth_date: Column, on: Column) -> Column:
    """Completed years on the date ``on``: this year's birthday counts only once reached.

    Calendar rule (year, then month and day), so a birthday on ``on`` counts. A 29 February
    birthday is reached on 1 March in common years. NULL when either date is NULL: an unknown
    birth date never becomes an age.
    """
    birthday_ahead = (F.month(on) < F.month(birth_date)) | (
        (F.month(on) == F.month(birth_date)) & (F.dayofmonth(on) < F.dayofmonth(birth_date))
    )
    years = F.year(on) - F.year(birth_date) - F.when(birthday_ahead, 1).otherwise(0)
    return years.cast("int")


def age_band_at(birth_date: Column, on: Column) -> Column:
    """The age band on the date ``on`` (one rule for the historized and the per-event band).

    ``unknown`` without a birth date or under 18 (invalid data for a customer).
    """
    age = age_in_years(birth_date, on)
    band = F.when(age.isNull() | (age < ADULT_AGE), F.lit(UNKNOWN_AGE_BAND))
    for upper, label in AGE_BANDS:
        band = band.when(age < upper, F.lit(label))
    return band.otherwise(F.lit(OLDEST_BAND))


def customer_birth_dates(customers: DataFrame) -> DataFrame:
    """One ``(customer_id, birth_date)`` per customer from the Silver versions: the birth date of
    the latest version that has one. A birth date is a fact about the person, not about a
    version: a correction applies to every event. The Gold never stores it (PII); facts use it
    only to derive the band at the event."""
    latest_first = Window.partitionBy("customer_id").orderBy(F.col("effective_from").desc())
    known = customers.where(F.col("birth_date").isNotNull())
    return (
        known.withColumn("_rank", F.row_number().over(latest_first))
        .where(F.col("_rank") == 1)
        .select("customer_id", "birth_date")
    )


def build_dim_customer(customers: DataFrame) -> DataFrame:
    """SCD Type 2 from Silver customer versions (rebuilt in full, deterministic).

    A new version opens only when a tracked attribute changes; ``valid_to`` is the next
    version's ``valid_from`` (half-open interval) or 9999-12-31 for the current one.
    ``customer_durable_key`` (hash of ``customer_id`` only) is the same on every version: facts
    keep the customer through it even when no version is valid at the event (ADR 0003).
    """
    by_time = Window.partitionBy("customer_id").orderBy("effective_from")
    is_first = F.lag("effective_from").over(by_time).isNull()
    changed = F.lit(False)
    for attribute in CUSTOMER_TRACKED_ATTRIBUTES:
        changed = changed | ~F.col(attribute).eqNullSafe(F.lag(attribute).over(by_time))
    versions = (  # window functions cannot be used directly in WHERE
        customers.withColumn("_opens_version", is_first | changed)
        .where("_opens_version")
        .drop("_opens_version")
    )
    next_start = F.lead("effective_from").over(by_time)
    dim = versions.select(
        surrogate_key("customer", F.col("customer_id"), F.col("effective_from")).alias(
            "customer_sk"
        ),
        surrogate_key(CUSTOMER_IDENTITY, F.col("customer_id")).alias("customer_durable_key"),
        "customer_id",
        *CUSTOMER_TRACKED_ATTRIBUTES,
        age_band_at(F.col("birth_date"), business_date(F.col("effective_from"))).alias("age_band"),
        business_date(F.col("registered_at")).alias("registered_date"),
        F.col("effective_from").alias("valid_from"),
        F.coalesce(next_start, F.lit(FAR_FUTURE)).alias("valid_to"),
        next_start.isNull().alias("is_current"),
    )
    return with_unknown_member(
        dim,
        {
            "customer_sk": UNKNOWN_KEY,
            "customer_durable_key": UNKNOWN_KEY,
            "customer_id": None,
            "age_band": UNKNOWN_AGE_BAND,
            "valid_from": FAR_PAST,
            "valid_to": FAR_FUTURE,
            "is_current": True,
        },
    )


def build_dim_supplier(suppliers: DataFrame) -> DataFrame:
    """SCD Type 1."""
    dim = suppliers.select(
        surrogate_key("supplier", F.col("supplier_id")).alias("supplier_sk"),
        "supplier_id",
        "supplier_name",
        "city",
        "state",
        "lead_time_days",
    )
    return with_unknown_member(dim, {"supplier_sk": UNKNOWN_KEY, "supplier_id": None})


def build_dim_store(stores: DataFrame) -> DataFrame:
    """SCD Type 1."""
    dim = stores.select(
        surrogate_key("store", F.col("store_id")).alias("store_sk"),
        "store_id",
        "store_name",
        "store_type",
        "city",
        "state",
        "region",
        F.col("opened_at").alias("opened_date"),
    )
    return with_unknown_member(dim, {"store_sk": UNKNOWN_KEY, "store_id": None})


def build_dim_product(
    products: DataFrame, categories: DataFrame, dim_supplier: DataFrame
) -> DataFrame:
    """SCD Type 1, with category (leaf), department (parent) and supplier key."""
    leaf = categories.select("category_id", "category_name", "parent_category_id")
    department = categories.select(
        F.col("category_id").alias("parent_category_id"),
        F.col("category_name").alias("department_name"),
    )
    suppliers = dim_supplier.where(F.col("supplier_sk") != UNKNOWN_KEY).select(
        "supplier_id", "supplier_sk"
    )
    enriched = (
        products.join(leaf, "category_id", "left")  # N:1
        .join(department, "parent_category_id", "left")  # N:1
        .join(suppliers, "supplier_id", "left")  # N:1
    )
    dim = enriched.select(
        surrogate_key("product", F.col("product_id")).alias("product_sk"),
        "product_id",
        "product_name",
        "brand",
        "category_id",
        "category_name",
        "department_name",
        F.coalesce(F.col("supplier_sk"), F.lit(UNKNOWN_KEY)).alias("supplier_sk"),
        "supplier_id",
        "list_price",
        "cost_price",
        "is_active",
    )
    return with_unknown_member(
        dim, {"product_sk": UNKNOWN_KEY, "product_id": None, "supplier_sk": UNKNOWN_KEY}
    )
