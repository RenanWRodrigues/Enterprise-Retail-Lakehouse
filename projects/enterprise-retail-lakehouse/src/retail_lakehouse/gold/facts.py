"""Gold facts. Grain of each fact: docs/DIMENSIONAL_MODEL.md §1.

Every join to a dimension is N:1 and ``left``: unmatched keys become the unknown member
(-1), so a fact never gains or loses rows.
"""

from typing import Final

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from retail_lakehouse.gold.dimensions import age_band_at
from retail_lakehouse.gold.keys import (
    SECONDS_PER_DAY,
    UNKNOWN_KEY,
    business_date,
    business_date_key,
)
from retail_lakehouse.gold.schemas import DAYS, LINE_AMOUNT, MONEY

_EXACT: Final = DecimalType(18, 0)


def _key_or_unknown(key: str) -> Column:
    return F.coalesce(F.col(key), F.lit(UNKNOWN_KEY)).alias(key)


def _date_key_or_unknown(timestamp: str, alias: str) -> Column:
    return F.coalesce(business_date_key(F.col(timestamp)), F.lit(UNKNOWN_KEY)).alias(alias)


def _event_date(timestamp: str, alias: str) -> Column:
    """Business date of the event, kept even when its key becomes -1 (outside the calendar)."""
    return business_date(F.col(timestamp)).alias(alias)


def _customer_version_at(events: DataFrame, at: str, dim_customer: DataFrame) -> DataFrame:
    """Add ``customer_sk`` of the version valid at ``events[at]`` (N:1: intervals never overlap)."""
    versions = dim_customer.where(F.col("customer_sk") != UNKNOWN_KEY).select(
        F.col("customer_id").alias("_customer_id"),
        "customer_sk",
        F.col("valid_from").alias("_valid_from"),
        F.col("valid_to").alias("_valid_to"),
    )
    matched = events.join(
        versions,
        (F.col("customer_id") == F.col("_customer_id"))
        & (F.col(at) >= F.col("_valid_from"))
        & (F.col(at) < F.col("_valid_to")),
        "left",
    )
    return _customer_identity(
        matched.drop("_customer_id", "_valid_from", "_valid_to"), dim_customer
    )


def _customer_identity(events: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Add ``customer_durable_key`` by ``customer_id`` (N:1: one durable key per customer).

    Independent of the event time: an event before the customer's first version keeps the
    customer (``customer_sk`` stays -1, since no attributes are known for that instant).
    """
    identities = (
        dim_customer.where(F.col("customer_sk") != UNKNOWN_KEY)
        .select(F.col("customer_id").alias("_identity_id"), "customer_durable_key")
        .distinct()
    )
    matched = events.join(identities, F.col("customer_id") == F.col("_identity_id"), "left")
    return matched.drop("_identity_id")


def _lookup(
    events: DataFrame, dim: DataFrame, natural_key: str, keys: tuple[str, ...]
) -> DataFrame:
    """Add ``keys`` from an SCD1 dimension by natural key (N:1). ``keys[0]`` is its own sk."""
    members = dim.where(F.col(keys[0]) != UNKNOWN_KEY).select(
        F.col(natural_key).alias("_natural_key"), *keys
    )
    matched = events.join(members, F.col(natural_key) == F.col("_natural_key"), "left")
    return matched.drop("_natural_key")


def _orders(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id", "customer_id", "store_id", "channel", "order_status", "order_timestamp"
    )


def _with_birth_date(events: DataFrame, birth_dates: DataFrame) -> DataFrame:
    """Add ``birth_date`` by ``customer_id`` (N:1: one row per customer in ``birth_dates``)."""
    births = birth_dates.select(F.col("customer_id").alias("_birth_customer_id"), "birth_date")
    matched = events.join(births, F.col("customer_id") == F.col("_birth_customer_id"), "left")
    return matched.drop("_birth_customer_id")


def build_fact_sales(  # noqa: PLR0913 - the fact's inputs, keyword-only birth dates
    orders: DataFrame,
    order_items: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_store: DataFrame,
    *,
    birth_dates: DataFrame,
) -> DataFrame:
    """Grain: one order line (order_id, line_number). No order-level values (freight, cash).

    ``customer_age_band`` is derived: the customer's age band on the order's business date
    (``birth_dates``: ``dimensions.customer_birth_dates``), unlike ``dim_customer.age_band``,
    historized at the start of the version. The birth date itself is not kept.
    """
    lines = order_items.join(_orders(orders), "order_id", "left")  # N:1
    lines = _with_birth_date(lines, birth_dates)
    lines = _customer_version_at(lines, "order_timestamp", dim_customer)
    lines = _lookup(lines, dim_product, "product_id", ("product_sk", "supplier_sk"))
    lines = _lookup(lines, dim_store, "store_id", ("store_sk",))
    gross = (F.col("quantity") * F.col("unit_price")).cast(LINE_AMOUNT)
    return lines.select(
        "order_id",
        "line_number",
        _date_key_or_unknown("order_timestamp", "date_key"),
        _event_date("order_timestamp", "order_date"),
        _key_or_unknown("customer_sk"),
        _key_or_unknown("customer_durable_key"),
        age_band_at(F.col("birth_date"), business_date(F.col("order_timestamp"))).alias(
            "customer_age_band"
        ),
        _key_or_unknown("product_sk"),
        _key_or_unknown("store_sk"),
        _key_or_unknown("supplier_sk"),
        "channel",
        "order_status",
        "quantity",
        "unit_price",
        gross.alias("gross_amount"),
        "discount_amount",
        (gross - F.col("discount_amount")).cast(LINE_AMOUNT).alias("net_amount"),
    )


def build_fact_payments(
    payments: DataFrame, orders: DataFrame, dim_customer: DataFrame, dim_store: DataFrame
) -> DataFrame:
    """Grain: one payment event (payment_id). ``signed_amount`` = cash effect of the event."""
    events = payments.join(
        _orders(orders).drop("channel", "order_status"), "order_id", "left"
    )  # N:1
    events = _customer_version_at(events, "order_timestamp", dim_customer)
    events = _lookup(events, dim_store, "store_id", ("store_sk",))
    status, amount = F.col("payment_status"), F.col("amount")
    signed = (
        F.when(status == "approved", amount).when(status == "refunded", -amount).otherwise(F.lit(0))
    ).cast(MONEY)
    return events.select(
        "payment_id",
        "order_id",
        _date_key_or_unknown("processed_at", "date_key"),
        _event_date("processed_at", "payment_date"),
        _key_or_unknown("customer_sk"),
        _key_or_unknown("customer_durable_key"),
        _key_or_unknown("store_sk"),
        "payment_method",
        "payment_status",
        "installments",
        "amount",
        signed.alias("signed_amount"),
    )


def build_fact_shipments(
    shipments: DataFrame, orders: DataFrame, dim_customer: DataFrame, dim_store: DataFrame
) -> DataFrame:
    """Grain: one shipment (shipment_id). ``delivery_days`` = order to delivery, in days."""
    events = shipments.join(
        _orders(orders).drop("channel", "order_status"), "order_id", "left"
    )  # N:1
    events = _customer_version_at(events, "order_timestamp", dim_customer)
    events = _lookup(events, dim_store, "store_id", ("store_sk",))
    elapsed = F.unix_timestamp("delivered_at") - F.unix_timestamp("order_timestamp")
    delivery_days = F.round(elapsed.cast(_EXACT) / F.lit(SECONDS_PER_DAY).cast(_EXACT), 2)
    return events.select(
        "shipment_id",
        "order_id",
        _date_key_or_unknown("shipped_at", "ship_date_key"),
        _event_date("shipped_at", "ship_date"),
        _date_key_or_unknown("delivered_at", "delivery_date_key"),
        _event_date("delivered_at", "delivery_date"),
        _key_or_unknown("customer_sk"),
        _key_or_unknown("customer_durable_key"),
        _key_or_unknown("store_sk"),
        "carrier_id",
        "shipment_status",
        "freight_amount",
        delivery_days.cast(DAYS).alias("delivery_days"),
    )


def build_fact_inventory_movements(
    movements: DataFrame, dim_product: DataFrame, dim_store: DataFrame
) -> DataFrame:
    """Grain: one stock movement (movement_id). Quantity is signed; balances are semi-additive."""
    events = _lookup(movements, dim_product, "product_id", ("product_sk", "supplier_sk"))
    events = _lookup(events, dim_store, "store_id", ("store_sk",))
    return events.select(
        "movement_id",
        _date_key_or_unknown("movement_timestamp", "date_key"),
        _event_date("movement_timestamp", "movement_date"),
        _key_or_unknown("store_sk"),
        _key_or_unknown("product_sk"),
        _key_or_unknown("supplier_sk"),
        "movement_type",
        "quantity",
        F.col("reference_id").alias("reference_order_id"),
    )
