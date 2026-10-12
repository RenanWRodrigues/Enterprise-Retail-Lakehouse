"""Gold output contract: columns and types, grain, natural keys and foreign keys of every table.

Grain of each table: docs/DIMENSIONAL_MODEL.md §1 (also stored as the table comment). The
publication gate (``retail_lakehouse.quality.suites.retail_gold_suite``) enforces this contract.
"""

from dataclasses import dataclass
from typing import Final

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY: Final = DecimalType(12, 2)
LINE_AMOUNT: Final = DecimalType(14, 2)  # quantity x unit_price
DAYS: Final = DecimalType(6, 2)
STR, INT, LONG, BOOL = StringType(), IntegerType(), LongType(), BooleanType()
TS, DT = TimestampType(), DateType()


def _schema(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, data_type, True) for name, data_type in fields])


GOLD_SCHEMAS: Final[dict[str, StructType]] = {
    "dim_date": _schema(
        ("date_key", INT),
        ("date", DT),
        ("year", INT),
        ("quarter", INT),
        ("month", INT),
        ("year_month", STR),
        ("month_name", STR),
        ("day", INT),
        ("day_of_week", INT),
        ("day_name", STR),
        ("week_of_year", INT),
        ("is_weekend", BOOL),
        ("is_black_friday", BOOL),
    ),
    "dim_customer": _schema(
        ("customer_sk", LONG),
        ("customer_id", STR),
        ("segment", STR),
        ("city", STR),
        ("state", STR),
        ("region", STR),
        ("age_band", STR),
        ("registered_date", DT),
        ("valid_from", TS),
        ("valid_to", TS),
        ("is_current", BOOL),
    ),
    "dim_product": _schema(
        ("product_sk", LONG),
        ("product_id", STR),
        ("product_name", STR),
        ("brand", STR),
        ("category_id", STR),
        ("category_name", STR),
        ("department_name", STR),
        ("supplier_sk", LONG),
        ("supplier_id", STR),
        ("list_price", MONEY),
        ("cost_price", MONEY),
        ("is_active", BOOL),
    ),
    "dim_store": _schema(
        ("store_sk", LONG),
        ("store_id", STR),
        ("store_name", STR),
        ("store_type", STR),
        ("city", STR),
        ("state", STR),
        ("region", STR),
        ("opened_date", DT),
    ),
    "dim_supplier": _schema(
        ("supplier_sk", LONG),
        ("supplier_id", STR),
        ("supplier_name", STR),
        ("city", STR),
        ("state", STR),
        ("lead_time_days", INT),
    ),
    "fact_sales": _schema(
        ("order_id", STR),
        ("line_number", INT),
        ("date_key", INT),
        ("order_date", DT),
        ("customer_sk", LONG),
        ("product_sk", LONG),
        ("store_sk", LONG),
        ("supplier_sk", LONG),
        ("channel", STR),
        ("order_status", STR),
        ("quantity", INT),
        ("unit_price", MONEY),
        ("gross_amount", LINE_AMOUNT),
        ("discount_amount", MONEY),
        ("net_amount", LINE_AMOUNT),
    ),
    "fact_payments": _schema(
        ("payment_id", STR),
        ("order_id", STR),
        ("date_key", INT),
        ("payment_date", DT),
        ("customer_sk", LONG),
        ("store_sk", LONG),
        ("payment_method", STR),
        ("payment_status", STR),
        ("installments", INT),
        ("amount", MONEY),
        ("signed_amount", MONEY),
    ),
    "fact_shipments": _schema(
        ("shipment_id", STR),
        ("order_id", STR),
        ("ship_date_key", INT),
        ("ship_date", DT),
        ("delivery_date_key", INT),
        ("delivery_date", DT),
        ("customer_sk", LONG),
        ("store_sk", LONG),
        ("carrier_id", STR),
        ("shipment_status", STR),
        ("freight_amount", MONEY),
        ("delivery_days", DAYS),
    ),
    "fact_inventory_movements": _schema(
        ("movement_id", STR),
        ("date_key", INT),
        ("movement_date", DT),
        ("store_sk", LONG),
        ("product_sk", LONG),
        ("supplier_sk", LONG),
        ("movement_type", STR),
        ("quantity", INT),
        ("reference_order_id", STR),
    ),
}

# Grain (facts) / natural key (dimensions): the columns that make a row unique.
GRAIN: Final[dict[str, tuple[str, ...]]] = {
    "dim_date": ("date_key",),
    "dim_customer": ("customer_sk",),
    "dim_product": ("product_sk",),
    "dim_store": ("store_sk",),
    "dim_supplier": ("supplier_sk",),
    "fact_sales": ("order_id", "line_number"),
    "fact_payments": ("payment_id",),
    "fact_shipments": ("shipment_id",),
    "fact_inventory_movements": ("movement_id",),
}

# Natural key of each dimension: required on every row except the unknown member (sk = -1),
# whose natural key is NULL by design (dimensions.with_unknown_member).
NATURAL_KEYS: Final[dict[str, str]] = {
    "dim_customer": "customer_id",
    "dim_product": "product_id",
    "dim_store": "store_id",
    "dim_supplier": "supplier_id",
}


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """``column`` of a Gold table references ``dimension.key`` (N:1; -1 = unknown member)."""

    column: str
    dimension: str
    key: str


def _date(column: str) -> ForeignKey:
    return ForeignKey(column, "dim_date", "date_key")


_CUSTOMER: Final = ForeignKey("customer_sk", "dim_customer", "customer_sk")
_PRODUCT: Final = ForeignKey("product_sk", "dim_product", "product_sk")
_STORE: Final = ForeignKey("store_sk", "dim_store", "store_sk")
_SUPPLIER: Final = ForeignKey("supplier_sk", "dim_supplier", "supplier_sk")

# Every relationship of the star (docs/DIMENSIONAL_MODEL.md §3), including the one between
# dimensions (dim_product -> dim_supplier). Facts never carry NULL keys: no match = -1.
FOREIGN_KEYS: Final[dict[str, tuple[ForeignKey, ...]]] = {
    "dim_product": (_SUPPLIER,),
    "fact_sales": (_date("date_key"), _CUSTOMER, _PRODUCT, _STORE, _SUPPLIER),
    "fact_payments": (_date("date_key"), _CUSTOMER, _STORE),
    "fact_shipments": (_date("ship_date_key"), _date("delivery_date_key"), _CUSTOMER, _STORE),
    "fact_inventory_movements": (_date("date_key"), _STORE, _PRODUCT, _SUPPLIER),
}

# Date key -> business date (Brasilia) of the event it encodes. The key points to dim_date only
# inside [calendar_start, calendar_end]; outside it (or with no date) the key is the unknown
# member -1 and the date column keeps the event date, so no information is lost.
EVENT_DATES: Final[dict[str, dict[str, str]]] = {
    "fact_sales": {"date_key": "order_date"},
    "fact_payments": {"date_key": "payment_date"},
    "fact_shipments": {"ship_date_key": "ship_date", "delivery_date_key": "delivery_date"},
    "fact_inventory_movements": {"date_key": "movement_date"},
}

# Foreign keys whose unknown member signals late or orphan master data (ADR 007): accepted, but
# reported as WARNING. Unknown dates (not shipped/delivered yet) and suppliers (supplier_id is
# nullable in Silver) are normal states and are not reported.
LATE_MASTER_DATA_KEYS: Final = frozenset({"customer_sk", "product_sk", "store_sk"})

TABLE_COMMENTS: Final[dict[str, str]] = {
    "dim_date": "Grain: one calendar day. date_key = yyyymmdd; -1 = unknown.",
    "dim_customer": "Grain: one customer version (SCD Type 2). customer_sk = -1: unknown.",
    "dim_product": "Grain: one product (SCD Type 1). product_sk = -1: unknown.",
    "dim_store": "Grain: one store (SCD Type 1). store_sk = -1: unknown.",
    "dim_supplier": "Grain: one supplier (SCD Type 1). supplier_sk = -1: unknown.",
    "fact_sales": "Grain: one order line (order_id, line_number), any order status.",
    "fact_payments": "Grain: one payment event (payment_id): declined, approved or refunded.",
    "fact_shipments": "Grain: one shipment (shipment_id) of an online/app order.",
    "fact_inventory_movements": "Grain: one stock movement (movement_id) of a product in a store.",
}
