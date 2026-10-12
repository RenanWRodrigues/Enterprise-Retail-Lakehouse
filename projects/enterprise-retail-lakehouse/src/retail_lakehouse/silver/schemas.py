"""Silver output contract — what Gold reads.

Typed, standardised, deduplicated entities (one row per business key; ``customers`` holds
one row per *version*, ordered by ``effective_from``, feeding SCD Type 2 in Gold).
Timestamps are instants (UTC). Orphan foreign keys may remain (DQ status WARNING, ADR 007):
Gold maps them to the unknown member.

``SILVER_SCHEMAS`` is the business contract (what Gold reads and the quality gate checks). The
stored tables append the Bronze lineage of each row (``LINEAGE_FIELDS``), so every Silver row is
traceable to its Bronze batch and file. Quarantine tables keep the Bronze row as delivered.
Implementation: ``retail_lakehouse.silver.standardize`` and ``.pipeline`` (docs/SILVER.md).
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
    TimestampType,
)

MONEY: Final = DecimalType(12, 2)


def _schema(*fields: tuple[str, DataType, bool]) -> StructType:
    return StructType(
        [StructField(name, data_type, nullable) for name, data_type, nullable in fields]
    )


STR, INT, TS, DT = StringType(), IntegerType(), TimestampType(), DateType()

SILVER_SCHEMAS: Final[dict[str, StructType]] = {
    "customers": _schema(
        ("customer_id", STR, False),
        ("city", STR, True),
        ("state", STR, True),
        ("region", STR, True),
        ("segment", STR, True),
        ("birth_date", DT, True),
        ("registered_at", TS, True),
        ("effective_from", TS, False),
    ),
    "categories": _schema(
        ("category_id", STR, False),
        ("category_name", STR, True),
        ("parent_category_id", STR, True),
    ),
    "suppliers": _schema(
        ("supplier_id", STR, False),
        ("supplier_name", STR, True),
        ("city", STR, True),
        ("state", STR, True),
        ("lead_time_days", INT, True),
    ),
    "stores": _schema(
        ("store_id", STR, False),
        ("store_name", STR, True),
        ("store_type", STR, True),
        ("city", STR, True),
        ("state", STR, True),
        ("region", STR, True),
        ("opened_at", DT, True),
    ),
    "products": _schema(
        ("product_id", STR, False),
        ("product_name", STR, True),
        ("category_id", STR, True),
        ("supplier_id", STR, True),
        ("brand", STR, True),
        ("list_price", MONEY, True),
        ("cost_price", MONEY, True),
        ("is_active", BooleanType(), True),
    ),
    "orders": _schema(
        ("order_id", STR, False),
        ("customer_id", STR, True),
        ("store_id", STR, True),
        ("channel", STR, True),
        ("order_status", STR, True),
        ("order_timestamp", TS, True),
        ("freight_amount", MONEY, True),
    ),
    "order_items": _schema(
        ("order_id", STR, False),
        ("line_number", INT, False),
        ("product_id", STR, True),
        ("quantity", INT, True),
        ("unit_price", MONEY, True),
        ("discount_amount", MONEY, True),
    ),
    "payments": _schema(
        ("payment_id", STR, False),
        ("order_id", STR, True),
        ("payment_method", STR, True),
        ("installments", INT, True),
        ("amount", MONEY, True),
        ("payment_status", STR, True),
        ("processed_at", TS, True),
    ),
    "shipments": _schema(
        ("shipment_id", STR, False),
        ("order_id", STR, True),
        ("carrier_id", STR, True),
        ("shipment_status", STR, True),
        ("shipped_at", TS, True),
        ("delivered_at", TS, True),
        ("freight_amount", MONEY, True),
    ),
    "inventory": _schema(
        ("snapshot_date", DT, False),
        ("store_id", STR, False),
        ("product_id", STR, False),
        ("quantity_on_hand", INT, True),
        ("reorder_point", INT, True),
    ),
    "inventory_movements": _schema(
        ("movement_id", STR, False),
        ("store_id", STR, True),
        ("product_id", STR, True),
        ("movement_type", STR, True),
        ("quantity", INT, True),
        ("movement_timestamp", TS, True),
        ("reference_id", STR, True),
    ),
}

SILVER_TABLES: Final = {entity: f"silver_{entity}" for entity in SILVER_SCHEMAS}
QUARANTINE_TABLES: Final = {entity: f"silver_{entity}_quarantine" for entity in SILVER_SCHEMAS}

LINEAGE_FIELDS: Final = (
    StructField("_ingestion_timestamp", TS, False),
    StructField("_source_file", STR, False),
    StructField("_batch_id", STR, False),
)
LINEAGE_COLUMNS: Final = tuple(field.name for field in LINEAGE_FIELDS)
ROW_HASH_COLUMN: Final = "_row_hash"  # fingerprint of the Bronze row (quarantine idempotency)


def silver_table_schema(entity: str) -> StructType:
    """Stored Silver table: the business contract followed by the Bronze lineage."""
    return StructType([*SILVER_SCHEMAS[entity].fields, *LINEAGE_FIELDS])
