"""Silver contracts: Bronze as read, the standardized series and the quarantine.

Conceptual schema requested for Silver (indicator_id, indicator_name, reference_date, value, unit,
frequency, source, updated_at) plus explicit period bounds and lineage. See docs/SILVER.md.
"""

from typing import Final

from pyspark.sql.types import (
    ArrayType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VALUE_TYPE: Final = DecimalType(24, 8)
"""16 integer digits (credit balances ~1e13) and 8 decimals (Selic daily has 6, PTAX 4-5)."""

STR, DT, TS, INT = StringType(), DateType(), TimestampType(), IntegerType()


def _schema(*fields: tuple[str, DataType, bool]) -> StructType:
    return StructType([StructField(name, kind, nullable) for name, kind, nullable in fields])


BRONZE_SCHEMA: Final = _schema(
    ("record_key", STR, False),
    ("source", STR, False),
    ("series_id", STR, False),
    ("indicator_id", STR, False),
    ("reference", STR, False),
    ("reference_date", DT, True),
    ("value", STR, True),
    ("attributes", STR, True),
    ("request_timestamp", TS, True),
    ("ingestion_timestamp", TS, False),
    ("run_id", STR, False),
    ("raw_file", STR, True),
    ("raw_sha256", STR, True),
)
"""Same columns and types as ``storage.bronze.BRONZE_DDL``; used to read Bronze without inference.

``reference_date`` is nullable here although the table declares it NOT NULL: reading is
deliberately permissive so a NULL reaches the Silver rule ``missing_reference_date`` (quarantine)
instead of failing the read. ``test_fin_bronze_contract.py`` guards the two definitions."""

CATALOG_SCHEMA: Final = _schema(
    ("indicator_id", STR, False),
    ("indicator_name", STR, False),
    ("unit", STR, False),
    ("frequency", STR, False),
)

SILVER_SCHEMA: Final = _schema(
    ("indicator_id", STR, False),
    ("indicator_name", STR, False),
    ("reference_date", DT, False),
    ("value", VALUE_TYPE, False),
    ("unit", STR, False),
    ("frequency", STR, False),
    ("source", STR, False),
    ("updated_at", TS, False),
    ("period_start", DT, False),
    ("period_end", DT, False),
    ("series_id", STR, False),
    ("revision_count", INT, False),
    ("record_key", STR, False),
    ("bronze_ingestion_timestamp", TS, False),
)

QUARANTINE_SCHEMA: Final = StructType(
    [
        *BRONZE_SCHEMA.fields,
        StructField("_dq_failed_checks", ArrayType(STR), False),
        StructField("_dq_execution_timestamp", TS, False),
    ]
)
