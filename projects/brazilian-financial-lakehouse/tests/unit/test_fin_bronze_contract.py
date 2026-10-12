"""The Bronze contract exists twice and must not drift.

``storage.bronze.BRONZE_DDL`` (Delta writes; the storage layer stays importable without Spark)
and ``silver.schemas.BRONZE_SCHEMA`` (reading Bronze and the CRITICAL schema check). A column
added to one and not the other would only fail at run time, as a schema-change incident.
"""

from financial_lakehouse.silver.schemas import BRONZE_SCHEMA
from financial_lakehouse.storage.bronze import BRONZE_DDL
from financial_lakehouse.transformation import BronzeRecord


def _ddl_columns() -> list[tuple[str, str, bool]]:
    columns = []
    for item in BRONZE_DDL.split(","):
        parts = item.split()
        columns.append((parts[0], parts[1].lower(), "NOT NULL" not in item.upper()))
    return columns


def test_ddl_and_struct_type_describe_the_same_columns() -> None:
    struct = [(f.name, f.dataType.simpleString()) for f in BRONZE_SCHEMA.fields]
    assert [(name, kind) for name, kind, _ in _ddl_columns()] == struct


def test_the_read_schema_is_never_stricter_than_the_table() -> None:
    """Reading may be more permissive (Silver quarantines NULL reference dates), never stricter."""
    stricter = [
        name
        for (name, _, table_nullable), field in zip(
            _ddl_columns(), BRONZE_SCHEMA.fields, strict=True
        )
        if table_nullable and not field.nullable
    ]
    assert stricter == []


def test_bronze_record_fields_match_the_contract() -> None:
    assert list(BronzeRecord.__dataclass_fields__) == BRONZE_SCHEMA.fieldNames()
