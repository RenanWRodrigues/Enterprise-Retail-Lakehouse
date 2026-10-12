"""Bronze naming, schemas, paths and settings — no Spark session needed."""

from pathlib import Path

import pytest
from pyspark.sql.types import DateType, StringType, TimestampType

from retail_lakehouse.bronze import (
    BRONZE_TABLES,
    METADATA_COLUMNS,
    BronzeTarget,
    bronze_schema,
    landing_path,
    load_bronze_settings,
    source_schema,
)
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS

CONFIG_FILE = Path("projects/enterprise-retail-lakehouse/config/bronze.toml")


def test_one_bronze_table_per_source_dataset() -> None:
    assert set(BRONZE_TABLES.values()) == {
        "bronze_customers",
        "bronze_products",
        "bronze_categories",
        "bronze_stores",
        "bronze_suppliers",
        "bronze_orders",
        "bronze_order_items",
        "bronze_payments",
        "bronze_shipments",
        "bronze_inventory",
        "bronze_inventory_movements",
    }
    assert set(BRONZE_TABLES) == set(RETAIL_SOURCE_CONTRACTS)


@pytest.mark.parametrize("dataset", sorted(RETAIL_SOURCE_CONTRACTS))
def test_source_schema_keeps_every_column_as_nullable_text(dataset: str) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    schema = source_schema(contract)

    assert schema.fieldNames() == list(contract.column_names)
    assert all(field.dataType == StringType() and field.nullable for field in schema.fields)


@pytest.mark.parametrize("dataset", sorted(RETAIL_SOURCE_CONTRACTS))
def test_bronze_schema_appends_metadata_columns(dataset: str) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    schema = bronze_schema(contract)

    assert schema.fieldNames() == [*contract.column_names, *METADATA_COLUMNS]
    assert [(f.name, f.dataType) for f in schema.fields[-4:]] == [
        ("_ingestion_timestamp", TimestampType()),
        ("_source_file", StringType()),
        ("_batch_id", StringType()),
        ("_ingestion_date", DateType()),
    ]


def test_target_qualifies_table_names() -> None:
    assert BronzeTarget("retail.bronze").table("customers") == "retail.bronze.bronze_customers"
    assert BronzeTarget("retail_bronze").table("orders") == "retail_bronze.bronze_orders"


@pytest.mark.parametrize(
    "namespace", ["", "retail.bronze; DROP TABLE x", "a.b.c", "1retail", "re tail"]
)
def test_target_rejects_unsafe_namespaces(namespace: str) -> None:
    with pytest.raises(ValueError, match="namespace"):
        BronzeTarget(namespace)


@pytest.mark.parametrize(
    ("dataset", "expected"),
    [
        ("orders", "/Volumes/retail/landing/files/ecommerce/orders/orders_initial.jsonl"),
        ("customers", "/Volumes/retail/landing/files/crm/customers/customers_initial.csv"),
        ("payments", "/Volumes/retail/landing/files/payments/payments/payments_initial.jsonl"),
    ],
)
def test_landing_path_follows_the_landing_zone_layout(dataset: str, expected: str) -> None:
    assert landing_path("/Volumes/retail/landing/files/", dataset, "initial") == expected


def test_landing_path_rejects_unsafe_batch_ids() -> None:
    with pytest.raises(ValueError, match="batch_id"):
        landing_path("/landing", "orders", "../../etc")


@pytest.mark.parametrize("environment", ["local", "databricks"])
def test_repository_settings_load_for_each_environment(repo_root: Path, environment: str) -> None:
    settings = load_bronze_settings(repo_root / CONFIG_FILE, environment)
    assert settings.landing_root
    assert settings.target.table("orders").endswith(".bronze_orders")


def test_unknown_environment_is_rejected(repo_root: Path) -> None:
    with pytest.raises(ValueError, match="environment"):
        load_bronze_settings(repo_root / CONFIG_FILE, "staging")
