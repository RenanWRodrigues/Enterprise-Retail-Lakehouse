"""Source contracts are internally consistent and documented."""

import re
from pathlib import Path

import pytest

from retail_lakehouse.ingestion.source_contracts import (
    RETAIL_SOURCE_CONTRACTS,
    ColumnContract,
    LogicalType,
)

EXPECTED_DATASETS = {
    "customers",
    "products",
    "categories",
    "stores",
    "suppliers",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory",
    "inventory_movements",
}
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
DOC = Path("projects/enterprise-retail-lakehouse/docs/DATA_CONTRACTS.md")


def test_every_requested_dataset_has_a_contract() -> None:
    assert set(RETAIL_SOURCE_CONTRACTS) == EXPECTED_DATASETS


@pytest.mark.parametrize("dataset", sorted(EXPECTED_DATASETS))
def test_contract_is_well_formed(dataset: str) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    names = [column.name for column in contract.columns]

    assert contract.name == dataset
    assert len(names) == len(set(names)), "duplicate column names"
    assert all(SNAKE_CASE.match(name) for name in names)
    assert contract.business_key, "business key is mandatory"
    for key_column in contract.business_key:
        assert not contract.column(key_column).nullable, f"{key_column} is part of the key"


@pytest.mark.parametrize("dataset", sorted(EXPECTED_DATASETS))
def test_foreign_keys_reference_single_column_business_keys(dataset: str) -> None:
    for foreign_key in RETAIL_SOURCE_CONTRACTS[dataset].foreign_keys:
        target = RETAIL_SOURCE_CONTRACTS[foreign_key.references_dataset]
        assert RETAIL_SOURCE_CONTRACTS[dataset].column(foreign_key.column)
        assert target.business_key == (foreign_key.references_column,)


def test_expected_relationships_are_declared() -> None:
    declared = {
        (dataset, fk.column, fk.references_dataset)
        for dataset, contract in RETAIL_SOURCE_CONTRACTS.items()
        for fk in contract.foreign_keys
    }
    assert declared >= {
        ("products", "category_id", "categories"),
        ("products", "supplier_id", "suppliers"),
        ("categories", "parent_category_id", "categories"),
        ("orders", "customer_id", "customers"),
        ("orders", "store_id", "stores"),
        ("order_items", "order_id", "orders"),
        ("order_items", "product_id", "products"),
        ("payments", "order_id", "orders"),
        ("shipments", "order_id", "orders"),
        ("inventory", "store_id", "stores"),
        ("inventory", "product_id", "products"),
        ("inventory_movements", "store_id", "stores"),
        ("inventory_movements", "product_id", "products"),
    }


def test_documentation_lists_every_dataset_and_column(repo_root: Path) -> None:
    doc = (repo_root / DOC).read_text(encoding="utf-8")
    for dataset, contract in RETAIL_SOURCE_CONTRACTS.items():
        section = doc.split(f"### {dataset} ")[1].split("\n### ")[0]
        for column in contract.columns:
            assert f"| `{column.name}` |" in section, f"{dataset}.{column.name} not documented"


@pytest.mark.parametrize(
    ("logical_type", "valid", "invalid"),
    [
        (LogicalType.STRING, "abc", ""),
        (LogicalType.INTEGER, "-3", "3.5"),
        (LogicalType.DECIMAL, "1299.90", "1.299,90"),
        (LogicalType.DATE, "2024-02-29", "2023-02-29"),
        (LogicalType.TIMESTAMP, "2024-03-05T14:23:11-03:00", "2024-02-30T10:00:00-03:00"),
        (LogicalType.BOOLEAN, "true", "yes"),
    ],
)
def test_column_contract_validates_text_values(
    logical_type: LogicalType, valid: str, invalid: str
) -> None:
    column = ColumnContract("c", logical_type, nullable=False)
    assert column.accepts(valid)
    assert not column.accepts(invalid)
    assert not column.accepts(None)


def test_nullable_and_accepted_values_rules() -> None:
    column = ColumnContract(
        "status", LogicalType.STRING, nullable=True, accepted_values=("paid", "created")
    )
    assert column.accepts(None)
    assert column.accepts("paid")
    assert not column.accepts("PAID")
