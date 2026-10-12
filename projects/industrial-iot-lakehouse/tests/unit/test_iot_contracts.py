"""IoT source contracts: complete, consistent and documented."""

import re
from pathlib import Path

import pytest

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS

DOC = Path("projects/industrial-iot-lakehouse/docs/DATA_CONTRACTS.md")
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
TYPES = {"string", "integer", "double", "date", "timestamp"}


def test_every_requested_entity_has_a_contract() -> None:
    assert set(IOT_SOURCE_CONTRACTS) == {
        "equipment",
        "sensors",
        "sensor_readings",
        "failure_events",
        "maintenance_orders",
    }


@pytest.mark.parametrize("dataset", sorted(IOT_SOURCE_CONTRACTS))
def test_contract_is_well_formed(dataset: str) -> None:
    contract = IOT_SOURCE_CONTRACTS[dataset]
    names = list(contract.column_names)
    assert len(names) == len(set(names))
    assert all(SNAKE_CASE.match(n) for n in names)
    assert {c.logical_type for c in contract.columns} <= TYPES
    assert set(contract.business_key) <= set(names)
    assert not any(c.nullable for c in contract.columns if c.name in contract.business_key)


@pytest.mark.parametrize("dataset", sorted(IOT_SOURCE_CONTRACTS))
def test_foreign_keys_reference_business_keys(dataset: str) -> None:
    for column, target, target_column in IOT_SOURCE_CONTRACTS[dataset].foreign_keys:
        assert column in IOT_SOURCE_CONTRACTS[dataset].column_names
        assert IOT_SOURCE_CONTRACTS[target].business_key == (target_column,)


@pytest.mark.parametrize("dataset", sorted(IOT_SOURCE_CONTRACTS))
def test_documentation_lists_every_column(repo_root: Path, dataset: str) -> None:
    doc = (repo_root / DOC).read_text(encoding="utf-8")
    section = doc.split(f"### {dataset} ")[1].split("\n### ")[0].split("\n## ")[0]
    for column in IOT_SOURCE_CONTRACTS[dataset].columns:
        assert f"| `{column.name}` | {column.logical_type} |" in section
