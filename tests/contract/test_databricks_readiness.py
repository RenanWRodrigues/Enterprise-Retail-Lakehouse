"""Databricks readiness: thin notebooks, an isolated smoke environment, a representative sample.

docs/databricks/DEPLOYMENT_GUIDE.md. These checks run locally; they prove nothing about the
workspace itself (that is the preflight notebook's job).
"""

import ast
import tomllib
from pathlib import Path
from typing import Any

import pytest

NOTEBOOKS = sorted(
    [*Path("projects").glob("*/notebooks/**/*.py"), *Path("shared/notebooks").glob("*.py")]
)
SMOKE_CATALOG_PREFIX = "workspace.smoke_"
SMOKE_VOLUME_PREFIX = "/Volumes/workspace/smoke_"
SMOKE_MAX_ORDERS = 1_000
# config file -> keys holding a table namespace, a table name or a Volume path
DATABRICKS_CONFIGS = {
    "projects/enterprise-retail-lakehouse/config/bronze.toml": ("landing_root", "target_namespace"),
    "projects/enterprise-retail-lakehouse/config/silver.toml": (
        "bronze_namespace", "silver_namespace",
    ),
    "projects/enterprise-retail-lakehouse/config/gold.toml": ("silver_namespace", "gold_namespace"),
    "projects/brazilian-financial-lakehouse/config/ingestion.toml": (
        "landing_root", "bronze_table",
    ),
    "projects/industrial-iot-lakehouse/config/bronze.toml": ("landing_root", "target_namespace"),
    "projects/industrial-iot-lakehouse/config/silver.toml": (
        "bronze_namespace", "silver_namespace",
    ),
    "projects/industrial-iot-lakehouse/config/gold.toml": (
        "silver_namespace", "gold_namespace",
    ),
    "projects/brazilian-financial-lakehouse/config/medallion.toml": (
        "bronze_table", "silver_namespace", "gold_namespace",
    ),
}  # fmt: skip


def _toml(repo_root: Path, path: str) -> dict[str, Any]:
    with (repo_root / path).open("rb") as handle:
        return tomllib.load(handle)


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.as_posix())
def test_notebooks_are_thin_databricks_sources(repo_root: Path, notebook: Path) -> None:
    text = (repo_root / notebook).read_text(encoding="utf-8")
    definitions = [
        node.name
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    ]

    assert text.startswith("# Databricks notebook source\n")
    assert definitions == [], f"logic belongs in a module, not in {notebook}: {definitions}"


@pytest.mark.parametrize("config", sorted(DATABRICKS_CONFIGS))
def test_the_smoke_environment_is_isolated_from_the_persistent_one(
    repo_root: Path, config: str
) -> None:
    document = _toml(repo_root, config)
    smoke, persistent = document["smoke"], document["databricks"]

    for key in DATABRICKS_CONFIGS[config]:
        value = str(smoke[key])
        prefix = SMOKE_VOLUME_PREFIX if value.startswith("/Volumes") else SMOKE_CATALOG_PREFIX
        assert value.startswith(prefix), f"{config} [smoke] {key} = {value}"
        assert value != str(persistent[key])


def test_the_smoke_landing_keeps_the_production_anomalies_at_a_small_size(
    repo_root: Path,
) -> None:
    config_dir = "projects/enterprise-retail-lakehouse/config"
    full = _toml(repo_root, f"{config_dir}/data_generation.toml")
    smoke = _toml(repo_root, f"{config_dir}/data_generation_smoke.toml")

    assert smoke["anomalies"] == full["anomalies"]
    assert smoke["generation"]["orders"] <= SMOKE_MAX_ORDERS
    for key in ("seed", "start_date", "end_date", "batch_id"):
        assert smoke["generation"][key] == full["generation"][key]
