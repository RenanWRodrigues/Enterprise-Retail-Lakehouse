"""Gold documentation stays in sync with the Gold contract (no Spark session)."""

from pathlib import Path

import pytest

from retail_lakehouse.gold import (
    GOLD_SCHEMAS,
    GRAIN,
    TABLE_COMMENTS,
    GoldTarget,
    load_gold_settings,
)

DOCS = Path("projects/enterprise-retail-lakehouse/docs")
SQL_DIR = Path("projects/enterprise-retail-lakehouse/sql/gold")
CONFIG_FILE = Path("projects/enterprise-retail-lakehouse/config/gold.toml")
FACTS = [name for name in GOLD_SCHEMAS if name.startswith("fact_")]
KPIS = (
    "gross_revenue",
    "net_revenue",
    "quantity_sold",
    "average_order_value",
    "discount_amount",
    "shipping_cost",
    "delivery_days",
)


@pytest.mark.parametrize("table", sorted(GOLD_SCHEMAS))
def test_data_dictionary_documents_every_column(repo_root: Path, table: str) -> None:
    doc = (repo_root / DOCS / "DATA_DICTIONARY.md").read_text(encoding="utf-8")
    section = doc.split(f"### {table}\n")[1].split("\n### ")[0]
    for field in GOLD_SCHEMAS[table]:
        assert f"| `{field.name}` |" in section, f"{table}.{field.name} not documented"


@pytest.mark.parametrize("fact", FACTS)
def test_every_fact_grain_is_documented_before_use(repo_root: Path, fact: str) -> None:
    model = (repo_root / DOCS / "DIMENSIONAL_MODEL.md").read_text(encoding="utf-8")
    grain_section = model.split("## 1. Grain")[1].split("\n## ")[0]
    assert f"| `{fact}` |" in grain_section
    assert TABLE_COMMENTS[fact].startswith("Grain:")
    assert GRAIN[fact]


@pytest.mark.parametrize("kpi", KPIS)
def test_every_kpi_has_a_business_definition(repo_root: Path, kpi: str) -> None:
    rules = (repo_root / DOCS / "BUSINESS_RULES.md").read_text(encoding="utf-8")
    assert f"| `{kpi}` |" in rules.split("## 2. KPIs")[1].split("\n## ")[0]


def test_kpi_summary_query_exposes_every_kpi(repo_root: Path) -> None:
    sql = (repo_root / SQL_DIR / "kpi_summary.sql").read_text(encoding="utf-8")
    for kpi in KPIS:
        assert f"AS {kpi}" in sql


@pytest.mark.parametrize("query", sorted(p.name for p in SQL_DIR.glob("*.sql")))
def test_analytical_queries_document_grain_and_avoid_select_star(
    repo_root: Path, query: str
) -> None:
    sql = (repo_root / SQL_DIR / query).read_text(encoding="utf-8")
    assert "-- Grain of the result:" in sql
    assert "SELECT *" not in sql.upper().replace("COUNT(*)", "")


def test_model_has_a_mermaid_er_diagram(repo_root: Path) -> None:
    model = (repo_root / DOCS / "DIMENSIONAL_MODEL.md").read_text(encoding="utf-8")
    assert "```mermaid\nerDiagram" in model


@pytest.mark.parametrize("environment", ["local", "databricks"])
def test_gold_settings_load(repo_root: Path, environment: str) -> None:
    settings = load_gold_settings(repo_root / CONFIG_FILE, environment)
    assert isinstance(settings.target, GoldTarget)
    assert settings.calendar_start < settings.calendar_end
