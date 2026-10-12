"""Gold documentation contract: every table, metric and query is documented; SQL is honest."""

from pathlib import Path

import pytest

from financial_lakehouse.gold import GOLD_SCHEMAS, TABLE_COMMENTS

PROJECT = Path("projects/brazilian-financial-lakehouse")
DERIVED_METRICS = (
    "inflation_12m",
    "selic_change",
    "usd_brl_change",
    "moving_average_3m",
    "moving_average_12m",
    "real_interest_rate",
)
SQL_FILES = sorted((PROJECT / "sql" / "gold").glob("*.sql"))


@pytest.fixture(scope="module")
def gold_doc(repo_root: Path) -> str:
    return (repo_root / PROJECT / "docs" / "GOLD.md").read_text(encoding="utf-8")


def test_every_table_is_documented_with_its_grain(gold_doc: str) -> None:
    for name in GOLD_SCHEMAS:
        assert f"`{name}`" in gold_doc, name
        assert TABLE_COMMENTS[name].startswith("Grain:"), name


def test_requested_derived_metrics_exist_and_are_defined(gold_doc: str) -> None:
    columns = {f.name for schema in GOLD_SCHEMAS.values() for f in schema.fields}
    for metric in DERIVED_METRICS:
        assert any(metric in column for column in columns), metric
        assert metric in gold_doc, metric


def test_real_interest_rate_formula_and_limitations_are_documented(gold_doc: str) -> None:
    section = gold_doc[gold_doc.index("### `real_interest_rate`") :]
    assert "selic_12m_pct" in section and "inflation_12m" in section
    assert "Ex-post" in section and "Limitações" in section


def test_resampling_rules_cover_every_frequency(gold_doc: str) -> None:
    section = gold_doc[gold_doc.index("## 2.") : gold_doc.index("## 3.")]
    for rule in ("diária", "mensal", "trimestral", "trimestre móvel", "último mês do trimestre"):
        assert rule in section, rule


def test_resampling_is_documented_per_frequency_transition(gold_doc: str) -> None:
    section = gold_doc[gold_doc.index("### Por transição de frequência") : gold_doc.index("## 3.")]
    for transition in ("diária → mensal", "mensal → mensal", "trimestral → mensal"):
        assert transition in section, transition


def test_forward_fill_policy_is_explicit_and_justified(gold_doc: str) -> None:
    section = gold_doc[gold_doc.index("### Forward fill") : gold_doc.index("## 3.")]
    assert "não" in section.lower() and "Justificativa" in section


def test_official_series_validation_is_reported(gold_doc: str) -> None:
    section = gold_doc[gold_doc.index("## 7. Validação contra séries oficiais") :]
    for official in ("SGS 4390", "SGS 3698", "SGS 3696", "SGS 1178", "2265"):
        assert official in section, official


def test_there_are_analysis_queries_for_each_requested_theme() -> None:
    names = {path.stem for path in SQL_FILES}
    assert names == {
        "selic_vs_inflation",
        "selic_vs_exchange",
        "inflation_over_time",
        "exchange_over_time",
        "economic_activity",
        "labor_market",
    }


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.stem)
def test_every_query_documents_grain_and_rejects_causal_reading(
    repo_root: Path, path: Path
) -> None:
    header = "\n".join(
        line
        for line in (repo_root / path).read_text(encoding="utf-8").splitlines()
        if line.startswith("--")
    )
    assert "Grain do resultado" in header
    assert "causalidade" in header


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.stem)
def test_queries_do_not_select_star(repo_root: Path, path: Path) -> None:
    assert "SELECT *" not in (repo_root / path).read_text(encoding="utf-8").upper()
