"""Indicator catalog: requested indicators covered, each one documented in docs/data_sources.md."""

from pathlib import Path

import pytest

from financial_lakehouse.ingestion import INDICATORS, SourceKind

DOC = Path("projects/brazilian-financial-lakehouse/docs/data_sources.md")
OFFICIAL_HOSTS = {
    SourceKind.BCB_SGS: "https://api.bcb.gov.br/dados/serie/bcdata.sgs.",
    SourceKind.BCB_PTAX: "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/",
    SourceKind.IBGE_AGREGADOS: "https://servicodados.ibge.gov.br/api/v3/agregados/",
}


def test_catalog_covers_the_requested_indicators() -> None:
    assert {i.indicator_id for i in INDICATORS} == {
        "selic_target",
        "selic_daily",
        "ipca_monthly",
        "igpm_monthly",
        "gdp_growth_yoy",
        "usd_brl_sell",
        "eur_brl_ptax",
        "international_reserves",
        "monetary_base",
        "credit_total",
        "unemployment_rate",
    }


def test_indicator_ids_are_unique() -> None:
    ids = [i.indicator_id for i in INDICATORS]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("item", INDICATORS, ids=lambda i: i.indicator_id)
def test_indicator_is_documented_with_its_official_identifier(
    repo_root: Path, item: object
) -> None:
    doc = (repo_root / DOC).read_text(encoding="utf-8")
    row = next(line for line in doc.splitlines() if line.startswith(f"| `{item.indicator_id}` |"))  # type: ignore[attr-defined]
    identifier = str(item.series_code or item.agregado or item.currency)  # type: ignore[attr-defined]
    assert identifier in row
    assert "https://" in row  # official verification link


@pytest.mark.parametrize(("kind", "endpoint"), sorted(OFFICIAL_HOSTS.items()))
def test_every_endpoint_used_is_in_the_documentation(
    repo_root: Path, kind: SourceKind, endpoint: str
) -> None:
    assert endpoint in (repo_root / DOC).read_text(encoding="utf-8")


def test_documentation_records_the_access_date(repo_root: Path) -> None:
    assert "2026-10-05" in (repo_root / DOC).read_text(encoding="utf-8")
