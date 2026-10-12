"""Indicator catalog — every entry verified in docs/data_sources.md (access date 2026-10-05)."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class SourceKind(StrEnum):
    BCB_SGS = "bcb_sgs"
    BCB_PTAX = "bcb_ptax"
    IBGE_AGREGADOS = "ibge_agregados"


@dataclass(frozen=True, slots=True)
class Indicator:
    indicator_id: str
    name: str
    institution: str
    source: SourceKind
    frequency: str
    unit: str
    series_code: int | None = None  # BCB SGS
    currency: str | None = None  # BCB PTAX
    agregado: int | None = None  # IBGE
    variavel: int | None = None  # IBGE
    classificacao: str | None = None  # IBGE, e.g. "11255[90707]"


BCB, IBGE = "Banco Central do Brasil", "IBGE"
SGS, PTAX, AGREGADOS = SourceKind.BCB_SGS, SourceKind.BCB_PTAX, SourceKind.IBGE_AGREGADOS

INDICATORS: Final[tuple[Indicator, ...]] = (
    Indicator(
        "selic_target",
        "Meta Selic definida pelo Copom",
        BCB,
        SGS,
        "daily",
        "% a.a.",
        series_code=432,
    ),
    Indicator("selic_daily", "Taxa de juros - Selic", BCB, SGS, "daily", "% a.d.", series_code=11),
    Indicator(
        "ipca_monthly",
        "IPCA - Variação mensal",
        IBGE,
        AGREGADOS,
        "monthly",
        "%",
        agregado=1737,
        variavel=63,
    ),
    Indicator(
        "igpm_monthly", "IGP-M (FGV) - variação mensal", BCB, SGS, "monthly", "%", series_code=189
    ),
    Indicator(
        "gdp_growth_yoy",
        "PIB a preços de mercado - taxa trimestral vs. mesmo período do ano anterior",
        IBGE,
        AGREGADOS,
        "quarterly",
        "%",
        agregado=5932,
        variavel=6561,
        classificacao="11255[90707]",
    ),
    Indicator(
        "usd_brl_sell",
        "Dólar americano (venda) - diário",
        BCB,
        SGS,
        "daily",
        "R$/US$",
        series_code=1,
    ),
    Indicator(
        "eur_brl_ptax",
        "Euro - PTAX (todos os boletins)",
        BCB,
        PTAX,
        "daily",
        "R$/EUR",
        currency="EUR",
    ),
    Indicator(
        "international_reserves",
        "Reservas internacionais - conceito liquidez - total",
        BCB,
        SGS,
        "daily",
        "US$ milhões",
        series_code=13982,
    ),
    Indicator(
        "monetary_base",
        "Base monetária restrita (saldo em final de período)",
        BCB,
        SGS,
        "monthly",
        "milhares de unidades monetárias correntes",
        series_code=1788,
    ),
    Indicator(
        "credit_total",
        "Saldo da carteira de crédito - Total",
        BCB,
        SGS,
        "monthly",
        "unidades monetárias correntes",
        series_code=20539,
    ),
    Indicator(
        "unemployment_rate",
        "Taxa de desocupação (PNAD Contínua)",
        IBGE,
        AGREGADOS,
        "rolling_quarter",
        "%",
        agregado=6381,
        variavel=4099,
    ),
)

_BY_ID: Final = {item.indicator_id: item for item in INDICATORS}


def indicator(indicator_id: str) -> Indicator:
    """Catalog entry by id (``KeyError`` if unknown)."""
    return _BY_ID[indicator_id]
