"""Raw pages -> uniform raw observations, still as TEXT (no casting: that is Silver's job).

Each source has its own shape; this flattens them to (indicator, reference, value, attributes)
so later layers read one format. Pure functions: no HTTP, no storage.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from financial_lakehouse.catalog import Indicator, SourceKind
from financial_lakehouse.clients.base import RawPage


@dataclass(frozen=True, slots=True)
class RawObservation:
    indicator_id: str
    source: str
    reference: str  # SGS: dd/MM/yyyy; PTAX: dataHoraCotacao; IBGE: period id
    value: str
    attributes: dict[str, str] = field(default_factory=dict)


def _sgs(indicator: Indicator, record: dict[str, str]) -> RawObservation:
    return RawObservation(
        indicator.indicator_id, SourceKind.BCB_SGS, record["data"], record["valor"]
    )


def _ptax(indicator: Indicator, record: dict[str, str]) -> RawObservation:
    attributes = {
        k: v for k, v in sorted(record.items()) if k not in {"cotacaoVenda", "dataHoraCotacao"}
    }
    return RawObservation(
        indicator.indicator_id,
        SourceKind.BCB_PTAX,
        record["dataHoraCotacao"],
        record["cotacaoVenda"],
        attributes,
    )


def _ibge(indicator: Indicator, record: dict[str, str]) -> RawObservation:
    return RawObservation(
        indicator.indicator_id,
        SourceKind.IBGE_AGREGADOS,
        record["periodo"],
        record["valor"],
        {"localidade": record["localidade"]},
    )


_MAPPERS: Final[dict[SourceKind, Callable[[Indicator, dict[str, str]], RawObservation]]] = {
    SourceKind.BCB_SGS: _sgs,
    SourceKind.BCB_PTAX: _ptax,
    SourceKind.IBGE_AGREGADOS: _ibge,
}


def to_raw_observations(indicator: Indicator, page: RawPage) -> list[RawObservation]:
    """Uniform text observations from a source page (PTAX value = ``cotacaoVenda``; other fields as
    attributes).
    """
    mapper = _MAPPERS[indicator.source]
    return [mapper(indicator, record) for record in page.records]
