"""BCB PTAX (Olinda OData) client (docs/data_sources.md §1.2).

Periods are split into yearly windows and each window is paginated with ``$top``/``$skip``
ordered by ``dataHoraCotacao`` (stable pages). Dates use the documented ``'MM-DD-YYYY'`` format.
"""

from datetime import date
from typing import Final

from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.clients.bcb_sgs import date_windows
from financial_lakehouse.clients.http import HttpClient, parse_json_text

SOURCE: Final = "bcb_ptax"
PTAX_URL: Final = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
)
FIELDS: Final = (
    "paridadeCompra",
    "paridadeVenda",
    "cotacaoCompra",
    "cotacaoVenda",
    "dataHoraCotacao",
    "tipoBoletim",
)
DEFAULT_PAGE_SIZE: Final = 1000
WINDOW_YEARS: Final = 1


def _validate(payload: object) -> tuple[dict[str, str], ...]:
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise ResponseSchemaError("PTAX: expected an OData object with a 'value' list")
    records = []
    for item in payload["value"]:
        if not isinstance(item, dict) or not set(FIELDS) <= set(item):
            raise ResponseSchemaError(f"PTAX: expected fields {FIELDS}, got {item!r}")
        records.append({key: str(item[key]) for key in FIELDS})
    return tuple(records)


class PtaxClient:
    def __init__(self, http: HttpClient, page_size: int = DEFAULT_PAGE_SIZE) -> None:
        if page_size < 1:
            raise ValueError("page_size must be >= 1")
        self._http = http
        self._page_size = page_size

    def fetch(self, currency: str, start: date, end: date) -> list[RawPage]:
        """All bulletins (opening, intermediate, closing) of ``currency`` in [start, end]."""
        pages = []
        for window_start, window_end in date_windows(start, end, years=WINDOW_YEARS):
            skip = 0
            while True:
                params = {
                    "@moeda": f"'{currency}'",
                    "@dataInicial": f"'{window_start:%m-%d-%Y}'",
                    "@dataFinalCotacao": f"'{window_end:%m-%d-%Y}'",
                    "$format": "json",
                    "$orderby": "dataHoraCotacao",
                    "$top": str(self._page_size),
                    "$skip": str(skip),
                }
                response = self._http.get(PTAX_URL, params)
                records = _validate(parse_json_text(response))
                pages.append(
                    RawPage(
                        SOURCE,
                        response.url,
                        response.body,
                        records,
                        window_start,
                        window_end,
                        response.requested_at,
                    )
                )
                if len(records) < self._page_size:
                    break
                skip += self._page_size
        return pages
