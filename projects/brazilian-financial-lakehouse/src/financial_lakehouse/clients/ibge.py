"""IBGE aggregated-data API (v3) client (docs/data_sources.md §1.3).

Available periods are listed first, then requested in explicit batches (``202401|202402``) so a
request never exceeds the documented 100,000-value limit. Only documented URL forms are used.
"""

from typing import Final

from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.clients.http import HttpClient, parse_json_text

SOURCE: Final = "ibge_agregados"
BASE_URL: Final = "https://servicodados.ibge.gov.br/api/v3/agregados"
NATIONAL_LEVEL: Final = "N1[all]"
DEFAULT_MAX_PERIODS: Final = 120  # x 1 locality x 1 category: far below 100,000 values


def _validate_values(payload: object) -> tuple[dict[str, str], ...]:
    if not isinstance(payload, list):
        raise ResponseSchemaError("IBGE: expected a JSON list of variables")
    records = []
    try:
        for variable in payload:
            for result in variable["resultados"]:
                for series in result["series"]:
                    locality = str(series["localidade"]["id"])
                    for period, value in series["serie"].items():
                        records.append(
                            {"periodo": str(period), "valor": str(value), "localidade": locality}
                        )
    except (KeyError, TypeError, AttributeError) as error:
        raise ResponseSchemaError(f"IBGE: unexpected values structure ({error})") from error
    return tuple(records)


class IbgeAgregadosClient:
    def __init__(
        self, http: HttpClient, max_periods_per_request: int = DEFAULT_MAX_PERIODS
    ) -> None:
        if max_periods_per_request < 1:
            raise ValueError("max_periods_per_request must be >= 1")
        self._http = http
        self._max_periods = max_periods_per_request

    def periods(self, agregado: int) -> list[str]:
        """Period ids available for ``agregado`` (e.g. ``"202401"``), in API order."""
        payload = parse_json_text(self._http.get(f"{BASE_URL}/{agregado}/periodos"))
        if not isinstance(payload, list) or not all(
            isinstance(p, dict) and "id" in p for p in payload
        ):
            raise ResponseSchemaError("IBGE: expected a list of periods with 'id'")
        return [str(p["id"]) for p in payload]

    def fetch(
        self,
        agregado: int,
        variavel: int,
        periods: list[str],
        *,
        localidades: str = NATIONAL_LEVEL,
        classificacao: str | None = None,
    ) -> list[RawPage]:
        pages = []
        for start in range(0, len(periods), self._max_periods):
            batch = "|".join(periods[start : start + self._max_periods])
            params = {"localidades": localidades}
            if classificacao:
                params["classificacao"] = classificacao
            url = f"{BASE_URL}/{agregado}/periodos/{batch}/variaveis/{variavel}"
            response = self._http.get(url, params)
            pages.append(
                RawPage(
                    SOURCE,
                    response.url,
                    response.body,
                    _validate_values(parse_json_text(response)),
                    requested_at=response.requested_at,
                )
            )
        return pages
