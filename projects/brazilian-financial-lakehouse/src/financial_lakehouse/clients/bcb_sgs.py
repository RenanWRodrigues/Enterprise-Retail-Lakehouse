"""BCB SGS client (docs/data_sources.md §1.1).

Date-range queries are limited to 10 years, so long ranges are split into windows (date-based
pagination). A window without values answers HTTP 404 with "Value(s) not found" (verified
2026-10-05): only that exact case is treated as an empty window.
"""

from datetime import date, timedelta
from typing import Final

from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.clients.http import HttpClient, HttpStatusError

SOURCE: Final = "bcb_sgs"
SGS_URL: Final = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
MAX_WINDOW_YEARS: Final = 10
NOT_FOUND_STATUS: Final = 404
NOT_FOUND_MARKER: Final = b"Value(s) not found"
_FIELDS: Final = ("data", "valor")


def _add_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:  # 29 February in a non-leap target year
        return day.replace(year=day.year + years, day=28)


def date_windows(start: date, end: date, years: int = MAX_WINDOW_YEARS) -> list[tuple[date, date]]:
    """Consecutive inclusive windows covering [start, end], each shorter than ``years``."""
    if end < start:
        return []
    windows = []
    window_start = start
    while window_start <= end:
        window_end = min(end, _add_years(window_start, years) - timedelta(days=1))
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows


def _validate(payload: object) -> tuple[dict[str, str], ...]:
    if not isinstance(payload, list):
        raise ResponseSchemaError("SGS: expected a JSON list")
    records = []
    for item in payload:
        if not isinstance(item, dict) or set(item) != set(_FIELDS):
            raise ResponseSchemaError(f"SGS: expected keys {_FIELDS}, got {item!r}")
        if not all(isinstance(item[key], str) for key in _FIELDS):
            raise ResponseSchemaError(f"SGS: values must be text, got {item!r}")
        records.append({key: item[key] for key in _FIELDS})
    return tuple(records)


class SgsClient:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def fetch(self, code: int, start: date, end: date) -> list[RawPage]:
        """Every observation of series ``code`` in [start, end], one page per 10-year window."""
        url = SGS_URL.format(code=code)
        pages = []
        for window_start, window_end in date_windows(start, end):
            params = {
                "formato": "json",
                "dataInicial": window_start.strftime("%d/%m/%Y"),
                "dataFinal": window_end.strftime("%d/%m/%Y"),
            }
            try:
                response = self._http.get(url, params)
            except HttpStatusError as error:
                if error.status == NOT_FOUND_STATUS and NOT_FOUND_MARKER in error.body:
                    pages.append(
                        RawPage(
                            SOURCE,
                            error.url,
                            error.body,
                            (),
                            window_start,
                            window_end,
                            error.requested_at,
                        )
                    )
                    continue
                raise
            records = _validate(response.json())  # SGS documents text values: numbers are invalid
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
        return pages
