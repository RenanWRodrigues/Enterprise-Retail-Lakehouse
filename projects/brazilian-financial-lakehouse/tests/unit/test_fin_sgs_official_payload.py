"""The real SGS 432 response (Meta Selic, ``selic_target``) through the client and into Bronze.

``official_sgs_432.json`` is the raw API body captured on 2026-10-07 for 2024-2025 (shared with
``tests/transformation/test_fin_gold_official_series.py``). Unlike the short recorded payloads,
it is a full two-year daily series, so it exercises the parser at real size and shape: 731 text
records, one per calendar day (weekends included). No network.
"""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from financial_lakehouse.clients import ResponseSchemaError, SgsClient
from financial_lakehouse.clients.http import HttpClient, HttpResponse
from financial_lakehouse.ingestion import indicator
from financial_lakehouse.transformation import to_bronze_records

OFFICIAL_432 = Path(__file__).parents[1] / "transformation" / "fixtures" / "official_sgs_432.json"
DAYS_2024_2025 = 731  # 366 + 365 calendar days
INGESTED = datetime(2026, 10, 7, 13, tzinfo=UTC)


@pytest.fixture
def body() -> bytes:
    return OFFICIAL_432.read_bytes()


def _fetch(http: HttpClient, transport: Any, response: HttpResponse) -> Any:
    transport.outcomes.append(response)
    [page] = SgsClient(http).fetch(432, date(2024, 1, 1), date(2025, 12, 31))
    return page


def test_the_official_payload_is_parsed_as_delivered(
    http: HttpClient, transport: Any, respond: Any, body: bytes
) -> None:
    page = _fetch(http, transport, respond(200, body))

    assert page.body == body  # raw bytes kept for Bronze lineage
    assert len(page.records) == DAYS_2024_2025
    assert page.records[0] == {"data": "01/01/2024", "valor": "11.75"}
    assert page.records[-1] == {"data": "31/12/2025", "valor": "15.00"}
    assert all(set(r) == {"data", "valor"} for r in page.records)


def test_bronze_gets_one_record_per_calendar_day_with_the_text_value(
    http: HttpClient, transport: Any, respond: Any, body: bytes
) -> None:
    page = _fetch(http, transport, respond(200, body))

    records = to_bronze_records(
        indicator("selic_target"),
        page,
        run_id="R1",
        ingestion_timestamp=INGESTED,
        raw_file="raw/R1-0000.json",
        raw_sha256="abc",
    )

    days = [r.reference_date for r in records]
    assert days == [date(2024, 1, 1) + timedelta(days=i) for i in range(DAYS_2024_2025)]
    assert {r.series_id for r in records} == {"432"}
    assert [r.value for r in records] == [item["valor"] for item in json.loads(body)]


@pytest.mark.parametrize(
    ("corrupt", "why"),
    [
        (lambda rows: rows[400].update(valor=10.5), "a number instead of the documented text"),
        (lambda rows: rows[400].pop("valor"), "a missing key"),
        (lambda rows: rows[400].update(extra="x"), "an unexpected key"),
    ],
)
def test_a_corrupted_official_payload_is_rejected(
    http: HttpClient, transport: Any, respond: Any, body: bytes, corrupt: Any, why: str
) -> None:
    rows = json.loads(body)
    corrupt(rows)

    with pytest.raises(ResponseSchemaError):
        _fetch(http, transport, respond(200, json.dumps(rows).encode()))
