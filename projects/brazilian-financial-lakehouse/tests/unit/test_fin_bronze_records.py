"""Bronze records: required columns, reference dates per source, idempotency key."""

from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from financial_lakehouse.clients import ResponseSchemaError
from financial_lakehouse.clients.base import RawPage
from financial_lakehouse.ingestion import indicator
from financial_lakehouse.transformation import BronzeRecord, series_id_of, to_bronze_records

REQUESTED = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)
INGESTED = datetime(2026, 10, 6, 14, 0, 5, tzinfo=UTC)


def _records(indicator_id: str, records: tuple[dict[str, str], ...]) -> list[BronzeRecord]:
    page = RawPage("x", "https://x.invalid", b"[]", records, requested_at=REQUESTED)
    return to_bronze_records(
        indicator(indicator_id),
        page,
        run_id="R1",
        ingestion_timestamp=INGESTED,
        raw_file="raw/R1-0000.json",
        raw_sha256="abc",
    )


def test_record_carries_every_required_column() -> None:
    [record] = _records("igpm_monthly", ({"data": "01/02/2025", "valor": "1.06"},))
    assert record.source == "bcb_sgs"
    assert record.series_id == "189"
    assert record.reference_date == date(2025, 2, 1)
    assert record.value == "1.06"  # kept as delivered text; typed in Silver
    assert record.ingestion_timestamp == INGESTED
    assert record.request_timestamp == REQUESTED
    assert (record.raw_file, record.raw_sha256, record.run_id) == ("raw/R1-0000.json", "abc", "R1")


@pytest.mark.parametrize(
    ("indicator_id", "record", "expected"),
    [
        ("selic_daily", {"data": "31/12/2025", "valor": "0.05"}, date(2025, 12, 31)),
        (
            "ipca_monthly",
            {"periodo": "202402", "valor": "0.83", "localidade": "1"},
            date(2024, 2, 1),
        ),
        (
            "unemployment_rate",
            {"periodo": "202608", "valor": "5.6", "localidade": "1"},
            date(2026, 8, 1),
        ),
        (
            "gdp_growth_yoy",
            {"periodo": "202602", "valor": "2.1", "localidade": "1"},
            date(2026, 4, 1),
        ),
    ],
)
def test_reference_date_per_source_and_frequency(
    indicator_id: str, record: dict[str, str], expected: date
) -> None:
    assert _records(indicator_id, (record,))[0].reference_date == expected


def test_ptax_reference_date_is_the_bulletin_day() -> None:
    record = {
        "cotacaoVenda": "6.42500",
        "dataHoraCotacao": "2025-01-02 10:10:25.056",
        "cotacaoCompra": "6.42320",
        "paridadeCompra": "1",
        "paridadeVenda": "1",
        "tipoBoletim": "Abertura",
    }
    [bronze] = _records("eur_brl_ptax", (record,))
    assert (bronze.reference_date, bronze.reference, bronze.value) == (
        date(2025, 1, 2),
        "2025-01-02 10:10:25.056",
        "6.42500",
    )


@pytest.mark.parametrize(
    ("indicator_id", "record"),
    [
        ("selic_daily", {"data": "2025-12-31", "valor": "1"}),
        ("ipca_monthly", {"periodo": "202413", "valor": "1", "localidade": "1"}),
        ("gdp_growth_yoy", {"periodo": "202605", "valor": "1", "localidade": "1"}),
        ("ipca_monthly", {"periodo": "2024", "valor": "1", "localidade": "1"}),
    ],
)
def test_unexpected_references_fail_loudly(indicator_id: str, record: dict[str, str]) -> None:
    with pytest.raises(ResponseSchemaError, match=indicator_id):
        _records(indicator_id, (record,))


def test_series_ids_identify_the_official_series() -> None:
    assert series_id_of(indicator("selic_target")) == "432"
    assert series_id_of(indicator("eur_brl_ptax")) == "EUR"
    assert series_id_of(indicator("ipca_monthly")) == "1737/63"
    assert series_id_of(indicator("gdp_growth_yoy")) == "5932/6561/11255[90707]"


def test_record_key_is_stable_and_content_based() -> None:
    [first] = _records("igpm_monthly", ({"data": "01/02/2025", "valor": "1.06"},))
    [same_later] = _records("igpm_monthly", ({"data": "01/02/2025", "valor": "1.06"},))
    [revised] = _records("igpm_monthly", ({"data": "01/02/2025", "valor": "1.07"},))

    assert first.record_key == same_later.record_key  # re-read => same key => no duplicate
    assert first.record_key != revised.record_key  # revision => new row (history kept)
    assert first.record_key == replace(first, run_id="R2", ingestion_timestamp=REQUESTED).record_key
