"""Raw ingestion: transformation to text observations, landing storage, watermarks, pipeline."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from financial_lakehouse.clients import IbgeAgregadosClient, PtaxClient, SgsClient
from financial_lakehouse.clients.base import RawPage
from financial_lakehouse.clients.http import HttpClient, HttpResponse, RequestMetrics
from financial_lakehouse.ingestion import (
    INDICATORS,
    IncrementalIngestion,
    IngestionSettings,
    indicator,
)
from financial_lakehouse.storage import InMemoryBronzeSink, LandingStore, WatermarkStore
from financial_lakehouse.transformation import to_raw_observations

Respond = Callable[..., HttpResponse]
Payload = Callable[[str], bytes]
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def _page(source: str, body: bytes, records: tuple[dict[str, str], ...]) -> RawPage:
    return RawPage(source=source, url="https://x.invalid", body=body, records=records)


# --- transformation (raw -> text observations) ---------------------------------------------


def test_sgs_records_become_text_observations() -> None:
    page = _page("bcb_sgs", b"[]", ({"data": "01/02/2025", "valor": "1.06"},))
    [obs] = to_raw_observations(indicator("igpm_monthly"), page)
    assert (obs.indicator_id, obs.reference, obs.value) == ("igpm_monthly", "01/02/2025", "1.06")


def test_ptax_numbers_keep_their_exact_decimal_text(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload
) -> None:
    transport.outcomes.append(respond(200, payload("ptax_eur_2025_01_02.json")))
    [page] = PtaxClient(http).fetch("EUR", date(2025, 1, 2), date(2025, 1, 3))

    first = to_raw_observations(indicator("eur_brl_ptax"), page)[0]

    assert first.value == "6.42500"  # not 6.425: no float round-trip
    assert first.reference == "2025-01-02 10:10:25.056"
    assert first.attributes == {
        "cotacaoCompra": "6.42320",
        "paridadeCompra": "1.03180",
        "paridadeVenda": "1.03200",
        "tipoBoletim": "Abertura",
    }


def test_ibge_records_become_text_observations() -> None:
    page = _page(
        "ibge_agregados", b"[]", ({"periodo": "202401", "valor": "0.42", "localidade": "1"},)
    )
    [obs] = to_raw_observations(indicator("ipca_monthly"), page)
    assert (obs.reference, obs.value, obs.attributes) == ("202401", "0.42", {"localidade": "1"})


# --- storage -------------------------------------------------------------------------------


def test_landing_store_keeps_the_raw_bytes_and_their_checksum(tmp_path: Path) -> None:
    body = b'[{"data":"01/01/2025","valor":"0.27"}]'
    landed = LandingStore(tmp_path).write_page(
        run_id="20261005T120000Z",
        ingestion_date=date(2026, 10, 5),
        indicator_id="igpm_monthly",
        source="bcb_sgs",
        page_index=0,
        page=_page("bcb_sgs", body, ()),
    )
    assert (
        landed.path
        == tmp_path / "bcb_sgs/igpm_monthly/ingestion_date=2026-10-05/20261005T120000Z-0000.json"
    )
    assert landed.path.read_bytes() == body
    assert landed.sha256 == hashlib.sha256(body).hexdigest()
    assert landed.size_bytes == len(body)


def test_watermarks_persist_between_instances(tmp_path: Path) -> None:
    WatermarkStore(tmp_path).set("selic_target", "2026-10-02")
    assert WatermarkStore(tmp_path).get("selic_target") == "2026-10-02"
    assert WatermarkStore(tmp_path).get("unknown") is None


# --- pipeline ------------------------------------------------------------------------------


def _route_everything(transport: Any, respond: Respond, payload: Payload) -> None:
    transport.route(
        lambda u: "bcdata.sgs" in u, lambda u: respond(200, payload("sgs_189_2025q1.json"))
    )
    transport.route(
        lambda u: "PTAX" in u, lambda u: respond(200, payload("ptax_eur_2025_01_02.json"))
    )
    transport.route(
        lambda u: u.endswith("/periodos"),
        lambda u: respond(200, payload("ibge_6381_periodos.json")),
    )
    transport.route(
        lambda u: "/variaveis/" in u, lambda u: respond(200, payload("ibge_1737_v63.json"))
    )


def _pipeline(
    http: HttpClient, root: Path, settings: IngestionSettings | None = None
) -> IncrementalIngestion:
    return IncrementalIngestion(
        sgs=SgsClient(http),
        ptax=PtaxClient(http),
        ibge=IbgeAgregadosClient(http),
        store=LandingStore(root),
        bronze=InMemoryBronzeSink(),
        checkpoints=WatermarkStore(root),
        settings=settings or IngestionSettings(backfill_start=date(2012, 1, 1)),
        http_metrics=RequestMetrics(),
        clock=lambda: NOW,
    )


def test_full_run_lands_every_indicator_and_advances_watermarks(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    _route_everything(transport, respond, payload)

    report = _pipeline(http, tmp_path).run()

    assert {r.indicator_id for r in report.results} == {i.indicator_id for i in INDICATORS}
    assert all(r.status == "succeeded" and r.pages > 0 for r in report.results), report.results
    manifest = json.loads(
        (tmp_path / "_manifests" / f"{report.run_id}.json").read_text(encoding="utf-8")
    )
    assert manifest["run_id"] == report.run_id == "20261005T120000Z"
    assert all(f["sha256"] and f["bytes"] > 0 for f in manifest["files"])
    watermarks = WatermarkStore(tmp_path)
    assert watermarks.get("igpm_monthly") == "2025-03-01"
    assert watermarks.get("eur_brl_ptax") == "2025-01-02"
    assert watermarks.get("ipca_monthly") == "202402"
    observations = list((tmp_path / "bcb_sgs" / "igpm_monthly").rglob("*.observations.jsonl"))
    assert len(observations) == 1


def test_a_failing_source_does_not_stop_the_run_nor_advance_its_watermark(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    transport.route(lambda u: "PTAX" in u, lambda u: respond(503))
    _route_everything(transport, respond, payload)

    report = _pipeline(http, tmp_path).run(raise_on_failure=False)

    status = {r.indicator_id: r.status for r in report.results}
    assert status["eur_brl_ptax"] == "failed"
    assert {s for k, s in status.items() if k != "eur_brl_ptax"} == {"succeeded"}
    assert WatermarkStore(tmp_path).get("eur_brl_ptax") is None
    failed = next(r for r in report.results if r.indicator_id == "eur_brl_ptax")
    assert "503" in str(failed.error)


def test_incremental_run_starts_from_watermark_minus_lookback(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    _route_everything(transport, respond, payload)
    WatermarkStore(tmp_path).set("selic_target", "2026-09-30")
    settings = IngestionSettings(backfill_start=date(2015, 1, 1), lookback_days=30)

    _pipeline(http, tmp_path, settings).run(indicators=[indicator("selic_target")])

    [call] = [c for c in transport.calls if "bcdata.sgs.432" in c]
    assert "dataInicial=31%2F08%2F2026" in call or "dataInicial=31/08/2026" in call
    assert "dataFinal=05%2F10%2F2026" in call or "dataFinal=05/10/2026" in call


def test_ibge_incremental_run_rereads_recent_periods(
    http: HttpClient, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    _route_everything(transport, respond, payload)
    WatermarkStore(tmp_path).set("unemployment_rate", "201205")
    settings = IngestionSettings(backfill_start=date(2012, 1, 1), lookback_periods=1)

    _pipeline(http, tmp_path, settings).run(indicators=[indicator("unemployment_rate")])

    [values_call] = [c for c in transport.calls if "/variaveis/" in c]
    assert "/periodos/201204|201205/" in values_call.replace("%7C", "|")


def test_unknown_indicator_is_rejected() -> None:
    with pytest.raises(KeyError):
        indicator("bitcoin")
