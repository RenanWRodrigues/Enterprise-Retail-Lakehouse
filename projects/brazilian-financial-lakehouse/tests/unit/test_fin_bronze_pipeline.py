"""Incremental Bronze pipeline: failure scenarios, metrics, checkpoint and idempotency.

Scenarios simulated with the fake transport (no network): API unavailable, HTTP 429,
HTTP 500, timeout, empty payload, unexpected schema.
"""

import json
import urllib.error
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from random import Random
from typing import Any

import pytest

from financial_lakehouse.clients import IbgeAgregadosClient, PtaxClient, SgsClient
from financial_lakehouse.clients.http import HttpClient, HttpResponse, RequestMetrics, RetryPolicy
from financial_lakehouse.ingestion import (
    IncrementalIngestion,
    IngestionFailedError,
    IngestionReport,
    IngestionSettings,
    indicator,
)
from financial_lakehouse.storage import InMemoryBronzeSink, LandingStore, WatermarkStore

Respond = Callable[..., HttpResponse]
Payload = Callable[[str], bytes]
NOW = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)
IGPM, EUR, IPCA = indicator("igpm_monthly"), indicator("eur_brl_ptax"), indicator("ipca_monthly")


def _ticks() -> Iterator[float]:
    value = 0.0
    while True:
        yield value
        value += 0.5


class _Harness:
    def __init__(self, transport: Any, root: Path, sleeps: list[float]) -> None:
        self.metrics = RequestMetrics()
        http = HttpClient(
            transport=transport,
            policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=4.0),
            sleep=sleeps.append,
            rng=Random(0),
            clock=lambda: NOW,
            metrics=self.metrics,
        )
        self.bronze = InMemoryBronzeSink()
        self.checkpoints = WatermarkStore(root)
        ticks = _ticks()
        self.pipeline = IncrementalIngestion(
            sgs=SgsClient(http),
            ptax=PtaxClient(http),
            ibge=IbgeAgregadosClient(http),
            store=LandingStore(root),
            bronze=self.bronze,
            checkpoints=self.checkpoints,
            settings=IngestionSettings(backfill_start=date(2025, 1, 1), lookback_days=30),
            http_metrics=self.metrics,
            clock=lambda: NOW,
            monotonic=lambda: next(ticks),
        )

    def run(self, *indicators: Any) -> IngestionReport:
        return self.pipeline.run(list(indicators))


@pytest.fixture
def harness(transport: Any, tmp_path: Path, sleeps: list[float]) -> _Harness:
    return _Harness(transport, tmp_path, sleeps)


def _sgs_ok(transport: Any, respond: Respond, payload: Payload) -> None:
    transport.route(
        lambda u: "bcdata.sgs" in u, lambda u: respond(200, payload("sgs_189_2025q1.json"))
    )


# --- happy path, columns, metrics, checkpoint -----------------------------------------------


def test_bronze_rows_metrics_and_checkpoint(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    _sgs_ok(transport, respond, payload)

    report = harness.run(IGPM)

    rows = harness.bronze.records()
    assert {(r.source, r.series_id, r.reference_date, r.value) for r in rows} == {
        ("bcb_sgs", "189", date(2025, 1, 1), "0.27"),
        ("bcb_sgs", "189", date(2025, 2, 1), "1.06"),
        ("bcb_sgs", "189", date(2025, 3, 1), "-0.34"),
    }
    assert {(r.request_timestamp, r.ingestion_timestamp) for r in rows} == {(NOW, NOW)}
    assert all(Path(r.raw_file).read_bytes() == payload("sgs_189_2025q1.json") for r in rows)
    metrics = report.metrics
    assert (metrics.requests, metrics.successful_requests, metrics.failed_requests) == (1, 1, 0)
    assert (metrics.records_received, metrics.records_ingested) == (3, 3)
    assert metrics.execution_time_seconds > 0
    assert harness.checkpoints.get("igpm_monthly") == "2025-03-01"
    saved = json.loads(
        (tmp_path / "_metrics" / f"{report.run_id}.json").read_text(encoding="utf-8")
    )
    assert saved["records_ingested"] == 3 and saved["requests"] == 1


def test_second_run_is_incremental_and_idempotent(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload
) -> None:
    _sgs_ok(transport, respond, payload)
    harness.run(IGPM)

    second = harness.run(IGPM)

    window = transport.calls[-1].replace("%2F", "/")
    assert "dataInicial=30/01/2025" in window  # checkpoint 2025-03-01 minus 30 days lookback
    assert second.metrics.records_received == 3
    assert second.metrics.records_ingested == 0  # same content => no new Bronze rows
    assert harness.bronze.count() == 3


def test_revised_values_are_kept_as_history(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload
) -> None:
    bodies = iter(
        [
            payload("sgs_189_2025q1.json"),
            payload("sgs_189_2025q1.json").replace(b'"1.06"', b'"1.10"'),
        ]
    )
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: respond(200, next(bodies)))

    harness.run(IGPM)
    revised = harness.run(IGPM)

    assert revised.metrics.records_ingested == 1
    february = sorted(
        r.value for r in harness.bronze.records() if r.reference_date == date(2025, 2, 1)
    )
    assert february == ["1.06", "1.10"]


# --- failure scenarios ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "first",
    ["429", "500", "timeout"],
)
def test_transient_failures_are_retried_and_counted(
    harness: _Harness,
    transport: Any,
    respond: Respond,
    payload: Payload,
    first: str,
    sleeps: list[float],
) -> None:
    failure = {
        "429": respond(429, headers={"Retry-After": "2"}),
        "500": respond(500),
        "timeout": TimeoutError("read timed out"),
    }[first]
    transport.outcomes.extend([failure, respond(200, payload("sgs_189_2025q1.json"))])

    report = harness.run(IGPM)

    assert report.results[0].status == "succeeded"
    assert (
        report.metrics.requests,
        report.metrics.successful_requests,
        report.metrics.failed_requests,
    ) == (2, 1, 1)
    assert len(sleeps) == 1
    assert harness.bronze.count() == 3


def test_api_unavailable_fails_in_a_controlled_and_visible_way(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload, tmp_path: Path
) -> None:
    transport.route(lambda u: "PTAX" in u, lambda u: urllib.error.URLError("connection refused"))
    _sgs_ok(transport, respond, payload)

    with pytest.raises(IngestionFailedError) as caught:
        harness.run(EUR, IGPM)

    report = caught.value.report
    status = {r.indicator_id: r.status for r in report.results}
    assert status == {"eur_brl_ptax": "failed", "igpm_monthly": "succeeded"}  # others continue
    assert "connection refused" in str(caught.value)
    assert report.metrics.failed_requests == 3  # every attempt is counted
    assert harness.checkpoints.get("eur_brl_ptax") is None  # checkpoint never moves on failure
    assert harness.bronze.count() == 3  # the healthy indicator was still loaded
    manifest = json.loads(
        (tmp_path / "_manifests" / f"{report.run_id}.json").read_text(encoding="utf-8")
    )
    assert any(r["status"] == "failed" and r["error"] for r in manifest["results"])


def test_timeouts_that_never_recover_fail_the_run(harness: _Harness, transport: Any) -> None:
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: TimeoutError("read timed out"))
    with pytest.raises(IngestionFailedError, match="TimeoutError"):
        harness.run(IGPM)


def test_unexpected_schema_fails_without_loading_partial_data(
    harness: _Harness, transport: Any, respond: Respond
) -> None:
    transport.route(
        lambda u: "bcdata.sgs" in u, lambda u: respond(200, b'[{"date": "01/01/2025", "value": 1}]')
    )

    with pytest.raises(IngestionFailedError, match="ResponseSchemaError"):
        harness.run(IGPM)

    assert harness.bronze.count() == 0
    assert harness.checkpoints.get("igpm_monthly") is None


@pytest.mark.parametrize("source", ["sgs_not_found", "ibge_empty"])
def test_empty_payload_is_reported_not_silenced_and_not_a_failure(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload, source: str
) -> None:
    if source == "sgs_not_found":
        transport.route(
            lambda u: "bcdata.sgs" in u, lambda u: respond(404, payload("sgs_not_found.json"))
        )
        item = IGPM
    else:
        transport.route(
            lambda u: u.endswith("/periodos"), lambda u: respond(200, b'[{"id": "202501"}]')
        )
        transport.route(lambda u: "/variaveis/" in u, lambda u: respond(200, b"[]"))
        item = IPCA

    report = harness.run(item)

    [result] = report.results
    assert result.status == "no_data"
    assert (result.records_received, result.records_ingested) == (0, 0)
    assert harness.checkpoints.get(item.indicator_id) is None


def test_bronze_write_failure_keeps_the_checkpoint(
    harness: _Harness, transport: Any, respond: Respond, payload: Payload
) -> None:
    _sgs_ok(transport, respond, payload)

    def broken(records: object) -> int:
        raise OSError("disk full")

    harness.bronze.merge = broken  # type: ignore[method-assign]

    with pytest.raises(IngestionFailedError, match="disk full"):
        harness.run(IGPM)
    assert harness.checkpoints.get("igpm_monthly") is None


def test_report_is_returned_without_raising_when_asked(harness: _Harness, transport: Any) -> None:
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: TimeoutError("down"))
    report = harness.pipeline.run([IGPM], raise_on_failure=False)
    assert report.results[0].status == "failed"
    assert report.failed


def test_quarterly_backfill_uses_quarter_period_ids(
    harness: _Harness, transport: Any, respond: Respond
) -> None:
    """Regression (live run 2026-10-06): YYYYMM was compared with IBGE quarter ids YYYYQQ."""
    transport.route(
        lambda u: u.endswith("/periodos"),
        lambda u: respond(200, b'[{"id": "202404"}, {"id": "202501"}, {"id": "202502"}]'),
    )
    transport.route(lambda u: "/variaveis/" in u, lambda u: respond(200, b"[]"))

    harness.run(indicator("gdp_growth_yoy"))  # backfill_start = 2025-01-01 -> quarter 202501

    [values_call] = [c for c in transport.calls if "/variaveis/" in c]
    assert "/periodos/202501|202502/" in values_call.replace("%7C", "|")
