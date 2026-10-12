"""Incremental Bronze, scenario by scenario (docs/INCREMENTAL_STRATEGY.md §Cenários).

first load, incremental load, rerun, duplicate response, late historical revision, empty
response and API failure, each run through the whole pipeline (fake transport, no network).
Every run gets a later clock, as in production: ``request_timestamp``/``ingestion_timestamp``
order the deliveries.
"""

import json
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
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
from financial_lakehouse.transformation import latest_deliveries
from lakehouse_common.observability import InMemoryObservabilitySink

Respond = Callable[..., HttpResponse]
FIRST_RUN = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)
IGPM, IPCA = indicator("igpm_monthly"), indicator("ipca_monthly")
BACKFILL = date(2025, 1, 1)
Q1 = [("01/01/2025", "0.27"), ("01/02/2025", "1.06"), ("01/03/2025", "-0.34")]


def sgs_body(*observations: tuple[str, str]) -> bytes:
    return json.dumps([{"data": d, "valor": v} for d, v in observations]).encode()


class _Clock:
    """One instant per run (one day apart): HTTP client and pipeline read the same clock."""

    def __init__(self) -> None:
        self.now = FIRST_RUN

    def __call__(self) -> datetime:
        return self.now

    def next_run(self) -> None:
        self.now += timedelta(days=1)


class _Scenario:
    def __init__(self, transport: Any, root: Path) -> None:
        self.clock = _Clock()
        self.metrics = RequestMetrics()
        http = HttpClient(
            transport=transport,
            policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=4.0),
            sleep=lambda _: None,
            rng=Random(0),
            clock=self.clock,
            metrics=self.metrics,
        )
        self.root = root
        self.bronze = InMemoryBronzeSink()
        self.observed = InMemoryObservabilitySink()
        self.checkpoints = WatermarkStore(root)
        ticks = (0.25 * i for i in range(10_000))
        self.pipeline = IncrementalIngestion(
            sgs=SgsClient(http),
            ptax=PtaxClient(http),
            ibge=IbgeAgregadosClient(http),
            store=LandingStore(root),
            bronze=self.bronze,
            checkpoints=self.checkpoints,
            settings=IngestionSettings(backfill_start=BACKFILL, lookback_days=60),
            http_metrics=self.metrics,
            clock=self.clock,
            monotonic=lambda: next(ticks),
            observer=self.observed,
        )

    def run(self, *indicators: Any, reprocess_from: date | None = None) -> IngestionReport:
        report = self.pipeline.run(list(indicators), reprocess_from=reprocess_from)
        self.clock.next_run()
        return report

    def current_values(self) -> dict[date, list[str]]:
        """Value(s) of the latest delivery per reference date — what Silver will pick."""
        latest = latest_deliveries(self.bronze.records())
        return {
            group[0].reference_date: sorted(r.value for r in group) for group in latest.values()
        }


@pytest.fixture
def scenario(transport: Any, tmp_path: Path) -> _Scenario:
    return _Scenario(transport, tmp_path)


def serve(transport: Any, respond: Respond, bodies: Iterator[bytes]) -> None:
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: respond(200, next(bodies)))


def window_start(transport: Any) -> str:
    query = str(transport.calls[-1]).replace("%2F", "/")
    return query.split("dataInicial=")[1].split("&")[0]


# --- first load ----------------------------------------------------------------------------


def test_first_load_reads_from_backfill_start_and_lands_everything(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    serve(transport, respond, iter([sgs_body(*Q1)]))

    report = scenario.run(IGPM)

    assert window_start(transport) == "01/01/2025"  # no checkpoint -> backfill_start
    rows = scenario.bronze.records()
    assert sorted((r.reference_date, r.value) for r in rows) == [
        (date(2025, 1, 1), "0.27"),
        (date(2025, 2, 1), "1.06"),
        (date(2025, 3, 1), "-0.34"),
    ]
    assert {(r.source, r.series_id) for r in rows} == {("bcb_sgs", "189")}
    assert {(r.request_timestamp, r.ingestion_timestamp) for r in rows} == {(FIRST_RUN, FIRST_RUN)}
    assert {Path(r.raw_file).read_bytes() for r in rows} == {sgs_body(*Q1)}  # raw kept exactly
    assert (report.metrics.records_received, report.metrics.records_ingested) == (3, 3)
    assert scenario.checkpoints.get("igpm_monthly") == "2025-03-01"


# --- incremental load ----------------------------------------------------------------------


def test_incremental_load_rereads_only_the_lookback_and_inserts_only_new_rows(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    april = ("01/04/2025", "0.24")
    serve(transport, respond, iter([sgs_body(*Q1), sgs_body(Q1[2], april)]))
    scenario.run(IGPM)

    second = scenario.run(IGPM)

    assert window_start(transport) == "01/01/2025"  # 2025-03-01 - 60 days, floored at backfill
    assert second.metrics.records_received == 2  # March (lookback) + April (new)
    assert second.metrics.records_ingested == 1
    assert scenario.bronze.count() == 4
    assert scenario.checkpoints.get("igpm_monthly") == "2025-04-01"


def test_incremental_window_starts_at_checkpoint_minus_lookback(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    late = [("01/06/2025", "0.10"), ("01/07/2025", "0.20")]
    serve(transport, respond, iter([sgs_body(*late), sgs_body(*late)]))
    scenario.run(IGPM)

    scenario.run(IGPM)

    assert window_start(transport) == "02/05/2025"  # 2025-07-01 minus 60 days, not backfill_start


# --- rerun ---------------------------------------------------------------------------------


def test_rerun_with_the_same_response_changes_nothing(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    serve(transport, respond, iter([sgs_body(*Q1), sgs_body(*Q1)]))
    first = scenario.run(IGPM)
    before = sorted(r.record_key for r in scenario.bronze.records())

    rerun = scenario.run(IGPM)

    assert rerun.results[0].status == "succeeded"
    assert (rerun.metrics.records_received, rerun.metrics.records_ingested) == (3, 0)
    assert sorted(r.record_key for r in scenario.bronze.records()) == before
    assert scenario.checkpoints.checkpoint("igpm_monthly") == {
        "reference": "2025-03-01",
        "run_id": first.run_id,  # an unchanged checkpoint keeps the run that set it
        "updated_at": FIRST_RUN.isoformat(),
    }


# --- duplicate response --------------------------------------------------------------------


def test_duplicate_observations_in_one_response_are_stored_once(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    duplicated = sgs_body(*Q1, Q1[1])  # the API repeats February in the same payload
    serve(transport, respond, iter([duplicated, duplicated]))

    first = scenario.run(IGPM)
    again = scenario.run(IGPM)

    assert (first.metrics.records_received, first.metrics.records_ingested) == (4, 3)
    assert (again.metrics.records_received, again.metrics.records_ingested) == (4, 0)
    assert scenario.bronze.count() == 3


# --- late historical revision --------------------------------------------------------------


def test_revision_inside_the_lookback_is_kept_as_a_new_version(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    revised = [Q1[0], Q1[1], ("01/03/2025", "-0.30")]
    serve(transport, respond, iter([sgs_body(*Q1), sgs_body(*revised)]))
    scenario.run(IGPM)

    report = scenario.run(IGPM)

    assert report.metrics.records_ingested == 1
    march = sorted(r.value for r in scenario.bronze.records() if r.reference.endswith("03/2025"))
    assert march == ["-0.30", "-0.34"]  # history preserved
    assert scenario.current_values()[date(2025, 3, 1)] == ["-0.30"]


def test_revision_older_than_the_lookback_needs_an_explicit_reprocess_window(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    """A normal run never asks for January again; ``reprocess_from`` re-reads that window."""
    year = [*Q1, ("01/04/2025", "0.24"), ("01/05/2025", "-0.49"), ("01/06/2025", "-1.67")]
    revised = [("01/01/2025", "0.29"), *year[1:]]
    recent = year[4:]  # what the API returns for the lookback window

    def answer(url: str) -> HttpResponse:
        start = url.replace("%2F", "/").split("dataInicial=")[1].split("&")[0]
        if start == "01/01/2025":
            return respond(200, sgs_body(*(year if scenario.bronze.count() == 0 else revised)))
        return respond(200, sgs_body(*recent))

    transport.route(lambda u: "bcdata.sgs" in u, answer)
    scenario.run(IGPM)
    normal = scenario.run(IGPM)
    assert window_start(transport) == "02/04/2025"  # 2025-06-01 minus 60 days
    assert normal.metrics.records_ingested == 0
    assert scenario.current_values()[date(2025, 1, 1)] == ["0.27"]  # revision not seen yet

    sweep = scenario.run(IGPM, reprocess_from=BACKFILL)

    assert window_start(transport) == "01/01/2025"
    assert (sweep.metrics.records_received, sweep.metrics.records_ingested) == (6, 1)
    assert scenario.current_values()[date(2025, 1, 1)] == ["0.29"]
    assert scenario.checkpoints.get("igpm_monthly") == "2025-06-01"  # never moves backwards


def test_a_revision_back_to_an_earlier_value_becomes_the_current_version(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    """Regression: A -> B -> A. The content key of A already existed, so the return to A was
    dropped and Silver kept B, a value the source had withdrawn.
    """
    corrected = [Q1[0], ("01/02/2025", "1.10"), Q1[2]]
    bodies = [sgs_body(*Q1), sgs_body(*corrected), sgs_body(*Q1), sgs_body(*Q1)]
    serve(transport, respond, iter(bodies))
    scenario.run(IGPM)
    scenario.run(IGPM)

    reverted = scenario.run(IGPM)
    again = scenario.run(IGPM)

    assert reverted.metrics.records_ingested == 1
    february = [r for r in scenario.bronze.records() if r.reference == "01/02/2025"]
    assert sorted(r.value for r in february) == ["1.06", "1.06", "1.10"]
    assert len({r.record_key for r in february}) == 3  # each delivery is its own version
    assert scenario.current_values()[date(2025, 2, 1)] == ["1.06"]
    assert again.metrics.records_ingested == 0  # and the revert itself is idempotent


# --- empty response ------------------------------------------------------------------------


def test_empty_response_after_a_load_keeps_data_and_checkpoint(
    scenario: _Scenario, transport: Any, respond: Respond, payload: Callable[[str], bytes]
) -> None:
    bodies = iter([respond(200, sgs_body(*Q1)), respond(404, payload("sgs_not_found.json"))])
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: next(bodies))
    scenario.run(IGPM)

    empty = scenario.run(IGPM)

    [result] = empty.results
    assert result.status == "no_data"
    assert (result.records_received, result.records_ingested) == (0, 0)
    assert scenario.bronze.count() == 3  # nothing is deleted or rewritten
    assert scenario.checkpoints.get("igpm_monthly") == "2025-03-01"
    assert empty.metrics.indicators_no_data == 1
    assert empty.metrics.indicators_failed == 0


def test_ibge_with_no_new_period_makes_no_values_request(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    transport.route(lambda u: u.endswith("/periodos"), lambda u: respond(200, b"[]"))

    report = scenario.run(IPCA)

    assert report.results[0].status == "no_data"
    assert not [c for c in transport.calls if "/variaveis/" in c]


# --- API failure ---------------------------------------------------------------------------


def test_api_failure_loads_nothing_and_the_next_run_repeats_the_window(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    transport.outcomes.extend(
        [respond(500), respond(503), respond(502), respond(200, sgs_body(*Q1))]
    )

    with pytest.raises(IngestionFailedError, match="RetriesExhaustedError") as caught:
        scenario.run(IGPM)

    failed = caught.value.report
    assert (failed.metrics.requests, failed.metrics.failed_requests) == (3, 3)
    assert failed.metrics.indicators_failed == 1
    assert scenario.bronze.count() == 0
    assert scenario.checkpoints.get("igpm_monthly") is None
    scenario.clock.next_run()

    recovered = scenario.run(IGPM)

    assert window_start(transport) == "01/01/2025"  # same window again: nothing was skipped
    assert recovered.metrics.records_ingested == 3
    assert recovered.metrics.indicators_succeeded == 1


# --- observability -------------------------------------------------------------------------


def test_run_metrics_count_requests_records_duration_and_indicator_outcomes(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    transport.route(lambda u: u.endswith("/periodos"), lambda u: respond(200, b"[]"))
    transport.outcomes.extend(
        [respond(429, headers={"Retry-After": "1"}), respond(200, sgs_body(*Q1))]
    )

    report = scenario.run(IGPM, IPCA)

    metrics = report.metrics
    assert (metrics.requests, metrics.successful_requests, metrics.failed_requests) == (3, 2, 1)
    assert (metrics.records_received, metrics.records_ingested) == (3, 3)
    assert (metrics.indicators_succeeded, metrics.indicators_no_data) == (1, 1)
    assert metrics.indicators_failed == 0
    assert metrics.execution_time_seconds > 0
    saved = json.loads((scenario.root / "_metrics" / f"{report.run_id}.json").read_text("utf-8"))
    assert saved["indicators_succeeded"] == 1 and saved["requests"] == 3
    manifest = json.loads(
        (scenario.root / "_manifests" / f"{report.run_id}.json").read_text("utf-8")
    )
    assert manifest["reprocess_from"] is None


# --- pipeline_runs (docs/OBSERVABILITY.md) -------------------------------------------------


def test_each_run_is_one_pipeline_runs_row(
    scenario: _Scenario, transport: Any, respond: Respond
) -> None:
    serve(transport, respond, iter([sgs_body(*Q1), sgs_body(*Q1)]))
    scenario.run(IGPM)
    scenario.run(IGPM)

    first, rerun = scenario.observed.runs
    assert (first.pipeline_name, first.project, first.layer, first.table) == (
        "financial_bronze",
        "financial",
        "bronze",
        "memory:bronze_indicator_observations",
    )
    assert (first.run_id, first.started_at) == ("20261006T140000Z", FIRST_RUN)
    assert (first.status, first.input_rows, first.output_rows, first.rejected_rows) == (
        "SUCCEEDED",
        3,
        3,
        0,
    )
    assert (rerun.input_rows, rerun.output_rows) == (3, 0)  # nothing new: zero rows written


def test_empty_response_is_a_zero_records_run(
    scenario: _Scenario, transport: Any, respond: Respond, payload: Callable[[str], bytes]
) -> None:
    transport.route(
        lambda u: "bcdata.sgs" in u, lambda u: respond(404, payload("sgs_not_found.json"))
    )

    scenario.run(IGPM)

    [run] = scenario.observed.runs
    assert (run.status, run.input_rows, run.output_rows) == ("SUCCEEDED", 0, 0)


@pytest.mark.parametrize("raise_on_failure", [True, False])
def test_a_failed_indicator_makes_the_run_failed_with_partial_counts(
    scenario: _Scenario, transport: Any, respond: Respond, raise_on_failure: bool
) -> None:
    transport.route(lambda u: "bcdata.sgs" in u, lambda u: respond(200, sgs_body(*Q1)))
    transport.route(lambda u: "/periodos" in u, lambda u: respond(500))

    if raise_on_failure:
        with pytest.raises(IngestionFailedError):
            scenario.pipeline.run([IGPM, IPCA])
    else:
        report = scenario.pipeline.run([IGPM, IPCA], raise_on_failure=False)
        assert [r.status for r in report.results] == ["succeeded", "failed"]

    [run] = scenario.observed.runs
    assert run.status == "FAILED"  # also when the caller asked for the report instead
    assert run.error_message is not None and "ipca_monthly" in run.error_message
    assert (run.input_rows, run.output_rows) == (3, 3)  # the healthy indicator was loaded


# --- backfill boundary ---------------------------------------------------------------------


def test_backfill_must_start_on_the_first_day_of_a_month() -> None:
    """Regression: a mid-month backfill made Gold treat a partial first month as complete.

    Gold flags only the month in progress as partial; it cannot tell a missing day from a
    holiday (no business-day calendar). A history starting on 2025-01-15 compounded the Selic of
    January over half its business days and averaged half a month of USD/BRL, silently, and
    every 12-month window containing January inherited the error.
    """
    with pytest.raises(ValueError, match="first day of a month"):
        IngestionSettings(backfill_start=date(2025, 1, 15))
    assert IngestionSettings(backfill_start=date(2025, 1, 1)).backfill_start.day == 1
