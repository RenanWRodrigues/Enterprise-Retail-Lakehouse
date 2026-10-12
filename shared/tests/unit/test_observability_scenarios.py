"""The five run scenarios, read back from what the sink persisted (docs/OBSERVABILITY.md §1).

successful run, failed run, zero records, rejected records, quality failure. The local JSON
Lines sink is used, so the assertions are on the stored ``pipeline_runs`` and
``data_quality_results`` rows, not on objects in memory. No Spark.
"""

import json
from collections.abc import Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from lakehouse_common.observability import (
    PIPELINE_RUNS_COLUMNS,
    JsonLinesObservabilitySink,
    Layer,
    RunTracker,
    track_run,
)
from lakehouse_common.quality import (
    CheckResult,
    CriticalQualityError,
    Dimension,
    Severity,
    Status,
)

START = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
REQUESTED = (
    "pipeline_name",
    "run_id",
    "project",
    "layer",
    "table",
    "started_at",
    "finished_at",
    "duration_seconds",
    "input_rows",
    "output_rows",
    "rejected_rows",
    "status",
    "error_message",
)


def _check(status: Status, *, failed: int = 0, checked: int = 10) -> CheckResult:
    severity = Severity.ERROR if status is Status.PASS else Severity(status.value)
    return CheckResult(
        check_name="valid_amount",
        check_type="custom_condition",
        table_name="silver_orders",
        column_name="amount",
        dimension=Dimension.VALIDITY,
        status=status,
        severity=severity,
        rows_checked=checked,
        failed_rows=failed,
        failure_rate=failed / checked if checked else 0.0,
        execution_timestamp=START,
    )


@pytest.fixture
def sink(tmp_path: Path) -> JsonLinesObservabilitySink:
    return JsonLinesObservabilitySink(tmp_path / "observability")


def _step(sink: JsonLinesObservabilitySink) -> AbstractContextManager[RunTracker]:
    clock: Iterator[datetime] = iter([START, START + timedelta(seconds=4)])
    ticks: Iterator[float] = iter([10.0, 14.25])
    return track_run(
        pipeline_name="retail_silver",
        run_id="20261007T120000Z",
        project="retail",
        layer=Layer.SILVER,
        table="retail.silver.orders",
        sink=sink,
        clock=lambda: next(clock),
        monotonic=lambda: next(ticks),
        alert=lambda message: None,
    )


def _stored(sink: JsonLinesObservabilitySink, name: str) -> list[dict[str, Any]]:
    path = sink.root / f"{name}.jsonl"
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def test_pipeline_runs_have_the_requested_columns_first() -> None:
    assert PIPELINE_RUNS_COLUMNS[: len(REQUESTED)] == REQUESTED


def test_successful_run(sink: JsonLinesObservabilitySink) -> None:
    with _step(sink) as step:
        step.record_counts(input_rows=100, output_rows=100, rejected_rows=0)
        step.record_quality([_check(Status.PASS)])

    [run] = _stored(sink, "pipeline_runs")
    assert {k: run[k] for k in REQUESTED} == {
        "pipeline_name": "retail_silver",
        "run_id": "20261007T120000Z",
        "project": "retail",
        "layer": "silver",
        "table": "retail.silver.orders",
        "started_at": "2026-10-07T12:00:00+00:00",
        "finished_at": "2026-10-07T12:00:04+00:00",
        "duration_seconds": 4.25,
        "input_rows": 100,
        "output_rows": 100,
        "rejected_rows": 0,
        "status": "SUCCEEDED",
        "error_message": None,
    }
    [check] = _stored(sink, "data_quality_results")
    assert (check["run_id"], check["status"]) == ("20261007T120000Z", "PASS")


def test_failed_run(sink: JsonLinesObservabilitySink) -> None:
    with pytest.raises(FileNotFoundError), _step(sink) as step:
        step.record_counts(input_rows=100)
        raise FileNotFoundError("landing/orders_initial.jsonl")

    [run] = _stored(sink, "pipeline_runs")
    assert run["status"] == "FAILED"
    assert run["error_message"] == "FileNotFoundError: landing/orders_initial.jsonl"
    assert (run["input_rows"], run["output_rows"]) == (100, None)  # never counted: not zero
    assert run["finished_at"] is not None and run["duration_seconds"] == 4.25


def test_zero_records(sink: JsonLinesObservabilitySink) -> None:
    """An empty batch is a successful run with explicit zeros, distinguishable from unknown."""
    with _step(sink) as step:
        step.record_counts(input_rows=0, output_rows=0, rejected_rows=0)

    [run] = _stored(sink, "pipeline_runs")
    assert (run["status"], run["input_rows"], run["output_rows"], run["rejected_rows"]) == (
        "SUCCEEDED",
        0,
        0,
        0,
    )
    assert not (sink.root / "data_quality_results.jsonl").exists()  # no checks ran


def test_rejected_records(sink: JsonLinesObservabilitySink) -> None:
    """ERROR rows go to quarantine; the good rows are written and the run alerts."""
    with _step(sink) as step:
        step.record_quality([_check(Status.ERROR, failed=3)])
        step.record_counts(input_rows=10, output_rows=7, rejected_rows=3)

    [run] = _stored(sink, "pipeline_runs")
    assert (run["status"], run["output_rows"], run["rejected_rows"]) == (
        "SUCCEEDED_WITH_ALERTS",
        7,
        3,
    )
    [check] = _stored(sink, "data_quality_results")
    assert (check["status"], check["failed_rows"], check["rows_checked"]) == ("ERROR", 3, 10)


def test_quality_failure(sink: JsonLinesObservabilitySink) -> None:
    """A CRITICAL check stops the step before it writes; run and check are still stored."""
    written: list[str] = []
    with pytest.raises(CriticalQualityError), _step(sink) as step:
        step.record_counts(input_rows=10)
        step.record_quality([_check(Status.CRITICAL, failed=10)])
        written.append("silver_orders")  # never reached

    assert written == []
    [run] = _stored(sink, "pipeline_runs")
    assert run["status"] == "FAILED"
    assert run["error_message"].startswith("CriticalQualityError")
    [check] = _stored(sink, "data_quality_results")
    assert check["status"] == "CRITICAL"


def test_runs_are_appended_never_rewritten(sink: JsonLinesObservabilitySink) -> None:
    for _ in range(2):
        with _step(sink) as step:
            step.record_counts(input_rows=1, output_rows=1)
    assert len(_stored(sink, "pipeline_runs")) == 2
