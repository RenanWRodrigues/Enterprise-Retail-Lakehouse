"""Pipeline run tracking and the continue / alert / fail policy (no Spark needed)."""

import logging
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta

import pytest

from lakehouse_common.observability import (
    InMemoryObservabilitySink,
    Layer,
    ObservabilitySink,
    PipelineRun,
    RunStatus,
    RunTracker,
    safe_error_message,
    track_run,
)
from lakehouse_common.quality import (
    CheckResult,
    CriticalQualityError,
    Dimension,
    QualityDecision,
    Severity,
    Status,
    decide,
    dimension_scores,
)

START = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def _result(
    status: Status,
    *,
    dimension: Dimension = Dimension.VALIDITY,
    failed: int = 0,
    checked: int = 10,
    name: str = "check",
) -> CheckResult:
    severity = Severity.ERROR if status is Status.PASS else Severity(status.value)
    return CheckResult(
        check_name=name,
        check_type="custom_condition",
        table_name="t",
        column_name=None,
        dimension=dimension,
        status=status,
        severity=severity,
        rows_checked=checked,
        failed_rows=failed,
        failure_rate=failed / checked if checked else 0.0,
        execution_timestamp=START,
    )


def _ticks(step: float) -> Callable[[], float]:
    values: Iterator[float] = iter(i * step for i in range(1_000))
    return lambda: next(values)


@pytest.fixture
def sink() -> InMemoryObservabilitySink:
    return InMemoryObservabilitySink()


def _track(
    sink: ObservabilitySink, alerts: list[str] | None = None
) -> AbstractContextManager[RunTracker]:
    clock = iter([START, START + timedelta(seconds=3)])
    return track_run(
        pipeline_name="retail_silver",
        run_id="R1",
        project="retail",
        layer=Layer.SILVER,
        table="retail.silver.orders",
        sink=sink,
        clock=lambda: next(clock),
        monotonic=_ticks(1.5),
        alert=(alerts.append if alerts is not None else lambda message: None),
    )


# --- decision policy ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("statuses", "decision"),
    [
        ([Status.PASS, Status.PASS], QualityDecision.CONTINUE),
        ([], QualityDecision.CONTINUE),
        ([Status.PASS, Status.WARNING], QualityDecision.ALERT),
        ([Status.ERROR], QualityDecision.ALERT),
        ([Status.WARNING, Status.CRITICAL], QualityDecision.FAIL),
    ],
)
def test_decision_is_driven_by_the_worst_status(
    statuses: list[Status], decision: QualityDecision
) -> None:
    assert decide([_result(s) for s in statuses]) is decision


def test_dimension_scores_weight_failures_by_rows_checked() -> None:
    results = [
        _result(Status.ERROR, dimension=Dimension.COMPLETENESS, failed=1, checked=10),
        _result(Status.PASS, dimension=Dimension.COMPLETENESS, failed=0, checked=30),
        _result(Status.PASS, dimension=Dimension.UNIQUENESS, failed=0, checked=0),
    ]
    assert dimension_scores(results) == {
        Dimension.COMPLETENESS: pytest.approx(1 - 1 / 40),
        Dimension.UNIQUENESS: 1.0,  # checks ran on an empty table
        Dimension.VALIDITY: None,  # no check of this dimension
        Dimension.REFERENTIAL_INTEGRITY: None,
    }


# --- run tracking -----------------------------------------------------------------------------


def test_successful_run_records_every_metric(sink: InMemoryObservabilitySink) -> None:
    with _track(sink) as step:
        step.record_counts(input_rows=100, output_rows=95, rejected_rows=3, duplicate_rows=2)
        decision = step.record_quality([_result(Status.PASS)])

    [run] = sink.runs
    assert decision is QualityDecision.CONTINUE
    assert (run.pipeline_name, run.run_id, run.layer, run.table) == (
        "retail_silver",
        "R1",
        Layer.SILVER,
        "retail.silver.orders",
    )
    assert (run.started_at, run.finished_at, run.duration_seconds) == (
        START,
        START + timedelta(seconds=3),
        1.5,
    )
    assert (run.input_rows, run.output_rows, run.rejected_rows, run.duplicate_rows) == (
        100,
        95,
        3,
        2,
    )
    assert (run.status, run.error_message) == (RunStatus.SUCCEEDED, None)


def test_warning_and_error_continue_with_alerts(sink: InMemoryObservabilitySink) -> None:
    alerts: list[str] = []
    with _track(sink, alerts) as step:
        decision = step.record_quality(
            [
                _result(Status.WARNING, failed=2, name="unknown_customer"),
                _result(Status.ERROR, failed=1, name="negative__amount"),
            ]
        )
        step.record_counts(output_rows=7)  # the step went on and wrote its output

    [run] = sink.runs
    assert decision is QualityDecision.ALERT
    assert run.status is RunStatus.SUCCEEDED_WITH_ALERTS
    assert len(alerts) == 2 and "unknown_customer failed 2/10" in alerts[0]
    assert run.alerts == alerts


def test_critical_fails_the_step_before_it_writes(sink: InMemoryObservabilitySink) -> None:
    written: list[str] = []
    with pytest.raises(CriticalQualityError), _track(sink) as step:
        step.record_quality([_result(Status.CRITICAL, failed=1, name="duplicate__orders")])
        written.append("output")  # never reached

    [run] = sink.runs
    assert written == []
    assert run.status is RunStatus.FAILED
    assert run.error_message is not None and "duplicate__orders" in run.error_message
    assert [r.check_name for r in run.quality] == ["duplicate__orders"]  # still persisted


def test_critical_failure_raises_a_critical_alert(
    sink: InMemoryObservabilitySink, caplog: pytest.LogCaptureFixture
) -> None:
    """Regression: CRITICAL used to stop the step silently (only WARNING/ERROR alerted)."""
    with (
        caplog.at_level(logging.WARNING, logger="lakehouse.alerts"),
        pytest.raises(CriticalQualityError),
        _track(sink) as step,
    ):
        step.record_quality(
            [
                _result(Status.WARNING, failed=1, name="unknown_customer"),
                _result(Status.CRITICAL, failed=2, name="grain__fact_sales"),
            ]
        )

    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert [r.getMessage() for r in critical] == [
        "[CRITICAL] retail_silver/t: grain__fact_sales failed 2/10 rows"
    ]
    [run] = sink.runs
    assert run.alerts[-1] == critical[0].getMessage()


def test_any_exception_is_recorded_and_propagated(sink: InMemoryObservabilitySink) -> None:
    with pytest.raises(OSError, match="disk full"), _track(sink):
        raise OSError("disk full")
    [run] = sink.runs
    assert (run.status, run.error_message) == (RunStatus.FAILED, "OSError: disk full")
    assert run.finished_at is not None and run.duration_seconds is not None


def test_sink_failure_on_a_failed_run_keeps_the_original_error() -> None:
    class BrokenSink:
        def write(self, run: PipelineRun) -> None:
            raise ConnectionError("observability store down")

    with pytest.raises(ValueError, match="bad input"), _track(BrokenSink()):
        raise ValueError("bad input")


def test_sink_failure_on_a_successful_run_is_not_silenced() -> None:
    class BrokenSink:
        def write(self, run: PipelineRun) -> None:
            raise ConnectionError("observability store down")

    with pytest.raises(ConnectionError), _track(BrokenSink()):
        pass


def test_negative_counts_are_rejected(sink: InMemoryObservabilitySink) -> None:
    with pytest.raises(ValueError, match="rejected_rows"), _track(sink) as step:
        step.record_counts(rejected_rows=-1)
    assert sink.runs[0].status is RunStatus.FAILED


FAKE = "fake" + "-value-0000"  # obviously fictitious; assembled so no scanner mistakes it


@pytest.mark.parametrize(
    "message",
    [
        "token=abc123 rejected",
        "password: hunter2",
        "API_KEY=xyz",
        "secret = s3cr3t",
        # Regression (security audit 2026-10-07): prefixed names, headers and URL credentials
        # were persisted unmasked in pipeline_runs.error_message.
        f"access_token={FAKE}",
        f"DATABRICKS_TOKEN={FAKE}",
        f"client_secret: {FAKE}",
        f"Authorization: Bearer {FAKE}",
        f"connect failed: jdbc:postgresql://user:{FAKE}@db.example.invalid/x",
        f"GET https://api.example.invalid/x?access_token={FAKE}&page=2",
    ],
)
def test_error_messages_mask_secret_assignments(message: str) -> None:
    text = safe_error_message(RuntimeError(message))
    assert "***" in text
    for secret in ("abc123", "hunter2", "xyz", "s3cr3t", FAKE):
        assert secret not in text


def test_masking_keeps_the_useful_part_of_the_message() -> None:
    text = safe_error_message(
        RuntimeError(f"GET https://api.example.invalid/x?token={FAKE}&page=2")
    )
    assert text == "RuntimeError: GET https://api.example.invalid/x?token=***&page=2"


def test_error_messages_are_bounded() -> None:
    assert len(safe_error_message(RuntimeError("x" * 10_000))) == 2000
