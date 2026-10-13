"""Pipeline run metrics (``pipeline_runs``) and the quality policy: continue, alert or fail.

One ``PipelineRun`` per (run, table) step. ``track_run`` times the step, records its row
counts and quality results, applies ``decide()`` and always writes the run — also when the
step fails, with ``status = FAILED`` and the error message (docs/OBSERVABILITY.md).

``run_record`` is the single mapping from a run to a ``pipeline_runs`` row; every sink (memory,
JSON Lines, Delta) stores that same row. Starting a step never starts Spark.
"""

import logging
import re
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse_common.logging.redaction import mask_secrets
from lakehouse_common.logging.structured import log_context
from lakehouse_common.quality import (
    CheckResult,
    CriticalQualityError,
    Dimension,
    QualityDecision,
    Status,
    decide,
    dimension_scores,
)

logger = logging.getLogger(__name__)
alert_logger = logging.getLogger("lakehouse.alerts")  # route this logger to the alert channel
MAX_ERROR_LENGTH: Final = 2000
RUN_ID_PATTERN: Final = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{32}$")
_STARTED_LABEL: Final = re.compile(r"^\d{8}T\d{6}Z")


def _uuid_hex() -> str:
    return uuid.uuid4().hex


def new_run_id(started: datetime, *, token: Callable[[], str] = _uuid_hex) -> str:
    """``<UTC start, to the second>Z-<uuid4 hex>``: one id per run, generated once and passed on.

    Uniqueness comes from the 122 random bits, never from the clock (two runs started in the
    same second, or on two machines, differ). The prefix keeps the id readable and sortable by
    start second; the exact start stays in ``pipeline_runs.started_at``. ``token`` is injectable
    for deterministic tests. Raises ``ValueError`` for a naive ``started``.
    """
    if started.tzinfo is None:
        raise ValueError("started must be timezone-aware (use UTC)")
    return f"{started.astimezone(UTC):%Y%m%dT%H%M%SZ}-{token()}"


def run_started_label(run_id: str) -> str | None:
    """The readable start (``yyyymmddThhmmssZ``) of a run id, new or legacy (seconds only)."""
    match = _STARTED_LABEL.match(run_id)
    return match.group(0) if match else None


class Layer(StrEnum):
    LANDING = "landing"  # source files produced or landed before Bronze (IoT generator)
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class RunStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    SUCCEEDED_WITH_ALERTS = "SUCCEEDED_WITH_ALERTS"  # WARNING/ERROR checks: data written
    FAILED = "FAILED"  # exception or CRITICAL check: nothing written by this step


@dataclass
class PipelineRun:
    pipeline_name: str
    run_id: str
    project: str
    layer: Layer
    table: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    input_rows: int | None = None
    output_rows: int | None = None
    rejected_rows: int | None = None
    duplicate_rows: int | None = None
    status: RunStatus | None = None
    error_message: str | None = None
    error_type: str | None = None  # exception class of a FAILED run (not in PIPELINE_RUNS_SCHEMA)
    quality: list[CheckResult] = field(default_factory=list)
    alerts: list[str] = field(default_factory=list)

    def scores(self) -> dict[Dimension, float | None]:
        return dimension_scores(self.quality)


PIPELINE_RUNS_SCHEMA: Final = StructType(
    [
        StructField("pipeline_name", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("project", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("duration_seconds", DoubleType(), False),
        StructField("input_rows", LongType(), True),
        StructField("output_rows", LongType(), True),
        StructField("rejected_rows", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        # beyond the requested columns: duplicates and the quality summary of the step
        StructField("duplicate_rows", LongType(), True),
        StructField("completeness", DoubleType(), True),
        StructField("uniqueness", DoubleType(), True),
        StructField("validity", DoubleType(), True),
        StructField("referential_integrity", DoubleType(), True),
        StructField("checks_failed", LongType(), False),
    ]
)
"""Rows of the ``pipeline_runs`` table: one per pipeline step (run_id + table)."""

PIPELINE_RUNS_COLUMNS: Final = tuple(PIPELINE_RUNS_SCHEMA.fieldNames())


def run_record(run: PipelineRun) -> dict[str, object]:
    """The ``pipeline_runs`` row of ``run`` (``PIPELINE_RUNS_COLUMNS`` order, Python values)."""
    scores = run.scores()
    record: dict[str, object] = {
        "pipeline_name": run.pipeline_name,
        "run_id": run.run_id,
        "project": run.project,
        "layer": run.layer.value,
        "table": run.table,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": run.duration_seconds,
        "input_rows": run.input_rows,
        "output_rows": run.output_rows,
        "rejected_rows": run.rejected_rows,
        "status": run.status.value if run.status else None,
        "error_message": run.error_message,
        "duplicate_rows": run.duplicate_rows,
        "completeness": scores[Dimension.COMPLETENESS],
        "uniqueness": scores[Dimension.UNIQUENESS],
        "validity": scores[Dimension.VALIDITY],
        "referential_integrity": scores[Dimension.REFERENTIAL_INTEGRITY],
        "checks_failed": sum(1 for r in run.quality if r.status is not Status.PASS),
    }
    return {column: record[column] for column in PIPELINE_RUNS_COLUMNS}


def runs_to_dataframe(spark: SparkSession, runs: Sequence[PipelineRun]) -> DataFrame:
    """``pipeline_runs`` rows (``PIPELINE_RUNS_SCHEMA``), one per run."""
    rows = [tuple(run_record(run).values()) for run in runs]
    return spark.createDataFrame(rows, PIPELINE_RUNS_SCHEMA)


class ObservabilitySink(Protocol):
    def write(self, run: PipelineRun) -> None:
        """Persist the run and its quality results (``pipeline_runs``, ``data_quality_results``)."""
        ...


def safe_error_message(error: BaseException) -> str:
    """``Type: message | note | ...``, secrets-looking values masked, bounded length.

    Notes (PEP 678, ``error.add_note``) carry the pipeline's context, e.g. the stage that
    failed and the progress already confirmed. They are kept at the end: when the text is too
    long, the message is cut, not the notes.
    """
    message = mask_secrets(f"{type(error).__name__}: {error}")
    notes = "".join(f" | {note}" for note in getattr(error, "__notes__", ()))
    notes = mask_secrets(notes)[:MAX_ERROR_LENGTH]
    return message[: MAX_ERROR_LENGTH - len(notes)] + notes


class RunTracker:
    """Handle given by ``track_run``: record counts and quality while the step runs."""

    def __init__(self, run: PipelineRun, alert: Callable[[str], None]) -> None:
        self.run = run
        self._alert = alert

    def record_counts(
        self,
        *,
        input_rows: int | None = None,
        output_rows: int | None = None,
        rejected_rows: int | None = None,
        duplicate_rows: int | None = None,
    ) -> None:
        for name, value in (
            ("input_rows", input_rows),
            ("output_rows", output_rows),
            ("rejected_rows", rejected_rows),
            ("duplicate_rows", duplicate_rows),
        ):
            if value is not None:
                if value < 0:
                    raise ValueError(f"{name} must not be negative")
                setattr(self.run, name, value)

    def record_quality(self, results: Sequence[CheckResult]) -> QualityDecision:
        """Store results and apply the policy: ALERT notifies, FAIL raises before any write.

        WARNING/ERROR go to the injectable ``alert``; CRITICAL is logged at CRITICAL level on
        ``lakehouse.alerts`` (never silenced by the injected callback) before raising.
        """
        self.run.quality.extend(results)
        decision = decide(results)
        for result in results:
            if result.status in (Status.WARNING, Status.ERROR):
                message = self._alert_message(result)
                self.run.alerts.append(message)
                self._alert(message)
        if decision is QualityDecision.FAIL:
            critical = tuple(r for r in results if r.status is Status.CRITICAL)
            for result in critical:
                message = self._alert_message(result)
                self.run.alerts.append(message)
                alert_logger.critical(message)
            raise CriticalQualityError(critical)
        return decision

    def _alert_message(self, result: CheckResult) -> str:
        return (
            f"[{result.status}] {self.run.pipeline_name}/{result.table_name}: "
            f"{result.check_name} failed {result.failed_rows}/{result.rows_checked} rows"
            + (f" ({result.details})" if result.details else "")
        )


def _log_alert(message: str) -> None:
    alert_logger.warning(message)


@contextmanager
def track_run(  # noqa: PLR0913 - keyword-only, injectable clocks for tests
    *,
    pipeline_name: str,
    run_id: str,
    project: str,
    layer: Layer,
    table: str,
    sink: ObservabilitySink,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    alert: Callable[[str], None] = _log_alert,
) -> Iterator[RunTracker]:
    """Time one pipeline step and always persist its run record; never swallow the error."""
    run = PipelineRun(pipeline_name, run_id, project, layer, table, clock())
    tracker = RunTracker(run, alert)
    started = monotonic()
    with log_context(project=project, pipeline=pipeline_name, run_id=run_id):
        try:
            yield tracker
        except BaseException as error:
            run.status = RunStatus.FAILED
            run.error_message = safe_error_message(error)
            run.error_type = type(error).__name__
            raise
        else:
            run.status = RunStatus.SUCCEEDED_WITH_ALERTS if run.alerts else RunStatus.SUCCEEDED
        finally:
            run.finished_at = clock()
            run.duration_seconds = round(monotonic() - started, 6)
            _log_summary(run)
            if run.status is RunStatus.FAILED:
                try:
                    sink.write(run)
                except Exception:  # the step's own error must stay the one that propagates
                    logger.exception("could not persist the failed run %s", run.run_id)
            else:
                sink.write(run)


def _log_summary(run: PipelineRun) -> None:
    """The run's closing line: same text as before, plus structured fields; ERROR if FAILED."""
    failed = run.status is RunStatus.FAILED
    logger.log(
        logging.ERROR if failed else logging.INFO,
        "run %s %s/%s: %s in %.3fs (in=%s out=%s rejected=%s duplicates=%s)",
        run.run_id,
        run.pipeline_name,
        run.table,
        run.status,
        run.duration_seconds,
        run.input_rows,
        run.output_rows,
        run.rejected_rows,
        run.duplicate_rows,
        extra={
            "status": run.status.value if run.status else None,
            "duration_ms": round((run.duration_seconds or 0.0) * 1000, 3),
            "error_type": run.error_type,
        },
    )
