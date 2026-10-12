"""Severities, statuses, check results and the ``dq_results`` contract."""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class Severity(StrEnum):
    """What a failing check means for the pipeline (docs/DATA_QUALITY.md §3)."""

    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Status(StrEnum):
    """Outcome of one check execution: PASS, or the check's severity."""

    PASS = "PASS"  # noqa: S105 - a status name, not a password
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Dimension(StrEnum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    FRESHNESS = "freshness"
    VOLUME = "volume"


def failure_rate_of(*, failed_rows: int, rows_checked: int) -> float:
    """Share of failing rows; 0.0 for an empty dataset (volume is ``row_count``'s job)."""
    if rows_checked == 0:
        return 0.0
    return failed_rows / rows_checked


def resolve_status(
    *, failed_rows: int, rows_checked: int, threshold: float, severity: Severity
) -> Status:
    """PASS while the failure rate stays within ``threshold``; otherwise the severity."""
    rate = failure_rate_of(failed_rows=failed_rows, rows_checked=rows_checked)
    if rate <= threshold:
        return Status.PASS
    return Status(severity.value)


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One check execution. ``column_name`` is ``None`` for dataset-level checks."""

    check_name: str
    check_type: str
    table_name: str
    column_name: str | None
    dimension: Dimension
    status: Status
    severity: Severity
    rows_checked: int
    failed_rows: int
    failure_rate: float
    execution_timestamp: datetime
    details: str | None = None  # e.g. the columns a schema check found missing

    def __post_init__(self) -> None:
        if self.rows_checked < 0 or not 0 <= self.failed_rows <= self.rows_checked:
            raise ValueError(
                f"inconsistent rows: checked={self.rows_checked}, failed={self.failed_rows}"
            )
        if not 0.0 <= self.failure_rate <= 1.0:
            raise ValueError(f"failure_rate must be within [0, 1], got {self.failure_rate}")
        if self.execution_timestamp.tzinfo is None:
            raise ValueError("execution_timestamp must be timezone-aware (use UTC)")


class CriticalQualityError(RuntimeError):
    """Raised before writing a layer when any check ends with status CRITICAL."""

    def __init__(self, failed_results: tuple[CheckResult, ...]) -> None:
        self.failed_results = failed_results
        summary = ", ".join(
            f"{result.check_name} on {result.table_name} "
            f"({result.failed_rows}/{result.rows_checked})"
            for result in failed_results
        )
        super().__init__(f"critical data quality checks failed: {summary}")


class QualityDecision(StrEnum):
    """What the pipeline does after a set of checks (docs/DATA_QUALITY.md §3)."""

    CONTINUE = "CONTINUE"  # every check passed
    ALERT = "ALERT"  # WARNING/ERROR: continue and alert (ERROR rows are quarantined)
    FAIL = "FAIL"  # CRITICAL: stop before writing


def decide(results: Iterable[CheckResult]) -> QualityDecision:
    """Worst status wins: any CRITICAL -> FAIL, any WARNING/ERROR -> ALERT, else CONTINUE."""
    statuses = {result.status for result in results}
    if Status.CRITICAL in statuses:
        return QualityDecision.FAIL
    if statuses & {Status.WARNING, Status.ERROR}:
        return QualityDecision.ALERT
    return QualityDecision.CONTINUE


SCORED_DIMENSIONS: Final = (
    Dimension.COMPLETENESS,
    Dimension.UNIQUENESS,
    Dimension.VALIDITY,
    Dimension.REFERENTIAL_INTEGRITY,
)


def dimension_scores(results: Iterable[CheckResult]) -> dict[Dimension, float | None]:
    """Score per dimension = 1 - failed rows / rows checked, summed over its checks.

    ``None`` when no check of that dimension ran; 1.0 when its checks saw no rows.
    """
    totals: dict[Dimension, tuple[int, int]] = {}
    for result in results:
        failed, checked = totals.get(result.dimension, (0, 0))
        totals[result.dimension] = (failed + result.failed_rows, checked + result.rows_checked)
    scores: dict[Dimension, float | None] = {}
    for dimension in SCORED_DIMENSIONS:
        if dimension not in totals:
            scores[dimension] = None
            continue
        failed, checked = totals[dimension]
        scores[dimension] = 1.0 - failure_rate_of(failed_rows=failed, rows_checked=checked)
    return scores


def raise_on_critical(results: Iterable[CheckResult]) -> None:
    """Stop the pipeline if any result has status CRITICAL."""
    failed = tuple(result for result in results if result.status is Status.CRITICAL)
    if failed:
        raise CriticalQualityError(failed)


DQ_RESULTS_SCHEMA: Final = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("dimension", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("rows_checked", LongType(), False),
        StructField("failed_rows", LongType(), False),
        StructField("failure_rate", DoubleType(), False),
        StructField("execution_timestamp", TimestampType(), False),
        StructField("details", StringType(), True),
    ]
)
"""Rows of the ``data_quality_results`` table: one per check per run."""


DQ_RESULTS_COLUMNS: Final = tuple(DQ_RESULTS_SCHEMA.fieldNames())


def result_record(result: CheckResult, *, run_id: str) -> dict[str, object]:
    """The ``data_quality_results`` row of one check (``DQ_RESULTS_COLUMNS`` order)."""
    return {
        "run_id": run_id,
        "table_name": result.table_name,
        "check_name": result.check_name,
        "check_type": result.check_type,
        "column_name": result.column_name,
        "dimension": result.dimension.value,
        "severity": result.severity.value,
        "status": result.status.value,
        "rows_checked": result.rows_checked,
        "failed_rows": result.failed_rows,
        "failure_rate": result.failure_rate,
        "execution_timestamp": result.execution_timestamp,
        "details": result.details,
    }


def results_to_dataframe(
    spark: SparkSession, results: Sequence[CheckResult], *, run_id: str
) -> DataFrame:
    """``data_quality_results`` rows (contract: ``DQ_RESULTS_SCHEMA``)."""
    rows = [tuple(result_record(result, run_id=run_id).values()) for result in results]
    return spark.createDataFrame(rows, schema=DQ_RESULTS_SCHEMA)
