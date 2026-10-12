"""Run checks and split rows by quality.

``run_checks`` triggers exactly **one** Spark action for any number of checks: every
row-level check adds a failure flag, then one aggregation counts rows and flags.
``split_by_quality`` reuses the same flags; rows failing ERROR or CRITICAL row-level
checks go to quarantine regardless of the threshold (the threshold decides the
status, not the row's fate).
"""

import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, TimestampType

from lakehouse_common.quality.checks import DatasetCheck, QualityCheck, RowCheck, SchemaMatches
from lakehouse_common.quality.results import (
    CheckResult,
    Severity,
    Status,
    failure_rate_of,
    resolve_status,
)

logger = logging.getLogger(__name__)

FAILED_CHECKS_COLUMN: Final = "_dq_failed_checks"
EXECUTION_TIMESTAMP_COLUMN: Final = "_dq_execution_timestamp"
QUARANTINE_SEVERITIES: Final = frozenset({Severity.ERROR, Severity.CRITICAL})
_ROWS_CHECKED: Final = "_dq_rows_checked"


def _flag_column(index: int) -> str:
    return f"_dq_flag_{index}"


def _validate(checks: Sequence[QualityCheck], execution_timestamp: datetime) -> None:
    duplicates = [name for name, n in Counter(c.check_name for c in checks).items() if n > 1]
    if duplicates:
        raise ValueError(f"duplicate check names: {sorted(duplicates)}")
    if execution_timestamp.tzinfo is None:
        raise ValueError("execution_timestamp must be timezone-aware (use UTC)")


def _annotate(df: DataFrame, row_checks: Sequence[RowCheck]) -> DataFrame:
    for index, check in enumerate(row_checks):
        df = check.annotate(df, _flag_column(index))
    return df


def run_checks(
    df: DataFrame,
    checks: Sequence[QualityCheck],
    *,
    table_name: str,
    execution_timestamp: datetime,
) -> list[CheckResult]:
    """Evaluate ``checks`` against ``df``; results follow the order of ``checks``.

    Schema checks run first, without a Spark action. If one fails, the other checks are *not*
    evaluated (the columns they read may not exist) and only the schema results are returned.
    """
    _validate(checks, execution_timestamp)
    if not checks:
        return []
    schema_results = _failed_schema_results(df, checks, table_name, execution_timestamp)
    if schema_results:
        return schema_results
    row_checks = [check for check in checks if isinstance(check, RowCheck)]
    flag_counts = [
        F.coalesce(F.sum(F.col(_flag_column(i)).cast("long")), F.lit(0)).alias(_flag_column(i))
        for i in range(len(row_checks))
    ]
    summary = (
        _annotate(df, row_checks).agg(F.count(F.lit(1)).alias(_ROWS_CHECKED), *flag_counts).first()
    )
    if summary is None:  # pragma: no cover - a global aggregation always returns one row
        raise RuntimeError("aggregation returned no rows")
    rows_checked = int(summary[_ROWS_CHECKED])
    failed_by_name = {
        check.check_name: int(summary[_flag_column(i)]) for i, check in enumerate(row_checks)
    }

    results = []
    for check in checks:
        details = None
        if isinstance(check, DatasetCheck):
            outcome = check.evaluate(df, rows_checked)
            failed_rows = 0 if outcome.passed else rows_checked
            failure_rate = 0.0 if outcome.passed else 1.0
            status = Status.PASS if outcome.passed else Status(check.severity.value)
            details = outcome.details
        else:
            failed_rows = failed_by_name[check.check_name]
            failure_rate = failure_rate_of(failed_rows=failed_rows, rows_checked=rows_checked)
            status = resolve_status(
                failed_rows=failed_rows,
                rows_checked=rows_checked,
                threshold=check.threshold,
                severity=check.severity,
            )
        result = CheckResult(
            check_name=check.check_name,
            check_type=check.check_type,
            table_name=table_name,
            column_name=check.column_name,
            dimension=check.dimension,
            status=status,
            severity=check.severity,
            rows_checked=rows_checked,
            failed_rows=failed_rows,
            failure_rate=failure_rate,
            execution_timestamp=execution_timestamp,
            details=details,
        )
        logger.info(
            "dq %s on %s: %s (%d/%d failed)",
            result.check_name,
            table_name,
            result.status,
            failed_rows,
            rows_checked,
        )
        results.append(result)
    return results


def _failed_schema_results(
    df: DataFrame, checks: Sequence[QualityCheck], table_name: str, executed_at: datetime
) -> list[CheckResult]:
    outcomes = [(c, c.evaluate(df, 0)) for c in checks if isinstance(c, SchemaMatches)]
    if all(outcome.passed for _, outcome in outcomes):
        return []
    rows_checked = df.count()
    results = []
    for check, outcome in outcomes:
        failed = 0 if outcome.passed else rows_checked
        results.append(
            CheckResult(
                check_name=check.check_name,
                check_type=check.check_type,
                table_name=table_name,
                column_name=None,
                dimension=check.dimension,
                status=Status.PASS if outcome.passed else Status(check.severity.value),
                severity=check.severity,
                rows_checked=rows_checked,
                failed_rows=failed,
                failure_rate=0.0 if outcome.passed else 1.0,
                execution_timestamp=executed_at,
                details=outcome.details,
            )
        )
    skipped = len(checks) - len(outcomes)
    logger.warning(
        "dq schema of %s does not match its contract; %d other check(s) not evaluated",
        table_name,
        skipped,
    )
    return results


def _quoted(column: str) -> Column:
    return F.col(f"`{column}`")


def split_by_quality(
    df: DataFrame, checks: Sequence[QualityCheck], *, execution_timestamp: datetime
) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``.

    ``quarantine`` has the original columns plus ``_dq_failed_checks`` (names, in check
    order) and ``_dq_execution_timestamp``. WARNING and dataset-level checks never
    quarantine rows. Both outputs are lazy: each write recomputes the flags.
    """
    _validate(checks, execution_timestamp)
    quarantining = [
        check
        for check in checks
        if isinstance(check, RowCheck) and check.severity in QUARANTINE_SEVERITIES
    ]
    failed_names = F.filter(
        F.array(
            *(
                F.when(F.col(_flag_column(i)), F.lit(check.check_name))
                for i, check in enumerate(quarantining)
            )
        ),
        lambda name: name.isNotNull(),
    ).cast(ArrayType(StringType()))
    tagged = _annotate(df, quarantining).select(
        *(_quoted(column) for column in df.columns), failed_names.alias(FAILED_CHECKS_COLUMN)
    )
    valid = tagged.filter(F.size(FAILED_CHECKS_COLUMN) == 0).drop(FAILED_CHECKS_COLUMN)
    quarantine = tagged.filter(F.size(FAILED_CHECKS_COLUMN) > 0).withColumn(
        EXECUTION_TIMESTAMP_COLUMN, F.lit(execution_timestamp).cast(TimestampType())
    )
    return valid, quarantine


@dataclass(frozen=True)
class CheckSet:
    """The checks of one table, evaluated on the DataFrame they were declared for."""

    table_name: str
    frame: DataFrame
    checks: Sequence[QualityCheck]


def run_suite(
    check_sets: Sequence[CheckSet], *, execution_timestamp: datetime
) -> list[CheckResult]:
    """``run_checks`` for each set (one Spark action per set), results in declaration order.

    A CRITICAL schema failure stops the suite: later sets usually derive from the same data.
    """
    results: list[CheckResult] = []
    for check_set in check_sets:
        batch = run_checks(
            check_set.frame,
            check_set.checks,
            table_name=check_set.table_name,
            execution_timestamp=execution_timestamp,
        )
        results.extend(batch)
        if any(
            r.check_type == SchemaMatches.check_type and r.status is Status.CRITICAL for r in batch
        ):
            logger.warning("dq suite stopped after the schema failure on %s", check_set.table_name)
            break
    return results
