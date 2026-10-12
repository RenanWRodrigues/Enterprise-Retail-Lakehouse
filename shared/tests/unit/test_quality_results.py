"""Check definitions, status resolution and result validation — pure Python, no Spark."""

from collections.abc import Callable
from datetime import UTC, date, datetime

import pytest

from lakehouse_common.quality import (
    AcceptedValues,
    CheckResult,
    CriticalQualityError,
    CustomCondition,
    Dimension,
    NotNull,
    QualityCheck,
    Range,
    Regex,
    RowCount,
    Severity,
    Status,
    Unique,
    failure_rate_of,
    raise_on_critical,
    resolve_status,
)

EXECUTED_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


def _result(status: Status, severity: Severity = Severity.CRITICAL) -> CheckResult:
    return CheckResult(
        check_name="not_null__order_id",
        check_type="not_null",
        table_name="silver.orders",
        column_name="order_id",
        dimension=Dimension.COMPLETENESS,
        status=status,
        severity=severity,
        rows_checked=4,
        failed_rows=0 if status is Status.PASS else 1,
        failure_rate=0.0 if status is Status.PASS else 0.25,
        execution_timestamp=EXECUTED_AT,
    )


# --- status resolution -------------------------------------------------------------------


@pytest.mark.parametrize("severity", list(Severity))
def test_zero_failures_pass(severity: Severity) -> None:
    assert resolve_status(failed_rows=0, rows_checked=10, threshold=0.0, severity=severity) is (
        Status.PASS
    )


@pytest.mark.parametrize(
    ("severity", "expected"),
    [
        (Severity.WARNING, Status.WARNING),
        (Severity.ERROR, Status.ERROR),
        (Severity.CRITICAL, Status.CRITICAL),
    ],
)
def test_some_failures_take_the_check_severity(severity: Severity, expected: Status) -> None:
    assert resolve_status(failed_rows=3, rows_checked=10, threshold=0.0, severity=severity) is (
        expected
    )


def test_all_rows_failing_take_the_check_severity() -> None:
    status = resolve_status(failed_rows=10, rows_checked=10, threshold=0.5, severity=Severity.ERROR)
    assert status is Status.ERROR


def test_failure_rate_exactly_at_threshold_passes() -> None:
    status = resolve_status(failed_rows=1, rows_checked=10, threshold=0.1, severity=Severity.ERROR)
    assert status is Status.PASS


def test_failure_rate_just_above_threshold_fails() -> None:
    status = resolve_status(failed_rows=2, rows_checked=10, threshold=0.1, severity=Severity.ERROR)
    assert status is Status.ERROR


def test_empty_dataset_passes_row_level_checks() -> None:
    status = resolve_status(
        failed_rows=0, rows_checked=0, threshold=0.0, severity=Severity.CRITICAL
    )
    assert status is Status.PASS


# --- results -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rows_checked", "failed_rows", "expected"),
    [(0, 0, 0.0), (4, 0, 0.0), (4, 1, 0.25), (4, 4, 1.0)],
)
def test_failure_rate_of(rows_checked: int, failed_rows: int, expected: float) -> None:
    assert failure_rate_of(failed_rows=failed_rows, rows_checked=rows_checked) == pytest.approx(
        expected
    )


def _build_result(
    rows_checked: int = 1,
    failed_rows: int = 0,
    failure_rate: float = 0.0,
    execution_timestamp: datetime = EXECUTED_AT,
) -> CheckResult:
    return CheckResult(
        check_name="c",
        check_type="custom_condition",
        table_name="t",
        column_name=None,
        dimension=Dimension.VALIDITY,
        status=Status.PASS,
        severity=Severity.ERROR,
        rows_checked=rows_checked,
        failed_rows=failed_rows,
        failure_rate=failure_rate,
        execution_timestamp=execution_timestamp,
    )


@pytest.mark.parametrize(("rows_checked", "failed_rows"), [(-1, 0), (1, -1), (1, 2)])
def test_result_rejects_inconsistent_counts(rows_checked: int, failed_rows: int) -> None:
    with pytest.raises(ValueError, match="rows"):
        _build_result(rows_checked=rows_checked, failed_rows=failed_rows)


@pytest.mark.parametrize("failure_rate", [-0.1, 1.1])
def test_result_rejects_failure_rate_outside_unit_interval(failure_rate: float) -> None:
    with pytest.raises(ValueError, match="failure_rate"):
        _build_result(failure_rate=failure_rate)


def test_result_rejects_naive_execution_timestamp() -> None:
    naive = datetime(2026, 1, 15, 12, 0)  # noqa: DTZ001 - naive on purpose
    with pytest.raises(ValueError, match="timezone"):
        _build_result(execution_timestamp=naive)


def test_raise_on_critical_reports_only_critical_failures() -> None:
    critical = _result(Status.CRITICAL)
    error = _result(Status.ERROR, severity=Severity.ERROR)

    with pytest.raises(CriticalQualityError) as exc_info:
        raise_on_critical([critical, error, _result(Status.PASS)])

    assert exc_info.value.failed_results == (critical,)
    assert "not_null__order_id" in str(exc_info.value)


def test_raise_on_critical_ignores_passing_and_non_critical_results() -> None:
    raise_on_critical([_result(Status.PASS), _result(Status.WARNING, Severity.WARNING)])


# --- check definitions ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("check", "expected_name", "expected_column"),
    [
        (NotNull("order_id"), "not_null__order_id", "order_id"),
        (
            Unique(("order_id", "line_number")),
            "unique__order_id__line_number",
            "order_id,line_number",
        ),
        (AcceptedValues("status", ("paid",)), "accepted_values__status", "status"),
        (Range("amount", min_value=0), "range__amount", "amount"),
        (Regex("email", r"[^@]+@[^@]+"), "regex__email", "email"),
        (RowCount(min_rows=1), "row_count", None),
        (
            CustomCondition("shipped_after_ordered", "shipped_at >= ordered_at"),
            "shipped_after_ordered",
            None,
        ),
    ],
)
def test_default_check_names_and_columns(
    check: QualityCheck, expected_name: str, expected_column: str | None
) -> None:
    assert check.check_name == expected_name
    assert check.column_name == expected_column


def test_explicit_name_overrides_default() -> None:
    assert NotNull("order_id", name="order_id_required").check_name == "order_id_required"


def test_checks_default_to_error_severity_and_zero_threshold() -> None:
    check = NotNull("order_id")
    assert (check.severity, check.threshold) == (Severity.ERROR, 0.0)


@pytest.mark.parametrize(
    "build",
    [
        lambda: NotNull("order_id", threshold=-0.1),
        lambda: NotNull("order_id", threshold=1.1),
        lambda: NotNull(" "),
        lambda: Unique(()),
        lambda: AcceptedValues("status", ()),
        lambda: Range("amount"),
        lambda: Range("amount", min_value=10, max_value=1),
        lambda: Range("amount", min_value=1, max_value=date(2026, 1, 1)),
        lambda: Regex("email", ""),
        RowCount,
        lambda: RowCount(min_rows=-1),
        lambda: RowCount(min_rows=5, max_rows=1),
        lambda: CustomCondition("", "x > 0"),
        lambda: CustomCondition("positive", "  "),
    ],
)
def test_invalid_check_definitions_are_rejected(build: Callable[[], object]) -> None:
    with pytest.raises(ValueError):
        build()
