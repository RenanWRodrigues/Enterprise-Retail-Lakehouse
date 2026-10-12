"""Row reconciliation: every input row has exactly one counted destination (no Spark needed)."""

from datetime import UTC, datetime

import pytest

from lakehouse_common.quality import (
    Dimension,
    Reconciliation,
    ReconciliationError,
    Severity,
    Status,
)

AT = datetime(2026, 10, 8, 12, tzinfo=UTC)


def test_a_balanced_equation_passes() -> None:
    balance = Reconciliation(10, {"valid": 6, "quarantined": 3, "duplicates": 1})

    balance.check()

    assert (balance.accounted, balance.difference, balance.balanced) == (10, 0, True)
    assert balance.describe() == "10 Bronze rows = 6 valid + 3 quarantined + 1 duplicates"


def test_a_lost_row_raises_with_the_whole_equation() -> None:
    balance = Reconciliation(10, {"valid": 6, "quarantined": 2, "duplicates": 1})

    with pytest.raises(ReconciliationError, match=r"10 Bronze rows != 6 valid \+ 2 quarantined"):
        balance.check()
    assert (balance.difference, balance.balanced) == (1, False)


def test_an_invented_row_is_also_unbalanced() -> None:
    assert Reconciliation(3, {"valid": 4}).difference == -1


def test_negative_counts_are_refused() -> None:
    with pytest.raises(ValueError, match="valid"):
        Reconciliation(3, {"valid": -1})


def test_the_check_result_is_critical_when_unbalanced() -> None:
    result = Reconciliation(10, {"valid": 6, "quarantined": 2}).to_check_result(
        table_name="silver_x", execution_timestamp=AT
    )

    assert (result.check_name, result.check_type, result.table_name) == (
        "reconciliation",
        "reconciliation",
        "silver_x",
    )
    assert (result.status, result.severity, result.dimension) == (
        Status.CRITICAL,
        Severity.CRITICAL,
        Dimension.COMPLETENESS,
    )
    assert (result.rows_checked, result.failed_rows) == (10, 2)
    assert result.details == "10 Bronze rows != 6 valid + 2 quarantined"


def test_the_check_result_passes_when_balanced() -> None:
    result = Reconciliation(0, {"valid": 0}).to_check_result(
        table_name="silver_x", execution_timestamp=AT
    )

    assert (result.status, result.failed_rows, result.failure_rate) == (Status.PASS, 0, 0.0)


def test_more_rows_out_than_in_stays_a_valid_check_result() -> None:
    result = Reconciliation(2, {"valid": 5}).to_check_result(
        table_name="silver_x", execution_timestamp=AT
    )

    assert (result.rows_checked, result.failed_rows, result.status) == (5, 3, Status.CRITICAL)
