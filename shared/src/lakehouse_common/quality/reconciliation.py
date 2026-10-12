"""Row reconciliation between a layer's input and every destination of its rows.

``input_rows = sum(destinations)``: each consumed row lands in exactly one counted destination
(published, quarantined, duplicate, or a documented exclusion). Pipelines count each destination
from its own DataFrame, so a row lost or invented by any step breaks the equation.

Used by the Retail Silver (per batch) and the Financial Silver (per run).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from lakehouse_common.quality.results import (
    CheckResult,
    Dimension,
    Severity,
    Status,
    failure_rate_of,
)

CHECK_TYPE: Final = "reconciliation"


class ReconciliationError(RuntimeError):
    """Rows were lost or invented between a layer's input and its destinations (CRITICAL)."""


@dataclass(frozen=True)
class Reconciliation:
    """Input rows and the rows counted per destination (insertion order = report order)."""

    input_rows: int
    destinations: Mapping[str, int]
    subject: str = field(default="Bronze rows", kw_only=True)

    def __post_init__(self) -> None:
        negative = [name for name, rows in self.destinations.items() if rows < 0]
        if self.input_rows < 0 or negative:
            raise ValueError(f"row counts must not be negative: input or {negative}")

    @property
    def accounted(self) -> int:
        return sum(self.destinations.values())

    @property
    def difference(self) -> int:
        """Rows lost (> 0) or invented (< 0)."""
        return self.input_rows - self.accounted

    @property
    def balanced(self) -> bool:
        return self.difference == 0

    def describe(self) -> str:
        """The equation with its numbers, e.g. ``10 Bronze rows != 6 valid + 3 quarantined``."""
        terms = " + ".join(f"{rows} {name}" for name, rows in self.destinations.items())
        return f"{self.input_rows} {self.subject} {'=' if self.balanced else '!='} {terms}"

    def check(self) -> None:
        """Raise ``ReconciliationError`` unless the equation holds."""
        if not self.balanced:
            raise ReconciliationError(self.describe())

    def to_check_result(
        self, *, table_name: str, execution_timestamp: datetime, name: str = CHECK_TYPE
    ) -> CheckResult:
        """The equation as a CRITICAL quality check (``details`` = the equation).

        Recorded with the other checks, it stops the step before any write and is persisted in
        ``data_quality_results`` with the full breakdown, also when it fails.
        """
        checked = max(self.input_rows, self.accounted)
        failed = abs(self.difference)
        return CheckResult(
            check_name=name,
            check_type=CHECK_TYPE,
            table_name=table_name,
            column_name=None,
            dimension=Dimension.COMPLETENESS,
            status=Status.PASS if self.balanced else Status.CRITICAL,
            severity=Severity.CRITICAL,
            rows_checked=checked,
            failed_rows=failed,
            failure_rate=failure_rate_of(failed_rows=failed, rows_checked=checked),
            execution_timestamp=execution_timestamp,
            details=self.describe(),
        )
