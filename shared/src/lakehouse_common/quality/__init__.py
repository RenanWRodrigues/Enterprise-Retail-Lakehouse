"""Domain-agnostic data quality checks on Spark DataFrames.

Usage and semantics: docs/DATA_QUALITY.md.

>>> results = run_checks(orders, [NotNull("order_id", severity=Severity.CRITICAL)],
...                      table_name="silver.orders", execution_timestamp=started_at)
>>> raise_on_critical(results)
"""

from lakehouse_common.quality.checks import (
    AcceptedValues,
    CustomCondition,
    DatasetCheck,
    DatasetOutcome,
    NotNull,
    PredicateCheck,
    QualityCheck,
    Range,
    ReferentialIntegrity,
    Regex,
    RowCheck,
    RowCount,
    SchemaMatches,
    SequenceGaps,
    Unique,
)
from lakehouse_common.quality.dedup import keep_first
from lakehouse_common.quality.reconciliation import Reconciliation, ReconciliationError
from lakehouse_common.quality.results import (
    DQ_RESULTS_COLUMNS,
    DQ_RESULTS_SCHEMA,
    SCORED_DIMENSIONS,
    CheckResult,
    CriticalQualityError,
    Dimension,
    QualityDecision,
    Severity,
    Status,
    decide,
    dimension_scores,
    failure_rate_of,
    raise_on_critical,
    resolve_status,
    result_record,
    results_to_dataframe,
)
from lakehouse_common.quality.runner import (
    EXECUTION_TIMESTAMP_COLUMN,
    FAILED_CHECKS_COLUMN,
    CheckSet,
    run_checks,
    run_suite,
    split_by_quality,
)

__all__ = [
    "DQ_RESULTS_COLUMNS",
    "DQ_RESULTS_SCHEMA",
    "EXECUTION_TIMESTAMP_COLUMN",
    "FAILED_CHECKS_COLUMN",
    "SCORED_DIMENSIONS",
    "AcceptedValues",
    "CheckResult",
    "CheckSet",
    "CriticalQualityError",
    "CustomCondition",
    "DatasetCheck",
    "DatasetOutcome",
    "Dimension",
    "NotNull",
    "PredicateCheck",
    "QualityCheck",
    "QualityDecision",
    "Range",
    "Reconciliation",
    "ReconciliationError",
    "ReferentialIntegrity",
    "Regex",
    "RowCheck",
    "RowCount",
    "SchemaMatches",
    "SequenceGaps",
    "Severity",
    "Status",
    "Unique",
    "decide",
    "dimension_scores",
    "failure_rate_of",
    "keep_first",
    "raise_on_critical",
    "resolve_status",
    "result_record",
    "results_to_dataframe",
    "run_checks",
    "run_suite",
    "split_by_quality",
]
