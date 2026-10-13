# ruff: noqa: E501 - GOLDEN holds recorded CheckResult reprs verbatim
"""Characterization of ``run_checks``: a refactor must return exactly the same results.

A fixed mix of row, dataset and referential checks (with examples) on a small DataFrame; the
complete ``CheckResult`` list was recorded before ``run_checks`` was split (2026-10-13). Also the
schema short-circuit: a failed schema returns only the schema results.
"""

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from lakehouse_common.quality import (
    AcceptedValues,
    CustomCondition,
    NotNull,
    Range,
    ReferentialIntegrity,
    RowCount,
    SchemaMatches,
    Severity,
    Unique,
    run_checks,
)

AT = datetime(2026, 10, 13, 12, tzinfo=UTC)
SCHEMA = "order_id string, customer_id string, status string, amount int"
GOLDEN = [
    "CheckResult(check_name='schema_matches', check_type='schema_matches', table_name='orders', column_name=None, dimension=<Dimension.CONSISTENCY: 'consistency'>, status=<Status.PASS: 'PASS'>, severity=<Severity.CRITICAL: 'CRITICAL'>, rows_checked=4, failed_rows=0, failure_rate=0.0, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='not_null__customer_id', check_type='not_null', table_name='orders', column_name='customer_id', dimension=<Dimension.COMPLETENESS: 'completeness'>, status=<Status.ERROR: 'ERROR'>, severity=<Severity.ERROR: 'ERROR'>, rows_checked=4, failed_rows=1, failure_rate=0.25, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='unique__order_id', check_type='unique', table_name='orders', column_name='order_id', dimension=<Dimension.UNIQUENESS: 'uniqueness'>, status=<Status.CRITICAL: 'CRITICAL'>, severity=<Severity.CRITICAL: 'CRITICAL'>, rows_checked=4, failed_rows=2, failure_rate=0.5, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='accepted_values__status', check_type='accepted_values', table_name='orders', column_name='status', dimension=<Dimension.VALIDITY: 'validity'>, status=<Status.WARNING: 'WARNING'>, severity=<Severity.WARNING: 'WARNING'>, rows_checked=4, failed_rows=1, failure_rate=0.25, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='range__amount', check_type='range', table_name='orders', column_name='amount', dimension=<Dimension.VALIDITY: 'validity'>, status=<Status.ERROR: 'ERROR'>, severity=<Severity.ERROR: 'ERROR'>, rows_checked=4, failed_rows=1, failure_rate=0.25, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='amount_known', check_type='custom_condition', table_name='orders', column_name=None, dimension=<Dimension.VALIDITY: 'validity'>, status=<Status.WARNING: 'WARNING'>, severity=<Severity.WARNING: 'WARNING'>, rows_checked=4, failed_rows=1, failure_rate=0.25, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
    "CheckResult(check_name='referential_integrity__customer_id', check_type='referential_integrity', table_name='orders', column_name='customer_id', dimension=<Dimension.REFERENTIAL_INTEGRITY: 'referential_integrity'>, status=<Status.WARNING: 'WARNING'>, severity=<Severity.WARNING: 'WARNING'>, rows_checked=4, failed_rows=1, failure_rate=0.25, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details='examples: C9')",
    "CheckResult(check_name='row_count', check_type='row_count', table_name='orders', column_name=None, dimension=<Dimension.VOLUME: 'volume'>, status=<Status.PASS: 'PASS'>, severity=<Severity.CRITICAL: 'CRITICAL'>, rows_checked=4, failed_rows=0, failure_rate=0.0, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details=None)",
]
GOLDEN_SCHEMA_ONLY = [
    "CheckResult(check_name='schema_matches', check_type='schema_matches', table_name='orders', column_name=None, dimension=<Dimension.CONSISTENCY: 'consistency'>, status=<Status.CRITICAL: 'CRITICAL'>, severity=<Severity.CRITICAL: 'CRITICAL'>, rows_checked=1, failed_rows=1, failure_rate=1.0, execution_timestamp=datetime.datetime(2026, 10, 13, 12, 0, tzinfo=datetime.timezone.utc), details='unexpected: amount, customer_id, status')"
]


def test_mixed_checks_return_the_recorded_results(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "paid", 10),
            ("O2", "C9", "lost", -5),
            ("O2", None, "paid", 3),
            ("O4", "C2", "paid", None),
        ],
        SCHEMA,
    )
    customers = spark.createDataFrame([("C1",), ("C2",)], "customer_id string")
    checks = [
        SchemaMatches(orders.schema, severity=Severity.CRITICAL),
        NotNull("customer_id", severity=Severity.ERROR),
        Unique(("order_id",), severity=Severity.CRITICAL),
        AcceptedValues("status", ("paid", "cancelled"), severity=Severity.WARNING),
        Range("amount", min_value=0, severity=Severity.ERROR),
        CustomCondition(
            name="amount_known", condition="amount IS NOT NULL", severity=Severity.WARNING
        ),
        ReferentialIntegrity(
            "customer_id", customers, "customer_id", severity=Severity.WARNING, examples=3
        ),
        RowCount(min_rows=1, severity=Severity.CRITICAL),
    ]

    results = run_checks(orders, checks, table_name="orders", execution_timestamp=AT)

    assert [repr(r) for r in results] == GOLDEN


def test_a_failed_schema_returns_only_schema_results(spark: SparkSession) -> None:
    orders = spark.createDataFrame([("O1", "C1", "paid", 10)], SCHEMA)
    expected = spark.createDataFrame([("O1",)], "order_id string").schema

    results = run_checks(
        orders,
        [
            SchemaMatches(expected, severity=Severity.CRITICAL),
            NotNull("customer_id", severity=Severity.ERROR),
        ],
        table_name="orders",
        execution_timestamp=AT,
    )

    assert [repr(r) for r in results] == GOLDEN_SCHEMA_ONLY
