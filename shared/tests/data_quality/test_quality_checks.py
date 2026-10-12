"""Every check type on small, deterministic DataFrames.

Each test runs ``run_checks`` once (a single Spark action for all row-level checks).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from lakehouse_common.quality import (
    AcceptedValues,
    CheckResult,
    CustomCondition,
    Dimension,
    NotNull,
    QualityCheck,
    Range,
    ReferentialIntegrity,
    Regex,
    RowCount,
    Severity,
    Status,
    Unique,
    run_checks,
)

EXECUTED_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("email", StringType(), True),
    ]
)
OrderRow = tuple[str | None, str | None, str | None, Decimal | None, str | None]


def _order(
    order_id: str | None = "o-1",
    customer_id: str | None = "c-1",
    status: str | None = "paid",
    amount: str | None = "10.00",
    email: str | None = "ana@example.com",
) -> OrderRow:
    return (order_id, customer_id, status, Decimal(amount) if amount else None, email)


def _orders(spark: SparkSession, rows: list[OrderRow]) -> DataFrame:
    return spark.createDataFrame(rows, schema=ORDERS_SCHEMA)


def _run(df: DataFrame, *checks: QualityCheck) -> dict[str, CheckResult]:
    results = run_checks(df, checks, table_name="silver.orders", execution_timestamp=EXECUTED_AT)
    return {result.check_name: result for result in results}


def _failed(results: dict[str, CheckResult]) -> dict[str, int]:
    return {name: result.failed_rows for name, result in results.items()}


# --- not_null ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("order_ids", "expected_failed", "expected_status"),
    [
        (["o-1", "o-2", "o-3"], 0, Status.PASS),  # zero failures
        (["o-1", None, "o-3"], 1, Status.CRITICAL),  # some failures
        ([None, None], 2, Status.CRITICAL),  # 100% failures
    ],
    ids=["zero", "some", "all"],
)
def test_not_null(
    spark: SparkSession, order_ids: list[str | None], expected_failed: int, expected_status: Status
) -> None:
    orders = _orders(spark, [_order(order_id=order_id) for order_id in order_ids])

    [result] = _run(orders, NotNull("order_id", severity=Severity.CRITICAL)).values()

    assert (result.failed_rows, result.rows_checked) == (expected_failed, len(order_ids))
    assert result.failure_rate == pytest.approx(expected_failed / len(order_ids))
    assert result.status is expected_status


def test_not_null_treats_blank_strings_as_null_only_when_asked(spark: SparkSession) -> None:
    orders = _orders(spark, [_order(email="  "), _order(email=""), _order()])

    results = _run(
        orders, NotNull("email"), NotNull("email", blank_is_null=True, name="email_not_blank")
    )

    assert _failed(results) == {"not_null__email": 0, "email_not_blank": 2}


# --- unique --------------------------------------------------------------------------------


def test_unique_counts_every_occurrence_of_a_duplicated_key(spark: SparkSession) -> None:
    orders = _orders(spark, [_order("o-1"), _order("o-1"), _order("o-1"), _order("o-2")])

    [result] = _run(orders, Unique(("order_id",))).values()

    assert (result.failed_rows, result.rows_checked) == (3, 4)


def test_unique_all_rows_duplicated(spark: SparkSession) -> None:
    orders = _orders(spark, [_order("o-1"), _order("o-1")])

    [result] = _run(orders, Unique(("order_id",))).values()

    assert result.failure_rate == 1.0


def test_unique_ignores_null_keys(spark: SparkSession) -> None:
    orders = _orders(spark, [_order(None), _order(None), _order("o-1")])

    [result] = _run(orders, Unique(("order_id",))).values()

    assert result.failed_rows == 0


def test_unique_composite_key(spark: SparkSession) -> None:
    orders = _orders(
        spark,
        [_order("o-1", "c-1"), _order("o-1", "c-2"), _order("o-2", "c-1"), _order("o-2", "c-1")],
    )

    [result] = _run(orders, Unique(("order_id", "customer_id"))).values()

    assert result.failed_rows == 2


# --- accepted_values -----------------------------------------------------------------------


def test_accepted_values_is_case_sensitive_and_ignores_null(spark: SparkSession) -> None:
    orders = _orders(
        spark,
        [_order(status="paid"), _order(status="PAID"), _order(status="lost"), _order(status=None)],
    )

    [result] = _run(orders, AcceptedValues("status", ("created", "paid"))).values()

    assert (result.failed_rows, result.rows_checked) == (2, 4)


# --- range ---------------------------------------------------------------------------------


def test_range_boundary_values(spark: SparkSession) -> None:
    orders = _orders(
        spark,
        [
            _order(amount="0.00"),
            _order(amount="50.00"),
            _order(amount="100.00"),
            _order(amount=None),
        ],
    )

    results = _run(
        orders,
        Range("amount", min_value=Decimal("0"), max_value=Decimal("100"), name="inclusive"),
        Range(
            "amount",
            min_value=Decimal("0"),
            max_value=Decimal("100"),
            inclusive=False,
            name="exclusive",
        ),
        Range("amount", min_value=Decimal("0.01"), name="min_only"),
        Range("amount", max_value=Decimal("99.99"), name="max_only"),
    )

    assert _failed(results) == {"inclusive": 0, "exclusive": 2, "min_only": 1, "max_only": 1}


# --- regex ---------------------------------------------------------------------------------


def test_regex_must_match_the_whole_value_and_ignores_null(spark: SparkSession) -> None:
    orders = _orders(
        spark,
        [
            _order(email="ana@example.com"),
            _order(email="not-an-email"),
            _order(email="ana@example.com trailing"),
            _order(email=None),
        ],
    )

    [result] = _run(orders, Regex("email", r"[^@\s]+@[^@\s]+\.[a-z]+")).values()

    assert result.failed_rows == 2


# --- referential_integrity -----------------------------------------------------------------


def test_referential_integrity_flags_orphans_without_multiplying_rows(spark: SparkSession) -> None:
    customers = spark.createDataFrame([("c-1",), ("c-1",), ("c-2",)], "customer_id string")
    orders = _orders(
        spark,
        [_order(customer_id="c-1"), _order(customer_id="c-3"), _order(customer_id=None)],
    )

    [result] = _run(orders, ReferentialIntegrity("customer_id", customers, "customer_id")).values()

    # c-3 is an orphan; NULL is not_null's concern; duplicated reference keys add no rows.
    assert (result.failed_rows, result.rows_checked) == (1, 3)
    assert result.dimension is Dimension.REFERENTIAL_INTEGRITY


# --- row_count -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("row_total", "expected_status", "expected_rate"),
    [
        (0, Status.CRITICAL, 1.0),
        (1, Status.PASS, 0.0),
        (3, Status.PASS, 0.0),
        (4, Status.CRITICAL, 1.0),
    ],
    ids=["empty-below-min", "at-min", "at-max", "above-max"],
)
def test_row_count_bounds(
    spark: SparkSession, row_total: int, expected_status: Status, expected_rate: float
) -> None:
    orders = _orders(spark, [_order(f"o-{index}") for index in range(row_total)])

    [result] = _run(orders, RowCount(min_rows=1, max_rows=3, severity=Severity.CRITICAL)).values()

    assert result.rows_checked == row_total
    assert (result.status, result.failure_rate) == (expected_status, expected_rate)


# --- custom_condition ----------------------------------------------------------------------


def test_custom_condition_counts_null_result_as_failure(spark: SparkSession) -> None:
    orders = _orders(spark, [_order(amount="10.00"), _order(amount="0.00"), _order(amount=None)])

    [result] = _run(orders, CustomCondition("amount_positive", "amount > 0")).values()

    assert result.failed_rows == 2


# --- whole run -----------------------------------------------------------------------------


def test_empty_dataset_passes_every_row_level_check(spark: SparkSession) -> None:
    customers = spark.createDataFrame([("c-1",)], "customer_id string")
    checks: list[QualityCheck] = [
        NotNull("order_id"),
        Unique(("order_id",)),
        AcceptedValues("status", ("paid",)),
        Range("amount", min_value=0),
        Regex("email", r".+@.+"),
        ReferentialIntegrity("customer_id", customers, "customer_id"),
        CustomCondition("amount_positive", "amount > 0"),
        RowCount(min_rows=0),
    ]

    results = _run(_orders(spark, []), *checks)

    assert {r.status for r in results.values()} == {Status.PASS}
    assert {(r.rows_checked, r.failed_rows, r.failure_rate) for r in results.values()} == {
        (0, 0, 0.0)
    }


def test_result_carries_full_context(spark: SparkSession) -> None:
    orders = _orders(spark, [_order(status="lost"), _order()])

    [result] = _run(orders, AcceptedValues("status", ("paid",), severity=Severity.WARNING)).values()

    assert result == CheckResult(
        check_name="accepted_values__status",
        check_type="accepted_values",
        table_name="silver.orders",
        column_name="status",
        dimension=Dimension.VALIDITY,
        status=Status.WARNING,
        severity=Severity.WARNING,
        rows_checked=2,
        failed_rows=1,
        failure_rate=0.5,
        execution_timestamp=EXECUTED_AT,
    )


def test_results_follow_check_order(spark: SparkSession) -> None:
    checks = [RowCount(min_rows=1), NotNull("order_id"), Unique(("order_id",))]

    results = run_checks(
        _orders(spark, [_order()]), checks, table_name="t", execution_timestamp=EXECUTED_AT
    )

    assert [r.check_name for r in results] == [
        "row_count",
        "not_null__order_id",
        "unique__order_id",
    ]


def test_duplicate_check_names_are_rejected(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        run_checks(
            _orders(spark, []),
            [NotNull("order_id"), NotNull("order_id")],
            table_name="t",
            execution_timestamp=EXECUTED_AT,
        )
