"""Splitting rows into valid and quarantine by ERROR/CRITICAL row-level checks."""

from datetime import UTC, datetime

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from lakehouse_common.quality import (
    EXECUTION_TIMESTAMP_COLUMN,
    FAILED_CHECKS_COLUMN,
    AcceptedValues,
    NotNull,
    ReferentialIntegrity,
    RowCount,
    Severity,
    Unique,
    split_by_quality,
)

EXECUTED_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)


def _orders(
    spark: SparkSession, rows: list[tuple[str | None, str | None, str | None]]
) -> DataFrame:
    return spark.createDataFrame(rows, schema=ORDERS_SCHEMA)


def test_quarantine_holds_rows_failing_error_or_critical_checks(spark: SparkSession) -> None:
    # Arrange
    customers = spark.createDataFrame([("c-1",), ("c-1",)], "customer_id string")
    orders = _orders(
        spark,
        [
            ("o-1", "c-1", "paid"),  # valid
            (None, "c-1", "paid"),  # not_null (CRITICAL)
            ("o-3", "c-1", "lost"),  # accepted_values (WARNING) only -> stays valid
            ("o-4", "c-9", "paid"),  # orphan (ERROR)
            ("o-5", "c-1", "paid"),  # duplicated key (ERROR)
            ("o-5", "c-1", "paid"),
        ],
    )
    checks = [
        NotNull("order_id", severity=Severity.CRITICAL),
        AcceptedValues("status", ("paid",), severity=Severity.WARNING),
        ReferentialIntegrity("customer_id", customers, "customer_id"),
        Unique(("order_id",)),
        RowCount(min_rows=100),  # dataset-level: never quarantines rows
    ]

    # Act
    valid, quarantine = split_by_quality(orders, checks, execution_timestamp=EXECUTED_AT)

    # Assert
    assertDataFrameEqual(valid, _orders(spark, [("o-1", "c-1", "paid"), ("o-3", "c-1", "lost")]))
    assertDataFrameEqual(
        quarantine.select("order_id", FAILED_CHECKS_COLUMN),
        [
            Row(order_id=None, **{FAILED_CHECKS_COLUMN: ["not_null__order_id"]}),
            Row(order_id="o-4", **{FAILED_CHECKS_COLUMN: ["referential_integrity__customer_id"]}),
            Row(order_id="o-5", **{FAILED_CHECKS_COLUMN: ["unique__order_id"]}),
            Row(order_id="o-5", **{FAILED_CHECKS_COLUMN: ["unique__order_id"]}),
        ],
    )


def test_quarantine_lists_every_failed_check_in_check_order(spark: SparkSession) -> None:
    orders = _orders(spark, [(None, "c-1", "lost")])
    checks = [AcceptedValues("status", ("paid",)), NotNull("order_id")]

    _, quarantine = split_by_quality(orders, checks, execution_timestamp=EXECUTED_AT)

    # Timestamps rendered by Spark in UTC: collect() would use the local timezone.
    [row] = quarantine.select(
        FAILED_CHECKS_COLUMN,
        F.date_format(EXECUTION_TIMESTAMP_COLUMN, "yyyy-MM-dd HH:mm:ss").alias("executed_at"),
    ).collect()
    assert row[FAILED_CHECKS_COLUMN] == ["accepted_values__status", "not_null__order_id"]
    assert row["executed_at"] == "2026-01-15 12:00:00"


def test_split_schemas(spark: SparkSession) -> None:
    valid, quarantine = split_by_quality(
        _orders(spark, []), [NotNull("order_id")], execution_timestamp=EXECUTED_AT
    )

    assertSchemaEqual(valid.schema, ORDERS_SCHEMA)
    assertSchemaEqual(
        quarantine.schema,
        StructType(
            [
                *ORDERS_SCHEMA.fields,
                StructField(FAILED_CHECKS_COLUMN, ArrayType(StringType(), True), False),
                StructField(EXECUTION_TIMESTAMP_COLUMN, TimestampType(), False),
            ]
        ),
    )


def test_split_without_quarantining_checks_keeps_every_row(spark: SparkSession) -> None:
    orders = _orders(spark, [("o-1", "c-1", "lost")])

    valid, quarantine = split_by_quality(
        orders,
        [AcceptedValues("status", ("paid",), severity=Severity.WARNING), RowCount(min_rows=5)],
        execution_timestamp=EXECUTED_AT,
    )

    assertDataFrameEqual(valid, orders)
    assert quarantine.isEmpty()
