"""SchemaMatches (schema drift), SequenceGaps (missing periods), keep_first and run_suite."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lakehouse_common.quality import (
    CheckSet,
    NotNull,
    SchemaMatches,
    SequenceGaps,
    Severity,
    Status,
    keep_first,
    run_checks,
    run_suite,
    split_by_quality,
)

AT = datetime(2026, 10, 6, 12, tzinfo=UTC)
EXPECTED = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amount", IntegerType(), True),
        StructField("city", StringType(), True),
    ]
)


@pytest.mark.parametrize(
    ("ddl", "details"),
    [
        ("city STRING, id STRING, amount INT", None),  # order does not matter
        ("id STRING, amount INT", "missing: city"),
        ("id STRING, amount INT, city STRING, extra STRING", "unexpected: extra"),
        ("id STRING, amount BIGINT, city STRING", "type changed: amount (int -> bigint)"),
        ("id INT, amount INT", "missing: city; type changed: id (string -> int)"),
    ],
)
def test_schema_drift_is_detected_and_described(
    spark: SparkSession, ddl: str, details: str | None
) -> None:
    frame = spark.createDataFrame([], ddl)
    outcome = SchemaMatches(EXPECTED).evaluate(frame, 0)
    assert (outcome.passed, outcome.details) == (details is None, details)


def test_nullability_is_checked_only_when_asked(spark: SparkSession) -> None:
    relaxed = StructType([StructField("id", StringType(), True), *EXPECTED.fields[1:]])
    frame = spark.createDataFrame([], relaxed)
    assert SchemaMatches(EXPECTED).evaluate(frame, 0).passed
    strict = SchemaMatches(EXPECTED, check_nullability=True).evaluate(frame, 0)
    assert (strict.passed, strict.details) == (False, "became nullable: id")


def test_schema_check_result_fails_the_whole_dataset_with_details(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a", 1)], "id STRING, amount INT")
    [result] = run_checks(
        frame,
        [SchemaMatches(EXPECTED, severity=Severity.CRITICAL)],
        table_name="t",
        execution_timestamp=AT,
    )
    assert (result.status, result.failed_rows, result.failure_rate) == (Status.CRITICAL, 1, 1.0)
    assert result.details == "missing: city"
    assert result.column_name is None


def test_sequence_gaps_per_partition_with_row_specific_allowance(spark: SparkSession) -> None:
    rows = [
        # monthly series (allowed step 1): months 1, 2, 4 -> month 4 follows a gap
        ("m", 1, 1, "first"),
        ("m", 2, 1, "ok"),
        ("m", 4, 1, "gap"),
        # business-day series (allowed step 5 days): 10 -> 15 ok, 15 -> 21 gap
        ("d", 10, 5, "first"),
        ("d", 15, 5, "ok"),
        ("d", 21, 5, "gap"),
        ("d", None, 5, "null ordinal"),
    ]
    frame = spark.createDataFrame(rows, "series STRING, period LONG, allowed INT, label STRING")
    check = SequenceGaps(("series",), "period", "allowed", severity=Severity.WARNING)

    valid, _ = split_by_quality(frame, [check], execution_timestamp=AT)  # WARNING: no quarantine
    [result] = run_checks(frame, [check], table_name="t", execution_timestamp=AT)

    assert (result.status, result.failed_rows) == (Status.WARNING, 2)
    assert valid.count() == len(rows)


def test_sequence_gaps_with_a_constant_allowance(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (3,), (7,)], "period INT")
    [result] = run_checks(
        frame, [SequenceGaps((), "period", 2)], table_name="t", execution_timestamp=AT
    )
    assert result.failed_rows == 1  # 3 -> 7


def test_sequence_gaps_rejects_a_non_positive_allowance() -> None:
    with pytest.raises(ValueError, match="max_gap"):
        SequenceGaps(("s",), "period", 0)


def test_keep_first_removes_duplicates_deterministically(spark: SparkSession) -> None:
    rows = [("r1", 3, "late copy"), ("r1", 1, "first"), ("r2", 2, "only")]
    frame = spark.createDataFrame(rows, "reading_id STRING, arrival LONG, label STRING")
    kept = keep_first(frame, ("reading_id",), (F.col("arrival"),)).collect()
    assert sorted(r.label for r in kept) == ["first", "only"]
    assert len(rows) - len(kept) == 1  # duplicate_rows


def test_keep_first_needs_keys_and_order(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a",)], "id STRING")
    with pytest.raises(ValueError, match="keep_first"):
        keep_first(frame, (), (F.col("id"),))


def test_run_suite_keeps_declaration_order_across_tables(spark: SparkSession) -> None:
    left = spark.createDataFrame([("a",), (None,)], "id STRING")
    right = spark.createDataFrame([(1,)], StructType([StructField("n", LongType())]))
    results = run_suite(
        [
            CheckSet("left", left, [NotNull("id")]),
            CheckSet("right", right, [SchemaMatches(EXPECTED, name="schema__right")]),
        ],
        execution_timestamp=AT,
    )
    assert [(r.table_name, r.check_name, r.status) for r in results] == [
        ("left", "not_null__id", Status.ERROR),
        ("right", "schema__right", Status.ERROR),
    ]


def test_failed_schema_skips_the_row_checks_that_would_crash(spark: SparkSession) -> None:
    drifted = spark.createDataFrame([("a", 1)], "id STRING, total INT")  # amount renamed
    checks = [SchemaMatches(EXPECTED, severity=Severity.CRITICAL), NotNull("amount")]
    [result] = run_checks(drifted, checks, table_name="t", execution_timestamp=AT)
    assert (result.check_type, result.status, result.rows_checked) == (
        "schema_matches",
        Status.CRITICAL,
        1,
    )


def test_critical_schema_failure_stops_the_suite(spark: SparkSession) -> None:
    drifted = spark.createDataFrame([("a", 1)], "id STRING, total INT")
    results = run_suite(
        [
            CheckSet("t", drifted, [SchemaMatches(EXPECTED, severity=Severity.CRITICAL)]),
            CheckSet("t_enriched", drifted, [NotNull("amount")]),  # would fail to resolve
        ],
        execution_timestamp=AT,
    )
    assert [r.table_name for r in results] == ["t"]
