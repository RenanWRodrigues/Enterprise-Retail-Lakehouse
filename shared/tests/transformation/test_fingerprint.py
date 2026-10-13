"""``lakehouse_common.fingerprint``: stability, NULL positions, types, delimiters, dates.

Every expected value is computed independently in Python (``_reference``: SHA-256 of the texts
joined by U+001F, NULL as U+0000). Matching it proves the Spark expression is byte-compatible
with the hashes Retail and IoT already persisted.
"""

import hashlib
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_common.fingerprint import FIELD_SEPARATOR, NULL_MARKER, fingerprint


def _reference(*texts: str | None) -> str:
    joined = FIELD_SEPARATOR.join(NULL_MARKER if t is None else t for t in texts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _hashes(frame: DataFrame, *columns: str) -> list[str]:
    return [row.h for row in frame.select(fingerprint(*columns).alias("h")).collect()]


def _text_rows(spark: SparkSession, rows: Sequence[tuple[str | None, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, "a string, b string, c string")


def test_same_content_gives_the_same_hash_in_every_run(spark: SparkSession) -> None:
    rows = [("ORD-1", "12.50", "paid"), ("ORD-1", "12.50", "paid")]
    first = _hashes(_text_rows(spark, rows), "a", "b", "c")
    again = _hashes(_text_rows(spark, list(reversed(rows))), "a", "b", "c")

    assert first == again == [_reference("ORD-1", "12.50", "paid")] * 2  # real duplicates


def test_different_records_give_different_hashes(spark: SparkSession) -> None:
    rows = [("ORD-1", "12.50", "paid"), ("ORD-1", "12.50", "PAID"), ("ORD-2", "12.50", "paid")]
    assert len(set(_hashes(_text_rows(spark, rows), "a", "b", "c"))) == len(rows)


def test_null_is_kept_in_its_position(spark: SparkSession) -> None:
    rows = [
        ("a", None, "b"),
        ("a", "b", None),
        (None, "a", "b"),
        ("a", "", "b"),
        (None, None, None),
    ]

    hashes = _hashes(_text_rows(spark, rows), "a", "b", "c")

    assert hashes == [_reference(*row) for row in rows]
    assert len(set(hashes)) == len(rows)  # NULL != "" and every position counts
    assert all(len(h) == 64 for h in hashes)  # an all-NULL row still has a hash


def test_column_order_is_part_of_the_hash(spark: SparkSession) -> None:
    frame = _text_rows(spark, [("x", "y", "z")])
    assert _hashes(frame, "a", "b", "c") != _hashes(frame, "c", "b", "a")


def test_names_and_expressions_are_interchangeable(spark: SparkSession) -> None:
    frame = _text_rows(spark, [(" a ", None, "b")])
    by_name = frame.select(fingerprint("a", "b", "c").alias("h")).first()
    by_expression = frame.select(fingerprint(F.col("a"), F.col("b"), F.col("c")).alias("h")).first()
    trimmed = frame.select(fingerprint(F.trim("a"), "b", "c").alias("h")).first()

    assert by_name == by_expression
    assert trimmed is not None and trimmed.h == _reference("a", None, "b")  # caller normalizes


@pytest.mark.parametrize(
    "value",
    [",", "|", ";", "\t", "\n", '"', "\\", "a,b", "a|b", "ação", "😀"],
)
def test_ordinary_delimiters_and_unicode_are_hashed_as_text(
    spark: SparkSession, value: str
) -> None:
    frame = _text_rows(spark, [(value, "x", None), ("x", value, None)])

    assert _hashes(frame, "a", "b", "c") == [
        _reference(value, "x", None),
        _reference("x", value, None),
    ]


def test_documented_limitation_the_separator_and_null_marker_inside_values(
    spark: SparkSession,
) -> None:
    """Not injective for U+001F / U+0000 inside values (absent from the sources; escaping would
    change persisted hashes). Pinned so that a change of behaviour is a conscious decision."""
    rows = [("a\u001fb", "c", "z"), ("a", "b\u001fc", "z"), ("\u0000", "c", "z"), (None, "c", "z")]

    shifted, other, marker, null = _hashes(_text_rows(spark, rows), "a", "b", "c")

    assert shifted == other
    assert marker == null


def test_types_are_hashed_by_their_text(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [(1, "1", Decimal("1.00"), 1.5, True)],
        "i int, s string, d decimal(10,2), f double, b boolean",
    )
    [row] = frame.select(
        fingerprint("i").alias("i"),
        fingerprint("s").alias("s"),
        fingerprint("d").alias("d"),
        fingerprint("f").alias("f"),
        fingerprint("b").alias("b"),
    ).collect()

    assert row.i == row.s == _reference("1")  # the type itself is not hashed
    assert row.d == _reference("1.00")  # decimal keeps its scale: 1.0 and 1.00 differ
    assert row.f == _reference("1.5")
    assert row.b == _reference("true")


def test_dates_and_timestamps_use_iso_text_in_the_session_time_zone(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"  # as in every pipeline
    frame = spark.createDataFrame(
        [(date(2024, 2, 29), datetime(2024, 1, 1, 12, 30, 5, tzinfo=UTC))],
        "d date, t timestamp",
    )

    [row] = frame.select(fingerprint("d", "t").alias("h")).collect()

    assert row.h == _reference("2024-02-29", "2024-01-01 12:30:05")


def test_no_columns_is_an_error() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        fingerprint()
