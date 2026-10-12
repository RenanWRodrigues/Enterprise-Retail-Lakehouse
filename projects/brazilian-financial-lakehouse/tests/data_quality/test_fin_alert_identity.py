"""Alerts are raised once per problem, and new problems are never suppressed.

Event identity: (record_key, rule) for row rules and plausibility; (indicator, dates around the
hole) for gaps. "Already recorded" is read from the state the pipeline keeps anyway: the
quarantine and the Silver table.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from financial_lakehouse.quality.suites import silver_suite, silver_table_suite
from financial_lakehouse.silver.standardize import not_in
from lakehouse_common.quality import CheckResult, Status, run_suite

AT = datetime(2026, 10, 8, tzinfo=UTC)
IPCA = "ipca_monthly"


def _warnings(results: list[CheckResult], name: str) -> list[int]:
    return [r.failed_rows for r in results if r.check_name == name and r.status is Status.WARNING]


@pytest.fixture
def ipca(silver_frame: DataFrame) -> DataFrame:
    """One monthly series of the Silver contract, with June 2024 missing (an existing gap)."""
    series = silver_frame.where(F.col("indicator_id") == IPCA)
    return series.where(F.col("reference_date") != date(2024, 6, 1))


def _keys(frame: DataFrame, *days: date) -> DataFrame:
    return frame.where(F.col("reference_date").isin(*days)).select("indicator_id", "reference_date")


def test_without_new_keys_every_gap_is_checked(ipca: DataFrame) -> None:
    results = run_suite(silver_table_suite(ipca), execution_timestamp=AT)

    assert _warnings(results, "unexpected_gaps") == [1]


def test_an_existing_gap_is_not_reported_again_when_other_keys_arrive(ipca: DataFrame) -> None:
    new_keys = _keys(ipca, date(2025, 1, 1))  # far from the June 2024 hole

    results = run_suite(silver_table_suite(ipca, new_keys=new_keys), execution_timestamp=AT)

    assert _warnings(results, "unexpected_gaps") == []


@pytest.mark.parametrize("side", [date(2024, 7, 1), date(2024, 5, 1)], ids=["after", "before"])
def test_a_gap_next_to_a_new_key_is_reported(ipca: DataFrame, side: date) -> None:
    results = run_suite(
        silver_table_suite(ipca, new_keys=_keys(ipca, side)), execution_timestamp=AT
    )

    assert _warnings(results, "unexpected_gaps") == [1]


def _implausible(silver_frame: DataFrame) -> DataFrame:
    batch = silver_frame.where(F.col("indicator_id") == IPCA).limit(3)
    return batch.withColumn("value", F.lit(Decimal("999")).cast("decimal(24,8)"))


def test_an_implausible_value_already_in_silver_is_not_reported_again(
    silver_frame: DataFrame,
) -> None:
    batch = _implausible(silver_frame)
    new_rows = not_in(batch, batch, ["record_key"])  # every version already published

    results = run_suite(silver_suite(batch, new_rows), execution_timestamp=AT)

    assert _warnings(results, "implausible_value") == []
    assert {r.status for r in results if r.check_name != "implausible_value"} == {Status.PASS}


def test_a_new_implausible_value_is_reported(silver_frame: DataFrame) -> None:
    batch = _implausible(silver_frame)
    published = batch.limit(1)  # one of the three versions was already in Silver

    results = run_suite(
        silver_suite(batch, not_in(batch, published, ["record_key"])), execution_timestamp=AT
    )

    assert _warnings(results, "implausible_value") == [2]


def test_the_critical_contract_still_covers_the_whole_batch(silver_frame: DataFrame) -> None:
    batch = silver_frame.where(F.col("indicator_id") == IPCA).limit(2)
    duplicated = batch.unionByName(batch.limit(1))
    nothing_new = not_in(duplicated, duplicated, ["record_key"])

    results = run_suite(silver_suite(duplicated, nothing_new), execution_timestamp=AT)

    assert {r.check_name for r in results if r.status is Status.CRITICAL} == {"duplicate_dates"}


def test_not_in_without_recorded_state_keeps_every_row(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a",), ("b",)], "record_key string")

    assert not_in(frame, None, ["record_key"]) is frame
