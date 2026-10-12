"""Financial quality suites: duplicated dates, nulls, gaps, invalid values, schema changes."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from financial_lakehouse.quality import (
    bronze_suite,
    gold_suite,
    silver_suite,
    silver_table_suite,
)
from financial_lakehouse.silver.pipeline import run_silver
from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus
from lakehouse_common.quality import CheckResult, CriticalQualityError, Status, run_suite

AT = datetime(2026, 10, 6, 15, tzinfo=UTC)
Factory = Callable[..., dict[str, Any]]
Frame = Callable[[list[dict[str, Any]]], DataFrame]


def _by_name(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {r.check_name: r for r in results}


def test_standardized_series_pass_the_gate(silver_frame: DataFrame) -> None:
    results = run_suite(silver_suite(silver_frame), execution_timestamp=AT)
    assert {r.check_name: r.status for r in results} == {r.check_name: Status.PASS for r in results}


def test_duplicated_dates_and_nulls_are_critical(silver_frame: DataFrame) -> None:
    one = silver_frame.limit(1)
    broken = silver_frame.unionByName(one).unionByName(
        one.withColumn("reference_date", F.lit(date(1999, 1, 1))).withColumn(
            "value", F.lit(None).cast("decimal(24,8)")
        )
    )
    results = _by_name(run_suite(silver_suite(broken), execution_timestamp=AT))
    assert (results["duplicate_dates"].status, results["duplicate_dates"].failed_rows) == (
        Status.CRITICAL,
        2,
    )
    assert results["not_null__value"].status is Status.CRITICAL


def test_implausible_values_only_warn(silver_frame: DataFrame) -> None:
    usd = F.col("indicator_id") == "usd_brl_sell"
    spiked = silver_frame.withColumn(
        "value",
        F.when(usd & (F.col("reference_date") == date(2025, 3, 3)), F.lit(55))
        .otherwise(F.col("value"))
        .cast("decimal(24,8)"),
    )
    result = _by_name(run_suite(silver_suite(spiked), execution_timestamp=AT))["implausible_value"]
    assert (result.status, result.failed_rows) == (Status.WARNING, 1)


def test_full_history_has_no_unexpected_gaps(silver_frame: DataFrame) -> None:
    [result] = run_suite(silver_table_suite(silver_frame), execution_timestamp=AT)
    assert result.status is Status.PASS


def test_missing_periods_are_reported_as_gaps(silver_frame: DataFrame) -> None:
    missing_month = (F.col("indicator_id") == "ipca_monthly") & (
        F.col("reference_date") == date(2024, 6, 1)
    )
    missing_week = (F.col("indicator_id") == "usd_brl_sell") & F.col("reference_date").between(
        date(2025, 3, 3), date(2025, 3, 7)
    )
    holed = silver_frame.where(~(missing_month | missing_week))
    [result] = run_suite(silver_table_suite(holed), execution_timestamp=AT)
    assert (result.check_name, result.status, result.failed_rows) == (
        "unexpected_gaps",
        Status.WARNING,
        2,  # July 2024 IPCA; the Monday after the missing week
    )


def test_bronze_schema_change_is_critical(bronze_frame: Frame, bronze_row: Factory) -> None:
    bronze = bronze_frame([bronze_row()]).withColumnRenamed("value", "valor")
    [result] = run_suite(bronze_suite(bronze), execution_timestamp=AT)
    assert (result.status, result.details) == (Status.CRITICAL, "missing: value; unexpected: valor")


def test_gold_tables_pass_contract_and_grain(gold_tables: dict[str, DataFrame]) -> None:
    results = run_suite(gold_suite(gold_tables), execution_timestamp=AT)
    assert len(results) == 3 * len(gold_tables)
    assert {r.status for r in results} == {Status.PASS}


def test_silver_run_with_a_changed_source_fails_and_is_recorded(
    spark: SparkSession,
    bronze_frame: Frame,
    bronze_row: Factory,
) -> None:
    drifted = bronze_frame([bronze_row()]).withColumnRenamed("value", "valor")
    drifted.createOrReplaceTempView("fin_bronze_drifted")
    sink = InMemoryObservabilitySink()
    try:
        with pytest.raises(CriticalQualityError):
            run_silver(
                spark,
                bronze_table="fin_bronze_drifted",
                namespace="fin_obs_none",
                now=AT,
                sink=sink,
            )
    finally:
        spark.catalog.dropTempView("fin_bronze_drifted")

    [run] = sink.runs
    assert (run.pipeline_name, run.layer, run.status) == (
        "financial_silver",
        "silver",
        RunStatus.FAILED,
    )
    assert run.error_message is not None and "schema__bronze" in run.error_message
    assert [(r.check_name, r.status) for r in run.quality] == [("schema__bronze", Status.CRITICAL)]
    assert run.output_rows is None  # stopped before reading or writing Silver
