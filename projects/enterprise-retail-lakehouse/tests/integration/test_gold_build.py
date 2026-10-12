"""Gold publication on Delta: staging, quality gate, previous Gold preserved, observability.

Requires Delta Lake: run in the test container (Linux). Each test publishes to its own schema.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus
from lakehouse_common.quality import CriticalQualityError, Status
from retail_lakehouse.gold import GOLD_SCHEMAS, GoldTarget
from retail_lakehouse.gold.keys import UNKNOWN_KEY
from retail_lakehouse.gold.pipeline import write_gold

AT = datetime(2026, 10, 8, 12, tzinfo=UTC)
Tables = dict[str, DataFrame]


def _version(spark: SparkSession, table: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version").alias("v")).first()
    assert row is not None
    return int(row["v"])


def _versions(spark: SparkSession, target: GoldTarget) -> dict[str, int]:
    """Latest Delta version of every published table (unchanged = not rewritten)."""
    return {name: _version(spark, target.table(name)) for name in GOLD_SCHEMAS}


def _counts(spark: SparkSession, target: GoldTarget) -> dict[str, int]:
    return {name: spark.table(target.table(name)).count() for name in GOLD_SCHEMAS}


def _extra_sale(tables: Tables, order_id: str, **columns: object) -> Tables:
    """One more ``fact_sales`` row: a copy of an existing line under a new order id."""
    sales = tables["fact_sales"]
    extra = sales.limit(1).withColumn("order_id", F.lit(order_id))
    for column, value in columns.items():
        extra = extra.withColumn(column, F.lit(value).cast(sales.schema[column].dataType))
    return {**tables, "fact_sales": sales.unionByName(extra)}


def _duplicated_order_line(tables: Tables) -> Tables:
    sales = tables["fact_sales"]
    return {**tables, "fact_sales": sales.unionByName(sales.limit(1))}


def _sale_outside_dim_date(tables: Tables) -> Tables:
    return _extra_sale(tables, "O-DANGLING", date_key=20991231)


def _sale_without_customer_key(tables: Tables) -> Tables:
    return _extra_sale(tables, "O-NULL", customer_sk=None)


INVALID: list[tuple[Callable[[Tables], Tables], str]] = [
    (_duplicated_order_line, "grain__fact_sales"),
    (_sale_outside_dim_date, "fk__date_key__dim_date"),
    (_sale_without_customer_key, "not_null__customer_sk"),
]


def test_a_valid_gold_is_published(delta_spark: SparkSession, gold_tables: Tables) -> None:
    target, runs = GoldTarget("gold_valid"), InMemoryObservabilitySink()

    write_gold(gold_tables, target, run_id="R1", sink=runs, now=AT)

    staged = {n: delta_spark.table(target.staging_table(n)).count() for n in GOLD_SCHEMAS}
    assert _counts(delta_spark, target) == staged
    gate, *published = runs.runs
    assert (gate.table, gate.status) == ("gold_valid_staging.*", RunStatus.SUCCEEDED)
    assert gate.output_rows == sum(staged.values())
    assert {r.status for r in gate.quality} == {Status.PASS}
    assert [r.table for r in published] == [target.table(n) for n in GOLD_SCHEMAS]
    assert {(r.project, r.layer, r.status) for r in published} == {
        ("retail", "gold", RunStatus.SUCCEEDED)
    }
    for name in GOLD_SCHEMAS:
        comment = (
            delta_spark.sql(f"DESCRIBE TABLE EXTENDED {target.table(name)}")
            .where("col_name = 'Comment'")
            .first()
        )
        assert comment is not None and "Grain" in comment["data_type"]


def test_a_rerun_is_idempotent(delta_spark: SparkSession, gold_tables: Tables) -> None:
    target, runs = GoldTarget("gold_rerun"), InMemoryObservabilitySink()
    write_gold(gold_tables, target, run_id="R1", sink=runs, now=AT)
    first = {n: delta_spark.read.format("delta").option("versionAsOf", v).table(target.table(n))
             for n, v in _versions(delta_spark, target).items()}  # fmt: skip

    write_gold(gold_tables, target, run_id="R2", sink=runs, now=AT)

    for name, before in first.items():
        after = delta_spark.table(target.table(name))
        assert after.count() == before.count(), name
        assert after.exceptAll(before).isEmpty() and before.exceptAll(after).isEmpty(), name


@pytest.mark.parametrize(("corrupt", "failed_check"), INVALID, ids=[check for _, check in INVALID])
def test_an_invalid_gold_is_not_published_and_the_previous_one_is_kept(
    delta_spark: SparkSession,
    gold_tables: Tables,
    caplog: pytest.LogCaptureFixture,
    corrupt: Callable[[Tables], Tables],
    failed_check: str,
) -> None:
    target, runs = GoldTarget(f"gold_{failed_check}"), InMemoryObservabilitySink()
    write_gold(gold_tables, target, run_id="R1", sink=runs, now=AT)
    versions, counts = _versions(delta_spark, target), _counts(delta_spark, target)

    with (
        caplog.at_level(logging.WARNING, logger="lakehouse.alerts"),
        pytest.raises(CriticalQualityError, match=failed_check),
    ):
        write_gold(corrupt(gold_tables), target, run_id="R2", sink=runs, now=AT)

    # the previous Gold is untouched: same Delta versions, same rows
    assert _versions(delta_spark, target) == versions
    assert _counts(delta_spark, target) == counts
    # pipeline_runs: one FAILED gate step, no table published
    [gate] = [r for r in runs.runs if r.run_id == "R2"]
    assert gate.status is RunStatus.FAILED
    assert gate.error_message is not None and failed_check in gate.error_message
    assert gate.output_rows is None and gate.input_rows is not None
    assert failed_check in {r.check_name for r in gate.quality if r.status is Status.CRITICAL}
    critical = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any(failed_check in message for message in critical)


def test_a_failed_first_run_creates_no_gold_table(
    delta_spark: SparkSession, gold_tables: Tables
) -> None:
    target, runs = GoldTarget("gold_first_failure"), InMemoryObservabilitySink()

    with pytest.raises(CriticalQualityError):
        write_gold(_duplicated_order_line(gold_tables), target, run_id="R1", sink=runs, now=AT)

    assert not any(delta_spark.catalog.tableExists(target.table(n)) for n in GOLD_SCHEMAS)
    assert [r.status for r in runs.runs] == [RunStatus.FAILED]


def test_a_sale_of_an_unknown_customer_is_published_with_an_alert(
    delta_spark: SparkSession, gold_tables: Tables
) -> None:
    target, runs = GoldTarget("gold_unknown_member"), InMemoryObservabilitySink()

    write_gold(
        _extra_sale(gold_tables, "O-ORPHAN", customer_sk=UNKNOWN_KEY),
        target,
        run_id="R1",
        sink=runs,
        now=AT,
    )

    published = delta_spark.table(target.table("fact_sales")).where("order_id = 'O-ORPHAN'")
    assert {row["customer_sk"] for row in published.select("customer_sk").collect()} == {
        UNKNOWN_KEY
    }
    gate = runs.runs[0]
    assert gate.status is RunStatus.SUCCEEDED_WITH_ALERTS
    assert {r.check_name for r in gate.quality if r.status is Status.WARNING} == {
        "unknown_member__customer_sk"
    }
