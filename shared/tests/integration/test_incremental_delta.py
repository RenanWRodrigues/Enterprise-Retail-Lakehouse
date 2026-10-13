"""Incremental discovery on real Delta tables (Linux/CI: Delta needs Hadoop helpers on Windows).

The decision logic is covered without Delta in ``shared/tests/unit/test_incremental_discovery.py``;
here, the Spark side: commits, Change Data Feed, missing change data and the checkpoint MERGE.
"""

import itertools
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from lakehouse_common.incremental import (
    BronzeContractError,
    Checkpoint,
    DeltaCheckpointStore,
    DiscoveryMode,
    SparkDeltaLog,
    discover,
)

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)
_names = itertools.count()


def _table(spark: SparkSession, *, feed: bool) -> str:
    """A fresh append-only Bronze-like table, created by its first batch."""
    name = f"incr_{next(_names)}"
    spark.sql("CREATE SCHEMA IF NOT EXISTS incr")
    table = f"incr.{name}"
    writer = (
        spark.createDataFrame([("b1", 1)], "_batch_id string, n int").writeTo(table).using("delta")
    )
    writer = writer.tableProperty("delta.appendOnly", "true")
    if feed:
        writer = writer.tableProperty("delta.enableChangeDataFeed", "true")
    writer.create()
    return table


def _append(spark: SparkSession, table: str, batch: str) -> None:
    spark.createDataFrame([(batch, 1), (batch, 2)], "_batch_id string, n int").writeTo(
        table
    ).append()


def test_first_run_then_only_the_new_commits(delta_spark: SparkSession) -> None:
    log = SparkDeltaLog(delta_spark)
    table = _table(delta_spark, feed=True)
    _append(delta_spark, table, "b2")

    first = discover(log, table, columns=("_batch_id",), checkpoint=None)
    _append(delta_spark, table, "b3")
    _append(delta_spark, table, "b4")
    second = discover(log, table, columns=("_batch_id",), checkpoint=first.end)

    assert (first.mode, first.units, first.end) == (DiscoveryMode.FIRST_RUN, (("b1",), ("b2",)), 1)
    assert (second.mode, second.units, second.end) == (
        DiscoveryMode.INCREMENTAL,
        (("b3",), ("b4",)),
        3,
    )


def test_missing_table_is_empty(delta_spark: SparkSession) -> None:
    result = discover(
        SparkDeltaLog(delta_spark), "incr.never_created", columns=("_batch_id",), checkpoint=None
    )

    assert result.mode is DiscoveryMode.EMPTY


def test_commits_without_change_data_fall_back_to_a_scan(delta_spark: SparkSession) -> None:
    """Feed enabled after the data was written: those versions have no change data."""
    table = _table(delta_spark, feed=False)
    _append(delta_spark, table, "b2")
    delta_spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    result = discover(SparkDeltaLog(delta_spark), table, columns=("_batch_id",), checkpoint=0)

    assert result.mode is DiscoveryMode.FULL_SCAN_FALLBACK
    assert result.units == (("b1",), ("b2",))


def test_feed_disabled_falls_back_to_a_scan(delta_spark: SparkSession) -> None:
    table = _table(delta_spark, feed=False)
    _append(delta_spark, table, "b2")

    result = discover(SparkDeltaLog(delta_spark), table, columns=("_batch_id",), checkpoint=0)

    assert (result.mode, result.units) == (DiscoveryMode.FULL_SCAN_FALLBACK, (("b1",), ("b2",)))
    assert "disabled" in result.reason


def test_a_delete_in_bronze_stops_discovery(delta_spark: SparkSession) -> None:
    name = f"incr.mutable_{next(_names)}"
    delta_spark.sql("CREATE SCHEMA IF NOT EXISTS incr")
    delta_spark.createDataFrame([("b1", 1)], "_batch_id string, n int").writeTo(name).using(
        "delta"
    ).tableProperty("delta.enableChangeDataFeed", "true").create()
    delta_spark.sql(f"DELETE FROM {name} WHERE _batch_id = 'b1'")  # noqa: S608 - test table name

    with pytest.raises(BronzeContractError):
        discover(SparkDeltaLog(delta_spark), name, columns=("_batch_id",), checkpoint=0)


def test_checkpoint_is_durable_and_never_rewinds(delta_spark: SparkSession) -> None:
    store = DeltaCheckpointStore(delta_spark, f"incr.checkpoints_{next(_names)}")
    source = "incr.bronze_x"

    assert store.read("silver", source) is None
    store.commit(Checkpoint("silver", source, 5, DiscoveryMode.INCREMENTAL, "fast", AT))
    store.commit(Checkpoint("silver", source, 3, DiscoveryMode.INCREMENTAL, "slow", AT))

    assert store.read("silver", source) == 5
