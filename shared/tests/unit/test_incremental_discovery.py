"""Incremental discovery of Bronze units over the Delta log (no Spark: the log is faked).

A unit is what a Silver run processes atomically: a Retail batch, an IoT file. Each Bronze commit
appends whole units, so the units of the versions after the checkpoint are exactly the new ones.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from lakehouse_common.incremental import (
    BronzeContractError,
    ChangesUnavailableError,
    Checkpoint,
    Discovery,
    DiscoveryMode,
    InMemoryCheckpointStore,
    discover,
    pending_units,
)

TABLE = "bronze.bronze_orders"
COLUMNS = ("_batch_id",)
AT = datetime(2026, 10, 12, 12, tzinfo=UTC)
Unit = tuple[object, ...]


@dataclass
class FakeLog:
    """A Delta table as a list of commits; each commit appends some units."""

    commits: list[list[Unit]] = field(default_factory=list)
    exists_: bool = True
    feed: bool = True
    retained_from: int = 0  # change data older than this version is gone (retention, VACUUM)
    deletes_in: set[int] = field(default_factory=set)
    reads: list[str] = field(default_factory=list)

    def append(self, *units: str) -> None:
        self.commits.append([(unit,) for unit in units])

    def exists(self, table: str) -> bool:
        return self.exists_ and bool(self.commits)

    def latest_version(self, table: str) -> int:
        return len(self.commits) - 1

    def change_feed_enabled(self, table: str) -> bool:
        return self.feed

    def changed_units(self, table: str, start: int, end: int, columns: Sequence[str]) -> list[Unit]:
        self.reads.append(f"changes {start}..{end}")
        if start < self.retained_from:
            raise ChangesUnavailableError(f"versions before {self.retained_from} were vacuumed")
        if self.deletes_in & set(range(start, end + 1)):
            raise BronzeContractError("delete in an append-only Bronze")
        return sorted({u for v in range(start, end + 1) for u in self.commits[v]})

    def snapshot_units(self, table: str, version: int, columns: Sequence[str]) -> list[Unit]:
        self.reads.append(f"snapshot {version}")
        return sorted({u for v in range(version + 1) for u in self.commits[v]})


def _discover(log: FakeLog, checkpoint: int | None, *, rediscover: bool = False) -> Discovery:
    return discover(log, TABLE, columns=COLUMNS, checkpoint=checkpoint, rediscover=rediscover)


def test_empty_bronze_has_nothing_to_discover() -> None:
    result = _discover(FakeLog(), None)

    assert (result.units, result.mode, result.end) == ((), DiscoveryMode.EMPTY, None)


def test_first_run_reads_the_snapshot_once() -> None:
    log = FakeLog()
    log.append("b1")
    log.append("b2")

    result = _discover(log, None)

    assert (result.units, result.mode, result.end) == (
        (("b1",), ("b2",)),
        DiscoveryMode.FIRST_RUN,
        1,
    )
    assert log.reads == ["snapshot 1"]


def test_a_new_batch_is_read_from_its_commit_only() -> None:
    log = FakeLog()
    log.append("b1")
    log.append("b2")  # version 1: new since the checkpoint 0

    result = _discover(log, 0)

    assert (result.units, result.mode, result.after, result.end) == (
        (("b2",),),
        DiscoveryMode.INCREMENTAL,
        0,
        1,
    )
    assert log.reads == ["changes 1..1"]  # cost: the new commit, not the table


def test_several_pending_batches_in_several_commits() -> None:
    log = FakeLog()
    for batch in ("b1", "b2", "b3", "b4"):
        log.append(batch)

    result = _discover(log, 1)

    assert result.units == (("b3",), ("b4",))
    assert log.reads == ["changes 2..3"]


def test_nothing_new_reads_nothing() -> None:
    log = FakeLog()
    log.append("b1")

    result = _discover(log, 0)

    assert (result.units, result.mode) == ((), DiscoveryMode.INCREMENTAL)
    assert log.reads == []


@pytest.mark.parametrize(
    ("setup", "reason"),
    [
        (lambda log: setattr(log, "retained_from", 2), "change data unavailable"),
        (lambda log: setattr(log, "feed", False), "change data feed disabled"),
    ],
    ids=["history-gone", "feed-disabled"],
)
def test_without_change_data_the_table_is_scanned_and_the_reason_kept(
    setup: object, reason: str
) -> None:
    log = FakeLog()
    for batch in ("b1", "b2", "b3"):
        log.append(batch)
    setup(log)  # type: ignore[operator]

    result = _discover(log, 0)

    assert result.mode is DiscoveryMode.FULL_SCAN_FALLBACK
    assert reason in result.reason
    assert result.units == (("b1",), ("b2",), ("b3",))  # processed ones are filtered later


def test_a_checkpoint_ahead_of_the_table_falls_back_to_a_scan() -> None:
    """Table recreated or restored: the version numbers no longer mean what the checkpoint says."""
    log = FakeLog()
    log.append("b1")

    result = _discover(log, 7)

    assert (result.mode, result.end) == (DiscoveryMode.FULL_SCAN_FALLBACK, 0)
    assert "ahead" in result.reason


def test_an_update_or_delete_in_bronze_stops_the_run() -> None:
    log = FakeLog()
    log.append("b1")
    log.append("b2")
    log.deletes_in = {1}

    with pytest.raises(BronzeContractError):
        _discover(log, 0)


def test_explicit_rediscovery_scans_regardless_of_the_checkpoint() -> None:
    log = FakeLog()
    log.append("b1")
    log.append("b2")

    result = _discover(log, 1, rediscover=True)

    assert (result.mode, result.units) == (DiscoveryMode.REDISCOVERY, (("b1",), ("b2",)))


# --- pending = discovered - processed (+ explicit reprocessing) ------------------------------


def test_partially_processed_range_keeps_only_the_unprocessed() -> None:
    """A run failed after b2: b2 is marked processed, the checkpoint did not move."""
    assert pending_units(["b2", "b3", "b4"], processed={"b1", "b2"}) == ["b3", "b4"]


def test_explicit_reprocessing_brings_processed_units_back() -> None:
    assert pending_units(
        ["b3"], processed={"b1", "b2"}, reprocess=["b1"], known={"b1", "b2", "b3"}
    ) == ["b1", "b3"]


def test_reprocessing_an_unknown_unit_is_refused() -> None:
    with pytest.raises(ValueError, match="b9"):
        pending_units([], processed=set(), reprocess=["b9"], known={"b1"})


# --- checkpoint: committed after the work, never rewound -------------------------------------


def _checkpoint(version: int, run_id: str = "r1") -> Checkpoint:
    return Checkpoint("retail_silver", TABLE, version, DiscoveryMode.INCREMENTAL, run_id, AT)


def test_failure_and_retry_rediscover_the_same_range() -> None:
    log = FakeLog()
    log.append("b1")
    store = InMemoryCheckpointStore()
    store.commit(_checkpoint(0))
    log.append("b2")
    log.append("b3")

    first = _discover(log, store.read("retail_silver", TABLE))
    # the run fails before its checkpoint commit: nothing is committed
    retry = _discover(log, store.read("retail_silver", TABLE))

    assert first.units == retry.units == (("b2",), ("b3",))


def test_concurrent_runs_never_rewind_the_checkpoint() -> None:
    """Two runs overlap; the slower one commits an older version last: it must not win."""
    store = InMemoryCheckpointStore()
    store.commit(_checkpoint(5, "fast"))
    store.commit(_checkpoint(3, "slow"))

    assert store.read("retail_silver", TABLE) == 5
    assert [c.run_id for c in store.history] == ["fast"]  # the stale commit is not recorded


def test_concurrent_discoveries_see_the_same_units() -> None:
    log = FakeLog()
    log.append("b1")
    log.append("b2")

    a, b = _discover(log, 0), _discover(log, 0)

    assert a.units == b.units == (("b2",),)  # idempotent MERGEs absorb the overlap
