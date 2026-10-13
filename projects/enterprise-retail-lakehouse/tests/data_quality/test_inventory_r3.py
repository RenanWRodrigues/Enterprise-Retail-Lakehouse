"""R3 (BUSINESS_RULES.md §3): the stock snapshot equals the movement ledger up to its date.

For each (snapshot_date, store_id, product_id): quantity_on_hand = Σ quantity of the Silver
movements whose Brasília business date is on or before snapshot_date. Evaluated by the Gold gate
before publication: a mismatched key is ERROR (alert, Gold published), more than 5% of the keys
is CRITICAL (nothing published, previous Gold kept).
"""

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakehouse_common.observability import InMemoryObservabilitySink, Layer, track_run
from lakehouse_common.quality import CheckResult, CriticalQualityError, Status, run_suite
from retail_lakehouse.gold.inventory import inventory_reconciliation
from retail_lakehouse.gold.schemas import GOLD_SCHEMAS
from retail_lakehouse.quality.suites import inventory_reconciliation_suite, retail_gold_suite
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)
DAY = date(2025, 12, 31)
EARLY = datetime(2025, 6, 1, 12, tzinfo=UTC)
LAST_MINUTE = datetime(2026, 1, 1, 2, 59, tzinfo=UTC)  # 2025-12-31 23:59 in Brasília
NEXT_DAY = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)  # 2026-01-01 00:00 in Brasília


def _frame(spark: SparkSession, entity: str, rows: Sequence[dict[str, Any]]) -> DataFrame:
    schema = SILVER_SCHEMAS[entity]
    return spark.createDataFrame(
        [tuple(r.get(n) for n in schema.fieldNames()) for r in rows], schema
    )


def _snapshot(spark: SparkSession, rows: Sequence[tuple[str, str, int]]) -> DataFrame:
    return _frame(
        spark,
        "inventory",
        [
            {"snapshot_date": DAY, "store_id": s, "product_id": p, "quantity_on_hand": q}
            for s, p, q in rows
        ],
    )


def _movements(spark: SparkSession, rows: Sequence[tuple[str, str, int, datetime]]) -> DataFrame:
    return _frame(
        spark,
        "inventory_movements",
        [
            {
                "movement_id": f"M{i}",
                "store_id": s,
                "product_id": p,
                "quantity": q,
                "movement_timestamp": at,
            }
            for i, (s, p, q, at) in enumerate(rows)
        ],
    )


def _check(frame: DataFrame) -> dict[str, CheckResult]:
    results = run_suite(inventory_reconciliation_suite(frame), execution_timestamp=AT)
    return {result.check_name: result for result in results}


def _mismatches(frame: DataFrame) -> dict[tuple[str, str], tuple[int | None, int | None]]:
    rows = frame.where("difference <> 0").collect()
    return {
        (r["store_id"], r["product_id"]): (r["quantity_on_hand"], r["movement_balance"])
        for r in rows
    }


def test_balance_equal_to_the_snapshot_passes(spark: SparkSession) -> None:
    snapshot = _snapshot(spark, [("ST1", "P1", 7)])
    movements = _movements(spark, [("ST1", "P1", 10, EARLY), ("ST1", "P1", -3, LAST_MINUTE)])

    results = _check(inventory_reconciliation(snapshot, movements))

    assert {name: r.status for name, r in results.items()} == {
        "r3__snapshot_matches_movements": Status.PASS,
        "r3__snapshot_matches_movements_systemic": Status.PASS,
    }
    assert results["r3__snapshot_matches_movements"].rows_checked == 1


def test_a_mismatched_key_is_an_error_not_a_block(spark: SparkSession) -> None:
    keys = [("ST1", f"P{i}", 5) for i in range(40)]
    snapshot = _snapshot(spark, [*keys[:39], ("ST1", "P39", 6)])  # one unit too many
    movements = _movements(spark, [(s, p, q, EARLY) for s, p, q in keys])

    frame = inventory_reconciliation(snapshot, movements)
    results = _check(frame)

    assert (
        results["r3__snapshot_matches_movements"].status,
        results["r3__snapshot_matches_movements"].failed_rows,
    ) == (Status.ERROR, 1)
    assert results["r3__snapshot_matches_movements_systemic"].status is Status.PASS  # 2.5%
    assert _mismatches(frame) == {("ST1", "P39"): (6, 5)}


def test_snapshot_key_without_movements(spark: SparkSession) -> None:
    snapshot = _snapshot(spark, [("ST1", "P1", 5), ("ST1", "P2", 0)])

    frame = inventory_reconciliation(snapshot, _movements(spark, []))

    # P2: nothing on hand and nothing moved is consistent; P1 claims stock nobody received
    assert _mismatches(frame) == {("ST1", "P1"): (5, None)}


def test_no_snapshot_means_nothing_to_reconcile(spark: SparkSession) -> None:
    movements = _movements(spark, [("ST1", "P1", 10, EARLY)])

    results = _check(inventory_reconciliation(_snapshot(spark, []), movements))

    result = results["r3__snapshot_matches_movements"]
    assert (result.status, result.rows_checked) == (Status.PASS, 0)


def test_stock_missing_from_the_snapshot_is_inconsistent(spark: SparkSession) -> None:
    snapshot = _snapshot(spark, [("ST1", "P1", 10)])
    movements = _movements(
        spark,
        [
            ("ST1", "P1", 10, EARLY),
            ("ST1", "P2", 4, EARLY),  # stock the snapshot does not list
            ("ST1", "P3", 2, EARLY),
            ("ST1", "P3", -2, EARLY),  # moved but back to zero: absent from the snapshot is fine
        ],
    )

    assert _mismatches(inventory_reconciliation(snapshot, movements)) == {("ST1", "P2"): (None, 4)}


def test_only_movements_up_to_the_snapshot_business_date_count(spark: SparkSession) -> None:
    """Reprocessing with later batches (incremental: no new snapshot) keeps the result."""
    snapshot = _snapshot(spark, [("ST1", "P1", 7)])
    first = [("ST1", "P1", 10, EARLY), ("ST1", "P1", -3, LAST_MINUTE)]
    later = [("ST1", "P1", -2, NEXT_DAY), ("ST1", "P1", 50, datetime(2026, 3, 1, tzinfo=UTC))]

    before = inventory_reconciliation(snapshot, _movements(spark, first))
    after = inventory_reconciliation(snapshot, _movements(spark, [*first, *later]))

    assert _mismatches(before) == _mismatches(after) == {}
    balances = [[r["movement_balance"] for r in f.collect()] for f in (before, after)]
    assert balances == [[7], [7]]


def test_widespread_mismatch_blocks_publication(spark: SparkSession) -> None:
    keys = [("ST1", f"P{i}", 5) for i in range(20)]
    snapshot = _snapshot(spark, keys)
    movements = _movements(
        spark,
        [(s, p, 4, EARLY) for s, p, _ in keys[:2]] + [(s, p, q, EARLY) for s, p, q in keys[2:]],
    )
    sink = InMemoryObservabilitySink()

    with (
        pytest.raises(CriticalQualityError),
        track_run(
            pipeline_name="retail_gold",
            run_id="G1",
            project="retail",
            layer=Layer.GOLD,
            table="retail_gold_staging.*",
            sink=sink,
        ) as step,
    ):
        step.record_quality(
            run_suite(
                inventory_reconciliation_suite(inventory_reconciliation(snapshot, movements)),
                execution_timestamp=AT,
            )
        )

    [run] = sink.runs
    critical = [r for r in run.quality if r.status is Status.CRITICAL]
    assert [(r.check_name, r.failed_rows, r.rows_checked) for r in critical] == [
        ("r3__snapshot_matches_movements_systemic", 2, 20)  # 10% > 5%
    ]


def test_the_gold_gate_includes_r3_when_given(spark: SparkSession) -> None:
    gold = {name: spark.createDataFrame([], schema) for name, schema in GOLD_SCHEMAS.items()}
    reconciliation = inventory_reconciliation(_snapshot(spark, []), _movements(spark, []))

    without = retail_gold_suite(gold)
    with_r3 = retail_gold_suite(gold, inventory_reconciliation=reconciliation)

    assert "inventory_reconciliation" not in {cs.table_name for cs in without}
    assert with_r3[-1].table_name == "inventory_reconciliation"
    assert [cs.table_name for cs in with_r3[:-1]] == [cs.table_name for cs in without]


def test_generated_data_reconciles_exactly(silver_tables: dict[str, DataFrame]) -> None:
    """The generator's snapshot and ledger (clean Silver) satisfy R3 with the date cut."""
    frame = inventory_reconciliation(
        silver_tables["inventory"], silver_tables["inventory_movements"]
    )

    results = _check(frame)

    result = results["r3__snapshot_matches_movements"]
    assert result.rows_checked > 0
    assert (result.status, result.failed_rows) == (Status.PASS, 0)
