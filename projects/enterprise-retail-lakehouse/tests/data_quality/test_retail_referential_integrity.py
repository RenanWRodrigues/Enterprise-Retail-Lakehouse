"""Retail Silver referential integrity: shipments -> orders, inventory_movements -> products/stores.

Policy (ARCHITECTURE.md §7, 007/047): a missing reference is WARNING per row (late master data
maps to the unknown member in Gold) and CRITICAL above 5% of the batch (systemic break); a NULL
required key is ERROR. Each finding carries the count and a few orphan keys for diagnosis.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakehouse_common.observability import InMemoryObservabilitySink, Layer, track_run
from lakehouse_common.quality import (
    CheckResult,
    CriticalQualityError,
    QualityDecision,
    Status,
    decide,
    run_suite,
)
from retail_lakehouse.quality.suites import REFERENCES, retail_entity_suite
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)


def _frame(spark: SparkSession, entity: str, rows: list[dict[str, Any]]) -> DataFrame:
    schema = SILVER_SCHEMAS[entity]
    return spark.createDataFrame(
        [tuple(r.get(n) for n in schema.fieldNames()) for r in rows], schema
    )


def _references(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "orders": _frame(spark, "orders", [{"order_id": f"O{i}"} for i in range(1, 41)]),
        "products": _frame(spark, "products", [{"product_id": f"P{i}"} for i in range(1, 41)]),
        "stores": _frame(spark, "stores", [{"store_id": "ST1"}, {"store_id": "ST2"}]),
    }


def _shipments(spark: SparkSession, order_ids: Sequence[str | None]) -> DataFrame:
    rows = [{"shipment_id": f"SH{i}", "order_id": o} for i, o in enumerate(order_ids)]
    return _frame(spark, "shipments", rows)


def _movements(
    spark: SparkSession, keys: Sequence[tuple[str | None, str | None]], reference: str | None = None
) -> DataFrame:
    rows = [
        {
            "movement_id": f"M{i}",
            "store_id": store,
            "product_id": product,
            "quantity": 1,
            "reference_id": reference,
        }
        for i, (store, product) in enumerate(keys)
    ]
    return _frame(spark, "inventory_movements", rows)


def _check(
    entity: str, frame: DataFrame, references: dict[str, DataFrame]
) -> dict[str, CheckResult]:
    needed = {name: references[name] for name in REFERENCES[entity]}
    results = run_suite(retail_entity_suite(entity, frame, needed), execution_timestamp=AT)
    return {result.check_name: result for result in results}


VALID_ORDERS = [f"O{i}" for i in range(1, 41)]
VALID_KEYS = [("ST1", f"P{i}") for i in range(1, 41)]


def test_the_new_relations_are_declared() -> None:
    assert REFERENCES["shipments"] == ("orders",)
    assert REFERENCES["inventory_movements"] == ("products", "stores")


def test_valid_references_pass(spark: SparkSession) -> None:
    refs = _references(spark)
    shipments = _check("shipments", _shipments(spark, VALID_ORDERS), refs)
    movements = _check("inventory_movements", _movements(spark, VALID_KEYS), refs)

    for name in ("missing_order", "unknown_order", "unknown_order_systemic"):
        assert shipments[name].status is Status.PASS, name
    for entity in ("product", "store"):
        for name in (f"missing_{entity}", f"unknown_{entity}", f"unknown_{entity}_systemic"):
            assert movements[name].status is Status.PASS, name


def test_shipment_of_a_nonexistent_order_is_reported(spark: SparkSession) -> None:
    # 1 orphan in 40 rows = 2.5%: late data (WARNING), below the systemic 5%
    results = _check(
        "shipments", _shipments(spark, [*VALID_ORDERS[:39], "O404"]), _references(spark)
    )

    unknown = results["unknown_order"]
    assert (unknown.status, unknown.failed_rows, unknown.rows_checked) == (Status.WARNING, 1, 40)
    assert unknown.details == "examples: O404"
    assert results["unknown_order_systemic"].status is Status.PASS


@pytest.mark.parametrize(
    ("bad_key", "check"),
    [(("ST1", "P404"), "unknown_product"), (("ST9", "P1"), "unknown_store")],
    ids=["product", "store"],
)
def test_movement_with_a_nonexistent_reference_is_reported(
    spark: SparkSession, bad_key: tuple[str, str], check: str
) -> None:
    results = _check(
        "inventory_movements", _movements(spark, [*VALID_KEYS[:39], bad_key]), _references(spark)
    )

    assert (results[check].status, results[check].failed_rows) == (Status.WARNING, 1)
    assert (
        results[check].details
        == f"examples: {bad_key[1] if check == 'unknown_product' else bad_key[0]}"
    )


def test_examples_are_limited_and_deterministic(spark: SparkSession) -> None:
    orphans = [f"O9{i:02d}" for i in range(12, 0, -1)]  # 12 distinct, delivered in reverse
    results = _check("shipments", _shipments(spark, orphans), _references(spark))

    assert results["unknown_order"].failed_rows == 12
    assert results["unknown_order"].details == "examples: O901, O902, O903, O904, O905"


def test_null_keys_are_errors_not_orphans(spark: SparkSession) -> None:
    refs = _references(spark)
    shipments = _check("shipments", _shipments(spark, [*VALID_ORDERS[:39], None]), refs)
    movements = _check(
        "inventory_movements",
        _movements(spark, [*VALID_KEYS[:38], (None, "P1"), ("ST1", None)]),
        refs,
    )

    assert (shipments["missing_order"].status, shipments["missing_order"].failed_rows) == (
        Status.ERROR,
        1,
    )
    assert shipments["unknown_order"].failed_rows == 0  # NULL is missing, not a dangling key
    for entity in ("store", "product"):
        assert movements[f"missing_{entity}"].status is Status.ERROR
        assert movements[f"unknown_{entity}"].failed_rows == 0


def test_optional_reference_is_not_a_foreign_key(spark: SparkSession) -> None:
    """reference_id is NULL for opening balances and points to different documents."""
    refs = _references(spark)
    for reference in (None, "PO-123"):
        results = _check("inventory_movements", _movements(spark, VALID_KEYS, reference), refs)

        assert not any(r.column_name == "reference_id" for r in results.values())
        assert decide(results.values()) is QualityDecision.CONTINUE


def test_empty_reference_table_is_a_systemic_break(spark: SparkSession) -> None:
    refs = {**_references(spark), "orders": _frame(spark, "orders", [])}
    results = _check("shipments", _shipments(spark, VALID_ORDERS[:3]), refs)

    assert results["unknown_order_systemic"].status is Status.CRITICAL
    assert results["unknown_order"].failed_rows == 3


def test_a_batch_with_a_critical_finding_is_stopped_before_writing(spark: SparkSession) -> None:
    # 3 orphan stores in 40 rows = 7.5% > 5%: the Silver gate raises before the MERGE
    keys = [*VALID_KEYS[:37], ("ST7", "P1"), ("ST8", "P1"), ("ST9", "P1")]
    check_sets = retail_entity_suite(
        "inventory_movements",
        _movements(spark, keys),
        {name: _references(spark)[name] for name in REFERENCES["inventory_movements"]},
    )
    sink = InMemoryObservabilitySink()

    with (
        pytest.raises(CriticalQualityError),
        track_run(
            pipeline_name="retail_silver",
            run_id="r1",
            project="retail",
            layer=Layer.SILVER,
            table="silver_inventory_movements",
            sink=sink,
        ) as step,
    ):
        step.record_quality(run_suite(check_sets, execution_timestamp=AT))

    [run] = sink.runs
    critical = {r.check_name: r for r in run.quality if r.status is Status.CRITICAL}
    assert set(critical) == {"unknown_store_systemic"}
    assert critical["unknown_store_systemic"].details == "examples: ST7, ST8, ST9"
