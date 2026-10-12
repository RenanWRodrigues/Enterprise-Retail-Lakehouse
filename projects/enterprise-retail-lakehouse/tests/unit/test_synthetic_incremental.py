"""Incremental (CDC) retail batches: deterministic, chained, referentially sound."""

from collections import Counter
from dataclasses import replace
from datetime import date, datetime

import pytest

from retail_lakehouse.synthetic import GenerationConfig, MemorySink, generate_retail_dataset
from retail_lakehouse.synthetic.incremental import (
    BatchPlan,
    IncrementalPlan,
    IncrementalSummary,
    base_state,
    generate_incremental_batch,
)

BASE = GenerationConfig(
    seed=7, start_date=date(2024, 1, 1), end_date=date(2024, 3, 31),
    customers=200, products=60, stores=4, suppliers=10, orders=150,
)  # fmt: skip
NO_CHANGE = BatchPlan("b_none", date(2024, 4, 10))
CUSTOMERS_5 = BatchPlan("b_cust", date(2024, 4, 10), customer_change_rate=0.05)
NEW_PRODUCTS = BatchPlan("b_prod", date(2024, 4, 10), new_products=4, product_change_rate=0.05)
SALES = BatchPlan("b_sales", date(2024, 4, 10), new_customers=3, orders=25)


def _run(
    batches: tuple[BatchPlan, ...], batch_id: str, base: GenerationConfig = BASE
) -> tuple[MemorySink, IncrementalSummary]:
    sink = MemorySink()
    summary = generate_incremental_batch(IncrementalPlan(base, batches), batch_id, sink)
    return sink, summary


def test_the_full_mode_is_unchanged_by_incremental_generation() -> None:
    before = MemorySink()
    generate_retail_dataset(BASE, before)
    _run((CUSTOMERS_5,), "b_cust")
    after = MemorySink()
    generate_retail_dataset(BASE, after)

    assert after.rows == before.rows


def test_an_incremental_batch_without_changes_is_empty() -> None:
    sink, summary = _run((NO_CHANGE,), "b_none")

    assert {d: len(rows) for d, rows in sink.rows.items() if rows} == {}
    assert summary.changes == ()


def test_five_percent_of_the_customers_change_and_nothing_else_about_them() -> None:
    base = base_state(BASE).customers
    sink, summary = _run((CUSTOMERS_5,), "b_cust")

    changed = sink.rows["customers"]
    assert len(changed) == round(0.05 * len(base))  # 10 of 200: not everyone at once
    for row in changed:
        before = base[row["customer_id"] or ""]
        assert row["segment"] != before["segment"] or row["city"] != before["city"]
        stable = ("first_name", "last_name", "email", "birth_date", "registered_at")
        assert {k: row[k] for k in stable} == {k: before[k] for k in stable}
    assert {c.entity for c in summary.changes} == {"customer"}


def test_changes_happen_at_different_dates_inside_the_window() -> None:
    _, summary = _run((CUSTOMERS_5,), "b_cust")
    moments = {datetime.fromisoformat(c.changed_at) for c in summary.changes}

    assert len({m.date() for m in moments}) > 1
    assert all(date(2024, 4, 1) <= m.date() <= date(2024, 4, 10) for m in moments)
    assert summary.window == ("2024-04-01", "2024-04-10")


def test_new_products_continue_the_numbering_and_changed_ones_keep_their_id() -> None:
    base = base_state(BASE).products
    sink, summary = _run((NEW_PRODUCTS,), "b_prod")

    inserted = [c.entity_id for c in summary.changes if c.change_type == "insert"]
    assert inserted == ["PRD-000061", "PRD-000062", "PRD-000063", "PRD-000064"]
    updated = {c.entity_id for c in summary.changes if c.change_type == "update"}
    assert updated and updated <= set(base)  # existing ids preserved
    for row in sink.rows["products"]:
        if row["product_id"] in updated:
            before = base[row["product_id"] or ""]
            assert (row["list_price"], row["category_id"]) != (
                before["list_price"],
                before["category_id"],
            )
            assert row["sku"] == before["sku"]


def test_new_sales_reference_existing_entities_and_never_collide_with_the_full_batch() -> None:
    sink, _ = _run((SALES,), "b_sales")
    state = base_state(BASE)
    customers = set(state.customers) | {r["customer_id"] for r in sink.rows["customers"]}
    products = set(state.products)
    orders = {r["order_id"] for r in sink.rows["orders"]}

    assert len(orders) == 25 and all((o or "").startswith("ORD-b_sales-") for o in orders)
    assert {r["customer_id"] for r in sink.rows["orders"]} <= customers
    assert {r["product_id"] for r in sink.rows["order_items"]} <= products
    assert {r["order_id"] for r in sink.rows["order_items"]} <= orders
    assert {r["order_id"] for r in sink.rows["payments"]} <= orders
    assert all(date(2024, 4, 1) <= datetime.fromisoformat(r["order_timestamp"] or "").date()
               <= date(2024, 4, 10) for r in sink.rows["orders"])  # fmt: skip


def test_a_customer_registered_in_the_batch_buys_only_after_registering() -> None:
    sink, _ = _run((SALES,), "b_sales")
    registered = {r["customer_id"]: r["registered_at"] for r in sink.rows["customers"]}

    for order in sink.rows["orders"]:
        if order["customer_id"] in registered:
            assert (order["order_timestamp"] or "") >= (registered[order["customer_id"]] or "")


def test_the_same_batch_twice_is_identical() -> None:
    first, one = _run((CUSTOMERS_5,), "b_cust")
    second, two = _run((CUSTOMERS_5,), "b_cust")

    assert first.rows == second.rows
    assert one == two  # changes, event ids and fingerprint


def test_another_seed_selects_other_customers() -> None:
    _, one = _run((CUSTOMERS_5,), "b_cust")
    _, other = _run((CUSTOMERS_5,), "b_cust", replace(BASE, seed=8))

    assert {c.entity_id for c in one.changes} != {c.entity_id for c in other.changes}
    assert one.batch_fingerprint != other.batch_fingerprint


def test_a_later_batch_starts_from_the_earlier_batches() -> None:
    first = replace(CUSTOMERS_5, customer_change_rate=0.5)
    second = BatchPlan("b_next", date(2024, 4, 20), customer_change_rate=0.5)
    _, earlier = _run((first, second), "b_cust")
    _, later = _run((first, second), "b_next")

    after_first = {(c.entity_id, c.attribute): c.new_value for c in earlier.changes}
    chained = [c for c in later.changes if (c.entity_id, c.attribute) in after_first]
    assert chained and all(c.old_value == after_first[(c.entity_id, c.attribute)] for c in chained)
    assert later.window == ("2024-04-11", "2024-04-20")


def test_event_ids_are_unique_and_deterministic() -> None:
    _, summary = _run((SALES,), "b_sales")
    _, again = _run((SALES,), "b_sales")

    ids = [c.event_id for c in summary.changes]
    assert len(ids) == len(set(ids)) and ids == [c.event_id for c in again.changes]
    assert Counter(c.change_type for c in summary.changes) == {"insert": 3}


@pytest.mark.parametrize(
    "batches",
    [
        (BatchPlan("b1", date(2024, 3, 31)),),  # window not after the base
        (BatchPlan("b1", date(2024, 4, 10)), BatchPlan("b1", date(2024, 4, 20))),  # repeated id
        (BatchPlan("b2", date(2024, 4, 20)), BatchPlan("b3", date(2024, 4, 10))),  # out of order
    ],
)
def test_an_inconsistent_plan_is_refused(batches: tuple[BatchPlan, ...]) -> None:
    with pytest.raises(ValueError, match=r"batch|as_of"):
        IncrementalPlan(BASE, batches)
