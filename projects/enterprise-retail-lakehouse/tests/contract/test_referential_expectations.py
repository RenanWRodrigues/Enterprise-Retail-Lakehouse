"""Relationships and business rules hold in clean data; only registered anomalies break them."""

from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal

import pytest

from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.synthetic import AnomalyType, GenerationConfig, MemorySink

Row = dict[str, str | None]


def _key(dataset: str, row: Row) -> str:
    return "|".join(str(row[c]) for c in RETAIL_SOURCE_CONTRACTS[dataset].business_key)


def _index(rows: list[Row], column: str) -> dict[str, Row]:
    return {str(row[column]): row for row in rows}


def _registered(sink: MemorySink, anomaly_type: AnomalyType, dataset: str) -> set[str]:
    return {
        a.record_key
        for a in sink.anomalies
        if a.anomaly_type is anomaly_type and a.dataset == dataset
    }


FOREIGN_KEYS = [
    (dataset, fk)
    for dataset, contract in sorted(RETAIL_SOURCE_CONTRACTS.items())
    for fk in contract.foreign_keys
]


@pytest.mark.parametrize(("dataset", "fk"), FOREIGN_KEYS, ids=lambda v: getattr(v, "column", v))
def test_clean_foreign_keys_resolve(clean_dataset: MemorySink, dataset: str, fk: object) -> None:
    targets = {row[fk.references_column] for row in clean_dataset.rows[fk.references_dataset]}  # type: ignore[attr-defined]
    orphans = [
        row
        for row in clean_dataset.rows[dataset]
        if row[fk.column] is not None and row[fk.column] not in targets  # type: ignore[attr-defined]
    ]
    assert not orphans


@pytest.mark.parametrize(("dataset", "fk"), FOREIGN_KEYS, ids=lambda v: getattr(v, "column", v))
def test_only_registered_orphans_break_foreign_keys(
    small_dataset: MemorySink, dataset: str, fk: object
) -> None:
    targets = {row[fk.references_column] for row in small_dataset.rows[fk.references_dataset]}  # type: ignore[attr-defined]
    orphan_keys = {
        _key(dataset, row)
        for row in small_dataset.rows[dataset]
        if row[fk.column] is not None and row[fk.column] not in targets  # type: ignore[attr-defined]
    }
    registered = {
        a.record_key
        for a in small_dataset.anomalies
        if a.anomaly_type is AnomalyType.ORPHAN_FOREIGN_KEY
        and a.dataset == dataset
        and a.column == fk.column  # type: ignore[attr-defined]
    }
    assert orphan_keys == registered


@pytest.mark.parametrize("dataset", sorted(RETAIL_SOURCE_CONTRACTS))
def test_business_keys_are_unique_in_clean_data(clean_dataset: MemorySink, dataset: str) -> None:
    keys = Counter(_key(dataset, row) for row in clean_dataset.rows[dataset])
    assert [k for k, n in keys.items() if n > 1] == []


@pytest.mark.parametrize(
    ("dataset", "anomaly_type"),
    [
        ("customers", AnomalyType.DUPLICATE_RECORD),
        ("order_items", AnomalyType.DUPLICATE_RECORD),
        ("payments", AnomalyType.DUPLICATE_PAYMENT),
    ],
)
def test_only_registered_duplicates_repeat_business_keys(
    small_dataset: MemorySink, dataset: str, anomaly_type: AnomalyType
) -> None:
    keys = Counter(_key(dataset, row) for row in small_dataset.rows[dataset])
    duplicated = {k for k, n in keys.items() if n > 1}
    assert duplicated == _registered(small_dataset, anomaly_type, dataset)


def test_every_order_has_items_and_items_use_distinct_products(clean_dataset: MemorySink) -> None:
    products_by_order: dict[str, list[str | None]] = defaultdict(list)
    for item in clean_dataset.rows["order_items"]:
        products_by_order[str(item["order_id"])].append(item["product_id"])

    assert set(products_by_order) == {o["order_id"] for o in clean_dataset.rows["orders"]}
    assert all(len(p) == len(set(p)) for p in products_by_order.values())


def test_online_orders_ship_and_store_orders_do_not(clean_dataset: MemorySink) -> None:
    orders = _index(clean_dataset.rows["orders"], "order_id")
    stores = _index(clean_dataset.rows["stores"], "store_id")
    shipped = Counter(s["order_id"] for s in clean_dataset.rows["shipments"])

    assert all(n == 1 for n in shipped.values())
    for order_id in shipped:
        assert orders[str(order_id)]["channel"] in {"online", "app"}
    for order in orders.values():
        is_online_store = stores[str(order["store_id"])]["store_type"] == "online"
        assert is_online_store == (order["channel"] in {"online", "app"})


def test_approved_payment_equals_net_items_plus_freight(clean_dataset: MemorySink) -> None:
    net: dict[str, Decimal] = defaultdict(Decimal)
    for item in clean_dataset.rows["order_items"]:
        net[str(item["order_id"])] += Decimal(str(item["quantity"])) * Decimal(
            str(item["unit_price"])
        ) - Decimal(str(item["discount_amount"]))
    freight = {
        o["order_id"]: Decimal(str(o["freight_amount"])) for o in clean_dataset.rows["orders"]
    }
    shipment_freight = {
        s["order_id"]: Decimal(str(s["freight_amount"])) for s in clean_dataset.rows["shipments"]
    }
    assert all(freight[order_id] == value for order_id, value in shipment_freight.items())

    approved = [p for p in clean_dataset.rows["payments"] if p["payment_status"] == "approved"]
    assert approved
    for payment in approved:
        order_id = str(payment["order_id"])
        assert Decimal(str(payment["amount"])) == net[order_id] + freight[order_id]


def test_customers_register_before_ordering_and_orders_stay_in_window(
    clean_dataset: MemorySink, small_config: GenerationConfig
) -> None:
    registered = {
        c["customer_id"]: datetime.fromisoformat(str(c["registered_at"]))
        for c in clean_dataset.rows["customers"]
    }
    for order in clean_dataset.rows["orders"]:
        ordered_at = datetime.fromisoformat(str(order["order_timestamp"]))
        assert small_config.start_date <= ordered_at.date() <= small_config.end_date
        assert registered[order["customer_id"]] <= ordered_at


def test_shipment_dates_are_coherent(clean_dataset: MemorySink) -> None:
    orders = _index(clean_dataset.rows["orders"], "order_id")
    for shipment in clean_dataset.rows["shipments"]:
        shipped = datetime.fromisoformat(str(shipment["shipped_at"]))
        assert shipped >= datetime.fromisoformat(
            str(orders[str(shipment["order_id"])]["order_timestamp"])
        )
        if shipment["delivered_at"] is not None:
            assert datetime.fromisoformat(shipment["delivered_at"]) >= shipped
        assert (shipment["delivered_at"] is None) == (shipment["shipment_status"] == "in_transit")


def test_inventory_snapshot_equals_sum_of_movements(
    clean_dataset: MemorySink, small_config: GenerationConfig
) -> None:
    balance: dict[tuple[str | None, str | None], int] = defaultdict(int)
    for movement in clean_dataset.rows["inventory_movements"]:
        balance[(movement["store_id"], movement["product_id"])] += int(str(movement["quantity"]))

    snapshot = {
        (row["store_id"], row["product_id"]): int(str(row["quantity_on_hand"]))
        for row in clean_dataset.rows["inventory"]
    }
    assert snapshot == dict(balance)
    assert all(qty >= 0 for qty in snapshot.values())
    assert {r["snapshot_date"] for r in clean_dataset.rows["inventory"]} == {
        small_config.end_date.isoformat()
    }


def test_sales_movements_match_items_of_non_cancelled_orders(clean_dataset: MemorySink) -> None:
    orders = _index(clean_dataset.rows["orders"], "order_id")
    sold = sum(
        int(str(item["quantity"]))
        for item in clean_dataset.rows["order_items"]
        if orders[str(item["order_id"])]["order_status"] != "cancelled"
    )
    sales = -sum(
        int(str(m["quantity"]))
        for m in clean_dataset.rows["inventory_movements"]
        if m["movement_type"] == "sale"
    )
    assert sales == sold


def test_birth_dates_make_adult_customers(
    clean_dataset: MemorySink, small_config: GenerationConfig
) -> None:
    for customer in clean_dataset.rows["customers"]:
        age_days = (small_config.start_date - date.fromisoformat(str(customer["birth_date"]))).days
        assert 18 * 365 <= age_days <= 81 * 366
