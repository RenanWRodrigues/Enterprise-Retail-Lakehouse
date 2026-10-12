"""Retail Silver standardization: typing, quarantine with reasons, duplicates, conflicts, PII.

Each scenario is a Bronze row (text, as delivered) whose identifier names the case. Every entity
batch is standardized once per module (each Spark action costs seconds on Windows); the
assertions stay one rule per test. Defects mirror the generator's anomalies
(``synthetic/anomalies.py``).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.testing import assertSchemaEqual

from retail_lakehouse.bronze import bronze_schema
from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS
from retail_lakehouse.silver import silver_table_schema
from retail_lakehouse.silver.standardize import standardize

BATCH_AT = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
RUN_AT = datetime(2026, 1, 15, 13, 0, tzinfo=UTC)


@dataclass(frozen=True)
class Outcome:
    valid: dict[str, Row]
    quarantine: dict[str, list[str]]
    duplicate_rows: int
    schema: Any


def bronze_frame(
    spark: SparkSession, entity: str, rows: list[dict[str, str | None]], batch: str = "b1"
) -> DataFrame:
    """Bronze rows: every contract column as text (defaults filled) plus ingestion metadata."""
    contract = RETAIL_SOURCE_CONTRACTS[entity]
    names = contract.column_names
    records = [
        (
            *(row.get(name) for name in names),
            BATCH_AT,
            f"landing/{entity}_{batch}",
            batch,
            BATCH_AT.date(),
        )
        for row in rows
    ]
    return spark.createDataFrame(records, bronze_schema(contract))


def _outcome(spark: SparkSession, entity: str, rows: list[dict[str, str | None]]) -> Outcome:
    key = RETAIL_SOURCE_CONTRACTS[entity].business_key[0]
    result = standardize(bronze_frame(spark, entity, rows), entity, execution_timestamp=RUN_AT)
    return Outcome(
        valid={r[key]: r for r in result.valid.collect()},
        quarantine={r[key]: list(r["_dq_failed_checks"]) for r in result.quarantine.collect()},
        duplicate_rows=result.duplicates.count(),
        schema=result.valid.schema,
    )


Make = Callable[..., dict[str, str | None]]


def _order(**changes: str | None) -> dict[str, str | None]:
    return {
        "order_id": "ORD-1",
        "customer_id": "CUS-1",
        "store_id": "STR-1",
        "channel": "online",
        "order_status": "paid",
        "order_timestamp": "2025-03-05T10:15:00-03:00",
        "currency": "BRL",
        "freight_amount": "12.50",
    } | changes


ORDERS = [
    _order(),
    _order(order_id="ORD-DUP"),
    _order(order_id="ORD-DUP"),  # exact resend: collapses, counted as a duplicate
    _order(order_id="ORD-CONFLICT", freight_amount="1.00"),
    _order(order_id="ORD-CONFLICT", freight_amount="2.00"),  # same key, other content
    _order(order_id="ORD-IMPOSSIBLE-DAY", order_timestamp="2025-02-30T10:15:00-03:00"),
    _order(order_id="ORD-WRONG-FORMAT", order_timestamp="05/03/2025 10:15:00"),
    _order(order_id="ORD-STATUS", order_status="lost"),
    _order(order_id="ORD-NO-CUSTOMER", customer_id=None),
    _order(order_id="ORD-BLANK-CUSTOMER", customer_id="  "),
    _order(order_id="ORD-NEGATIVE", freight_amount="-3.00"),
    _order(order_id="ORD-NOT-MONEY", freight_amount="12,50"),
    _order(order_id="ORD-ORPHAN", customer_id="CUS-9999"),  # unknown customer: kept (ADR 007)
]


@pytest.fixture(scope="module")
def orders(spark: SparkSession) -> Outcome:
    return _outcome(spark, "orders", ORDERS)


def test_silver_rows_follow_the_table_contract(orders: Outcome) -> None:
    assertSchemaEqual(orders.schema, silver_table_schema("orders"))


def test_values_are_typed_and_timestamps_become_utc_instants(orders: Outcome) -> None:
    row = orders.valid["ORD-1"]
    assert row["freight_amount"] == Decimal("12.50")
    assert row["order_timestamp"] == datetime(2025, 3, 5, 13, 15, tzinfo=UTC).astimezone(
        tz=None
    ).replace(tzinfo=None)  # collect() renders UTC instants in the local Python timezone
    assert "currency" not in row.asDict()  # not part of the Silver contract


def test_lineage_is_kept(orders: Outcome) -> None:
    row = orders.valid["ORD-1"]
    assert (row["_batch_id"], row["_source_file"]) == ("b1", "landing/orders_b1")


def test_exact_duplicates_collapse(orders: Outcome) -> None:
    assert "ORD-DUP" in orders.valid
    assert orders.duplicate_rows == 1  # the resend of ORD-DUP


@pytest.mark.parametrize(
    ("order_id", "reasons"),
    [
        ("ORD-CONFLICT", ["conflicting_values"]),
        ("ORD-IMPOSSIBLE-DAY", ["invalid__order_timestamp"]),
        ("ORD-WRONG-FORMAT", ["invalid__order_timestamp"]),
        ("ORD-STATUS", ["out_of_domain__order_status"]),
        ("ORD-NO-CUSTOMER", ["missing__customer_id"]),
        ("ORD-BLANK-CUSTOMER", ["missing__customer_id"]),
        ("ORD-NEGATIVE", ["negative__freight_amount"]),
        ("ORD-NOT-MONEY", ["invalid__freight_amount"]),
    ],
)
def test_invalid_rows_are_quarantined_with_the_reason(
    orders: Outcome, order_id: str, reasons: list[str]
) -> None:
    assert order_id not in orders.valid
    assert orders.quarantine[order_id] == reasons


def test_both_conflicting_versions_go_to_quarantine(spark: SparkSession) -> None:
    rows = [
        _order(order_id="X", freight_amount="1.00"),
        _order(order_id="X", freight_amount="2.00"),
    ]
    result = standardize(bronze_frame(spark, "orders", rows), "orders", execution_timestamp=RUN_AT)
    assert result.quarantine.count() == len(rows)


def test_unknown_reference_is_not_a_row_defect(orders: Outcome) -> None:
    assert "ORD-ORPHAN" in orders.valid  # checked as WARNING by the suite, mapped to -1 in Gold


# --- customers: PII, region, birth date, tolerated null -------------------------------------


def _customer(**changes: str | None) -> dict[str, str | None]:
    return {
        "customer_id": "CUS-1",
        "first_name": "Ana",
        "last_name": "Souza",
        "email": "ana.souza1@example.com",
        "birth_date": "1990-04-20",
        "city": "Recife",
        "state": "PE",
        "segment": "premium",
        "registered_at": "2024-02-01T09:00:00-03:00",
    } | changes


CUSTOMERS = [
    _customer(),
    _customer(customer_id="CUS-BAD-EMAIL", email="ana.souza1#example.com"),
    _customer(customer_id="CUS-NO-BIRTH", birth_date=None),
    _customer(customer_id="CUS-FUTURE-BIRTH", birth_date="2093-04-20"),
    _customer(customer_id="CUS-NO-STATE", state="XX"),
    _customer(customer_id="CUS-SEGMENT", segment="gold"),
]


@pytest.fixture(scope="module")
def customers(spark: SparkSession) -> Outcome:
    return _outcome(spark, "customers", CUSTOMERS)


def test_pii_does_not_enter_silver(customers: Outcome) -> None:
    columns = set(customers.valid["CUS-1"].asDict())
    assert not columns & {"first_name", "last_name", "email"}


def test_region_comes_from_the_state(customers: Outcome) -> None:
    assert customers.valid["CUS-1"]["region"] == "Nordeste"


def test_first_version_starts_at_registration(customers: Outcome) -> None:
    row = customers.valid["CUS-1"]
    assert row["effective_from"] == row["registered_at"]


def test_dropped_pii_defects_do_not_quarantine_the_customer(customers: Outcome) -> None:
    assert "CUS-BAD-EMAIL" in customers.valid  # e-mail is not carried (minimization)


def test_missing_birth_date_is_tolerated(customers: Outcome) -> None:
    assert customers.valid["CUS-NO-BIRTH"]["birth_date"] is None  # Gold: age band "unknown"


@pytest.mark.parametrize(
    ("customer_id", "reasons"),
    [
        ("CUS-FUTURE-BIRTH", ["future__birth_date"]),
        ("CUS-NO-STATE", ["out_of_domain__state"]),
        ("CUS-SEGMENT", ["out_of_domain__segment"]),
    ],
)
def test_customer_defects_are_quarantined(
    customers: Outcome, customer_id: str, reasons: list[str]
) -> None:
    assert customers.quarantine[customer_id] == reasons


# --- products, order lines, shipments, stock movements ----------------------------------------


def _product(**changes: str | None) -> dict[str, str | None]:
    return {
        "product_id": "PRD-1",
        "sku": "SKU-1",
        "product_name": "Fone",
        "category_id": "CAT-1",
        "supplier_id": "SUP-1",
        "brand": "Marca",
        "list_price": "100.00",
        "cost_price": "60.00",
        "is_active": "true",
        "created_at": "2023-01-01T00:00:00-03:00",
        "updated_at": "2023-01-01T00:00:00-03:00",
    } | changes


def test_product_rules(spark: SparkSession) -> None:
    outcome = _outcome(
        spark,
        "products",
        [
            _product(),
            _product(product_id="PRD-NO-SUPPLIER", supplier_id=None),
            _product(product_id="PRD-NEGATIVE", list_price="-100.00"),
            _product(product_id="PRD-FLAG", is_active="yes"),
        ],
    )
    assert outcome.valid["PRD-1"]["is_active"] is True
    assert outcome.valid["PRD-NO-SUPPLIER"]["supplier_id"] is None  # Gold: unknown supplier
    assert outcome.quarantine == {
        "PRD-NEGATIVE": ["negative__list_price"],
        "PRD-FLAG": ["invalid__is_active"],
    }


def _line(**changes: str | None) -> dict[str, str | None]:
    return {
        "order_id": "ORD-1",
        "line_number": "1",
        "product_id": "PRD-1",
        "quantity": "2",
        "unit_price": "10.00",
        "discount_amount": "1.00",
    } | changes


def test_order_line_rules(spark: SparkSession) -> None:
    outcome = _outcome(
        spark,
        "order_items",
        [
            _line(),
            _line(order_id="ORD-ZERO", quantity="0"),
            _line(order_id="ORD-DISCOUNT", discount_amount="25.00"),
        ],
    )
    assert outcome.valid["ORD-1"]["quantity"] == 2
    assert outcome.quarantine == {
        "ORD-ZERO": ["non_positive__quantity", "discount_exceeds_line"],
        "ORD-DISCOUNT": ["discount_exceeds_line"],
    }


def _shipment(**changes: str | None) -> dict[str, str | None]:
    return {
        "shipment_id": "SHP-1",
        "order_id": "ORD-1",
        "carrier_id": "CAR-01",
        "shipment_status": "delivered",
        "shipped_at": "2025-03-06T08:00:00-03:00",
        "delivered_at": "2025-03-09T15:00:00-03:00",
        "freight_amount": "12.50",
    } | changes


def test_shipment_rules(spark: SparkSession) -> None:
    outcome = _outcome(
        spark,
        "shipments",
        [
            _shipment(),
            _shipment(shipment_id="SHP-TRANSIT", shipment_status="in_transit", delivered_at=None),
            _shipment(shipment_id="SHP-BACKWARDS", delivered_at="2025-03-04T08:00:00-03:00"),
        ],
    )
    assert outcome.valid["SHP-TRANSIT"]["delivered_at"] is None
    assert outcome.quarantine == {"SHP-BACKWARDS": ["delivered_before_shipped"]}


def test_stock_movement_rules(spark: SparkSession) -> None:
    movement: dict[str, str | None] = {
        "movement_id": "MOV-1",
        "store_id": "STR-1",
        "product_id": "PRD-1",
        "movement_type": "sale",
        "quantity": "-2",
        "movement_timestamp": "2025-03-05T10:15:00-03:00",
        "reference_id": "ORD-1",
    }
    outcome = _outcome(
        spark,
        "inventory_movements",
        [movement, movement | {"movement_id": "MOV-0", "quantity": "0"}],
    )
    assert outcome.valid["MOV-1"]["quantity"] == -2  # movements are signed
    assert outcome.quarantine == {"MOV-0": ["zero__quantity"]}


def test_date_columns_are_calendar_dates(spark: SparkSession) -> None:
    store: dict[str, str | None] = {
        "store_id": "STR-1",
        "store_name": "Loja Centro",
        "store_type": "physical",
        "city": "Recife",
        "state": "PE",
        "region": "Nordeste",
        "opened_at": "2019-08-01",
    }
    outcome = _outcome(
        spark, "stores", [store, store | {"store_id": "STR-2", "opened_at": "2019-13-01"}]
    )
    assert outcome.valid["STR-1"]["opened_at"] == date(2019, 8, 1)
    assert outcome.quarantine == {"STR-2": ["invalid__opened_at"]}
