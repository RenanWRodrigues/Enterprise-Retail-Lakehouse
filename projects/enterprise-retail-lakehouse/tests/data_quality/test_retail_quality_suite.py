"""Retail quality suite: each requested defect is caught, valid rows raise nothing.

Orders without customer, nonexistent products, negative values, duplicates, schema change.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakehouse_common.quality import CheckResult, QualityDecision, Status, decide, run_suite
from retail_lakehouse.quality.suites import retail_entity_suite, retail_silver_suite
from retail_lakehouse.silver.schemas import SILVER_SCHEMAS

AT = datetime(2026, 10, 6, 12, tzinfo=UTC)
TS = datetime(2026, 9, 1, 10, tzinfo=UTC)


def _frame(spark: SparkSession, entity: str, rows: list[dict[str, Any]]) -> DataFrame:
    schema = SILVER_SCHEMAS[entity]
    return spark.createDataFrame(
        [tuple(r.get(n) for n in schema.fieldNames()) for r in rows], schema
    )


def _tables(spark: SparkSession) -> dict[str, DataFrame]:
    money = Decimal
    return {
        "customers": _frame(
            spark,
            "customers",
            [
                {"customer_id": "C1", "effective_from": TS},
                {"customer_id": "C2", "effective_from": TS},
            ],
        ),
        "products": _frame(
            spark,
            "products",
            [
                {"product_id": "P1", "list_price": money("10.00"), "cost_price": money("6.00")},
                {"product_id": "P2", "list_price": money("-1.00"), "cost_price": money("1.00")},
            ],
        ),
        "orders": _frame(
            spark,
            "orders",
            [
                {"order_id": "O1", "customer_id": "C1", "freight_amount": money("5.00")},
                {
                    "order_id": "O2",
                    "customer_id": None,
                    "freight_amount": money("0.00"),
                },  # no customer
                {"order_id": "O3", "customer_id": "C9", "freight_amount": money("0.00")},  # unknown
                {
                    "order_id": "O4",
                    "customer_id": "C2",
                    "freight_amount": money("-2.00"),
                },  # negative
                {
                    "order_id": "O4",
                    "customer_id": "C2",
                    "freight_amount": money("1.00"),
                },  # duplicate
            ],
        ),
        "order_items": _frame(
            spark,
            "order_items",
            [
                {
                    "order_id": "O1",
                    "line_number": 1,
                    "product_id": "P1",
                    "quantity": 2,
                    "unit_price": money("10.00"),
                    "discount_amount": money("1.00"),
                },
                {
                    "order_id": "O1",
                    "line_number": 2,
                    "product_id": "P404",
                    "quantity": 1,
                    "unit_price": money("10.00"),
                    "discount_amount": money("0.00"),
                },  # unknown product
                {
                    "order_id": "O3",
                    "line_number": 1,
                    "product_id": "P1",
                    "quantity": 0,
                    "unit_price": money("-3.00"),
                    "discount_amount": money("0.00"),
                },  # qty 0, negative
                {
                    "order_id": "O4",
                    "line_number": 1,
                    "product_id": "P1",
                    "quantity": 1,
                    "unit_price": money("5.00"),
                    "discount_amount": money("9.00"),
                },  # discount > line
            ],
        ),
        "payments": _frame(
            spark,
            "payments",
            [
                {"payment_id": "PAY1", "order_id": "O1", "amount": money("24.00")},
                {"payment_id": "PAY2", "order_id": "O1", "amount": money("-24.00")},  # negative
            ],
        ),
    }


@pytest.fixture(scope="module")
def results(spark: SparkSession) -> dict[tuple[str, str], CheckResult]:
    outcome = run_suite(retail_silver_suite(_tables(spark)), execution_timestamp=AT)
    return {(r.table_name, r.check_name): r for r in outcome}


@pytest.mark.parametrize(
    ("table", "check", "status", "failed"),
    [
        ("silver_orders", "missing_customer", Status.ERROR, 1),
        ("silver_orders", "unknown_customer", Status.WARNING, 1),  # ADR 007: late master data
        ("silver_orders", "unknown_customer_systemic", Status.CRITICAL, 1),  # 1/5 rows > 5%
        ("silver_orders", "negative__freight_amount", Status.ERROR, 1),
        ("silver_orders", "duplicate__orders", Status.CRITICAL, 2),  # both occurrences
        ("silver_order_items", "unknown_product", Status.WARNING, 1),
        ("silver_order_items", "non_positive__quantity", Status.ERROR, 1),
        ("silver_order_items", "negative__unit_price", Status.ERROR, 1),
        ("silver_order_items", "discount_exceeds_line", Status.ERROR, 1),
        ("silver_products", "negative__list_price", Status.ERROR, 1),
        ("silver_payments", "negative__amount", Status.ERROR, 1),
    ],
)
def test_requested_defects_are_caught(
    results: dict[tuple[str, str], CheckResult], table: str, check: str, status: Status, failed: int
) -> None:
    result = results[(table, check)]
    assert (result.status, result.failed_rows) == (status, failed)


@pytest.mark.parametrize(
    ("table", "check"),
    [
        ("silver_customers", "duplicate__customers"),
        ("silver_customers", "schema__customers"),
        ("silver_order_items", "duplicate__order_items"),
        ("silver_order_items", "missing_product"),
        ("silver_payments", "unknown_order"),
        ("silver_products", "negative__cost_price"),
    ],
)
def test_valid_data_raises_nothing(
    results: dict[tuple[str, str], CheckResult], table: str, check: str
) -> None:
    assert results[(table, check)].status is Status.PASS


def test_the_run_fails_on_critical_findings(results: dict[tuple[str, str], CheckResult]) -> None:
    assert decide(results.values()) is QualityDecision.FAIL


def test_schema_change_is_critical(spark: SparkSession) -> None:
    customers = _frame(spark, "customers", [{"customer_id": "C1", "effective_from": TS}])
    renamed = customers.withColumnRenamed("segment", "customer_segment")
    [schema, *_] = run_suite(retail_silver_suite({"customers": renamed}), execution_timestamp=AT)
    assert (schema.status, schema.details) == (
        Status.CRITICAL,
        "missing: segment; unexpected: customer_segment",
    )


def test_foreign_keys_need_their_reference_tables(spark: SparkSession) -> None:
    orders = _frame(spark, "orders", [{"order_id": "O1", "customer_id": "C1"}])
    with pytest.raises(ValueError, match="customers"):
        retail_silver_suite({"orders": orders})


def test_one_entity_is_gated_without_its_references_references(spark: SparkSession) -> None:
    """Regression: gating order lines passed orders as a reference, and the suite then demanded
    the references of orders (customers) as well. One entity is checked, its targets are inputs.
    """
    tables = _tables(spark)
    [check_set] = retail_entity_suite(
        "order_items",
        tables["order_items"],
        {"products": tables["products"], "orders": tables["orders"]},
    )
    assert check_set.table_name == "silver_order_items"
    with pytest.raises(ValueError, match=r"needs reference tables \['orders'\]"):
        retail_entity_suite("order_items", tables["order_items"], {"products": tables["products"]})
