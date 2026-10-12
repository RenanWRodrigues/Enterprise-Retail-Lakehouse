"""Reconciliations R1-R3 from BUSINESS_RULES.md §3."""

from decimal import Decimal

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ZERO = Decimal("0.00")


@pytest.mark.parametrize(
    ("fact", "source", "fact_expr", "source_expr"),
    [
        ("fact_sales", "order_items", "quantity", "quantity"),
        ("fact_sales", "order_items", "gross_amount", "quantity * unit_price"),
        ("fact_sales", "order_items", "discount_amount", "discount_amount"),
        ("fact_sales", "order_items", "net_amount", "quantity * unit_price - discount_amount"),
        ("fact_payments", "payments", "amount", "amount"),
        ("fact_shipments", "shipments", "freight_amount", "freight_amount"),
        ("fact_inventory_movements", "inventory_movements", "quantity", "quantity"),
    ],
)
def test_r1_gold_totals_equal_silver_totals(
    gold_tables: dict[str, DataFrame],
    silver_tables: dict[str, DataFrame],
    fact: str,
    source: str,
    fact_expr: str,
    source_expr: str,
) -> None:
    gold_total = gold_tables[fact].agg(F.sum(F.expr(fact_expr))).first()[0]  # type: ignore[index]
    silver_total = silver_tables[source].agg(F.sum(F.expr(source_expr))).first()[0]  # type: ignore[index]
    assert gold_total == silver_total


def _per_order(gold: dict[str, DataFrame], silver: dict[str, DataFrame]) -> DataFrame:
    """Every grain aggregated to the ORDER grain first, then joined 1:1 (no double counting)."""
    sales = gold["fact_sales"].groupBy("order_id").agg(F.sum("net_amount").alias("net"))
    cash = (
        gold["fact_payments"]
        .groupBy("order_id")
        .agg(F.sum("signed_amount").alias("cash"), F.count(F.lit(1)).alias("payments"))
    )
    shipped_freight = (
        gold["fact_shipments"]
        .groupBy("order_id")
        .agg(F.sum("freight_amount").alias("shipped_freight"))
    )
    orders = silver["orders"].select("order_id", "order_status", "freight_amount")
    return (
        orders.join(sales, "order_id", "left")  # 1:1
        .join(cash, "order_id", "left")  # 1:1
        .join(shipped_freight, "order_id", "left")  # 1:1
        .fillna({"cash": 0, "payments": 0, "shipped_freight": 0})
    )


def test_r2_cash_reconciles_with_revenue_for_every_order(
    gold_tables: dict[str, DataFrame], silver_tables: dict[str, DataFrame]
) -> None:
    per_order = _per_order(gold_tables, silver_tables)
    expected_cash = (
        F.when(
            F.col("order_status").isin("delivered", "shipped"),
            F.col("net") + F.col("shipped_freight"),
        )
        .when(F.col("order_status") == "paid", F.col("net") + F.col("freight_amount"))
        .otherwise(F.lit(ZERO))
    )
    mismatches = per_order.where(F.col("cash") != expected_cash)
    unpaid_created = per_order.where((F.col("order_status") == "created") & (F.col("payments") > 0))

    assert per_order.select("order_status").distinct().count() >= 4  # several statuses exercised
    assert mismatches.isEmpty(), mismatches.limit(5).collect()
    assert unpaid_created.isEmpty()


def test_r2_consolidated_cash_equation(
    gold_tables: dict[str, DataFrame], silver_tables: dict[str, DataFrame]
) -> None:
    per_order = _per_order(gold_tables, silver_tables)
    [row] = per_order.agg(
        F.sum("cash").alias("cash"),
        F.sum(F.when(~F.col("order_status").isin("cancelled", "returned"), F.col("net"))).alias(
            "net_revenue"
        ),
        F.sum(F.when(F.col("order_status") == "created", F.col("net")).otherwise(0)).alias(
            "created_net"
        ),
        F.sum(F.when(F.col("order_status") == "paid", F.col("freight_amount")).otherwise(0)).alias(
            "pending"
        ),
    ).collect()
    shipping_cost = (
        gold_tables["fact_shipments"]
        .where(F.col("shipment_status").isin("in_transit", "delivered"))
        .agg(F.sum("freight_amount"))
        .first()[0]  # type: ignore[index]
    )

    assert row["cash"] == row["net_revenue"] - row["created_net"] + shipping_cost + row["pending"]


def test_r3_inventory_movements_add_up_to_the_snapshot(
    gold_tables: dict[str, DataFrame], silver_tables: dict[str, DataFrame]
) -> None:
    dim_store = gold_tables["dim_store"].select("store_sk", "store_id")
    dim_product = gold_tables["dim_product"].select("product_sk", "product_id")
    balances = (
        gold_tables["fact_inventory_movements"]
        .groupBy("store_sk", "product_sk")
        .agg(F.sum("quantity").alias("balance"))
        .join(dim_store, "store_sk")  # N:1
        .join(dim_product, "product_sk")  # N:1
        .select("store_id", "product_id", "balance")
    )
    snapshot = silver_tables["inventory"].select("store_id", "product_id", "quantity_on_hand")

    differences = balances.join(snapshot, ["store_id", "product_id"], "full_outer").where(
        ~F.col("balance").eqNullSafe(F.col("quantity_on_hand"))
    )
    assert differences.isEmpty()


def test_r3_sale_movements_match_units_of_non_cancelled_orders(
    gold_tables: dict[str, DataFrame],
) -> None:
    sold_units = (
        gold_tables["fact_sales"]
        .where(F.col("order_status") != "cancelled")
        .agg(F.sum("quantity"))
        .first()[0]  # type: ignore[index]
    )
    sale_movements = (
        gold_tables["fact_inventory_movements"]
        .where(F.col("movement_type") == "sale")
        .agg(F.sum("quantity"))
        .first()[0]  # type: ignore[index]
    )
    assert -sale_movements == sold_units
