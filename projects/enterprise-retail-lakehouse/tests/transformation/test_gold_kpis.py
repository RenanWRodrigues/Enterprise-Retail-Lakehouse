"""Analytical SQL (sql/gold/*.sql): KPI values against an independent Python oracle,
aggregation consistency and absence of double counting."""

from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from retail_lakehouse.synthetic import MemorySink

SQL_DIR = Path("projects/enterprise-retail-lakehouse/sql/gold")
CENT = Decimal("0.01")
KEPT_EXCLUDED = {"cancelled", "returned"}


@pytest.fixture(scope="module")
def gold_views(spark: SparkSession, gold_tables: dict[str, DataFrame]) -> Iterator[None]:
    for name, frame in gold_tables.items():
        frame.createOrReplaceTempView(name)
    yield
    for name in gold_tables:
        spark.catalog.dropTempView(name)


def _run(spark: SparkSession, repo_root: Path, name: str) -> list[Row]:
    sql = (repo_root / SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    return spark.sql(sql).collect()


def _elapsed_seconds(
    shipment: dict[str, str | None], ordered_at: dict[str | None, datetime]
) -> float:
    delivered = datetime.fromisoformat(str(shipment["delivered_at"]))
    return (delivered - ordered_at[shipment["order_id"]]).total_seconds()


def _oracle(rows: MemorySink) -> dict[str, Decimal | int]:
    """KPIs computed from the clean source rows with plain Python (BUSINESS_RULES.md §2)."""
    status = {o["order_id"]: o["order_status"] for o in rows.rows["orders"]}
    ordered_at = {
        o["order_id"]: datetime.fromisoformat(str(o["order_timestamp"]))
        for o in rows.rows["orders"]
    }
    gross = discount = returned = net = Decimal(0)
    quantity, kept_orders = 0, set()
    for item in rows.rows["order_items"]:
        order_status = status[item["order_id"]]
        line_gross = int(str(item["quantity"])) * Decimal(str(item["unit_price"]))
        line_discount = Decimal(str(item["discount_amount"]))
        if order_status == "cancelled":
            continue
        gross += line_gross
        discount += line_discount
        if order_status == "returned":
            returned += line_gross - line_discount
            continue
        net += line_gross - line_discount
        quantity += int(str(item["quantity"]))
        kept_orders.add(item["order_id"])
    shipping = sum(
        (
            Decimal(str(s["freight_amount"]))
            for s in rows.rows["shipments"]
            if s["shipment_status"] in {"in_transit", "delivered"}
        ),
        Decimal(0),
    )
    delivery = [
        (Decimal(int(_elapsed_seconds(s, ordered_at))) / Decimal(86400)).quantize(
            CENT, ROUND_HALF_UP
        )
        for s in rows.rows["shipments"]
        if s["shipment_status"] in {"delivered", "returned"}
    ]
    return {
        "gross_revenue": gross,
        "discount_amount": discount,
        "returned_amount": returned,
        "net_revenue": net,
        "quantity_sold": quantity,
        "average_order_value": (net / len(kept_orders)).quantize(CENT, ROUND_HALF_UP),
        "shipping_cost": shipping,
        "delivery_days": (sum(delivery, Decimal(0)) / len(delivery)).quantize(CENT, ROUND_HALF_UP),
    }


@pytest.mark.parametrize("query", sorted(p.stem for p in SQL_DIR.glob("*.sql")))
def test_every_analytical_query_runs(
    spark: SparkSession, repo_root: Path, gold_views: None, query: str
) -> None:
    assert _run(spark, repo_root, query), f"{query} returned no rows"


def test_kpi_summary_matches_the_business_definitions(
    spark: SparkSession, repo_root: Path, gold_views: None, gold_source_rows: MemorySink
) -> None:
    [kpis] = _run(spark, repo_root, "kpi_summary")
    expected = _oracle(gold_source_rows)

    assert {name: kpis[name] for name in expected} == expected
    assert (
        kpis["net_revenue"]
        == kpis["gross_revenue"] - kpis["discount_amount"] - kpis["returned_amount"]
    )


def _total(rows: list[Row], column: str) -> Decimal | int:
    return sum(
        (row[column] for row in rows), Decimal(0) if isinstance(rows[0][column], Decimal) else 0
    )


@pytest.mark.parametrize(
    ("query", "column", "kpi"),
    [
        ("monthly_sales", "net_revenue", "net_revenue"),
        ("monthly_sales", "gross_revenue", "gross_revenue"),
        ("monthly_sales", "quantity_sold", "quantity_sold"),
        ("sales_by_department", "net_revenue", "net_revenue"),
        ("sales_by_customer_segment", "net_revenue", "net_revenue"),
        ("delivery_performance_by_region", "shipping_cost", "shipping_cost"),
    ],
)
def test_breakdowns_add_up_to_the_total_without_double_counting(
    spark: SparkSession, repo_root: Path, gold_views: None, query: str, column: str, kpi: str
) -> None:
    [summary] = _run(spark, repo_root, "kpi_summary")
    assert _total(_run(spark, repo_root, query), column) == summary[kpi]


def test_department_shares_add_up_to_one_hundred_percent(
    spark: SparkSession, repo_root: Path, gold_views: None
) -> None:
    shares = _total(_run(spark, repo_root, "sales_by_department"), "net_revenue_share_pct")
    assert abs(shares - Decimal(100)) <= Decimal("0.10")  # rounding of each share to 2 decimals


def test_monthly_order_counts_add_up(
    spark: SparkSession, repo_root: Path, gold_views: None, gold_source_rows: MemorySink
) -> None:
    kept = {
        o["order_id"]
        for o in gold_source_rows.rows["orders"]
        if o["order_status"] not in KEPT_EXCLUDED
    }
    assert _total(_run(spark, repo_root, "monthly_sales"), "orders") == len(kept)


def test_payment_mix_totals_match_payment_events(
    spark: SparkSession, repo_root: Path, gold_views: None, gold_source_rows: MemorySink
) -> None:
    expected: dict[str, Decimal] = defaultdict(Decimal)
    for payment in gold_source_rows.rows["payments"]:
        if payment["payment_status"] == "approved":
            expected[str(payment["payment_method"])] += Decimal(str(payment["amount"]))

    mix = {
        row["payment_method"]: row["approved_amount"]
        for row in _run(spark, repo_root, "payment_mix")
    }

    assert mix == dict(expected)


def test_inventory_balance_equals_snapshot_total(
    spark: SparkSession, repo_root: Path, gold_views: None, gold_source_rows: MemorySink
) -> None:
    snapshot = sum(int(str(r["quantity_on_hand"])) for r in gold_source_rows.rows["inventory"])
    assert _total(_run(spark, repo_root, "inventory_balance_by_store"), "units_on_hand") == snapshot
