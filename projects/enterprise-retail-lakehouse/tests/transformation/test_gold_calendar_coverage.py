"""No sale disappears because of the calendar range (M3).

Real orders from the clean Silver contract are moved before ``calendar_start`` and after
``calendar_end``; another order is removed, as the Silver does when it quarantines an order with
an invalid timestamp (``invalid__order_timestamp``), which leaves its lines without a date. The
Gold must keep every line, map those dates to the unknown member (-1) without losing the event
date, and ``monthly_sales`` must add up to ``kpi_summary``.
"""

import re
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from lakehouse_common.quality import Status, run_suite
from retail_lakehouse.gold import build_gold
from retail_lakehouse.gold.dimensions import UNKNOWN_DATE_LABEL
from retail_lakehouse.gold.keys import UNKNOWN_KEY
from retail_lakehouse.gold.schemas import EVENT_DATES
from retail_lakehouse.quality.suites import retail_gold_suite
from retail_lakehouse.synthetic import MemorySink

SQL_DIR = Path("projects/enterprise-retail-lakehouse/sql/gold")
DAYS_OUTSIDE = 10
NOON_IN_BRASILIA = time(15, tzinfo=UTC)  # 12:00 UTC-3: same business date
Gold = dict[str, DataFrame]


def _delivered_orders(rows: MemorySink, count: int) -> list[str]:
    delivered = (o for o in rows.rows["orders"] if o["order_status"] == "delivered")
    return sorted(str(o["order_id"]) for o in delivered)[:count]


@pytest.fixture(scope="module")
def chosen(gold_source_rows: MemorySink) -> dict[str, str]:
    inside, before, after, quarantined = _delivered_orders(gold_source_rows, 4)
    return {"inside": inside, "before": before, "after": after, "quarantined": quarantined}


@pytest.fixture(scope="module")
def calendar(gold_calendar: tuple[date, date]) -> dict[str, date]:
    start, end = gold_calendar
    return {
        "start": start,
        "end": end,
        "before": start - timedelta(days=DAYS_OUTSIDE),
        "after": end + timedelta(days=DAYS_OUTSIDE),
    }


@pytest.fixture(scope="module")
def shifted_silver(
    silver_tables: dict[str, DataFrame], chosen: dict[str, str], calendar: dict[str, date]
) -> dict[str, DataFrame]:
    order_id, timestamp = F.col("order_id"), F.col("order_timestamp")
    moved = (
        silver_tables["orders"]
        .where(order_id != chosen["quarantined"])  # quarantined by Silver: lines keep no date
        .withColumn(
            "order_timestamp",
            F.when(order_id == chosen["before"], F.lit(_noon(calendar["before"])))
            .when(order_id == chosen["after"], F.lit(_noon(calendar["after"])))
            .otherwise(timestamp),
        )
    )
    return {**silver_tables, "orders": moved}


def _noon(day: date) -> datetime:
    return datetime.combine(day, NOON_IN_BRASILIA)


def _build(silver: dict[str, DataFrame], calendar: dict[str, date]) -> Gold:
    return build_gold(silver, calendar_start=calendar["start"], calendar_end=calendar["end"])


@pytest.fixture(scope="module")
def gold(shifted_silver: dict[str, DataFrame], calendar: dict[str, date]) -> Gold:
    return _build(shifted_silver, calendar)


@pytest.fixture(scope="module")
def original_gold(silver_tables: dict[str, DataFrame], calendar: dict[str, date]) -> Gold:
    return _build(silver_tables, calendar)


def _lines(gold: Gold, order_id: str) -> list[Row]:
    sales = gold["fact_sales"].where(F.col("order_id") == order_id)
    return sales.select("date_key", "order_date").collect()


def _day_key(day: date) -> int:
    return day.year * 10_000 + day.month * 100 + day.day


def test_a_sale_inside_the_calendar_keeps_its_day(gold: Gold, chosen: dict[str, str]) -> None:
    lines = _lines(gold, chosen["inside"])

    assert lines
    assert all(r["date_key"] == _day_key(r["order_date"]) for r in lines)


@pytest.mark.parametrize("position", ["before", "after"])
def test_a_sale_outside_the_calendar_goes_to_the_unknown_date_and_keeps_its_day(
    gold: Gold, chosen: dict[str, str], calendar: dict[str, date], position: str
) -> None:
    lines = _lines(gold, chosen[position])

    assert lines
    assert {(r["date_key"], r["order_date"]) for r in lines} == {(UNKNOWN_KEY, calendar[position])}


def test_a_line_of_a_quarantined_order_has_an_unknown_date(
    gold: Gold, chosen: dict[str, str]
) -> None:
    lines = _lines(gold, chosen["quarantined"])

    assert lines
    assert {(r["date_key"], r["order_date"]) for r in lines} == {(UNKNOWN_KEY, None)}


def _totals(sales: DataFrame) -> Row:
    row = sales.agg(
        F.count(F.lit(1)).alias("lines"),
        F.sum("quantity").alias("quantity"),
        F.sum("gross_amount").alias("gross"),
        F.sum("discount_amount").alias("discount"),
        F.sum("net_amount").alias("net"),
    ).first()
    assert row is not None
    return row


def test_line_count_quantity_and_amounts_are_preserved(
    gold: Gold, original_gold: Gold, silver_tables: dict[str, DataFrame]
) -> None:
    shifted, original = _totals(gold["fact_sales"]), _totals(original_gold["fact_sales"])

    assert shifted == original
    assert shifted["lines"] == silver_tables["order_items"].count()


def test_every_date_key_of_every_fact_exists_in_dim_date(gold: Gold) -> None:
    calendar_keys = gold["dim_date"].select("date_key")
    for fact, dates in EVENT_DATES.items():
        for key in dates:
            dangling = gold[fact].join(
                calendar_keys, gold[fact][key] == calendar_keys["date_key"], "left_anti"
            )
            assert dangling.isEmpty(), f"{fact}.{key}"


def test_the_publication_gate_accepts_it_and_reports_the_out_of_calendar_sales(
    gold: Gold,
) -> None:
    facts = [cs for cs in retail_gold_suite(gold) if cs.table_name.startswith("fact_")]

    results = run_suite(facts, execution_timestamp=datetime(2026, 10, 8, tzinfo=UTC))

    assert {r.check_name for r in results if r.status is Status.CRITICAL} == set()
    out_of_calendar = {
        (r.table_name, r.check_name): r.failed_rows
        for r in results
        if r.status is Status.WARNING and r.check_name.startswith("out_of_calendar")
    }
    assert set(out_of_calendar) == {("fact_sales", "out_of_calendar__order_date")}


def test_dim_date_has_one_unknown_member_with_its_own_label(gold: Gold) -> None:
    unknown = gold["dim_date"].where(F.col("date_key") == UNKNOWN_KEY).collect()

    assert [(r["year_month"], r["year"], r["month"]) for r in unknown] == [
        (UNKNOWN_DATE_LABEL, None, None)
    ]
    labels = {r["year_month"] for r in gold["dim_date"].select("year_month").collect()}
    assert all(re.fullmatch(r"\d{4}-\d{2}", label) for label in labels - {UNKNOWN_DATE_LABEL})


@pytest.fixture(scope="module")
def views(spark: SparkSession, gold: Gold) -> Iterator[None]:
    for name, frame in gold.items():
        frame.createOrReplaceTempView(name)
    yield
    for name in gold:
        spark.catalog.dropTempView(name)


def _query(spark: SparkSession, repo_root: Path, name: str) -> list[Row]:
    sql = (repo_root / SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    return spark.sql(sql).collect()


ADDITIVE = ("gross_revenue", "discount_amount", "net_revenue", "quantity_sold")


@pytest.mark.usefixtures("views")
def test_monthly_sales_adds_up_to_kpi_summary(spark: SparkSession, repo_root: Path) -> None:
    monthly = _query(spark, repo_root, "monthly_sales")
    [kpi] = _query(spark, repo_root, "kpi_summary")
    kept_orders = spark.sql(
        "SELECT COUNT(DISTINCT order_id) AS n FROM fact_sales "
        "WHERE order_status NOT IN ('cancelled', 'returned')"
    ).first()

    for measure in ADDITIVE:
        assert sum((r[measure] for r in monthly), Decimal(0)) == kpi[measure], measure
    assert kept_orders is not None and sum(r["orders"] for r in monthly) == kept_orders["n"]


@pytest.mark.usefixtures("views")
def test_monthly_sales_reports_dates_outside_the_calendar_apart(
    spark: SparkSession, repo_root: Path, gold: Gold, chosen: dict[str, str]
) -> None:
    monthly = _query(spark, repo_root, "monthly_sales")
    outside = gold["fact_sales"].where(F.col("order_id").isin(chosen["before"], chosen["after"]))
    expected = outside.agg(F.sum("net_amount").alias("net")).first()

    unknown = {r["date_classification"]: r for r in monthly if r["period"] == UNKNOWN_DATE_LABEL}
    assert set(unknown) == {"OUT_OF_CALENDAR", "UNKNOWN_DATE"}
    assert expected is not None and unknown["OUT_OF_CALENDAR"]["net_revenue"] == expected["net"]
    assert all((r["year"], r["month"]) == (None, None) for r in unknown.values())
    in_calendar = [r for r in monthly if r["period"] != UNKNOWN_DATE_LABEL]
    assert {r["date_classification"] for r in in_calendar} == {"IN_CALENDAR"}
    assert all(r["period"] == f"{r['year']:04d}-{r['month']:02d}" for r in in_calendar)


def test_a_rebuild_is_identical(
    shifted_silver: dict[str, DataFrame], calendar: dict[str, date], gold: Gold
) -> None:
    again = _build(shifted_silver, calendar)

    for fact in EVENT_DATES:
        assert gold[fact].exceptAll(again[fact]).isEmpty(), fact
        assert again[fact].exceptAll(gold[fact]).isEmpty(), fact
