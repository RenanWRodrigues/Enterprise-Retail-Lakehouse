"""Build the whole Gold model from Silver (full, deterministic rebuild); targets and settings.

Publication (staging, quality gate, then replace) lives in ``gold/pipeline.py``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_namespace
from retail_lakehouse.gold.dimensions import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_dim_store,
    build_dim_supplier,
)
from retail_lakehouse.gold.facts import (
    build_fact_inventory_movements,
    build_fact_payments,
    build_fact_sales,
    build_fact_shipments,
)
from retail_lakehouse.gold.keys import within_calendar
from retail_lakehouse.gold.schemas import EVENT_DATES, GOLD_SCHEMAS

GOLD_PIPELINE_NAME: Final = "retail_gold"
STAGING_SUFFIX: Final = "_staging"
REQUIRED_SILVER: Final = (
    "customers", "categories", "suppliers", "stores", "products",
    "orders", "order_items", "payments", "shipments", "inventory_movements",
)  # fmt: skip


def conform_dates(fact: str, frame: DataFrame, *, start: date, end: date) -> DataFrame:
    """Point every date key of ``fact`` to dim_date: outside ``[start, end]`` it becomes -1.

    The event date columns (``EVENT_DATES``) are untouched, so the day is never lost.
    """
    keys = EVENT_DATES.get(fact, {})
    return frame.withColumns({key: within_calendar(F.col(key), start, end) for key in keys})


def build_gold(
    silver: Mapping[str, DataFrame], *, calendar_start: date, calendar_end: date
) -> dict[str, DataFrame]:
    """All dimensions and facts, columns in contract order (``GOLD_SCHEMAS``).

    Fact dates outside ``[calendar_start, calendar_end]`` get the unknown date key (-1) and keep
    their event date (``conform_dates``): a sale never disappears from a calendar report.
    """
    missing = sorted(set(REQUIRED_SILVER) - set(silver))
    if missing:
        raise ValueError(f"missing Silver tables: {missing}")
    if calendar_end < calendar_start:
        raise ValueError("calendar_end must not precede calendar_start")

    dim_supplier = build_dim_supplier(silver["suppliers"])
    dim_customer = build_dim_customer(silver["customers"])
    dim_store = build_dim_store(silver["stores"])
    dim_product = build_dim_product(silver["products"], silver["categories"], dim_supplier)
    orders = silver["orders"]
    tables = {
        "dim_date": build_dim_date(orders.sparkSession, calendar_start, calendar_end),
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_supplier": dim_supplier,
        "fact_sales": build_fact_sales(
            orders, silver["order_items"], dim_customer, dim_product, dim_store
        ),
        "fact_payments": build_fact_payments(silver["payments"], orders, dim_customer, dim_store),
        "fact_shipments": build_fact_shipments(
            silver["shipments"], orders, dim_customer, dim_store
        ),
        "fact_inventory_movements": build_fact_inventory_movements(
            silver["inventory_movements"], dim_product, dim_store
        ),
    }
    return {
        name: conform_dates(name, tables[name], start=calendar_start, end=calendar_end).select(
            *GOLD_SCHEMAS[name].fieldNames()
        )
        for name in GOLD_SCHEMAS
    }


@dataclass(frozen=True, slots=True)
class GoldTarget:
    """Where Gold is published (``namespace``) and staged before the gate (``<namespace>_staging``).

    Staging is a separate schema so consumers of the Gold schema never see candidate tables.
    """

    namespace: str

    def __post_init__(self) -> None:
        validate_namespace(self.namespace)

    @property
    def staging_namespace(self) -> str:
        return validate_namespace(f"{self.namespace}{STAGING_SUFFIX}")

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def staging_table(self, name: str) -> str:
        return f"{self.staging_namespace}.{name}"


@dataclass(frozen=True, slots=True)
class GoldSettings:
    silver_namespace: str
    target: GoldTarget
    calendar_start: date
    calendar_end: date


def load_gold_settings(path: Path, environment: str) -> GoldSettings:
    """Silver/Gold namespaces and calendar bounds of ``environment``."""
    section = load_environment(path, environment)
    return GoldSettings(
        silver_namespace=validate_namespace(str(section["silver_namespace"])),
        target=GoldTarget(str(section["gold_namespace"])),
        calendar_start=section["calendar_start"],
        calendar_end=section["calendar_end"],
    )
