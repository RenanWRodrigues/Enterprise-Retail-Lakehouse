"""Gold layer: star schema (docs/DIMENSIONAL_MODEL.md) built from the Silver contract.

Publication (``write_gold``) is imported from ``retail_lakehouse.gold.pipeline``: it depends on
the quality suites, which depend on this package's contract (re-exporting it here would
create an import cycle).
"""

from retail_lakehouse.gold.build import (
    GoldSettings,
    GoldTarget,
    build_gold,
    load_gold_settings,
)
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
from retail_lakehouse.gold.keys import UNKNOWN_KEY, business_date_key, surrogate_key
from retail_lakehouse.gold.schemas import GOLD_SCHEMAS, GRAIN, TABLE_COMMENTS

__all__ = [
    "GOLD_SCHEMAS",
    "GRAIN",
    "TABLE_COMMENTS",
    "UNKNOWN_KEY",
    "GoldSettings",
    "GoldTarget",
    "build_dim_customer",
    "build_dim_date",
    "build_dim_product",
    "build_dim_store",
    "build_dim_supplier",
    "build_fact_inventory_movements",
    "build_fact_payments",
    "build_fact_sales",
    "build_fact_shipments",
    "build_gold",
    "business_date_key",
    "load_gold_settings",
    "surrogate_key",
]
