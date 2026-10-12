"""Build every Gold table from Silver and write them (full, deterministic rebuild).

Full rebuild on purpose (ADR 040): trailing 12-month metrics mean one revised Silver value changes
up to 12 later Gold rows; Silver is small (tens of thousands of rows), so recomputing is cheaper
and safer than tracking every affected window. Same Silver -> same Gold (idempotent).
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from financial_lakehouse.gold.schemas import GOLD_SCHEMAS, TABLE_COMMENTS
from financial_lakehouse.gold.tables import (
    build_economic_activity,
    build_exchange_rates,
    build_inflation,
    build_interest_rates,
    build_labor_market,
    enrich_monthly,
    monthly_panel,
)
from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_table_name

logger = logging.getLogger(__name__)


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    """All Gold tables, columns in contract order and types (``GOLD_SCHEMAS``)."""
    monthly = enrich_monthly(monthly_panel(silver))
    tables = {
        "gold_interest_rates": build_interest_rates(silver),
        "gold_exchange_rates": build_exchange_rates(silver),
        "gold_inflation": build_inflation(monthly),
        "gold_economic_activity": build_economic_activity(monthly),
        "gold_labor_market": build_labor_market(monthly),
        "gold_brazil_macro_monthly": monthly,
    }
    order = {name: schema.fieldNames()[0] for name, schema in GOLD_SCHEMAS.items()}
    return {
        name: tables[name]
        .select(*(F.col(f.name).cast(f.dataType) for f in schema.fields))
        .orderBy(order[name])
        for name, schema in GOLD_SCHEMAS.items()
    }


def write_gold(tables: Mapping[str, DataFrame], namespace: str) -> None:
    """Replace every Gold table (Delta) and set its comment to the documented grain."""
    spark = next(iter(tables.values())).sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {validate_table_name(namespace)}")
    for name, frame in tables.items():
        table = validate_table_name(f"{namespace}.{name}")
        frame.writeTo(table).using("delta").createOrReplace()
        spark.sql(f"COMMENT ON TABLE {table} IS '{TABLE_COMMENTS[name]}'")
        logger.info("gold %s written", table)


@dataclass(frozen=True, slots=True)
class MedallionSettings:
    bronze_table: str
    silver_namespace: str
    gold_namespace: str


def load_medallion_settings(path: Path, environment: str) -> MedallionSettings:
    """Bronze table and Silver/Gold namespaces of ``environment`` (validated SQL identifiers)."""
    section = load_environment(path, environment)
    return MedallionSettings(
        bronze_table=validate_table_name(str(section["bronze_table"])),
        silver_namespace=validate_table_name(str(section["silver_namespace"])),
        gold_namespace=validate_table_name(str(section["gold_namespace"])),
    )
