"""IoT Gold run (observed): read the Silver, build, gate every table, then replace them.

One ``pipeline_runs`` row for the whole Gold (``<gold>.*``): the gate's results go to
``data_quality_results``; a CRITICAL result stops the run before any table is replaced, so the
previous Gold stays as it was. Tables are replaced dimensions first, each with its grain as the
table comment. No cache (serverless): the gate counts each table once (``rows_checked``).
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession

from iot_lakehouse import PROJECT
from iot_lakehouse.bronze.ingest import utc
from iot_lakehouse.gold.build import Period, build_gold
from iot_lakehouse.gold.schemas import TABLE_COMMENTS, TABLE_ORDER
from iot_lakehouse.quality.suites import iot_gold_suite
from iot_lakehouse.silver.cmms import CMMS_DATASETS, silver_cmms_table
from iot_lakehouse.silver.pipeline import SilverTarget
from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability import Layer, ObservabilitySink, track_run
from lakehouse_common.quality import run_suite

PIPELINE_NAME: Final = "iot_gold"


@dataclass(frozen=True, slots=True)
class GoldSettings:
    silver_namespace: str
    gold_namespace: str
    period: Period


def load_gold_settings(path: Path, environment: str) -> GoldSettings:
    section = load_environment(path, environment)
    return GoldSettings(
        validate_namespace(str(section["silver_namespace"])),
        validate_namespace(str(section["gold_namespace"])),
        Period(section["period_start"], section["period_end"]),
    )


def read_silver(spark: SparkSession, silver_namespace: str) -> dict[str, DataFrame]:
    readings = SilverTarget(silver_namespace)
    tables = {d: spark.table(silver_cmms_table(silver_namespace, d)) for d in CMMS_DATASETS}
    return {
        **tables,
        "readings": spark.table(readings.readings),
        "readings_quarantine": spark.table(readings.quarantine),
    }


def write_gold(tables: dict[str, DataFrame], namespace: str) -> None:
    """Replace every table (dimensions first) and set its comment to the documented grain."""
    spark = next(iter(tables.values())).sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
    for name in TABLE_ORDER:
        table = f"{namespace}.{name}"
        tables[name].writeTo(table).using("delta").createOrReplace()
        spark.sql(f"COMMENT ON TABLE {table} IS '{TABLE_COMMENTS[name]}'")


def run_gold(
    spark: SparkSession, *, settings: GoldSettings, now: datetime, sink: ObservabilitySink
) -> dict[str, int]:
    """Build, gate (CRITICAL stops before writing) and publish; rows per table."""
    at = utc(now)
    with track_run(
        pipeline_name=PIPELINE_NAME,
        run_id=f"{at:%Y%m%dT%H%M%SZ}",
        project=PROJECT,
        layer=Layer.GOLD,
        table=f"{settings.gold_namespace}.*",
        sink=sink,
    ) as step:
        silver = read_silver(spark, settings.silver_namespace)
        tables = build_gold(silver, settings.period)
        results = run_suite(iot_gold_suite(tables), execution_timestamp=at)
        rows = {r.table_name: r.rows_checked for r in results}
        step.record_counts(input_rows=silver["readings"].count(), output_rows=sum(rows.values()))
        step.record_quality(results)  # CRITICAL raises here: nothing is replaced
        write_gold(tables, settings.gold_namespace)
    return rows
