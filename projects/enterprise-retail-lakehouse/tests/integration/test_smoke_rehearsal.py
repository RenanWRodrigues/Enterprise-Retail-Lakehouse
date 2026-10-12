"""Local rehearsal of the Databricks smoke run: the smoke landing goes through every layer.

Same code and same configuration file the Databricks notebooks use
(``config/data_generation_smoke.toml``), with local Delta tables instead of Unity Catalog.
Proves the sample passes the Silver systemic-break rule and the Gold publication gate before
anyone spends workspace compute on it. Needs Delta: test container (Linux).
"""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakehouse_common.observability import InMemoryObservabilitySink, RunStatus
from retail_lakehouse.bronze import BronzeTarget, ingest_all
from retail_lakehouse.gold import GoldTarget, build_gold
from retail_lakehouse.gold.pipeline import write_gold
from retail_lakehouse.silver import SILVER_SCHEMAS
from retail_lakehouse.silver.pipeline import SilverSettings, SilverTarget, run_silver
from retail_lakehouse.silver.schemas import SILVER_TABLES
from retail_lakehouse.synthetic import FileSink, generate_retail_dataset, load_config

SMOKE_CONFIG = Path("projects/enterprise-retail-lakehouse/config/data_generation_smoke.toml")
AT = datetime(2026, 10, 8, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def landing(repo_root: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    config = load_config(repo_root / SMOKE_CONFIG)
    root = tmp_path_factory.mktemp("smoke_landing")
    with FileSink(root, config=config) as sink:
        generate_retail_dataset(config, sink)
    return root


def test_the_smoke_landing_runs_bronze_silver_and_gold(
    delta_spark: SparkSession, landing: Path
) -> None:
    settings = SilverSettings(bronze=BronzeTarget("smoke_bronze"), target=SilverTarget("smoke"))
    runs = InMemoryObservabilitySink()

    bronze = ingest_all(
        delta_spark,
        landing_root=landing.as_posix(),
        batch_id="initial",
        target=settings.bronze,
        ingestion_timestamp=AT,
        sink=runs,
    )
    silver = run_silver(delta_spark, settings=settings, sink=runs, now=AT)
    tables = {
        e: delta_spark.table(f"{settings.target.namespace}.{SILVER_TABLES[e]}")
        for e in SILVER_SCHEMAS
    }
    gold = build_gold(tables, calendar_start=date(2024, 1, 1), calendar_end=date(2026, 1, 31))
    write_gold(gold, GoldTarget("smoke_gold"), run_id="G1", sink=runs, now=AT)

    assert len(bronze) == 11 and len(silver) == 11
    assert all(r.status is not RunStatus.FAILED for r in runs.runs)
    assert sum(r.rows_written for r in bronze) > 0
    assert delta_spark.table("smoke_gold.fact_sales").count() > 0
