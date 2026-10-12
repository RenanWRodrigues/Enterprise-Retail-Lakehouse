# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Gold (star schema)
# MAGIC
# MAGIC Rebuilds the dimensional model from the Silver tables (`docs/DIMENSIONAL_MODEL.md`).
# MAGIC Every table is staged in `<gold>_staging` and checked by the quality gate; nothing is
# MAGIC published if any CRITICAL check fails (the previous Gold stays as it was).
# MAGIC Orchestration only: the logic lives in `retail_lakehouse.gold`.
# MAGIC **Prerequisite:** Silver tables `silver_<entity>` (`notebooks/silver/01_build_silver.py`).

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])

# COMMAND ----------

# Until the package is installable (Phase 1), import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/gold -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from datetime import UTC, datetime

from lakehouse_common.observability import DeltaObservabilitySink
from retail_lakehouse.gold import build_gold, load_gold_settings
from retail_lakehouse.gold.build import REQUIRED_SILVER
from retail_lakehouse.gold.pipeline import write_gold
from retail_lakehouse.silver.schemas import SILVER_TABLES

settings = load_gold_settings(
    PROJECT_ROOT / "config" / "gold.toml", dbutils.widgets.get("environment")
)
silver = {
    entity: spark.table(f"{settings.silver_namespace}.{SILVER_TABLES[entity]}")
    for entity in REQUIRED_SILVER
}
gold = build_gold(
    silver, calendar_start=settings.calendar_start, calendar_end=settings.calendar_end
)
write_gold(
    gold,
    settings.target,
    run_id=f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}",
    sink=DeltaObservabilitySink(spark, settings.target.namespace),  # pipeline_runs
)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(name, spark.table(settings.target.table(name)).count()) for name in gold],
        "table string, rows long",
    )
)
