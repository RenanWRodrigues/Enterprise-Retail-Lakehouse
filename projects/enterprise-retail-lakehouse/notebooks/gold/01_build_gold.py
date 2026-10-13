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

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/gold -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from datetime import UTC, datetime

from lakehouse_common.observability import new_run_id
from lakehouse_common.observability.ops import load_ops_namespace, standard_sink
from retail_lakehouse.gold import build_gold, load_gold_settings
from retail_lakehouse.gold.build import REQUIRED_SILVER
from retail_lakehouse.gold.inventory import inventory_reconciliation
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
r3 = inventory_reconciliation(  # R3: stock snapshot vs movement ledger, gated with the tables
    spark.table(f"{settings.silver_namespace}.{SILVER_TABLES['inventory']}"),
    silver["inventory_movements"],
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
write_gold(
    gold,
    settings.target,
    run_id=new_run_id(datetime.now(UTC)),
    sink=standard_sink(
        spark,
        layer_namespace=settings.target.namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),  # pipeline_runs
    inventory_reconciliation=r3,
)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(name, spark.table(settings.target.table(name)).count()) for name in gold],
        "table string, rows long",
    )
)
