# Databricks notebook source
# MAGIC %md
# MAGIC # IoT — Gold (reliability and sensor analytics)
# MAGIC
# MAGIC Rebuilds the CMMS Silver (small master data) and then every Gold table: `dim_equipment`,
# MAGIC `dim_date`, `fact_equipment_failures`, `fact_maintenance`, `agg_equipment_hourly`,
# MAGIC `agg_equipment_daily`, `agg_equipment_reliability`. Formulas in `docs/GOLD.md`.
# MAGIC A CRITICAL gate finding stops before any Gold table is replaced.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])

# COMMAND ----------

# Until the package is installable, import it from the repository (Git folder).
# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/gold -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from datetime import UTC, datetime

from iot_lakehouse.gold.pipeline import load_gold_settings, run_gold
from iot_lakehouse.silver.cmms import run_cmms_silver
from iot_lakehouse.silver.pipeline import load_silver_settings
from lakehouse_common.observability.ops import load_ops_namespace, standard_sink

environment = dbutils.widgets.get("environment")
silver = load_silver_settings(PROJECT_ROOT / "config" / "silver.toml", environment)
gold = load_gold_settings(PROJECT_ROOT / "config" / "gold.toml", environment)
now = datetime.now(UTC)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
run_cmms_silver(
    spark,
    bronze=silver.bronze,
    silver_namespace=gold.silver_namespace,
    at=now,
    sink=standard_sink(
        spark,
        layer_namespace=gold.silver_namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),
)
rows = run_gold(
    spark,
    settings=gold,
    now=now,
    sink=standard_sink(
        spark,
        layer_namespace=gold.gold_namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),
)

# COMMAND ----------

display(spark.table(f"{gold.gold_namespace}.agg_equipment_reliability").orderBy("availability"))
