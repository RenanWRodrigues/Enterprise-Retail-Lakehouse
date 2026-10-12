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
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/gold -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from datetime import UTC, datetime

from iot_lakehouse.gold.pipeline import load_gold_settings, run_gold
from iot_lakehouse.silver.cmms import run_cmms_silver
from iot_lakehouse.silver.pipeline import load_silver_settings
from lakehouse_common.observability import DeltaObservabilitySink

environment = dbutils.widgets.get("environment")
silver = load_silver_settings(PROJECT_ROOT / "config" / "silver.toml", environment)
gold = load_gold_settings(PROJECT_ROOT / "config" / "gold.toml", environment)
now = datetime.now(UTC)
run_cmms_silver(
    spark,
    bronze=silver.bronze,
    silver_namespace=gold.silver_namespace,
    at=now,
    sink=DeltaObservabilitySink(spark, gold.silver_namespace),
)
rows = run_gold(
    spark, settings=gold, now=now, sink=DeltaObservabilitySink(spark, gold.gold_namespace)
)

# COMMAND ----------

display(spark.table(f"{gold.gold_namespace}.agg_equipment_reliability").orderBy("availability"))
