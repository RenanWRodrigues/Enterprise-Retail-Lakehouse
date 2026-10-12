# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Silver
# MAGIC
# MAGIC Pending Bronze batches → typed, standardised `silver_<entity>` tables (SCD1; customers keep
# MAGIC versions for SCD Type 2), invalid rows in `silver_<entity>_quarantine` with reasons.
# MAGIC Orchestration only: the logic lives in `retail_lakehouse.silver` (see `docs/SILVER.md`).
# MAGIC Observed: `pipeline_runs` and `data_quality_results`. A CRITICAL finding fails the notebook
# MAGIC before that entity is written.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])

# COMMAND ----------

# Until the package is installable (Phase 1), import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/silver -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from lakehouse_common.observability import DeltaObservabilitySink
from retail_lakehouse.silver.pipeline import load_silver_settings, run_silver

settings = load_silver_settings(
    PROJECT_ROOT / "config" / "silver.toml", dbutils.widgets.get("environment")
)
runs = run_silver(
    spark,
    settings=settings,
    sink=DeltaObservabilitySink(spark, settings.target.namespace),  # pipeline_runs
)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(r.table, r.status.value, r.input_rows, r.output_rows, r.rejected_rows) for r in runs],
        "table string, status string, input_rows long, output_rows long, rejected_rows long",
    )
)
