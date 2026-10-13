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

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/silver -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from lakehouse_common.observability.ops import load_ops_namespace, standard_sink
from retail_lakehouse.silver.pipeline import load_silver_settings, run_silver

settings = load_silver_settings(
    PROJECT_ROOT / "config" / "silver.toml", dbutils.widgets.get("environment")
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
runs = run_silver(
    spark,
    settings=settings,
    sink=standard_sink(
        spark,
        layer_namespace=settings.target.namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),  # pipeline_runs
)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(r.table, r.status.value, r.input_rows, r.output_rows, r.rejected_rows) for r in runs],
        "table string, status string, input_rows long, output_rows long, rejected_rows long",
    )
)
