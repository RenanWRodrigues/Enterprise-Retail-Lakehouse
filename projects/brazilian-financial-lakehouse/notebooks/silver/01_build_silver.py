# Databricks notebook source
# MAGIC %md
# MAGIC # Financial — Silver (standardized economic series)
# MAGIC
# MAGIC Bronze -> `silver_indicator_observations` (typed values, revisions resolved) and its
# MAGIC quarantine. Incremental and idempotent (MERGE); rules in `docs/SILVER.md`.
# MAGIC Orchestration only. A failed critical quality check stops this notebook before writing.

# COMMAND ----------

dbutils.widgets.text("environment", "databricks")

# COMMAND ----------

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/silver -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from financial_lakehouse.gold import load_medallion_settings
from financial_lakehouse.silver.pipeline import run_silver
from lakehouse_common.observability.ops import load_ops_namespace, standard_sink

settings = load_medallion_settings(
    PROJECT_ROOT / "config" / "medallion.toml", dbutils.widgets.get("environment")
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
report = run_silver(
    spark,
    bronze_table=settings.bronze_table,
    namespace=settings.silver_namespace,
    sink=standard_sink(
        spark,
        layer_namespace=settings.silver_namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),  # pipeline_runs
)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(r.check_name, str(r.status), r.failed_rows) for r in report.quality],
        "check STRING, status STRING, failed_rows LONG",
    )
)
