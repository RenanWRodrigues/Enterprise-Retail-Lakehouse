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

# Until the package is installable (Phase 1), import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/silver -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from financial_lakehouse.gold import load_medallion_settings
from financial_lakehouse.silver.pipeline import run_silver

settings = load_medallion_settings(
    PROJECT_ROOT / "config" / "medallion.toml", dbutils.widgets.get("environment")
)
report = run_silver(spark, bronze_table=settings.bronze_table, namespace=settings.silver_namespace)

# COMMAND ----------

display(
    spark.createDataFrame(
        [(r.check_name, str(r.status), r.failed_rows) for r in report.quality],
        "check STRING, status STRING, failed_rows LONG",
    )
)
