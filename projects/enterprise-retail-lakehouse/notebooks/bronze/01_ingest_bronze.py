# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Bronze ingestion
# MAGIC
# MAGIC Lands one batch of the 11 retail source datasets into append-only Delta tables
# MAGIC (`bronze_<dataset>`), preserving the data exactly as delivered.
# MAGIC Orchestration only: the logic lives in `retail_lakehouse.bronze` (see `docs/BRONZE.md`).

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])
dbutils.widgets.text("batch_id", "initial")

# COMMAND ----------

# Until the package is installable (Phase 1), import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/bronze -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from dataclasses import asdict
from datetime import UTC, datetime

from lakehouse_common.observability import DeltaObservabilitySink
from retail_lakehouse.bronze import ingest_all, load_bronze_settings

settings = load_bronze_settings(
    PROJECT_ROOT / "config" / "bronze.toml", dbutils.widgets.get("environment")
)
results = ingest_all(
    spark,
    landing_root=settings.landing_root,
    batch_id=dbutils.widgets.get("batch_id"),
    target=settings.target,
    ingestion_timestamp=datetime.now(UTC),
    sink=DeltaObservabilitySink(spark, settings.target.namespace),  # pipeline_runs
)

# COMMAND ----------

display(spark.createDataFrame([{**asdict(r), "status": r.status.value} for r in results]))
