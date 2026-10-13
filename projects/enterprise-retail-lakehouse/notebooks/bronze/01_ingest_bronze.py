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

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/bronze -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from dataclasses import asdict
from datetime import UTC, datetime

from lakehouse_common.observability.ops import load_ops_namespace, standard_sink
from retail_lakehouse.bronze import ingest_all, load_bronze_settings

settings = load_bronze_settings(
    PROJECT_ROOT / "config" / "bronze.toml", dbutils.widgets.get("environment")
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
results = ingest_all(
    spark,
    landing_root=settings.landing_root,
    batch_id=dbutils.widgets.get("batch_id"),
    target=settings.target,
    ingestion_timestamp=datetime.now(UTC),
    sink=standard_sink(
        spark,
        layer_namespace=settings.target.namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),  # pipeline_runs
)

# COMMAND ----------

display(spark.createDataFrame([{**asdict(r), "status": r.status.value} for r in results]))
