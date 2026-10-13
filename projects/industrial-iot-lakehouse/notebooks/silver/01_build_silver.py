# Databricks notebook source
# MAGIC %md
# MAGIC # IoT — Silver (sensor readings)
# MAGIC
# MAGIC Pending Bronze files → `silver_sensor_readings` (typed, canonical units, one row per
# MAGIC `reading_id`, late and out-of-order readings kept and flagged) and its quarantine.
# MAGIC Orchestration only: the logic lives in `iot_lakehouse.silver` (see `docs/SILVER.md`).
# MAGIC A CRITICAL finding (unbalanced reconciliation, duplicated id, systemic unknown sensors)
# MAGIC fails the notebook before anything is written.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])

# COMMAND ----------

# Until the package is installable, import it from the repository (Git folder).
# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/silver -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from datetime import UTC, datetime

from iot_lakehouse.silver.pipeline import load_silver_settings, run_silver
from lakehouse_common.observability.ops import load_ops_namespace, standard_sink

settings = load_silver_settings(
    PROJECT_ROOT / "config" / "silver.toml", dbutils.widgets.get("environment")
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
files = run_silver(
    spark,
    settings=settings,
    ingestion_timestamp=datetime.now(UTC),
    sink=standard_sink(
        spark,
        layer_namespace=settings.target.namespace,
        ops_namespace=OPS_NAMESPACE,
        environment=dbutils.widgets.get("environment"),
    ),  # pipeline_runs
)

# COMMAND ----------

display(spark.table(settings.target.metrics).orderBy("recorded_at", ascending=False).limit(30))
