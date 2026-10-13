# Databricks notebook source
# MAGIC %md
# MAGIC # IoT — Bronze ingestion
# MAGIC
# MAGIC Every new landing file (CMMS CSV, historian JSON gzip) into append-only Delta tables
# MAGIC `bronze_<dataset>`, file by file: a file already decided is skipped, a broken file is
# MAGIC quarantined with its reason in `bronze_files`. Orchestration only: the logic lives in
# MAGIC `iot_lakehouse.bronze` (see `docs/BRONZE.md`). Observed in `pipeline_runs`.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])

# COMMAND ----------

# Until the package is installable, import it from the repository (Git folder).
# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/bronze -> project root (config files)
REPO_ROOT = PROJECT_ROOT.parents[1]  # shared config files

# COMMAND ----------

from datetime import UTC, datetime

from iot_lakehouse.bronze.pipeline import load_bronze_settings, run_bronze
from lakehouse_common.observability.ops import load_ops_namespace, standard_sink

settings = load_bronze_settings(
    PROJECT_ROOT / "config" / "bronze.toml", dbutils.widgets.get("environment")
)
OPS_NAMESPACE = load_ops_namespace(  # central operational record (docs/OBSERVABILITY.md §8)
    REPO_ROOT / "shared" / "config" / "ops.toml", dbutils.widgets.get("environment")
)
outcomes = run_bronze(
    spark,
    landing_root=settings.landing_root,
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

display(
    spark.createDataFrame(
        [
            (
                o.dataset,
                o.files_found,
                o.files_skipped,
                len(o.ingested),
                len(o.quarantined),
                o.rows_written,
            )
            for o in outcomes
        ],
        "dataset string, files long, skipped long, ingested long, quarantined long, rows long",
    )
)
