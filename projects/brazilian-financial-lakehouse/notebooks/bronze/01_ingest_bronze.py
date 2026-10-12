# Databricks notebook source
# MAGIC %md
# MAGIC # Financial — incremental Bronze (BCB SGS, BCB PTAX, IBGE)
# MAGIC
# MAGIC Official APIs -> raw landing (exact responses) -> `bronze_indicator_observations` (Delta,
# MAGIC insert-only MERGE). Incremental by logical checkpoint; see `docs/INCREMENTAL_STRATEGY.md`.
# MAGIC Orchestration only. A failed indicator makes this notebook fail (after the others ran).
# MAGIC `reprocess_from` (optional, `YYYY-MM-DD`): also re-read the history since that date to
# MAGIC recapture revisions older than the lookback; the checkpoint never moves backwards.
# MAGIC `source`: `api` (official APIs) or `replay` (responses recorded with
# MAGIC `--record-responses` and copied to `replay_root`, for a workspace without internet; the
# MAGIC run uses the recording instant, see `docs/databricks/DEPLOYMENT_GUIDE.md`).

# COMMAND ----------

dbutils.widgets.dropdown("environment", "databricks", ["databricks", "smoke", "local"])
dbutils.widgets.text("reprocess_from", "")
dbutils.widgets.dropdown("source", "api", ["api", "replay"])
dbutils.widgets.text("replay_root", "/Volumes/workspace/smoke_financial/replay")

# COMMAND ----------

# Until the package is installable (Phase 1), import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/bronze -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from dataclasses import asdict
from datetime import date

from financial_lakehouse.clients.replay import transport_for
from financial_lakehouse.ingestion import build_incremental_ingestion, load_settings
from financial_lakehouse.storage import DeltaBronzeSink
from lakehouse_common.observability import DeltaObservabilitySink

config = load_settings(
    PROJECT_ROOT / "config" / "ingestion.toml", dbutils.widgets.get("environment")
)
bronze_namespace = config.bronze_table.rsplit(".", 1)[0]  # financial.bronze
transport, recorded_at = transport_for(
    dbutils.widgets.get("source"), Path(dbutils.widgets.get("replay_root"))
)
pipeline = build_incremental_ingestion(
    config.landing_root,
    config.settings,
    DeltaBronzeSink(spark, config.bronze_table),
    observer=DeltaObservabilitySink(spark, bronze_namespace),  # pipeline_runs
    transport=transport,
    clock=(lambda: recorded_at) if recorded_at else None,
)
reprocess_from = dbutils.widgets.get("reprocess_from").strip()
report = pipeline.run(  # raises IngestionFailedError if any indicator failed
    reprocess_from=date.fromisoformat(reprocess_from) if reprocess_from else None
)

# COMMAND ----------

display(spark.createDataFrame([asdict(r) for r in report.results]))
