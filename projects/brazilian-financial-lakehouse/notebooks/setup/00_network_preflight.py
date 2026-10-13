# Databricks notebook source
# MAGIC %md
# MAGIC # Financial — network preflight (official APIs)
# MAGIC
# MAGIC Can this workspace reach the hosts the Bronze ingestion calls? One small read-only GET per
# MAGIC host (BCB SGS, BCB PTAX, IBGE), after a separate DNS lookup; no retries, 10 s timeout, no
# MAGIC credentials, nothing written. Outcomes and how to read them: `financial_lakehouse.clients.
# MAGIC network_probe`. Record the printed evidence in `docs/databricks/network-preflight.md`.
# MAGIC If egress is closed, use the replay fallback (`docs/databricks/DEPLOYMENT_GUIDE.md` §5).

# COMMAND ----------

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/setup -> project root (config files)

# COMMAND ----------

import json
import os
import platform
from datetime import UTC, datetime

from financial_lakehouse.clients.network_probe import probe_all

tested_at = datetime.now(UTC)
results = [result.as_row() for result in probe_all()]
evidence = {
    "tested_at_utc": tested_at.isoformat(timespec="seconds"),
    "environment": "databricks",
    "databricks_runtime": os.environ.get("DATABRICKS_RUNTIME_VERSION", "unknown"),
    "python": platform.python_version(),
    "spark": spark.version,
    "results": results,
}

# COMMAND ----------

display(spark.createDataFrame(results))
# Evidence to paste into docs/databricks/network-preflight.md
display(spark.createDataFrame([(json.dumps(evidence, indent=2),)], "evidence string"))
