# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace preflight (Retail and Financial)
# MAGIC
# MAGIC Detects what the pipelines need instead of presuming it: Python/Spark, the catalog, schema
# MAGIC and Volume creation, files in the Volume, Delta Change Data Feed and outbound HTTPS to the
# MAGIC official APIs. Each check reports OK or the reason; none stops the others.
# MAGIC **Creates** (if missing) the schema and Volume below and the probe table `_preflight_cdf`.
# MAGIC Run only with authorization, in the smoke schema (`docs/databricks/DEPLOYMENT_GUIDE.md`).

# COMMAND ----------

dbutils.widgets.text("namespace", "workspace.smoke_preflight")
dbutils.widgets.text("volume", "probe")

# COMMAND ----------

import sys
from pathlib import Path

REPO_ROOT = Path.cwd().parents[1]  # shared/notebooks -> repository root
sys.path[:0] = [str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from dataclasses import asdict

from lakehouse_common.preflight import run_preflight

results = run_preflight(
    spark, namespace=dbutils.widgets.get("namespace"), volume=dbutils.widgets.get("volume")
)
display(
    spark.createDataFrame([asdict(r) for r in results], "check string, ok boolean, detail string")
)
