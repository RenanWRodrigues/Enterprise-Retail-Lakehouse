# Databricks notebook source
# MAGIC %md
# MAGIC # Financial — ordered run: Bronze → Silver → Gold
# MAGIC
# MAGIC Runs the layer notebooks in order with the same `environment`; stops at the first failure
# MAGIC (the failed step's `pipeline_runs` row says why). `source = replay` reads recorded API
# MAGIC responses from `replay_root` instead of the internet. Wall-clock time per step at the end.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "smoke", ["smoke", "databricks"])
dbutils.widgets.dropdown("source", "api", ["api", "replay"])
dbutils.widgets.text("replay_root", "/Volumes/workspace/smoke_financial/replay")

# COMMAND ----------

import time

environment = dbutils.widgets.get("environment")
bronze = {
    "environment": environment,
    "source": dbutils.widgets.get("source"),
    "replay_root": dbutils.widgets.get("replay_root"),
}
steps = [
    ("bronze/01_ingest_bronze", bronze),
    ("silver/01_build_silver", {"environment": environment}),
    ("gold/01_build_gold", {"environment": environment}),
]
timings = []
for notebook, parameters in steps:
    started = time.monotonic()
    dbutils.notebook.run(f"./{notebook}", 3600, parameters)
    timings.append((notebook, round(time.monotonic() - started, 1)))

# COMMAND ----------

display(spark.createDataFrame(timings, "step string, seconds double"))
