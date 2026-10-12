# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — ordered run: landing (smoke) → Bronze → Silver → Gold
# MAGIC
# MAGIC Runs the layer notebooks in order with the same `environment`; stops at the first failure
# MAGIC (the failed step's `pipeline_runs` row says why). `generate_landing` writes the smoke
# MAGIC landing first (the Volume must exist). Wall-clock time per step is shown at the end.

# COMMAND ----------

dbutils.widgets.dropdown("environment", "smoke", ["smoke", "databricks"])
dbutils.widgets.dropdown("generate_landing", "true", ["true", "false"])
dbutils.widgets.text("batch_id", "initial")

# COMMAND ----------

import time

environment = dbutils.widgets.get("environment")
steps = [
    *(
        [("setup/00_generate_landing", {})]
        if dbutils.widgets.get("generate_landing") == "true"
        else []
    ),
    (
        "bronze/01_ingest_bronze",
        {"environment": environment, "batch_id": dbutils.widgets.get("batch_id")},
    ),
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
