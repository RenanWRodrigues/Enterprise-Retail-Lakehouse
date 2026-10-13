# Databricks notebook source
# MAGIC %md
# MAGIC # IoT — synthetic sources (landing zone)
# MAGIC
# MAGIC Generates CMMS master data and ~32M sensor readings (Spark) plus the ground truth used to
# MAGIC validate detections later. Orchestration only: logic in `iot_lakehouse.synthetic`.
# MAGIC This is a source simulator, not the Bronze layer.

# COMMAND ----------

dbutils.widgets.text("output", "/Volumes/iot/landing/files")

# COMMAND ----------

# MAGIC %pip install --quiet --no-deps /Volumes/workspace/libs/wheels/databricks_lakehouse_portfolio-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # the installed packages are importable from here on

# COMMAND ----------

from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/setup -> project root (config files)

# COMMAND ----------

from iot_lakehouse.synthetic import generate_master_data, load_config, write_landing
from lakehouse_common.observability import JsonLinesObservabilitySink

config = load_config(PROJECT_ROOT / "config" / "data_generation.toml")
output = dbutils.widgets.get("output")
sink = JsonLinesObservabilitySink(Path(output) / "_observability")  # pipeline_runs in the Volume
counts = write_landing(spark, config, generate_master_data(config), output, sink=sink)

# COMMAND ----------

display(spark.createDataFrame(list(counts.items()), "dataset string, rows long"))
