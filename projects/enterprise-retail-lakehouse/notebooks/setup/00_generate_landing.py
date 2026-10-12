# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — synthetic sources (landing zone in a Volume)
# MAGIC
# MAGIC Writes one batch of the 11 source datasets (CSV/JSON) and the anomaly register to a Unity
# MAGIC Catalog Volume. Default: the smoke size (`config/data_generation_smoke.toml`, a few
# MAGIC thousand rows). Orchestration only: the logic lives in `retail_lakehouse.synthetic`.
# MAGIC This is a source simulator, not the Bronze layer. The Volume must exist.

# COMMAND ----------

dbutils.widgets.text("config", "data_generation_smoke.toml")
dbutils.widgets.text("output", "/Volumes/workspace/smoke_retail/landing")

# COMMAND ----------

# Until the package is installable, import it from the repository (Git folder).
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/setup -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from retail_lakehouse.synthetic import FileSink, generate_retail_dataset, load_config

config = load_config(PROJECT_ROOT / "config" / dbutils.widgets.get("config"))
with FileSink(Path(dbutils.widgets.get("output")), config=config) as sink:
    summary = generate_retail_dataset(config, sink)

# COMMAND ----------

display(spark.createDataFrame([(str(summary),)], "summary string"))
