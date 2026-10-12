# Databricks notebook source
# MAGIC %md
# MAGIC # Financial — Gold (analytical tables and monthly macro panel)
# MAGIC
# MAGIC Silver -> `gold_interest_rates`, `gold_exchange_rates`, `gold_inflation`,
# MAGIC `gold_economic_activity`, `gold_labor_market`, `gold_brazil_macro_monthly`.
# MAGIC Full deterministic rebuild; resampling rules and metric definitions in `docs/GOLD.md`.
# MAGIC Observed: `pipeline_runs` and `data_quality_results` (`docs/OBSERVABILITY.md`).

# COMMAND ----------

dbutils.widgets.text("environment", "databricks")

# COMMAND ----------

import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parents[1]  # notebooks/gold -> project root
REPO_ROOT = PROJECT_ROOT.parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"), str(REPO_ROOT / "shared" / "src")]

# COMMAND ----------

from financial_lakehouse.gold import load_medallion_settings
from financial_lakehouse.gold.pipeline import run_gold
from financial_lakehouse.silver.pipeline import SILVER_TABLE

settings = load_medallion_settings(
    PROJECT_ROOT / "config" / "medallion.toml", dbutils.widgets.get("environment")
)
run = run_gold(  # raises CriticalQualityError before replacing any table
    spark,
    silver_table=f"{settings.silver_namespace}.{SILVER_TABLE}",
    gold_namespace=settings.gold_namespace,
)

# COMMAND ----------

display(
    spark.table(f"{settings.gold_namespace}.gold_brazil_macro_monthly").orderBy(
        "reference_month", ascending=False
    )
)
