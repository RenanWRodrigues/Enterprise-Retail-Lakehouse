"""CLI: run the reproducible detector evaluation and write the report.

Example (from the repository root):
  PYTHONPATH="projects/industrial-iot-lakehouse/src;shared/src" \
  python -m iot_lakehouse.evaluation --output docs/iot/evaluation
"""

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from iot_lakehouse.evaluation.report import write_report
from iot_lakehouse.evaluation.run import evaluate, load_evaluation_config
from lakehouse_common.logging import setup_logging

DEFAULT_CONFIG = Path(__file__).parents[3] / "config" / "evaluation.toml"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, required=True, help="report directory")
    args = parser.parse_args(argv)
    setup_logging(os.environ)  # text by default; LAKEHOUSE_LOG_FORMAT=json; optional webhook
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # Windows has no "python3"
    spark = (
        SparkSession.builder.appName("iot-evaluation")
        .config("spark.sql.session.timeZone", "UTC")  # days and ISO texts are UTC
        .getOrCreate()
    )
    try:
        report = evaluate(spark, load_evaluation_config(args.config))
    finally:
        spark.stop()
    write_report(report, args.output, generated_at=f"{datetime.now(UTC):%Y-%m-%d %H:%M}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
