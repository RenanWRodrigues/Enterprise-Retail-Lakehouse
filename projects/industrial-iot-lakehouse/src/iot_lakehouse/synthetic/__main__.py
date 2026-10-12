"""CLI: generate the IoT landing zone with Spark (Linux, container or Databricks).

Spark file writes need Hadoop native helpers that Windows lacks: run it in
docker/tests.Dockerfile or on Databricks (notebooks/setup/00_generate_synthetic_sources.py).
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from iot_lakehouse.synthetic import (
    estimated_readings,
    generate_master_data,
    load_config,
    write_landing,
)
from lakehouse_common.observability import JsonLinesObservabilitySink

logger = logging.getLogger("iot_lakehouse.synthetic")
DEFAULT_CONFIG = Path(__file__).parents[3] / "config" / "data_generation.toml"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", required=True, help="landing root (local path or Volume)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    config = load_config(args.config)
    logger.info("~%d readings planned", estimated_readings(config))
    spark = SparkSession.builder.appName("iot-synthetic-sources").getOrCreate()
    sink = JsonLinesObservabilitySink(Path(args.output) / "_observability")  # pipeline_runs
    counts = write_landing(spark, config, generate_master_data(config), args.output, sink=sink)
    for dataset, rows in counts.items():
        logger.info("%-20s %12d rows", dataset, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
