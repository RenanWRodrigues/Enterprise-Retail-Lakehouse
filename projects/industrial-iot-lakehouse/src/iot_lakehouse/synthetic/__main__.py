"""CLI: generate the IoT landing zone with Spark (Linux, container or Databricks).

Spark file writes need Hadoop native helpers that Windows lacks: run it in
docker/tests.Dockerfile or on Databricks (notebooks/setup/00_generate_synthetic_sources.py).
"""

import argparse
import logging
import os
from pathlib import Path
from typing import Final

from pyspark.sql import SparkSession

from iot_lakehouse.synthetic import (
    estimated_readings,
    generate_master_data,
    load_config,
    write_landing,
)
from lakehouse_common.logging import setup_logging
from lakehouse_common.observability import JsonLinesObservabilitySink

logger = logging.getLogger("iot_lakehouse.synthetic")
DEFAULT_CONFIG = Path(__file__).parents[3] / "config" / "data_generation.toml"
EXIT_WRITE_FAILED: Final = 1
CONFIGURATION_ERRORS: Final = (OSError, ValueError, TypeError)  # TOML errors: ValueError


def main(argv: list[str] | None = None) -> int:
    """Write the landing zone: 0 written, 1 write failed, 2 usage or configuration error.

    The configuration is read and validated before the Spark session starts.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", required=True, help="landing root (local path or Volume)")
    args = parser.parse_args(argv)
    setup_logging(os.environ)  # text by default; LAKEHOUSE_LOG_FORMAT=json; optional webhook

    try:
        config = load_config(args.config)
    except CONFIGURATION_ERRORS as invalid:  # missing file, bad TOML, unknown key, bad value
        parser.error(f"invalid configuration: {invalid}")
    logger.info("~%d readings planned", estimated_readings(config))
    spark = SparkSession.builder.appName("iot-synthetic-sources").getOrCreate()
    sink = JsonLinesObservabilitySink(Path(args.output) / "_observability")  # pipeline_runs
    try:
        counts = write_landing(spark, config, generate_master_data(config), args.output, sink=sink)
    except OSError as failure:
        logger.error("could not write the landing zone to %s: %s", args.output, failure)
        return EXIT_WRITE_FAILED
    for dataset, rows in counts.items():
        logger.info("%-20s %12d rows", dataset, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
