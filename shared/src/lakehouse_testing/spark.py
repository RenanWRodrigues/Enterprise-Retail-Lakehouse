"""Local SparkSession for tests: small, fast and deterministic (Delta Lake when possible)."""

import importlib.util
import os
import sys
from pathlib import Path
from typing import Final

from pyspark.sql import SparkSession

LOCAL_MASTER: Final = "local[2]"
"""Two threads: cheap, yet exposes code that silently depends on a single partition."""

TEST_SPARK_CONF: Final[dict[str, str]] = {
    "spark.sql.shuffle.partitions": "2",
    "spark.default.parallelism": "2",
    "spark.sql.session.timeZone": "UTC",
    "spark.ui.enabled": "false",
    "spark.ui.showConsoleProgress": "false",
    "spark.driver.host": "127.0.0.1",
    "spark.driver.bindAddress": "127.0.0.1",
}
DELTA_EXTENSION: Final = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CONF: Final[dict[str, str]] = {
    "spark.sql.extensions": DELTA_EXTENSION,
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def delta_supported() -> bool:
    """Delta needs the ``delta-spark`` package and, on Windows, Hadoop's winutils."""
    if importlib.util.find_spec("delta") is None:
        return False
    return sys.platform != "win32" or bool(os.environ.get("HADOOP_HOME"))


def is_delta_enabled(session: SparkSession) -> bool:
    return DELTA_EXTENSION in (session.conf.get("spark.sql.extensions") or "")


def build_local_spark_session(
    warehouse_dir: Path, app_name: str = "lakehouse-tests", *, enable_delta: bool | None = None
) -> SparkSession:
    """Create (or reuse) the local test session.

    ``warehouse_dir`` should be a temporary directory so tests never write into the
    repository. Python workers run on the current interpreter (the project venv):
    PySpark reads ``PYSPARK_PYTHON`` from the environment and otherwise launches
    ``python3``, which does not exist on Windows. An explicit user value is kept.
    Delta is enabled when supported (``enable_delta=None``); its JARs are resolved by
    ``delta.configure_spark_with_delta_pip`` (downloaded once into the Ivy cache).
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    use_delta = delta_supported() if enable_delta is None else enable_delta
    conf = {**TEST_SPARK_CONF, "spark.sql.warehouse.dir": warehouse_dir.resolve().as_uri()}
    if use_delta:
        conf.update(DELTA_CONF)
    builder = SparkSession.builder.master(LOCAL_MASTER).appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    if use_delta:
        from delta import configure_spark_with_delta_pip  # noqa: PLC0415 - optional dependency

        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session
