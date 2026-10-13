"""The central operational record: one ``<ops>.pipeline_runs`` for the three projects.

Each project writes its run records in the schema of every layer (``<layer>.pipeline_runs``, the
shared ``PIPELINE_RUNS_SCHEMA``). Those tables stay as they are (history, existing readers). On
top of them, ``FanOutSink`` also writes each run to ``<ops>.pipeline_runs`` in standard columns
(``OPS_RUNS_SCHEMA``), so one table answers "what ran, where, how did it end" across projects.

- Standard columns are common to every project; project-specific values go to ``extensions``
  (``map<string,string>``), so a new metric never changes the shared schema.
- ``consolidated_runs`` is the single read point: the ops table plus the per-layer history
  mapped to the standard columns, with no duplicate for runs present in both.
- Delta has no transaction across tables: the fan-out tries every destination and reports the
  first failure; a reader may briefly see a run in one table and not yet in the other.

docs/OBSERVABILITY.md §8.
"""

import json
import threading
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse_common.config import load_environment
from lakehouse_common.naming import validate_namespace
from lakehouse_common.observability.runs import ObservabilitySink, PipelineRun
from lakehouse_common.observability.sinks import _append
from lakehouse_common.quality import Dimension

OPS_RUNS_TABLE: Final = "pipeline_runs"
MS_PER_SECOND: Final = 1000.0
_KEY: Final = ("run_id", "source_name", "environment")
_SCORED: Final = (
    Dimension.COMPLETENESS,
    Dimension.UNIQUENESS,
    Dimension.VALIDITY,
    Dimension.REFERENTIAL_INTEGRITY,
)
_SCORE_COLUMNS: Final = tuple(dimension.value for dimension in _SCORED)

OPS_RUNS_SCHEMA: Final = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("project", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_name", StringType(), False),  # the step's table (or staging set)
        StructField("layer", StringType(), False),
        StructField("status", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_ms", DoubleType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_valid", LongType(), True),
        StructField("records_quarantined", LongType(), True),
        StructField("records_deduplicated", LongType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("environment", StringType(), False),
        StructField("extensions", MapType(StringType(), StringType()), True),
    ]
)
OPS_RUNS_COLUMNS: Final = tuple(OPS_RUNS_SCHEMA.fieldNames())


def ops_record(run: PipelineRun, *, environment: str) -> dict[str, object]:
    """The standard row of ``run``. ``extensions``: failed checks and quality scores, as text."""
    scores = run.scores()
    extensions = {"checks_failed": str(sum(1 for r in run.quality if r.status.value != "PASS"))}
    extensions |= {d.value: str(v) for d, v in scores.items() if v is not None}
    record: dict[str, object] = {
        "run_id": run.run_id,
        "project": run.project,
        "pipeline_name": run.pipeline_name,
        "source_name": run.table,
        "layer": run.layer.value,
        "status": run.status.value if run.status else None,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_ms": None
        if run.duration_seconds is None
        else round(run.duration_seconds * MS_PER_SECOND, 3),
        "records_read": run.input_rows,
        "records_valid": run.output_rows,
        "records_quarantined": run.rejected_rows,
        "records_deduplicated": run.duplicate_rows,
        "error_type": run.error_type,
        "error_message": run.error_message,
        "environment": environment,
        "extensions": extensions,
    }
    return {column: record[column] for column in OPS_RUNS_COLUMNS}


def ops_runs_to_dataframe(
    spark: SparkSession, runs: Sequence[PipelineRun], *, environment: str
) -> DataFrame:
    rows = [tuple(ops_record(run, environment=environment).values()) for run in runs]
    return spark.createDataFrame(rows, OPS_RUNS_SCHEMA)


def legacy_as_ops(legacy: DataFrame, *, environment: str) -> DataFrame:
    """A per-layer ``pipeline_runs`` (``PIPELINE_RUNS_SCHEMA``) read in the standard columns.

    ``error_type`` comes from the ``Type: message`` prefix of ``error_message``.
    """
    error_type = F.regexp_extract("error_message", r"^([A-Za-z_][A-Za-z0-9_.]*):", 1)
    entries = [F.lit("checks_failed"), F.col("checks_failed").cast("string")]
    for column in _SCORE_COLUMNS:
        entries += [F.lit(column), F.col(column).cast("string")]
    extensions = F.map_filter(F.create_map(*entries), lambda _k, v: v.isNotNull())
    return legacy.select(
        "run_id",
        "project",
        "pipeline_name",
        F.col("table").alias("source_name"),
        "layer",
        "status",
        "started_at",
        "finished_at",
        F.round(F.col("duration_seconds") * MS_PER_SECOND, 3).alias("duration_ms"),
        F.col("input_rows").alias("records_read"),
        F.col("output_rows").alias("records_valid"),
        F.col("rejected_rows").alias("records_quarantined"),
        F.col("duplicate_rows").alias("records_deduplicated"),
        F.when(error_type != "", error_type).alias("error_type"),
        "error_message",
        F.lit(environment).alias("environment"),
        extensions.alias("extensions"),
    )


def consolidated_runs(ops: DataFrame | None, legacy: Sequence[tuple[DataFrame, str]]) -> DataFrame:
    """Every run of every project, once: the ops record, then per-layer history it lacks.

    ``legacy``: (per-layer ``pipeline_runs`` table, its environment). A run written by the
    fan-out exists in both and is taken from the ops record (key: run, source, environment).
    """
    mapped = [legacy_as_ops(frame, environment=env) for frame, env in legacy]
    if ops is None and not mapped:
        raise ValueError("nothing to consolidate: no ops table and no per-layer table")
    history = mapped[0] if mapped else None
    for frame in mapped[1:]:
        history = history.unionByName(frame)  # type: ignore[union-attr]
    if ops is None:
        return history  # type: ignore[return-value]
    if history is None:
        return ops.select(*OPS_RUNS_COLUMNS)
    only_history = history.join(ops.select(*_KEY), list(_KEY), "left_anti")
    return ops.select(*OPS_RUNS_COLUMNS).unionByName(only_history.select(*OPS_RUNS_COLUMNS))


# --- sinks ---------------------------------------------------------------------------------


class OpsDeltaSink:
    """Appends to ``<namespace>.pipeline_runs`` (blind appends: concurrent runs do not conflict)."""

    def __init__(self, spark: SparkSession, namespace: str, *, environment: str) -> None:
        self._spark, self._environment = spark, environment
        self.namespace = validate_namespace(namespace)

    def write(self, run: PipelineRun) -> None:
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.namespace}")
        frame = ops_runs_to_dataframe(self._spark, [run], environment=self._environment)
        _append(frame, f"{self.namespace}.{OPS_RUNS_TABLE}")


def _json_value(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


class OpsJsonLinesSink:
    """``<root>/pipeline_runs.jsonl`` in the standard columns (local runs, no Spark)."""

    _lock = threading.Lock()  # one line per run, never interleaved

    def __init__(self, root: Path, *, environment: str) -> None:
        self.root, self._environment = root, environment

    def write(self, run: PipelineRun) -> None:
        record = ops_record(run, environment=self._environment)
        line = json.dumps({k: _json_value(v) for k, v in record.items()}, ensure_ascii=False)
        with self._lock:
            self.root.mkdir(parents=True, exist_ok=True)
            with (self.root / f"{OPS_RUNS_TABLE}.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(line + "\n")


class FanOutSink:
    """Writes each run to every sink (per-layer table, then the ops record), in order.

    Not atomic: every sink is tried even if one fails, then the first failure is raised (a
    FAILED run's own error still wins in ``track_run``).
    """

    def __init__(self, *sinks: ObservabilitySink) -> None:
        if not sinks:
            raise ValueError("FanOutSink needs at least one sink")
        self._sinks = sinks

    def write(self, run: PipelineRun) -> None:
        first: Exception | None = None
        for sink in self._sinks:
            try:
                sink.write(run)
            except Exception as error:  # re-raised below, after the other sinks
                first = first or error
        if first is not None:
            raise first


def load_ops_namespace(path: Path, environment: str) -> str:
    """``ops_namespace`` of ``environment`` in ``shared/config/ops.toml``."""
    return validate_namespace(str(load_environment(path, environment)["ops_namespace"]))


def standard_sink(
    spark: SparkSession, *, layer_namespace: str, ops_namespace: str, environment: str
) -> ObservabilitySink:
    """What a Databricks notebook passes as ``sink``: per-layer tables + the ops record."""
    from lakehouse_common.observability.sinks import DeltaObservabilitySink  # noqa: PLC0415

    return FanOutSink(
        DeltaObservabilitySink(spark, layer_namespace),
        OpsDeltaSink(spark, ops_namespace, environment=environment),
    )


__all__ = [
    "OPS_RUNS_COLUMNS",
    "OPS_RUNS_SCHEMA",
    "OPS_RUNS_TABLE",
    "FanOutSink",
    "OpsDeltaSink",
    "OpsJsonLinesSink",
    "consolidated_runs",
    "legacy_as_ops",
    "load_ops_namespace",
    "ops_record",
    "ops_runs_to_dataframe",
    "standard_sink",
]
