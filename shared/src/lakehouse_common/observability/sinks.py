"""Where run records go. Every sink stores the same rows (``run_record``, ``result_record``).

- ``InMemoryObservabilitySink``: tests.
- ``JsonLinesObservabilitySink``: ``<root>/pipeline_runs.jsonl`` and
  ``<root>/data_quality_results.jsonl``, append-only, pure Python: local runs (Windows included,
  no Spark) and pipelines that never start Spark.
- ``DeltaObservabilitySink``: ``<namespace>.pipeline_runs`` and
  ``<namespace>.data_quality_results``, append-only Delta tables (Databricks/Linux).
"""

import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from lakehouse_common.naming import validate_table_name
from lakehouse_common.observability.runs import PipelineRun, run_record, runs_to_dataframe
from lakehouse_common.quality import CheckResult, result_record, results_to_dataframe

PIPELINE_RUNS_TABLE = "pipeline_runs"
DATA_QUALITY_RESULTS_TABLE = "data_quality_results"


class InMemoryObservabilitySink:
    def __init__(self) -> None:
        self.runs: list[PipelineRun] = []

    def write(self, run: PipelineRun) -> None:
        self.runs.append(run)

    @property
    def quality(self) -> list[CheckResult]:
        return [result for run in self.runs for result in run.quality]


def _json_value(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


class JsonLinesObservabilitySink:
    """One JSON object per line; a run and its checks are appended, never rewritten."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _append(self, name: str, records: Iterable[Mapping[str, object]]) -> None:
        lines = [
            json.dumps({k: _json_value(v) for k, v in record.items()}, ensure_ascii=False)
            for record in records
        ]
        if not lines:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / f"{name}.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n")

    def write(self, run: PipelineRun) -> None:
        self._append(PIPELINE_RUNS_TABLE, [run_record(run)])
        self._append(
            DATA_QUALITY_RESULTS_TABLE, (result_record(r, run_id=run.run_id) for r in run.quality)
        )


def _append(frame: DataFrame, table: str) -> None:
    if frame.sparkSession.catalog.tableExists(table):
        frame.writeTo(table).append()
    else:
        frame.writeTo(table).using("delta").create()


class DeltaObservabilitySink:
    def __init__(self, spark: SparkSession, namespace: str) -> None:
        self._spark = spark
        self._namespace = validate_table_name(namespace)

    def table(self, name: str) -> str:
        return f"{self._namespace}.{name}"

    def write(self, run: PipelineRun) -> None:
        _append(runs_to_dataframe(self._spark, [run]), self.table(PIPELINE_RUNS_TABLE))
        if run.quality:
            results = results_to_dataframe(self._spark, run.quality, run_id=run.run_id)
            _append(results, self.table(DATA_QUALITY_RESULTS_TABLE))
