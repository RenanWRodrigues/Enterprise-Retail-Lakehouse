"""Pipeline observability shared by the three lakehouses (docs/OBSERVABILITY.md).

>>> with track_run(pipeline_name="retail_silver", run_id=run_id, project="retail",
...                layer=Layer.SILVER, table="retail.silver.orders", sink=sink) as step:
...     step.record_counts(input_rows=n_in, output_rows=n_out, rejected_rows=n_bad)
...     step.record_quality(run_checks(orders, checks, ...))  # raises on CRITICAL
"""

from lakehouse_common.observability.runs import (
    PIPELINE_RUNS_COLUMNS,
    PIPELINE_RUNS_SCHEMA,
    Layer,
    ObservabilitySink,
    PipelineRun,
    RunStatus,
    RunTracker,
    run_record,
    runs_to_dataframe,
    safe_error_message,
    track_run,
)
from lakehouse_common.observability.sinks import (
    DATA_QUALITY_RESULTS_TABLE,
    PIPELINE_RUNS_TABLE,
    DeltaObservabilitySink,
    InMemoryObservabilitySink,
    JsonLinesObservabilitySink,
)

__all__ = [
    "DATA_QUALITY_RESULTS_TABLE",
    "PIPELINE_RUNS_COLUMNS",
    "PIPELINE_RUNS_SCHEMA",
    "PIPELINE_RUNS_TABLE",
    "DeltaObservabilitySink",
    "InMemoryObservabilitySink",
    "JsonLinesObservabilitySink",
    "Layer",
    "ObservabilitySink",
    "PipelineRun",
    "RunStatus",
    "RunTracker",
    "run_record",
    "runs_to_dataframe",
    "safe_error_message",
    "track_run",
]
