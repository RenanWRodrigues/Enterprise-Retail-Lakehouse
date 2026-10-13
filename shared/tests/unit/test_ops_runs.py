"""The central operational record (``<ops>.pipeline_runs``): mapping, sinks, fan-out, config.

No Spark here; the consolidated reading over Delta/DataFrames is in
``shared/tests/transformation/test_ops_consolidation.py``.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse_common.observability import (
    InMemoryObservabilitySink,
    Layer,
    PipelineRun,
    RunStatus,
    track_run,
)
from lakehouse_common.observability.ops import (
    OPS_RUNS_COLUMNS,
    FanOutSink,
    OpsJsonLinesSink,
    load_ops_namespace,
    ops_record,
)

AT = datetime(2026, 10, 12, 12, tzinfo=UTC)
REPO = Path(__file__).resolve().parents[3]


def _run(sink: object, *, fail: BaseException | None = None, counts: bool = True) -> PipelineRun:
    expected = pytest.raises(type(fail)) if fail else nullcontext()
    tracked = track_run(
        pipeline_name="retail_silver", run_id="r-1", project="retail", layer=Layer.SILVER,
        table="retail.silver.silver_orders", sink=sink,  # type: ignore[arg-type]
        clock=lambda: AT, monotonic=iter([0.0, 2.5]).__next__,
    )  # fmt: skip
    with expected, tracked as step:
        if counts:
            step.record_counts(input_rows=10, output_rows=7, rejected_rows=2, duplicate_rows=1)
        if fail:
            raise fail
    return step.run


def test_the_standard_columns() -> None:
    assert OPS_RUNS_COLUMNS == (
        "run_id", "project", "pipeline_name", "source_name", "layer", "status", "started_at",
        "finished_at", "duration_ms", "records_read", "records_valid", "records_quarantined",
        "records_deduplicated", "error_type", "error_message", "environment", "extensions",
    )  # fmt: skip


def test_a_successful_run_maps_to_the_standard_record() -> None:
    run = _run(InMemoryObservabilitySink())

    record = ops_record(run, environment="smoke")

    assert list(record) == list(OPS_RUNS_COLUMNS)
    identity = ("run_id", "project", "pipeline_name", "source_name", "layer")
    assert {k: record[k] for k in identity} == {
        "run_id": "r-1", "project": "retail", "pipeline_name": "retail_silver",
        "source_name": "retail.silver.silver_orders", "layer": "silver",
    }  # fmt: skip
    assert (record["status"], record["duration_ms"], record["environment"]) == (
        "SUCCEEDED",
        2500.0,
        "smoke",
    )
    assert (
        record["records_read"],
        record["records_valid"],
        record["records_quarantined"],
        record["records_deduplicated"],
    ) == (10, 7, 2, 1)
    assert (record["error_type"], record["error_message"]) == (None, None)
    assert record["extensions"] == {"checks_failed": "0"}  # project specifics as strings


def test_a_partial_failure_keeps_the_confirmed_counts_and_the_error() -> None:
    run = _run(InMemoryObservabilitySink(), fail=OSError("merge failed"))

    record = ops_record(run, environment="databricks")

    assert (record["status"], record["records_read"], record["records_valid"]) == ("FAILED", 10, 7)
    assert (record["error_type"], record["error_message"]) == ("OSError", "OSError: merge failed")


def test_a_complete_failure_has_no_counts() -> None:
    run = _run(InMemoryObservabilitySink(), fail=ValueError("no input"), counts=False)

    record = ops_record(run, environment="local")

    assert (record["status"], record["records_read"], record["error_type"]) == (
        "FAILED",
        None,
        "ValueError",
    )


def test_ops_json_lines_are_safe_under_concurrent_runs(tmp_path: Path) -> None:
    sink = OpsJsonLinesSink(tmp_path, environment="local")

    def write(i: int) -> None:
        sink.write(
            PipelineRun(
                "p", f"r-{i}", "iot", Layer.SILVER, "t", AT, AT, 0.1, status=RunStatus.SUCCEEDED
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(400)))

    lines = [
        json.loads(line)
        for line in (tmp_path / "pipeline_runs.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(lines) == len({row["run_id"] for row in lines}) == 400
    assert all(list(row) == list(OPS_RUNS_COLUMNS) for row in lines)


class _Broken:
    def __init__(self) -> None:
        self.calls = 0

    def write(self, run: PipelineRun) -> None:
        self.calls += 1
        raise OSError("ops table unavailable")


def test_fan_out_writes_every_sink_and_reports_the_first_failure() -> None:
    """No transaction across tables: each destination is tried, the failure is not hidden."""
    legacy, broken, after = InMemoryObservabilitySink(), _Broken(), InMemoryObservabilitySink()

    with pytest.raises(OSError, match="ops table unavailable"):
        FanOutSink(legacy, broken, after).write(PipelineRun("p", "r", "x", Layer.GOLD, "t", AT))

    assert (len(legacy.runs), broken.calls, len(after.runs)) == (1, 1, 1)


def test_a_failed_ops_write_never_replaces_the_pipeline_error() -> None:
    legacy = InMemoryObservabilitySink()

    tracked = track_run(
        pipeline_name="p", run_id="r", project="x", layer=Layer.SILVER, table="t",
        sink=FanOutSink(legacy, _Broken()),
    )  # fmt: skip
    with pytest.raises(KeyError), tracked:
        raise KeyError("pipeline error")

    assert [r.status for r in legacy.runs] == [RunStatus.FAILED]


def test_one_ops_namespace_per_environment() -> None:
    config = REPO / "shared" / "config" / "ops.toml"

    namespaces = {env: load_ops_namespace(config, env) for env in ("local", "smoke", "databricks")}

    assert namespaces["local"] == "ops"
    assert namespaces["smoke"] != namespaces["databricks"]  # smoke never writes the real record
    assert all(ns.endswith("ops") for ns in namespaces.values())
