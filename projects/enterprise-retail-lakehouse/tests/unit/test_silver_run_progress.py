"""Retail Silver run record under partial failure: only confirmed batches are counted.

A batch is confirmed once its MERGEs succeeded and it was marked processed. The batch work is
injected (``process_batch``), so every failure point is reproduced without Spark or Delta.
"""

from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime

import pytest

from lakehouse_common.observability import (
    InMemoryObservabilitySink,
    Layer,
    ObservabilitySink,
    PipelineRun,
    RunStatus,
    track_run,
)
from retail_lakehouse.silver.pipeline import (
    EntityProgress,
    SilverStage,
    process_pending,
)
from retail_lakehouse.silver.standardize import BatchCounts

STARTED = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
INGESTED = datetime(2026, 10, 12, 11, 0, tzinfo=UTC)
COUNTS = {  # input = valid + quarantined + duplicates
    "b1": BatchCounts(input_rows=10, output_rows=8, rejected_rows=1, duplicate_rows=1),
    "b2": BatchCounts(input_rows=5, output_rows=5, rejected_rows=0, duplicate_rows=0),
    "b3": BatchCounts(input_rows=7, output_rows=4, rejected_rows=3, duplicate_rows=0),
}
STAGES = (
    SilverStage.STANDARDIZE,
    SilverStage.QUALITY_GATE,
    SilverStage.MERGE,
    SilverStage.MARK_PROCESSED,
)
BatchWork = Callable[[str, datetime, EntityProgress], BatchCounts]


class BatchFailedError(RuntimeError):
    pass


def _work(fail_at: tuple[str, SilverStage] | None = None) -> BatchWork:
    """Walk every stage of a batch; raise at ``fail_at`` (batch, stage)."""

    def process(batch_id: str, _ingested_at: datetime, progress: EntityProgress) -> BatchCounts:
        for stage in STAGES:
            progress.enter(stage)
            if (batch_id, stage) == fail_at:
                raise BatchFailedError(f"{stage} failed")
        return COUNTS[batch_id]

    return process


def _pending(*batches: str) -> Callable[[], Sequence[tuple[str, datetime]]]:
    return lambda: [(batch, INGESTED) for batch in batches]


def _run(
    pending: Callable[[], Sequence[tuple[str, datetime]]],
    work: BatchWork,
    sink: ObservabilitySink,
    run_id: str = "run-1",
) -> None:
    with track_run(
        pipeline_name="retail_silver",
        run_id=run_id,
        project="retail",
        layer=Layer.SILVER,
        table="silver.silver_orders",
        sink=sink,
    ) as step:
        process_pending(step, EntityProgress("orders"), pending, work)


def _counts(run: PipelineRun) -> tuple[int | None, ...]:
    return (run.input_rows, run.output_rows, run.rejected_rows, run.duplicate_rows)


@pytest.fixture
def sink() -> InMemoryObservabilitySink:
    return InMemoryObservabilitySink()


def test_successful_run_counts_every_batch(sink: InMemoryObservabilitySink) -> None:
    _run(_pending("b1", "b2", "b3"), _work(), sink)

    [run] = sink.runs
    assert run.status is RunStatus.SUCCEEDED
    assert _counts(run) == (22, 17, 4, 1)


def test_failure_before_the_first_batch_records_zero_confirmed(
    sink: InMemoryObservabilitySink,
) -> None:
    def unreadable() -> Sequence[tuple[str, datetime]]:
        raise BatchFailedError("bronze table unreadable")

    with pytest.raises(BatchFailedError):
        _run(unreadable, _work(), sink)

    [run] = sink.runs
    assert run.status is RunStatus.FAILED
    assert _counts(run) == (0, 0, 0, 0)  # nothing confirmed, which is known, not unknown
    assert run.error_message is not None
    assert "failed at pending_batches" in run.error_message
    assert "confirmed batches: none" in run.error_message


def test_failure_after_one_batch_keeps_its_counts(sink: InMemoryObservabilitySink) -> None:
    with pytest.raises(BatchFailedError):
        _run(_pending("b1", "b2", "b3"), _work(fail_at=("b2", SilverStage.MERGE)), sink)

    [run] = sink.runs
    assert run.status is RunStatus.FAILED
    assert _counts(run) == (10, 8, 1, 1)  # b1 only: b2 failed mid-write and is not confirmed
    assert run.error_message is not None
    assert run.error_message.startswith("BatchFailedError: merge failed")
    assert "failed at merge of batch b2" in run.error_message
    assert "confirmed batches: b1" in run.error_message
    assert (run.run_id, run.started_at is not None, run.finished_at is not None) == (
        "run-1",
        True,
        True,
    )


def test_failure_after_several_batches_sums_only_the_confirmed(
    sink: InMemoryObservabilitySink,
) -> None:
    # b3 wrote its rows but was not marked processed: it will be re-processed, so not counted
    with pytest.raises(BatchFailedError):
        _run(_pending("b1", "b2", "b3"), _work(fail_at=("b3", SilverStage.MARK_PROCESSED)), sink)

    [run] = sink.runs
    assert _counts(run) == (15, 13, 1, 1)
    assert run.error_message is not None
    assert "failed at mark_processed of batch b3" in run.error_message
    assert "confirmed batches: b1, b2" in run.error_message


def test_failing_run_record_does_not_hide_the_batch_error() -> None:
    class BrokenSink:
        def write(self, run: PipelineRun) -> None:
            raise OSError("pipeline_runs unavailable")

    with pytest.raises(BatchFailedError) as raised:
        _run(_pending("b1", "b2"), _work(fail_at=("b2", SilverStage.QUALITY_GATE)), BrokenSink())

    assert "failed at quality_gate of batch b2" in "".join(raised.value.__notes__)


def test_reprocessing_after_a_failure_counts_each_batch_once(
    sink: InMemoryObservabilitySink,
) -> None:
    control: set[str] = set()  # silver_processed_batches: only confirmed batches are marked

    def pending() -> Iterator[tuple[str, datetime]]:
        return ((b, INGESTED) for b in ("b1", "b2", "b3") if b not in control)

    def tracked(work: BatchWork) -> BatchWork:
        def process(batch_id: str, at: datetime, progress: EntityProgress) -> BatchCounts:
            counts = work(batch_id, at, progress)
            control.add(batch_id)
            return counts

        return process

    with pytest.raises(BatchFailedError):
        _run(lambda: list(pending()), tracked(_work(fail_at=("b2", SilverStage.MERGE))), sink)
    _run(lambda: list(pending()), tracked(_work()), sink, run_id="run-2")

    first, second = sink.runs
    assert (first.status, second.status) == (RunStatus.FAILED, RunStatus.SUCCEEDED)
    assert _counts(second) == (12, 9, 3, 0)  # b2 + b3; b1 was confirmed by the first run
    total_input = sum(run.input_rows or 0 for run in sink.runs)
    assert total_input == sum(c.input_rows for c in COUNTS.values())  # no batch counted twice
