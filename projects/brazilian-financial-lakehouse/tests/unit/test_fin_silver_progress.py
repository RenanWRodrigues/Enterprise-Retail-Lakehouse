"""Which Bronze commits a Silver run consumes (pure planning; the Delta reads are integration)."""

import pytest

from financial_lakehouse.silver.progress import CheckpointAheadError, CommitRange, plan_range

SOURCE, CHECKPOINTS = "financial.bronze.bronze_indicator_observations", "financial.silver.cp"


def _plan(checkpoint: int | None, latest: int) -> CommitRange:
    return plan_range(checkpoint, latest, source=SOURCE, checkpoint_table=CHECKPOINTS)


def test_without_a_checkpoint_the_whole_table_is_read_up_to_the_latest_version() -> None:
    commits = _plan(None, 7)

    assert (commits.after, commits.end, commits.is_first_run, commits.is_empty) == (
        None, 7, True, False,
    )  # fmt: skip


def test_the_range_starts_after_the_checkpoint_and_ends_at_the_captured_version() -> None:
    commits = _plan(4, 7)

    assert (commits.after, commits.end, commits.is_first_run, commits.is_empty) == (
        4, 7, False, False,
    )  # fmt: skip


def test_no_commit_since_the_checkpoint_is_an_empty_range() -> None:
    assert _plan(7, 7).is_empty


def test_a_checkpoint_after_the_latest_version_is_refused_with_the_procedure() -> None:
    with pytest.raises(CheckpointAheadError) as error:
        _plan(9, 7)

    message = str(error.value)
    assert "checkpoint version 9 is after the latest version 7" in message
    assert f"delete this pipeline's row from {CHECKPOINTS} for source {SOURCE}" in message
