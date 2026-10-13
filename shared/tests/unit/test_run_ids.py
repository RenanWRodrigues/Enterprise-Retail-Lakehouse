"""Run ids: unique without trusting the clock, readable and time-sortable by their prefix."""

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from lakehouse_common.observability import (
    RUN_ID_PATTERN,
    JsonLinesObservabilitySink,
    Layer,
    new_run_id,
    run_started_label,
    track_run,
)

START = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)


def test_format_is_the_utc_second_then_a_uuid() -> None:
    run_id = new_run_id(START)

    assert RUN_ID_PATTERN.fullmatch(run_id)
    assert re.fullmatch(r"20261012T120000Z-[0-9a-f]{32}", run_id)
    assert run_started_label(run_id) == "20261012T120000Z"


def test_the_prefix_is_utc_whatever_the_input_zone() -> None:
    brasilia = START.astimezone(timezone(timedelta(hours=-3)))

    assert new_run_id(brasilia).startswith("20261012T120000Z-")


def test_a_naive_start_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone"):
        new_run_id(datetime(2026, 10, 12, 12))  # noqa: DTZ001 - the case under test


def test_many_runs_in_the_same_second_never_collide() -> None:
    ids = {new_run_id(START) for _ in range(20_000)}

    assert len(ids) == 20_000


def test_concurrent_runs_never_collide() -> None:
    with ThreadPoolExecutor(max_workers=8) as pool:
        batches = list(pool.map(lambda _: [new_run_id(START) for _ in range(2_000)], range(8)))

    ids = [run_id for batch in batches for run_id in batch]
    assert len(set(ids)) == len(ids) == 16_000


def test_ids_sort_by_start_second() -> None:
    later = new_run_id(START + timedelta(seconds=1))

    assert max(new_run_id(START), later) == later


def test_the_random_part_is_injectable_for_deterministic_tests() -> None:
    assert new_run_id(START, token=lambda: "0" * 32) == f"20261012T120000Z-{'0' * 32}"


def test_a_run_id_is_persisted_unchanged(tmp_path: Path) -> None:
    sink = JsonLinesObservabilitySink(tmp_path)
    run_id = new_run_id(START)

    with track_run(
        pipeline_name="p", run_id=run_id, project="x", layer=Layer.SILVER, table="t", sink=sink
    ):
        pass

    [line] = (tmp_path / "pipeline_runs.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(line)["run_id"] == run_id


def test_legacy_ids_are_still_readable() -> None:
    """Rows written before this change keep their second-based id."""
    assert run_started_label("20261007T120000Z") == "20261007T120000Z"
    assert not RUN_ID_PATTERN.fullmatch("20261007T120000Z")
