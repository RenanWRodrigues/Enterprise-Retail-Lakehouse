"""Characterization of the Retail generator: refactors must not change one byte of its output.

The digest covers every row of every dataset and every registered anomaly, generated from the
versioned smoke configuration. It was recorded from the code before ``_Transactions._order`` was
split into named steps (2026-10-13). A different digest means the random draws or the emitted rows
changed: a behaviour change, not a refactor.
"""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

from retail_lakehouse.synthetic import MemorySink, generate_retail_dataset, load_config

SMOKE = Path("projects/enterprise-retail-lakehouse/config/data_generation_smoke.toml")
GOLDEN = "3ff59ffdda91eba92738651a305b27d92367af4c10de8bfef40b08f386337d5c"


def _digest(sink: MemorySink) -> str:
    digest = hashlib.sha256()
    for dataset in sorted(sink.rows):
        for row in sink.rows[dataset]:
            digest.update(json.dumps([dataset, row], sort_keys=True, default=str).encode())
    for anomaly in sink.anomalies:
        digest.update(json.dumps(asdict(anomaly), sort_keys=True, default=str).encode())
    return digest.hexdigest()


def test_the_smoke_dataset_is_byte_identical(repo_root: Path) -> None:
    sink = MemorySink()

    generate_retail_dataset(load_config(repo_root / SMOKE), sink)

    assert _digest(sink) == GOLDEN


INCREMENTAL = Path("projects/enterprise-retail-lakehouse/config/incremental.toml")
GOLDEN_INCREMENTAL = "05baf5a4e3bb9b8e299bdf2f3e3475ce27fb59c781be67457bd8963e14270b9a"


def test_the_incremental_batches_are_byte_identical(repo_root: Path) -> None:
    """Incremental batches reuse ``_order`` (subclass of the full-mode transactions)."""
    from retail_lakehouse.synthetic.incremental import (  # noqa: PLC0415
        generate_incremental_batch,
        load_plan,
    )

    plan = load_plan(repo_root / INCREMENTAL)
    sink = MemorySink()
    for batch in plan.batches:
        generate_incremental_batch(plan, batch.batch_id, sink)

    assert _digest(sink) == GOLDEN_INCREMENTAL
