"""Same seed + config => byte-identical output; different seed => different data."""

import hashlib
from dataclasses import replace
from pathlib import Path

from retail_lakehouse.synthetic import (
    FileSink,
    GenerationConfig,
    MemorySink,
    generate_retail_dataset,
)

TINY = GenerationConfig(customers=60, products=30, stores=3, suppliers=4, orders=150)


def _generate(config: GenerationConfig) -> MemorySink:
    sink = MemorySink()
    generate_retail_dataset(config, sink)
    return sink


def _digest_tree(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_same_seed_produces_identical_rows(small_config: GenerationConfig) -> None:
    first, second = _generate(small_config), _generate(small_config)
    assert first.rows == second.rows
    assert first.anomalies == second.anomalies


def test_different_seed_produces_different_rows(small_config: GenerationConfig) -> None:
    first = _generate(small_config)
    other = _generate(replace(small_config, seed=small_config.seed + 1))
    assert first.rows["orders"] != other.rows["orders"]
    assert first.rows["customers"] != other.rows["customers"]


def test_file_output_is_byte_identical_across_runs(
    small_config: GenerationConfig, tmp_path: Path
) -> None:
    for run in ("a", "b"):
        with FileSink(tmp_path / run, config=small_config) as sink:
            generate_retail_dataset(small_config, sink)

    assert _digest_tree(tmp_path / "a") == _digest_tree(tmp_path / "b")


def test_file_output_layout_and_manifest(tmp_path: Path) -> None:
    with FileSink(tmp_path, config=TINY) as sink:
        summary = generate_retail_dataset(TINY, sink)

    assert (tmp_path / "_manifest_initial.json").is_file()
    assert (tmp_path / "_anomalies_initial.csv").is_file()
    assert (tmp_path / "crm/customers/customers_initial.csv").is_file()
    assert (tmp_path / "ecommerce/orders/orders_initial.jsonl").is_file()
    assert summary.row_counts["orders"] == TINY.orders
    manifest = (tmp_path / "_manifest_initial.json").read_text(encoding="utf-8")
    assert "generated_at" not in manifest, "timestamps would break byte-level determinism"


def test_changing_one_volume_does_not_reshuffle_master_data(small_config: GenerationConfig) -> None:
    base = _generate(small_config)
    more_orders = _generate(replace(small_config, orders=small_config.orders + 100))
    for dataset in ("categories", "suppliers", "stores", "products", "customers"):
        assert base.rows[dataset] == more_orders.rows[dataset], dataset
