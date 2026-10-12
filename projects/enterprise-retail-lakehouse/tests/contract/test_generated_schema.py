"""Generated rows follow the source contracts (columns, order, types, domains)."""

import csv
import json
from pathlib import Path

import pytest

from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS, FileFormat
from retail_lakehouse.synthetic import (
    FileSink,
    GenerationConfig,
    MemorySink,
    generate_retail_dataset,
)

DATASETS = sorted(RETAIL_SOURCE_CONTRACTS)


@pytest.mark.parametrize("dataset", DATASETS)
def test_every_dataset_is_generated(small_dataset: MemorySink, dataset: str) -> None:
    assert small_dataset.rows[dataset], f"{dataset} is empty"


@pytest.mark.parametrize("dataset", DATASETS)
def test_rows_have_exactly_the_contract_columns_in_order(
    small_dataset: MemorySink, dataset: str
) -> None:
    expected = [column.name for column in RETAIL_SOURCE_CONTRACTS[dataset].columns]
    assert all(list(row) == expected for row in small_dataset.rows[dataset])


@pytest.mark.parametrize("dataset", DATASETS)
def test_clean_values_satisfy_type_nullability_and_domain(
    clean_dataset: MemorySink, dataset: str
) -> None:
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    violations = [
        (column.name, row[column.name])
        for row in clean_dataset.rows[dataset]
        for column in contract.columns
        if not column.accepts(row[column.name])
    ]
    assert not violations, f"{dataset}: {violations[:5]}"


@pytest.mark.parametrize("dataset", DATASETS)
def test_files_round_trip_with_contract_headers(
    small_config: GenerationConfig, small_dataset: MemorySink, tmp_path: Path, dataset: str
) -> None:
    with FileSink(tmp_path, config=small_config) as sink:
        generate_retail_dataset(small_config, sink)
    contract = RETAIL_SOURCE_CONTRACTS[dataset]
    path = (
        tmp_path
        / contract.source_system
        / dataset
        / f"{dataset}_{small_config.batch_id}.{contract.file_format}"
    )

    with path.open(encoding="utf-8", newline="") as handle:
        if contract.file_format is FileFormat.CSV:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == [column.name for column in contract.columns]
            rows = [
                {key: (value if value != "" else None) for key, value in row.items()}
                for row in reader
            ]
        else:
            rows = [json.loads(line) for line in handle]

    assert rows == small_dataset.rows[dataset]
