"""Landing discovery, deterministic identity and file-level problems (pure Python)."""

import gzip
from pathlib import Path
from typing import Any

from iot_lakehouse.bronze.landing import (
    CORRUPT_GZIP,
    EMPTY_FILE,
    MALFORMED_RECORDS,
    SCHEMA_DRIFT,
    discover,
    file_id_of,
    inspect,
)

Kit = Any  # tests/conftest.py::LandingKit


def test_files_are_found_with_their_batch_and_a_content_identity(
    tmp_path: Path, landing_kit: Kit
) -> None:
    landing_kit.write_readings(tmp_path, "b1", "2025-01-03", "part-0", [landing_kit.reading("R1")])
    landing_kit.write_readings(tmp_path, "b2", "2025-01-04", "part-0", [landing_kit.reading("R2")])
    (tmp_path / "historian/sensor_readings/batch_id=b1/_SUCCESS").write_text("")

    files = discover(tmp_path, "sensor_readings")

    assert [(f.relative_path, f.batch_id) for f in files] == [
        ("historian/sensor_readings/batch_id=b1/ingestion_date=2025-01-03/part-0.json.gz", "b1"),
        ("historian/sensor_readings/batch_id=b2/ingestion_date=2025-01-04/part-0.json.gz", "b2"),
    ]
    assert all(len(f.file_id) == 64 for f in files)


def test_the_identity_follows_the_bytes_not_the_name(tmp_path: Path, landing_kit: Kit) -> None:
    write, reading = landing_kit.write_readings, landing_kit.reading
    first = write(tmp_path, "b1", "2025-01-03", "a", [reading("R1")])
    twin = write(tmp_path, "b1", "2025-01-03", "b", [reading("R1")])
    other = write(tmp_path, "b1", "2025-01-03", "c", [reading("R2")])

    assert file_id_of(first) == file_id_of(twin) != file_id_of(other)


def test_csv_batches_come_from_the_file_name(tmp_path: Path, landing_kit: Kit) -> None:
    landing_kit.write_csv(tmp_path, "equipment", "initial", [landing_kit.equipment_row])
    (tmp_path / "cmms/equipment/notes.txt").write_text("not a source file")

    [file] = discover(tmp_path, "equipment")

    assert (file.relative_path, file.batch_id) == (
        "cmms/equipment/equipment_initial.csv",
        "initial",
    )
    assert inspect(file) is None


def test_a_missing_dataset_directory_is_no_file(tmp_path: Path) -> None:
    assert discover(tmp_path, "failure_events") == []


def test_an_empty_file_is_a_file_problem(tmp_path: Path, landing_kit: Kit) -> None:
    landing_kit.readings_path(tmp_path, "b1", "2025-01-03", "empty").write_bytes(b"")

    [file] = discover(tmp_path, "sensor_readings")
    problem = inspect(file)

    assert problem is not None and problem.reason == EMPTY_FILE


def test_a_gzip_without_records_is_a_valid_empty_file(tmp_path: Path, landing_kit: Kit) -> None:
    landing_kit.write_readings(tmp_path, "b1", "2025-01-03", "none", [])

    [file] = discover(tmp_path, "sensor_readings")

    assert inspect(file) is None


def test_a_truncated_gzip_is_corrupt(tmp_path: Path, landing_kit: Kit) -> None:
    payload = gzip.compress(b'{"reading_id": "R1"}\n' * 100)
    cut = landing_kit.readings_path(tmp_path, "b1", "2025-01-03", "cut")
    cut.write_bytes(payload[: len(payload) // 2])

    [file] = discover(tmp_path, "sensor_readings")
    problem = inspect(file)

    assert problem is not None and problem.reason == CORRUPT_GZIP


def test_a_csv_header_different_from_the_contract_is_schema_drift(
    tmp_path: Path, landing_kit: Kit
) -> None:
    row = landing_kit.equipment_row
    landing_kit.write_csv(tmp_path, "equipment", "b2", [row], header=[*row, "commissioned_by"])

    [file] = discover(tmp_path, "equipment")
    problem = inspect(file)

    assert problem is not None and problem.reason == SCHEMA_DRIFT
    assert "unexpected ['commissioned_by']" in problem.detail


def test_a_csv_line_with_extra_fields_is_malformed(tmp_path: Path, landing_kit: Kit) -> None:
    """Regression: Spark's CSV reader drops the extra fields silently (even PERMISSIVE)."""
    path = landing_kit.write_csv(tmp_path, "equipment", "b1", [landing_kit.equipment_row])
    with path.open("a", encoding="utf-8") as handle:
        handle.write("EQ-2,pump,P,Acme,S1,Plant,SP,2020-01-01,10.0,EXTRA\n")

    [file] = discover(tmp_path, "equipment")
    problem = inspect(file)

    assert problem is not None and problem.reason == MALFORMED_RECORDS
    assert problem.detail == "line 3: 10 fields, expected 9"


def test_a_csv_with_an_unclosed_quote_is_malformed(tmp_path: Path, landing_kit: Kit) -> None:
    path = landing_kit.write_csv(tmp_path, "equipment", "b1", [landing_kit.equipment_row])
    with path.open("a", encoding="utf-8") as handle:
        handle.write('EQ-2,"pump,P,Acme,S1,Plant,SP,2020-01-01,10.0\n')

    [file] = discover(tmp_path, "equipment")
    problem = inspect(file)

    assert problem is not None and problem.reason == MALFORMED_RECORDS
