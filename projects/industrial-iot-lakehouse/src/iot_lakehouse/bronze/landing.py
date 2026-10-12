"""Landing-zone files of the IoT sources: discovery, deterministic identity, file-level checks.

Layout written by ``iot_lakehouse.synthetic.writer`` (docs/DATA_CONTRACTS.md):

    <root>/cmms/<dataset>/<dataset>_<batch>.csv
    <root>/historian/sensor_readings/batch_id=<batch>/ingestion_date=<date>/<part>.json.gz

Identity: a file is its landing-relative path plus the SHA-256 of its bytes (``file_id``). The
same bytes always give the same id; a rewritten file gets a new one. Pure Python (POSIX paths,
local disk or ``/Volumes``): the bytes are streamed in chunks, never loaded at once.

File-level problems (the whole file is quarantined, never partially loaded):
``empty_file``, ``corrupt_gzip``, ``not_utf8``, ``schema_drift`` (CSV header) and
``malformed_records`` (CSV lines). Malformed JSON lines and JSON key drift need Spark:
``bronze.readers``.
"""

import csv
import gzip
import hashlib
import re
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from iot_lakehouse.ingestion.source_contracts import (
    IOT_SOURCE_CONTRACTS,
    DatasetContract,
    FileFormat,
)

CHUNK_BYTES: Final = 1 << 20
EMPTY_FILE, CORRUPT_GZIP, NOT_UTF8 = "empty_file", "corrupt_gzip", "not_utf8"
SCHEMA_DRIFT, MALFORMED_RECORDS = "schema_drift", "malformed_records"
CHANGED_AFTER_INGESTION: Final = "changed_after_ingestion"
_CSV_NAME: Final = re.compile(r"^(?P<dataset>[a-z_]+)_(?P<batch>[A-Za-z0-9_-]+)\.csv$")
_BATCH_DIR: Final = re.compile(r"^batch_id=(?P<batch>[A-Za-z0-9_-]+)$")
_PARTITION_DIR: Final = re.compile(r"^ingestion_date=\d{4}-\d{2}-\d{2}$")
READINGS_DEPTH: Final = 3  # batch_id=<b>/ingestion_date=<d>/<file>.json.gz


@dataclass(frozen=True, slots=True)
class LandingFile:
    dataset: str
    path: Path
    relative_path: str  # from the landing root, POSIX separators: the stable name
    batch_id: str
    file_id: str  # SHA-256 of the bytes
    size_bytes: int


@dataclass(frozen=True, slots=True)
class FileProblem:
    file: LandingFile
    reason: str
    detail: str


def _chunks(handle: object) -> Iterator[bytes]:
    while chunk := handle.read(CHUNK_BYTES):  # type: ignore[attr-defined]
        yield chunk


def file_id_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in _chunks(handle):
            digest.update(chunk)
    return digest.hexdigest()


def _batch_of(contract: DatasetContract, relative: Path) -> str | None:
    """The batch id encoded in the layout, or ``None`` if the path is not a source file."""
    if contract.file_format is FileFormat.CSV:
        match = _CSV_NAME.match(relative.name)
        return match["batch"] if match and match["dataset"] == contract.name else None
    if len(relative.parts) != READINGS_DEPTH or not relative.name.endswith(
        f".{FileFormat.JSONL_GZIP}"
    ):
        return None
    batch_dir, partition_dir, _ = relative.parts
    batch = _BATCH_DIR.match(batch_dir)
    return batch["batch"] if batch and _PARTITION_DIR.match(partition_dir) else None


def discover(root: Path, dataset: str) -> list[LandingFile]:
    """Every source file of ``dataset`` under ``root``, sorted by path (deterministic order).

    Hidden and temporary files (``_*``, ``.*``, e.g. Spark's ``_SUCCESS`` and ``.crc``) are not
    source files.
    """
    contract = IOT_SOURCE_CONTRACTS[dataset]
    directory = root / str(contract.source_system) / dataset
    if not directory.is_dir():
        return []
    files = []
    for path in sorted(p for p in directory.rglob("*") if p.is_file()):
        if path.name.startswith(("_", ".")):
            continue
        relative = path.relative_to(root)
        batch = _batch_of(contract, path.relative_to(directory))
        if batch is not None:
            files.append(
                LandingFile(
                    dataset,
                    path,
                    relative.as_posix(),
                    batch,
                    file_id_of(path),
                    path.stat().st_size,
                )
            )
    return files


def _gzip_problem(path: Path) -> tuple[str, str] | None:
    try:
        with gzip.open(path, "rb") as handle:
            for _ in _chunks(handle):
                pass
    except (OSError, EOFError, zlib.error) as error:  # BadGzipFile is an OSError
        return CORRUPT_GZIP, f"{type(error).__name__}: {error}"
    return None


def _header_drift(header: list[str], contract: DatasetContract) -> tuple[str, str] | None:
    if tuple(header) == contract.column_names:
        return None
    missing = sorted(set(contract.column_names) - set(header))
    unexpected = sorted(set(header) - set(contract.column_names))
    detail = f"header {header}; missing {missing}; unexpected {unexpected}"
    return SCHEMA_DRIFT, detail if missing or unexpected else f"{detail}; order differs"


def _csv_problem(path: Path, contract: DatasetContract) -> tuple[str, str] | None:
    """Header = contract, and every line has exactly the header's fields (strict quoting).

    Checked here, streaming, because Spark's CSV reader silently drops extra fields of a line
    even in PERMISSIVE mode: Bronze would lose data without any error.
    """
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            rows = csv.reader(handle, strict=True)
            header = next(rows, [])
            if drift := _header_drift(header, contract):
                return drift
            for line, row in enumerate(rows, start=2):
                if len(row) != len(header):
                    return (
                        MALFORMED_RECORDS,
                        f"line {line}: {len(row)} fields, expected {len(header)}",
                    )
    except UnicodeDecodeError as error:
        return NOT_UTF8, str(error)
    except csv.Error as error:
        return MALFORMED_RECORDS, f"line {rows.line_num}: {error}"
    return None


def inspect(file: LandingFile) -> FileProblem | None:
    """File-level problems detectable without Spark; ``None`` when the file can be read."""
    contract = IOT_SOURCE_CONTRACTS[file.dataset]
    if file.size_bytes == 0:
        return FileProblem(file, EMPTY_FILE, "0 bytes")
    if contract.file_format is FileFormat.JSONL_GZIP:
        problem = _gzip_problem(file.path)
    else:
        problem = _csv_problem(file.path, contract)
    return FileProblem(file, *problem) if problem else None
