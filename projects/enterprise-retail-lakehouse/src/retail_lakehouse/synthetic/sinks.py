"""Where generated rows go: memory (tests) or landing-zone files (CSV / JSON Lines)."""

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Final, Protocol, Self

from retail_lakehouse.ingestion.source_contracts import RETAIL_SOURCE_CONTRACTS, FileFormat
from retail_lakehouse.synthetic.anomalies import AnomalyRecord
from retail_lakehouse.synthetic.config import GenerationConfig

GENERATOR_VERSION: Final = "1.0.0"
MANIFEST_FILE: Final = "_manifest_{batch_id}.json"  # one per batch: batches share a landing root
ANOMALIES_FILE: Final = "_anomalies_{batch_id}.csv"
ANOMALY_COLUMNS: Final = tuple(AnomalyRecord.__dataclass_fields__)


class Sink(Protocol):
    def write(self, dataset: str, row: Mapping[str, str | None]) -> None: ...
    def record_anomaly(self, anomaly: AnomalyRecord) -> None: ...


class MemorySink:
    """Keeps everything in memory. Intended for small configurations (tests)."""

    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, str | None]]] = {
            name: [] for name in RETAIL_SOURCE_CONTRACTS
        }
        self.anomalies: list[AnomalyRecord] = []

    def write(self, dataset: str, row: Mapping[str, str | None]) -> None:
        self.rows[dataset].append(dict(row))

    def record_anomaly(self, anomaly: AnomalyRecord) -> None:
        self.anomalies.append(anomaly)


def _config_as_json(config: GenerationConfig) -> dict[str, Any]:
    data = asdict(config)
    data["start_date"] = config.start_date.isoformat()
    data["end_date"] = config.end_date.isoformat()
    data["anomalies"] = [
        {"type": r.anomaly_type.value, "dataset": r.dataset, "column": r.column, "rate": r.rate}
        for r in config.anomalies
    ]
    return data


class FileSink:
    """Writes the landing zone: one file per dataset, the anomaly registry and a manifest.

    Output is byte-for-byte reproducible: no timestamps, fixed line endings, stable order.
    Use as a context manager; the manifest is written only when generation succeeds.
    """

    def __init__(self, root: Path, *, config: GenerationConfig) -> None:
        self._root = root
        self._config = config
        self._handles: dict[str, IO[str]] = {}
        self._writers: dict[str, csv.DictWriter[str]] = {}
        self._row_counts: Counter[str] = Counter()
        self._anomaly_counts: Counter[str] = Counter()
        self._anomalies_file = ANOMALIES_FILE.format(batch_id=config.batch_id)
        self._manifest_file = MANIFEST_FILE.format(batch_id=config.batch_id)

    def __enter__(self) -> Self:
        self._root.mkdir(parents=True, exist_ok=True)
        handle = (self._root / self._anomalies_file).open("w", encoding="utf-8", newline="")
        self._handles[self._anomalies_file] = handle
        writer = csv.DictWriter(handle, fieldnames=ANOMALY_COLUMNS, lineterminator="\n")
        writer.writeheader()
        self._writers[self._anomalies_file] = writer
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        for dataset in RETAIL_SOURCE_CONTRACTS:  # empty datasets still get a file
            self._open(dataset)
        for handle in self._handles.values():
            handle.close()
        if exc_type is None:
            self._write_manifest()

    def relative_path(self, dataset: str) -> Path:
        contract = RETAIL_SOURCE_CONTRACTS[dataset]
        filename = f"{dataset}_{self._config.batch_id}.{contract.file_format}"
        return Path(contract.source_system) / dataset / filename

    def _open(self, dataset: str) -> IO[str]:
        if dataset in self._handles:
            return self._handles[dataset]
        path = self._root / self.relative_path(dataset)
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("w", encoding="utf-8", newline="")
        self._handles[dataset] = handle
        contract = RETAIL_SOURCE_CONTRACTS[dataset]
        if contract.file_format is FileFormat.CSV:
            writer = csv.DictWriter(handle, fieldnames=contract.column_names, lineterminator="\n")
            writer.writeheader()
            self._writers[dataset] = writer
        return handle

    def write(self, dataset: str, row: Mapping[str, str | None]) -> None:
        handle = self._open(dataset)
        if dataset in self._writers:
            self._writers[dataset].writerow(row)
        else:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        self._row_counts[dataset] += 1

    def record_anomaly(self, anomaly: AnomalyRecord) -> None:
        self._writers[self._anomalies_file].writerow(asdict(anomaly))
        self._anomaly_counts[anomaly.anomaly_type.value] += 1

    def _write_manifest(self) -> None:
        files = [
            self._anomalies_file,
            *(self.relative_path(d).as_posix() for d in RETAIL_SOURCE_CONTRACTS),
        ]
        manifest = {
            "generator": "retail_lakehouse.synthetic",
            "generator_version": GENERATOR_VERSION,
            "config": _config_as_json(self._config),
            "row_counts": {d: self._row_counts[d] for d in RETAIL_SOURCE_CONTRACTS},
            "anomaly_counts": dict(sorted(self._anomaly_counts.items())),
            "files_sha256": {
                name: hashlib.sha256((self._root / name).read_bytes()).hexdigest() for name in files
            },
        }
        (self._root / self._manifest_file).write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )
