"""Raw landing zone on a POSIX path (local disk or Databricks Volume ``/Volumes/...``).

<root>/<source>/<indicator>/ingestion_date=<yyyy-mm-dd>/<run_id>-<nnnn>.json        exact bytes
<root>/<source>/<indicator>/ingestion_date=<yyyy-mm-dd>/<run_id>.observations.jsonl  text records
<root>/_manifests/<run_id>.json                                                      run manifest
"""

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from financial_lakehouse.clients.base import RawPage
from financial_lakehouse.storage.files import atomic_write
from financial_lakehouse.transformation.raw_records import RawObservation


@dataclass(frozen=True, slots=True)
class LandedFile:
    path: Path
    sha256: str
    size_bytes: int
    url: str


class LandingStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _directory(self, source: str, indicator_id: str, ingestion_date: date) -> Path:
        return self.root / source / indicator_id / f"ingestion_date={ingestion_date.isoformat()}"

    def write_page(  # noqa: PLR0913 - keyword-only description of one landed file
        self,
        *,
        run_id: str,
        ingestion_date: date,
        indicator_id: str,
        source: str,
        page_index: int,
        page: RawPage,
    ) -> LandedFile:
        """Store the response body exactly as received (never re-serialised)."""
        path = (
            self._directory(source, indicator_id, ingestion_date)
            / f"{run_id}-{page_index:04d}.json"
        )
        atomic_write(path, page.body)
        return LandedFile(path, hashlib.sha256(page.body).hexdigest(), len(page.body), page.url)

    def write_observations(
        self,
        *,
        run_id: str,
        ingestion_date: date,
        indicator_id: str,
        source: str,
        observations: Iterable[RawObservation],
    ) -> Path:
        lines = (json.dumps(asdict(o), ensure_ascii=False, sort_keys=True) for o in observations)
        path = (
            self._directory(source, indicator_id, ingestion_date) / f"{run_id}.observations.jsonl"
        )
        atomic_write(path, ("\n".join(lines) + "\n").encode("utf-8"))
        return path

    def write_metrics(self, run_id: str, metrics: dict[str, Any]) -> Path:
        path = self.root / "_metrics" / f"{run_id}.json"
        atomic_write(path, json.dumps(metrics, indent=2, sort_keys=True).encode("utf-8"))
        return path

    def write_manifest(self, run_id: str, manifest: dict[str, Any]) -> Path:
        path = self.root / "_manifests" / f"{run_id}.json"
        atomic_write(
            path, json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        )
        return path
