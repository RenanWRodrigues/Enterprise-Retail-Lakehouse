"""Logical checkpoint per indicator: last reference committed to Bronze, run and time.

Stored as JSON under the landing root (``_state/watermarks.json``), written atomically and
advanced only after Bronze accepted the indicator's records: a failed run never moves it.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from financial_lakehouse.storage.files import atomic_write_text


class WatermarkStore:
    def __init__(self, root: Path) -> None:
        self._path = root / "_state" / "watermarks.json"

    def _load(self) -> dict[str, dict[str, Any]]:
        if not self._path.is_file():
            return {}
        data = json.loads(self._path.read_text(encoding="utf-8"))
        return {k: v if isinstance(v, dict) else {"reference": v} for k, v in data.items()}

    def get(self, indicator_id: str) -> str | None:
        checkpoint = self._load().get(indicator_id)
        return str(checkpoint["reference"]) if checkpoint else None

    def checkpoint(self, indicator_id: str) -> dict[str, Any] | None:
        return self._load().get(indicator_id)

    def set(
        self,
        indicator_id: str,
        reference: str,
        *,
        run_id: str | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        entry = {
            "reference": reference,
            "run_id": run_id,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }
        data = self._load() | {indicator_id: entry}
        atomic_write_text(self._path, json.dumps(data, indent=2, sort_keys=True))
