"""Bronze sinks: insert-only MERGE of new *versions* (idempotent; revisions appended).

Every sink applies the same rule (``transformation.bronze_versions.new_versions``): an incoming
observation is stored only when it differs from the latest stored delivery of its version key, so
a rerun inserts nothing and a value that returns after a revision (A -> B -> A) is a new version.

- ``InMemoryBronzeSink``: tests.
- ``LocalBronzeSink``: JSON Lines under a directory (local runs, Windows included).
- ``DeltaBronzeSink``: Delta table via ``MERGE ... WHEN NOT MATCHED INSERT`` (Databricks/Linux).
"""

import json
import re
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol

from financial_lakehouse.storage.files import atomic_write_text
from financial_lakehouse.transformation.bronze_records import BronzeRecord
from financial_lakehouse.transformation.bronze_versions import latest_deliveries, new_versions
from lakehouse_common.naming import validate_table_name

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

BRONZE_TABLE: Final = "bronze_indicator_observations"
BRONZE_PROPERTIES: Final = "delta.enableChangeDataFeed = true"  # read by the Silver


class BronzeSink(Protocol):
    @property
    def name(self) -> str:
        """Where the rows go (``table`` of ``pipeline_runs``)."""
        ...

    def merge(self, records: Sequence[BronzeRecord]) -> int:
        """Insert the records that are new versions; return how many were inserted."""
        ...

    def count(self) -> int: ...

    def records(self) -> list[BronzeRecord]: ...


class InMemoryBronzeSink:
    def __init__(self) -> None:
        self._rows: dict[str, BronzeRecord] = {}

    @property
    def name(self) -> str:
        return f"memory:{BRONZE_TABLE}"

    def merge(self, records: Sequence[BronzeRecord]) -> int:
        new = new_versions(records, latest_deliveries(self._rows.values()))
        self._rows.update((record.record_key, record) for record in new)
        return len(new)

    def count(self) -> int:
        return len(self._rows)

    def records(self) -> list[BronzeRecord]:
        return list(self._rows.values())


def _to_json(record: BronzeRecord) -> dict[str, Any]:
    row: dict[str, Any] = {name: getattr(record, name) for name in record.__dataclass_fields__}
    for name, value in row.items():
        if isinstance(value, date | datetime):
            row[name] = value.isoformat()
    return row


def _from_json(row: dict[str, Any]) -> BronzeRecord:
    values = dict(row)
    values["reference_date"] = date.fromisoformat(values["reference_date"])
    for name in ("request_timestamp", "ingestion_timestamp"):
        values[name] = datetime.fromisoformat(values[name]) if values[name] else None
    return BronzeRecord(**values)


class LocalBronzeSink:
    """``<root>/<source>/<series>/part-<run_id>-<n>.jsonl``; versions re-read on every merge."""

    def __init__(self, root: Path) -> None:
        self._root = root

    @property
    def name(self) -> str:
        return self._root.as_posix()

    def records(self) -> list[BronzeRecord]:
        rows: list[BronzeRecord] = []
        for path in sorted(self._root.rglob("part-*.jsonl")):
            rows.extend(
                _from_json(json.loads(line))
                for line in path.read_text(encoding="utf-8").splitlines()
                if line
            )
        return rows

    def count(self) -> int:
        return len(self.records())

    def merge(self, records: Sequence[BronzeRecord]) -> int:
        new = new_versions(records, latest_deliveries(self.records()))
        if not new:
            return 0
        first = new[0]
        directory = self._root / first.source / re.sub(r"[^\w.-]", "_", first.series_id)
        directory.mkdir(parents=True, exist_ok=True)
        index = len(list(directory.glob(f"part-{first.run_id}-*.jsonl")))
        path = directory / f"part-{first.run_id}-{index:04d}.jsonl"
        lines = (json.dumps(_to_json(r), ensure_ascii=False, sort_keys=True) for r in new)
        atomic_write_text(path, "\n".join(lines) + "\n")  # a crash never leaves half a batch
        return len(new)


BRONZE_DDL: Final = (
    "record_key STRING NOT NULL, source STRING NOT NULL, series_id STRING NOT NULL, "
    "indicator_id STRING NOT NULL, reference STRING NOT NULL, reference_date DATE NOT NULL, "
    "value STRING, attributes STRING, request_timestamp TIMESTAMP, "
    "ingestion_timestamp TIMESTAMP NOT NULL, run_id STRING NOT NULL, "
    "raw_file STRING, raw_sha256 STRING"
)


VERSION_COLUMNS: Final = ("source", "series_id", "reference")


def _from_row(row: dict[str, Any]) -> BronzeRecord:
    values = dict(row)
    values["attributes"] = json.loads(values["attributes"]) if values["attributes"] else {}
    return BronzeRecord(**values)


class DeltaBronzeSink:
    """Delta table; created on first write. Insert-only MERGE of new versions on ``record_key``."""

    def __init__(self, spark: "SparkSession", table: str) -> None:
        self._spark = spark
        self._table = validate_table_name(table)

    @property
    def name(self) -> str:
        return self._table

    def _frame(self, records: Sequence[BronzeRecord]) -> Any:  # noqa: ANN401 - Spark DataFrame
        rows = [
            {
                **_to_json(r),
                "reference_date": r.reference_date,
                "request_timestamp": r.request_timestamp,
                "ingestion_timestamp": r.ingestion_timestamp,
                "attributes": json.dumps(r.attributes, sort_keys=True),
            }
            for r in records
        ]
        return self._spark.createDataFrame(rows, BRONZE_DDL)

    def _stored_versions(self, incoming: Any) -> list[BronzeRecord]:  # noqa: ANN401 - DataFrame
        """Stored rows of the incoming (source, series, reference) keys only.

        ``collect`` is bounded by the batch: one indicator's request window times its revisions
        (empty on a first backfill), never the table.
        """
        keys = incoming.select(*VERSION_COLUMNS).distinct()
        stored = self._spark.table(self._table).join(keys, list(VERSION_COLUMNS), "left_semi")
        return [_from_row(row.asDict()) for row in stored.collect()]

    def merge(self, records: Sequence[BronzeRecord]) -> int:
        if not records:
            return 0
        # Change Data Feed: the Silver consumes Bronze commits by version (silver.progress).
        self._spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self._table} ({BRONZE_DDL}) USING DELTA "
            f"TBLPROPERTIES ({BRONZE_PROPERTIES})"
        )
        stored = self._stored_versions(self._frame(records))
        new = new_versions(records, latest_deliveries(stored))
        if not new:
            return 0
        self._frame(new).createOrReplaceTempView("_bronze_incoming")
        self._spark.sql(  # the key match is a safety net: new versions have unseen keys
            f"MERGE INTO {self._table} AS t USING _bronze_incoming AS s "
            "ON t.record_key = s.record_key WHEN NOT MATCHED THEN INSERT *"
        )
        metrics = self._spark.sql(f"DESCRIBE HISTORY {self._table} LIMIT 1").first()
        operation = (metrics["operationMetrics"] if metrics else None) or {}
        return int(operation.get("numTargetRowsInserted", 0))

    def count(self) -> int:
        return self._spark.table(self._table).count()

    def records(self) -> list[BronzeRecord]:
        raise NotImplementedError("read Bronze with Spark: spark.table(...)")
