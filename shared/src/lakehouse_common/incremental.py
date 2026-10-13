"""Incremental discovery of new Bronze units over the Delta log, with a durable checkpoint.

A *unit* is what a Silver run processes atomically and marks in its control table: a Retail batch
(``_batch_id``), an IoT file (``_file_id``). Bronze is append-only and each commit appends whole
units, so the units of the commits after the checkpoint are exactly the new ones. They are read
from the Change Data Feed of those commits only: discovery costs the new data, not the table.

Correctness never depends on the checkpoint. The Silver control table (processed units) filters
what was already done, so when change data cannot be trusted (feed disabled, history gone, table
recreated) discovery falls back to a full scan of the table, the previous method: slower, still
exact, with the reason kept. Only a Bronze that updates or deletes rows stops the run.

The checkpoint is committed by the caller *after* the units are written and marked, and it never
moves back, so a failed run re-reads the same commits and a stale concurrent run cannot rewind it.
docs/ADR/0004-incremental-bronze-discovery.md. Nothing is cached (serverless, ADR 0001).
"""

import logging
from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Final, Protocol

from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from lakehouse_common.naming import validate_table_name

logger = logging.getLogger(__name__)

CHANGE_FEED_PROPERTY: Final = "delta.enableChangeDataFeed"
CHANGE_TYPE: Final = "_change_type"
INSERT: Final = "insert"
Unit = tuple[object, ...]


class DiscoveryError(RuntimeError):
    """Discovery cannot say which units are new."""


class ChangesUnavailableError(DiscoveryError):
    """The change data of the pending commits is gone (retention, VACUUM) or was never recorded."""


class BronzeContractError(DiscoveryError):
    """The pending commits update or delete Bronze rows: the table must be append-only."""


class DiscoveryMode(StrEnum):
    EMPTY = "empty"  # no Bronze table or no commit yet
    FIRST_RUN = "first_run"  # no checkpoint: the table VERSION AS OF its latest commit
    INCREMENTAL = "incremental"  # the Change Data Feed of the commits after the checkpoint
    FULL_SCAN_FALLBACK = "full_scan_fallback"  # change data not usable: whole table, reason kept
    REDISCOVERY = "rediscovery"  # explicit full scan requested by the caller


@dataclass(frozen=True, slots=True)
class Discovery:
    """Units found in the commits ``(after, end]`` (``after=None``: the whole table)."""

    units: tuple[Unit, ...]
    mode: DiscoveryMode
    after: int | None
    end: int | None  # the version to checkpoint once the units are done; None: nothing to read
    reason: str = ""


class DeltaLog(Protocol):
    """What discovery needs from Delta (``SparkDeltaLog``; faked in tests)."""

    def exists(self, table: str) -> bool: ...
    def latest_version(self, table: str) -> int: ...
    def change_feed_enabled(self, table: str) -> bool: ...
    def changed_units(
        self, table: str, start: int, end: int, columns: Sequence[str]
    ) -> list[Unit]: ...
    def snapshot_units(self, table: str, version: int, columns: Sequence[str]) -> list[Unit]: ...


def _full_scan_reason(
    log: DeltaLog, table: str, checkpoint: int | None, end: int, rediscover: bool
) -> tuple[DiscoveryMode, str] | None:
    """Why the whole table must be scanned; None when the change data can be read."""
    if rediscover:
        return DiscoveryMode.REDISCOVERY, ""
    if checkpoint is None:
        return DiscoveryMode.FIRST_RUN, ""
    if checkpoint > end:
        return (
            DiscoveryMode.FULL_SCAN_FALLBACK,
            f"checkpoint {checkpoint} is ahead of the latest version {end} "
            "(table recreated or restored)",
        )
    if checkpoint < end and not log.change_feed_enabled(table):
        return (
            DiscoveryMode.FULL_SCAN_FALLBACK,
            f"change data feed disabled ({CHANGE_FEED_PROPERTY}); enabling it on an existing "
            "table is an ALTER TABLE that needs authorization",
        )
    return None


def _scan(
    log: DeltaLog, table: str, end: int, columns: Sequence[str], why: tuple[DiscoveryMode, str]
) -> Discovery:
    mode, reason = why
    if reason:
        logger.warning("discovery of %s: full scan (%s)", table, reason)
    units = tuple(sorted(log.snapshot_units(table, end, columns), key=repr))
    return Discovery(units, mode, None, end, reason)


def discover(
    log: DeltaLog,
    table: str,
    *,
    columns: Sequence[str],
    checkpoint: int | None,
    rediscover: bool = False,
) -> Discovery:
    """Distinct ``columns`` of the units committed after ``checkpoint`` (see the module doc).

    The latest version is captured once: commits made during the run belong to the next one.
    Raises ``BronzeContractError`` if the pending commits update or delete rows.
    """
    if not log.exists(table):
        return Discovery((), DiscoveryMode.EMPTY, None, None)
    end = log.latest_version(table)
    if (full_scan := _full_scan_reason(log, table, checkpoint, end, rediscover)) is not None:
        return _scan(log, table, end, columns, full_scan)
    after = int(checkpoint)  # type: ignore[arg-type]  # None handled by _full_scan_reason
    if after == end:
        return Discovery((), DiscoveryMode.INCREMENTAL, after, end)
    try:
        units = log.changed_units(table, after + 1, end, columns)
    except ChangesUnavailableError as error:
        reason = f"change data unavailable: {error}"
        return _scan(log, table, end, columns, (DiscoveryMode.FULL_SCAN_FALLBACK, reason))
    logger.info(
        "discovery of %s: %d new unit(s) in versions %d..%d", table, len(units), after + 1, end
    )
    return Discovery(tuple(sorted(units, key=repr)), DiscoveryMode.INCREMENTAL, after, end)


def pending_units(
    discovered: Sequence[str],
    *,
    processed: Collection[str],
    reprocess: Sequence[str] = (),
    known: Collection[str] = (),
) -> list[str]:
    """Discovered units not processed yet, plus the ones explicitly asked to be reprocessed.

    ``known``: every unit the Bronze holds (discovered or processed); asking to reprocess an
    unknown unit is an error rather than a silent no-op.
    """
    unknown = sorted(set(reprocess) - set(known) - set(discovered))
    if unknown:
        raise ValueError(f"cannot reprocess units that Bronze does not hold: {unknown}")
    return sorted({u for u in discovered if u not in processed} | set(reprocess))


class SparkDeltaLog:
    """``DeltaLog`` over Spark SQL: ``DESCRIBE HISTORY``, ``table_changes``, ``VERSION AS OF``."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def exists(self, table: str) -> bool:
        name = validate_table_name(table)
        if not self._spark.catalog.tableExists(name):
            return False
        return self._spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first() is not None

    def latest_version(self, table: str) -> int:
        name = validate_table_name(table)
        row = self._spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").select("version").first()
        if row is None:
            raise DiscoveryError(f"{name} has no committed version")
        return int(row["version"])

    def change_feed_enabled(self, table: str) -> bool:
        name = validate_table_name(table)
        rows = self._spark.sql(f"SHOW TBLPROPERTIES {name}").collect()  # a few properties
        properties = {row["key"]: row["value"] for row in rows}
        return str(properties.get(CHANGE_FEED_PROPERTY, "false")).lower() == "true"

    def changed_units(self, table: str, start: int, end: int, columns: Sequence[str]) -> list[Unit]:
        name = validate_table_name(table)
        # name validated, versions are ints -> no injection surface
        changes = self._spark.sql(
            f"SELECT * FROM table_changes('{name}', {int(start)}, {int(end)})"  # noqa: S608
        )
        try:  # the first action forces the read: missing change data fails here
            kinds = {r[CHANGE_TYPE] for r in changes.select(CHANGE_TYPE).distinct().collect()}
        except (AnalysisException, IllegalArgumentException) as error:
            raise ChangesUnavailableError(
                f"{name} versions {start}..{end}: {str(error).splitlines()[0][:200]}"
            ) from error
        if kinds - {INSERT}:
            raise BronzeContractError(
                f"{name} versions {start}..{end} contain {sorted(kinds - {INSERT})}: Bronze is "
                "append-only. Restore the rows, or rebuild the Silver with rediscovery."
            )
        rows = changes.select(*columns).distinct().collect()  # one row per unit, never data
        return [tuple(row) for row in rows]

    def snapshot_units(self, table: str, version: int, columns: Sequence[str]) -> list[Unit]:
        name = validate_table_name(table)
        frame = self._spark.sql(f"SELECT * FROM {name} VERSION AS OF {int(version)}")  # noqa: S608
        return [tuple(row) for row in frame.select(*columns).distinct().collect()]


# --- checkpoint ------------------------------------------------------------------------------

CHECKPOINT_SCHEMA: Final = StructType(
    [
        StructField("pipeline_name", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("last_version", LongType(), False),
        StructField("discovery_mode", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("committed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class Checkpoint:
    pipeline: str
    source: str
    version: int
    mode: DiscoveryMode
    run_id: str
    committed_at: datetime


class CheckpointStore(Protocol):
    def read(self, pipeline: str, source: str) -> int | None: ...
    def commit(self, checkpoint: Checkpoint) -> None: ...


@dataclass
class InMemoryCheckpointStore:
    """Tests: the monotonic rule of ``DeltaCheckpointStore``; ``history`` = accepted commits."""

    history: list[Checkpoint] = field(default_factory=list)

    def read(self, pipeline: str, source: str) -> int | None:
        key = (pipeline, source)
        versions = [c.version for c in self.history if (c.pipeline, c.source) == key]
        return max(versions) if versions else None

    def commit(self, checkpoint: Checkpoint) -> None:
        current = self.read(checkpoint.pipeline, checkpoint.source)
        if current is None or checkpoint.version >= current:
            self.history.append(checkpoint)


def _ddl(schema: StructType) -> str:
    return ", ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )


class DeltaCheckpointStore:
    """One row per (pipeline, source table) in a Delta table; its history is the audit trail."""

    def __init__(self, spark: SparkSession, table: str) -> None:
        self._spark = spark
        self.table = validate_table_name(table)

    def read(self, pipeline: str, source: str) -> int | None:
        if not self._spark.catalog.tableExists(self.table):
            return None
        row = (
            self._spark.table(self.table)
            .where((F.col("pipeline_name") == pipeline) & (F.col("source_table") == source))
            .agg(F.max("last_version").alias("v"))
            .first()
        )
        return None if row is None or row["v"] is None else int(row["v"])

    def commit(self, checkpoint: Checkpoint) -> None:
        """Record ``version`` as consumed. Never moves back (a stale run cannot rewind it)."""
        self._spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table} ({_ddl(CHECKPOINT_SCHEMA)}) USING DELTA"
        )
        row = (checkpoint.pipeline, checkpoint.source, checkpoint.version, checkpoint.mode.value,
               checkpoint.run_id, checkpoint.committed_at)  # fmt: skip
        view = "_discovery_checkpoint_incoming"
        self._spark.createDataFrame([row], CHECKPOINT_SCHEMA).createOrReplaceTempView(view)
        try:
            self._spark.sql(
                f"MERGE INTO {self.table} AS t USING {view} AS s "
                "ON t.pipeline_name = s.pipeline_name AND t.source_table = s.source_table "
                "WHEN MATCHED AND s.last_version >= t.last_version THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            )
        finally:
            self._spark.catalog.dropTempView(view)
