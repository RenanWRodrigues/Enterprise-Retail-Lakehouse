"""Silver progress over the Bronze Delta log: a durable checkpoint of committed Bronze versions.

Each run (docs/INCREMENTAL_STRATEGY.md, "Silver: progresso por versão Delta"):

1. reads the checkpoint: the last Bronze version whose changes were published *and* reconciled;
2. captures the latest Bronze version once (``end``): commits after it belong to the next run;
3. reads the changes of the versions ``(checkpoint, end]`` with Change Data Feed (first run: the
   whole table ``VERSION AS OF end``);
4. after the Silver, the quarantine and the reconciliation are written, commits ``end``.

The Bronze contract is append-only (insert-only MERGE of new versions), so its feed holds only
``insert`` rows. Any ``update``/``delete`` breaks the contract and stops the run: the Silver
cannot tell which observation should disappear.

Failures carry their recovery procedure in the message (``ProgressError``). Nothing here is
cached (serverless, docs/ADR/0001-spark-without-cache.md); reads are pinned to Delta versions.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from financial_lakehouse.silver.schemas import BRONZE_SCHEMA
from lakehouse_common.naming import validate_table_name

CHECKPOINT_TABLE: Final = "silver_checkpoints"
CHECKPOINT_SCHEMA: Final = StructType(
    [
        StructField("pipeline_name", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("last_version", LongType(), False),
        StructField("run_id", StringType(), False),
        StructField("committed_at", TimestampType(), False),
    ]
)
CHANGE_FEED_PROPERTY: Final = "delta.enableChangeDataFeed"
CHANGE_TYPE: Final = "_change_type"
INSERT: Final = "insert"
_FEED_COLUMNS: Final = (CHANGE_TYPE, "_commit_version", "_commit_timestamp")

RESET_PROCEDURE: Final = (
    "Recovery: (1) confirm the Bronze table is the expected one and still append-only; "
    "(2) delete this pipeline's row from {checkpoint} for source {source}; "
    "(3) rerun: with no checkpoint the run reads the whole Bronze VERSION AS OF its latest "
    "commit, and the idempotent MERGEs keep the Silver and its quarantine unchanged for "
    "rows they already hold (docs/INCREMENTAL_STRATEGY.md)."
)


class ProgressError(RuntimeError):
    """The run cannot know which Bronze commits are new; it stops before reading data."""


class CheckpointAheadError(ProgressError):
    """The checkpoint is after the latest Bronze version (table recreated or restored)."""


class ChangeFeedDisabledError(ProgressError):
    """The Bronze table does not record its changes (``delta.enableChangeDataFeed``)."""


class ChangesUnavailableError(ProgressError):
    """The change data of the pending versions is gone (retention, VACUUM) or never existed."""


class BronzeContractError(ProgressError):
    """The pending versions update or delete Bronze rows: the table must be append-only."""


@dataclass(frozen=True, slots=True)
class CommitRange:
    """Bronze versions to consume: ``(after, end]``; ``after=None`` means the whole table."""

    after: int | None
    end: int

    @property
    def is_first_run(self) -> bool:
        return self.after is None

    @property
    def is_empty(self) -> bool:
        return self.after == self.end


def plan_range(
    checkpoint: int | None, latest: int, *, source: str, checkpoint_table: str
) -> CommitRange:
    """The versions not yet committed; ``CheckpointAheadError`` if the checkpoint is impossible."""
    if checkpoint is not None and checkpoint > latest:
        raise CheckpointAheadError(
            f"checkpoint version {checkpoint} is after the latest version {latest} of {source}. "
            + RESET_PROCEDURE.format(checkpoint=checkpoint_table, source=source)
        )
    return CommitRange(checkpoint, latest)


def latest_version(spark: SparkSession, table: str) -> int:
    """The latest committed version of a Delta table (captured once per run)."""
    name = validate_table_name(table)
    row = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").select("version").first()
    if row is None:
        raise ProgressError(f"{name} has no committed version")
    return int(row["version"])


def snapshot(spark: SparkSession, table: str, version: int) -> DataFrame:
    """The table ``VERSION AS OF version``: every action of the run sees the same rows."""
    name = validate_table_name(table)
    # name: validate_table_name; version: int -> no injection surface
    return spark.sql(f"SELECT * FROM {name} VERSION AS OF {int(version)}")  # noqa: S608


def change_feed_enabled(spark: SparkSession, table: str) -> bool:
    name = validate_table_name(table)
    properties = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()}
    return str(properties.get(CHANGE_FEED_PROPERTY, "false")).lower() == "true"


def _change_types(changes: DataFrame) -> set[str]:
    """Distinct change types of the range (at most 4 values: a bounded collect)."""
    return {row[CHANGE_TYPE] for row in changes.select(CHANGE_TYPE).distinct().collect()}


def changed_rows(
    spark: SparkSession, table: str, commits: CommitRange, *, checkpoint_table: str
) -> DataFrame:
    """Bronze rows written by the versions of ``commits`` (``BRONZE_SCHEMA`` columns).

    First run: the whole table ``VERSION AS OF end``. Otherwise the Change Data Feed of
    ``(after, end]``, which must hold only inserts (append-only Bronze).
    """
    name = validate_table_name(table)
    procedure = RESET_PROCEDURE.format(checkpoint=checkpoint_table, source=name)
    if commits.is_first_run:
        return snapshot(spark, name, commits.end).select(*BRONZE_SCHEMA.fieldNames())
    if not change_feed_enabled(spark, name):
        raise ChangeFeedDisabledError(
            f"{name} has no Change Data Feed ({CHANGE_FEED_PROPERTY}). Enabling it is a table "
            f"change: run ALTER TABLE {name} SET TBLPROPERTIES ({CHANGE_FEED_PROPERTY} = true) "
            "with authorization, then " + procedure
        )
    start = int(commits.after) + 1  # type: ignore[arg-type]  # not a first run
    try:
        # name validated, versions are ints -> no injection surface
        changes = spark.sql(f"SELECT * FROM table_changes('{name}', {start}, {commits.end})")  # noqa: S608
        kinds = _change_types(changes)  # forces the read: unavailable data fails here
    except (AnalysisException, IllegalArgumentException) as error:
        raise ChangesUnavailableError(
            f"change data of {name} versions {start}..{commits.end} is not available "
            f"({str(error).splitlines()[0][:200]}). " + procedure
        ) from error
    if kinds - {INSERT}:
        raise BronzeContractError(
            f"{name} versions {start}..{commits.end} contain {sorted(kinds - {INSERT})}: Bronze "
            "is append-only (new versions are inserted, never updated or deleted). Restore the "
            "deleted/updated rows or, if the change is intended, rebuild the Silver: " + procedure
        )
    return changes.drop(*_FEED_COLUMNS).select(*BRONZE_SCHEMA.fieldNames())


def read_checkpoint(spark: SparkSession, table: str, *, pipeline: str, source: str) -> int | None:
    """The last committed Bronze version of ``pipeline`` for ``source``; ``None`` if none."""
    name = validate_table_name(table)
    if not spark.catalog.tableExists(name):
        return None
    row = (
        spark.table(name)
        .where((F.col("pipeline_name") == pipeline) & (F.col("source_table") == source))
        .agg(F.max("last_version").alias("v"))
        .first()
    )
    return None if row is None or row["v"] is None else int(row["v"])


def _ddl(schema: StructType) -> str:
    return ", ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )


@dataclass(frozen=True, slots=True)
class Checkpoint:
    pipeline: str
    source: str
    version: int
    run_id: str
    committed_at: datetime


def commit_checkpoint(spark: SparkSession, table: str, checkpoint: Checkpoint) -> None:
    """Record ``version`` as consumed. Never moves back (a stale retry cannot rewind it)."""
    name = validate_table_name(table)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({_ddl(CHECKPOINT_SCHEMA)}) USING DELTA")
    row = (checkpoint.pipeline, checkpoint.source, checkpoint.version, checkpoint.run_id,
           checkpoint.committed_at)  # fmt: skip
    spark.createDataFrame([row], CHECKPOINT_SCHEMA).createOrReplaceTempView("_checkpoint_incoming")
    spark.sql(
        f"MERGE INTO {name} AS t USING _checkpoint_incoming AS s "
        "ON t.pipeline_name = s.pipeline_name AND t.source_table = s.source_table "
        "WHEN MATCHED AND s.last_version >= t.last_version THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
