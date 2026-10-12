"""IoT Bronze run (observed): every dataset, one ``pipeline_runs`` row each.

Per dataset: ``ingest_dataset`` appends the readable new files; then the checks are recorded
and, last, the file decisions (``bronze_files``). Checks:

- ``file_problems`` (ERROR): files quarantined by this run, by reason. The decision is recorded,
  so a quarantined file is reported once (a corrected re-delivery has a new ``file_id``);
- ``reconciliation`` (CRITICAL): rows of the appended files = rows of the Delta commit.

The append happens before the checks (Delta has no multi-table transaction). A CRITICAL
reconciliation leaves the decisions unrecorded: the next run finds the rows by ``_file_id``,
records them as ingested and never writes them twice (``ingest.py``, crash repair).
"""

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

from pyspark.sql import SparkSession

from iot_lakehouse import PROJECT
from iot_lakehouse.bronze.ingest import (
    BronzeTarget,
    DatasetOutcome,
    ingest_dataset,
    record_decisions,
    utc,
)
from lakehouse_common.config import load_environment
from lakehouse_common.observability import Layer, ObservabilitySink, track_run
from lakehouse_common.quality import (
    CheckResult,
    Dimension,
    Reconciliation,
    Severity,
    Status,
    failure_rate_of,
)

PIPELINE_NAME: Final = "iot_bronze"
# Master data first: a reader of the Bronze finds equipment and sensors before their readings.
DATASET_ORDER: Final = (
    "equipment", "sensors", "failure_events", "maintenance_orders", "sensor_readings",
)  # fmt: skip
FILE_CHECK: Final = "file_problems"
MAX_DETAIL: Final = 1000


def file_check(outcome: DatasetOutcome, at: datetime) -> CheckResult:
    """ERROR when this run quarantined files; ``details`` lists reason counts and the files."""
    new_files = len(outcome.decisions)
    quarantined = outcome.quarantined
    reasons = Counter(d.reason for d in quarantined)
    details = "; ".join(f"{reason}={n}" for reason, n in sorted(reasons.items()))
    files = ", ".join(d.file.relative_path for d in quarantined)
    return CheckResult(
        check_name=FILE_CHECK,
        check_type="file_ingestion",
        table_name=outcome.table,
        column_name=None,
        dimension=Dimension.VALIDITY,
        status=Status.ERROR if quarantined else Status.PASS,
        severity=Severity.ERROR,
        rows_checked=new_files,
        failed_rows=len(quarantined),
        failure_rate=failure_rate_of(failed_rows=len(quarantined), rows_checked=new_files),
        execution_timestamp=at,
        details=f"{details} ({files})"[:MAX_DETAIL] if quarantined else None,
    )


def run_bronze(  # noqa: PLR0913 - keyword-only options of one run
    spark: SparkSession,
    *,
    landing_root: Path,
    target: BronzeTarget,
    ingestion_timestamp: datetime,
    sink: ObservabilitySink,
    datasets: Sequence[str] = DATASET_ORDER,
) -> list[DatasetOutcome]:
    """Ingest every new file of each dataset. A FAILED dataset stops the run; a rerun resumes
    from the files not yet decided (nothing already appended is appended again)."""
    at = utc(ingestion_timestamp)
    run_id = f"{at:%Y%m%dT%H%M%SZ}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.namespace}")
    outcomes = []
    for dataset in datasets:
        with track_run(
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            project=PROJECT,
            layer=Layer.BRONZE,
            table=target.table(dataset),
            sink=sink,
        ) as step:
            outcome = ingest_dataset(
                spark, dataset, landing_root=landing_root, target=target, at=at
            )
            step.record_counts(
                input_rows=outcome.rows_read,
                output_rows=outcome.rows_written,
                rejected_rows=outcome.rows_read
                - outcome.rows_expected,  # rows of quarantined files
            )
            balance = Reconciliation(
                outcome.rows_expected,
                {"written": outcome.rows_written},
                subject="rows of the appended files",
            )
            step.record_quality(
                [
                    file_check(outcome, at),
                    balance.to_check_result(table_name=outcome.table, execution_timestamp=at),
                ]
            )
            record_decisions(spark, target, outcome.decisions, run_id=run_id, at=at)
        outcomes.append(outcome)
    return outcomes


@dataclass(frozen=True, slots=True)
class BronzeSettings:
    landing_root: Path
    target: BronzeTarget


def load_bronze_settings(path: Path, environment: str) -> BronzeSettings:
    """Landing root and Bronze namespace of ``environment`` (``config/bronze.toml``)."""
    section = load_environment(path, environment)
    return BronzeSettings(
        Path(str(section["landing_root"])), BronzeTarget(str(section["target_namespace"]))
    )
