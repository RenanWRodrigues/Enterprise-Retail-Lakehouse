"""Incremental ingestion: official APIs -> raw landing -> Bronze, with a logical checkpoint.

Per indicator (see docs/INCREMENTAL_STRATEGY.md):
1. range = checkpoint - lookback (or ``backfill_start`` on the first run); ``reprocess_from``
   widens it back in time to recapture revisions older than the lookback;
2. fetch pages (retries/backoff inside the HTTP client);
3. land the exact response bytes (+ text observations);
4. build Bronze records and MERGE the new versions (insert-only: idempotent);
5. advance the checkpoint — only after step 4 succeeded, and never backwards.

Failures are isolated per indicator, recorded in metrics and manifest, and the run then raises
``IngestionFailedError`` (unless ``raise_on_failure=False``): nothing fails silently.

With an ``observer`` (production wiring always passes one) the run is also one ``pipeline_runs``
row: input = observations received, output = new versions stored, rejected = 0 (Bronze never
quarantines), FAILED when any indicator failed. HTTP metrics stay in ``_metrics/``.
"""

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

from financial_lakehouse import PROJECT
from financial_lakehouse.catalog import INDICATORS, Indicator, SourceKind
from financial_lakehouse.clients import (
    HttpClient,
    IbgeAgregadosClient,
    PtaxClient,
    RetryPolicy,
    SgsClient,
)
from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.clients.http import (
    HttpError,
    RequestMetrics,
    Transport,
    urllib_transport,
)
from financial_lakehouse.storage import BronzeSink, LandingStore, WatermarkStore
from financial_lakehouse.transformation import BronzeRecord, to_bronze_records, to_raw_observations
from lakehouse_common.config import load_environment
from lakehouse_common.observability import Layer, ObservabilitySink, track_run

logger = logging.getLogger(__name__)
BRASILIA_OFFSET: Final = timedelta(hours=-3)
SUCCEEDED, NO_DATA, FAILED = "succeeded", "no_data", "failed"
PIPELINE_NAME: Final = "financial_bronze"
EXPECTED_FAILURES: Final = (HttpError, ResponseSchemaError, OSError, ValueError)
MONTHS_PER_QUARTER: Final = 3


@dataclass(frozen=True, slots=True)
class IngestionSettings:
    backfill_start: date
    lookback_days: int = 90  # re-read recent data: the sources revise values
    lookback_periods: int = 6  # same idea for IBGE period ids
    timeout_seconds: float = 30.0
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if self.lookback_days < 0 or self.lookback_periods < 0 or self.max_attempts < 1:
            raise ValueError("lookbacks must be >= 0 and max_attempts >= 1")
        # Gold flags only the month in progress as partial: a history that starts mid-month would
        # make the first month look complete (Selic compounded, USD averaged over half of it).
        if self.backfill_start.day != 1:
            raise ValueError(
                f"backfill_start must be the first day of a month, got {self.backfill_start}"
            )


@dataclass(frozen=True, slots=True)
class IndicatorResult:
    indicator_id: str
    status: str  # succeeded | no_data | failed
    pages: int = 0
    records_received: int = 0
    records_ingested: int = 0
    requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    execution_time_seconds: float = 0.0
    watermark: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RunMetrics:
    requests: int
    successful_requests: int
    failed_requests: int
    records_received: int
    records_ingested: int
    execution_time_seconds: float
    indicators_failed: int
    indicators_succeeded: int = 0
    indicators_no_data: int = 0


@dataclass
class IngestionReport:
    run_id: str
    results: list[IndicatorResult] = field(default_factory=list)
    metrics: RunMetrics = field(default_factory=lambda: RunMetrics(0, 0, 0, 0, 0, 0.0, 0))
    reprocess_from: date | None = None

    @property
    def failed(self) -> list[IndicatorResult]:
        return [r for r in self.results if r.status == FAILED]


class IngestionFailedError(RuntimeError):
    """Raised after the whole run when at least one indicator failed (report attached)."""

    def __init__(self, report: IngestionReport) -> None:
        details = "; ".join(f"{r.indicator_id}: {r.error}" for r in report.failed)
        super().__init__(
            f"run {report.run_id}: {len(report.failed)} indicator(s) failed: {details}"
        )
        self.report = report


def _sortable(indicator: Indicator, reference: str) -> str:
    """Comparable key: ISO date for SGS (dd/MM/yyyy) and PTAX (date part), period id for IBGE."""
    if indicator.source is SourceKind.BCB_SGS:
        day, month, year = reference.split("/")
        return f"{year}-{month}-{day}"
    if indicator.source is SourceKind.BCB_PTAX:
        return reference[:10]
    return reference


def _first_period(item: Indicator, start: date) -> str:
    """IBGE period id of ``start``: ``YYYYQQ`` for quarterly tables, ``YYYYMM`` otherwise."""
    if item.frequency == "quarterly":
        return f"{start.year}{(start.month - 1) // MONTHS_PER_QUARTER + 1:02d}"
    return f"{start:%Y%m}"


def _required(value: int | None, item: Indicator, name: str) -> int:
    if value is None:
        raise ValueError(f"{item.indicator_id}: catalog entry has no {name}")
    return value


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: str
    pages: int
    received: int
    inserted: int


class IncrementalIngestion:
    def __init__(  # noqa: PLR0913 - explicit dependencies, keyword-only
        self,
        *,
        sgs: SgsClient,
        ptax: PtaxClient,
        ibge: IbgeAgregadosClient,
        store: LandingStore,
        bronze: BronzeSink,
        checkpoints: WatermarkStore,
        settings: IngestionSettings,
        http_metrics: RequestMetrics,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        observer: ObservabilitySink | None = None,
    ) -> None:
        self._sgs, self._ptax, self._ibge = sgs, ptax, ibge
        self._store, self._bronze, self._checkpoints = store, bronze, checkpoints
        self._settings, self._http_metrics = settings, http_metrics
        self._clock, self._monotonic = clock, monotonic
        self._observer = observer

    # --- what to fetch ------------------------------------------------------------------------

    def _date_range(
        self, item: Indicator, today: date, reprocess_from: date | None
    ) -> tuple[date, date]:
        checkpoint = self._checkpoints.get(item.indicator_id)
        start = self._settings.backfill_start
        if checkpoint:
            lookback = timedelta(days=self._settings.lookback_days)
            start = max(start, date.fromisoformat(checkpoint) - lookback)
        if reprocess_from:
            start = min(start, reprocess_from)
        return start, today

    def _periods(self, item: Indicator, reprocess_from: date | None) -> list[str]:
        available = self._ibge.periods(_required(item.agregado, item, "agregado"))
        checkpoint = self._checkpoints.get(item.indicator_id)
        if checkpoint is None:
            keep_from = _first_period(item, self._settings.backfill_start)
        else:
            known = [p for p in available if p <= checkpoint]
            keep_from = known[-(self._settings.lookback_periods + 1)] if known else checkpoint
        if reprocess_from:
            keep_from = min(keep_from, _first_period(item, reprocess_from))
        return [p for p in available if p >= keep_from]

    def _extract(self, item: Indicator, today: date, reprocess_from: date | None) -> list[RawPage]:
        if item.source is SourceKind.IBGE_AGREGADOS:
            periods = self._periods(item, reprocess_from)
            if not periods:
                return []
            return self._ibge.fetch(
                _required(item.agregado, item, "agregado"),
                _required(item.variavel, item, "variavel"),
                periods,
                classificacao=item.classificacao,
            )
        start, end = self._date_range(item, today, reprocess_from)
        if item.source is SourceKind.BCB_SGS:
            return self._sgs.fetch(_required(item.series_code, item, "series_code"), start, end)
        return self._ptax.fetch(str(item.currency), start, end)

    # --- one indicator ------------------------------------------------------------------------

    def _ingest_one(
        self, item: Indicator, report: IngestionReport, now: datetime, files: list[dict[str, Any]]
    ) -> _Outcome:
        run_id = report.run_id
        today = (now + BRASILIA_OFFSET).date()  # business date of the Brazilian sources
        pages = self._extract(item, today, report.reprocess_from)
        records: list[BronzeRecord] = []
        observations = []
        for index, page in enumerate(pages):
            landed = self._store.write_page(
                run_id=run_id,
                ingestion_date=now.date(),
                indicator_id=item.indicator_id,
                source=item.source,
                page_index=index,
                page=page,
            )
            files.append(
                {
                    "indicator_id": item.indicator_id,
                    "path": str(landed.path),
                    "url": landed.url,
                    "sha256": landed.sha256,
                    "bytes": landed.size_bytes,
                    "records": len(page.records),
                }
            )
            observations.extend(to_raw_observations(item, page))
            records.extend(
                to_bronze_records(
                    item,
                    page,
                    run_id=run_id,
                    ingestion_timestamp=now,
                    raw_file=str(landed.path),
                    raw_sha256=landed.sha256,
                )
            )
        if observations:
            self._store.write_observations(
                run_id=run_id,
                ingestion_date=now.date(),
                indicator_id=item.indicator_id,
                source=item.source,
                observations=observations,
            )
        if not records:
            logger.warning("%s: no data returned for the requested range", item.indicator_id)
            return _Outcome(NO_DATA, len(pages), 0, 0)
        inserted = self._bronze.merge(records)
        latest = max(_sortable(item, r.reference) for r in records)
        previous = self._checkpoints.get(item.indicator_id)
        if previous is None or latest > previous:  # after Bronze accepted them; never backwards
            self._checkpoints.set(item.indicator_id, latest, run_id=run_id, updated_at=now)
        return _Outcome(SUCCEEDED, len(pages), len(records), inserted)

    def _run_indicator(
        self, item: Indicator, report: IngestionReport, now: datetime, files: list[dict[str, Any]]
    ) -> IndicatorResult:
        before = RequestMetrics(**asdict(self._http_metrics))
        started = self._monotonic()
        error: str | None = None
        try:
            outcome = self._ingest_one(item, report, now, files)
        except EXPECTED_FAILURES as failure:
            logger.exception("ingestion of %s failed", item.indicator_id)
            outcome = _Outcome(FAILED, 0, 0, 0)
            error = f"{type(failure).__name__}: {failure}"
        after = self._http_metrics
        return IndicatorResult(
            indicator_id=item.indicator_id,
            status=outcome.status,
            pages=outcome.pages,
            records_received=outcome.received,
            records_ingested=outcome.inserted,
            requests=after.requests - before.requests,
            successful_requests=after.successful_requests - before.successful_requests,
            failed_requests=after.failed_requests - before.failed_requests,
            execution_time_seconds=round(self._monotonic() - started, 6),
            watermark=self._checkpoints.get(item.indicator_id),
            error=error,
        )

    # --- run ----------------------------------------------------------------------------------

    def run(
        self,
        indicators: Sequence[Indicator] = INDICATORS,
        *,
        raise_on_failure: bool = True,
        reprocess_from: date | None = None,
    ) -> IngestionReport:
        """Ingest ``indicators``; ``reprocess_from`` also re-reads the history since that date.

        A reprocess window recaptures revisions older than the lookback: only observations that
        changed are inserted, and the checkpoint does not move backwards.
        """
        now = self._clock().astimezone(UTC)
        if self._observer is None:
            return self._run(indicators, now, raise_on_failure, reprocess_from)
        try:
            with track_run(
                pipeline_name=PIPELINE_NAME,
                run_id=f"{now:%Y%m%dT%H%M%SZ}",
                project=PROJECT,
                layer=Layer.BRONZE,
                table=self._bronze.name,
                sink=self._observer,
                clock=self._clock,
                monotonic=self._monotonic,
            ) as step:
                try:  # always raise inside the step: a failed indicator makes the run FAILED
                    report = self._run(indicators, now, True, reprocess_from)
                except IngestionFailedError as failure:
                    _record(step.record_counts, failure.report)
                    raise
                _record(step.record_counts, report)
        except IngestionFailedError as failure:
            if raise_on_failure:
                raise
            return failure.report
        return report

    def _run(
        self,
        indicators: Sequence[Indicator],
        now: datetime,
        raise_on_failure: bool,
        reprocess_from: date | None,
    ) -> IngestionReport:
        started = self._monotonic()
        report = IngestionReport(run_id=f"{now:%Y%m%dT%H%M%SZ}", reprocess_from=reprocess_from)
        if reprocess_from:
            logger.info("reprocessing since %s (on top of the incremental window)", reprocess_from)
        files: list[dict[str, Any]] = []
        for item in indicators:
            result = self._run_indicator(item, report, now, files)
            logger.info(
                "%s: %s (received=%d, ingested=%d, requests=%d, failed requests=%d)",
                item.indicator_id,
                result.status,
                result.records_received,
                result.records_ingested,
                result.requests,
                result.failed_requests,
            )
            report.results.append(result)
        report.metrics = RunMetrics(
            requests=sum(r.requests for r in report.results),
            successful_requests=sum(r.successful_requests for r in report.results),
            failed_requests=sum(r.failed_requests for r in report.results),
            records_received=sum(r.records_received for r in report.results),
            records_ingested=sum(r.records_ingested for r in report.results),
            execution_time_seconds=round(self._monotonic() - started, 6),
            indicators_failed=len(report.failed),
            indicators_succeeded=sum(r.status == SUCCEEDED for r in report.results),
            indicators_no_data=sum(r.status == NO_DATA for r in report.results),
        )
        self._store.write_manifest(
            report.run_id,
            {
                "run_id": report.run_id,
                "started_at": now.isoformat(),
                "settings": asdict(self._settings),
                "reprocess_from": reprocess_from.isoformat() if reprocess_from else None,
                "metrics": asdict(report.metrics),
                "results": [asdict(r) for r in report.results],
                "files": files,
            },
        )
        self._store.write_metrics(report.run_id, asdict(report.metrics))
        if report.failed and raise_on_failure:
            raise IngestionFailedError(report)
        return report


def _record(record_counts: Callable[..., None], report: IngestionReport) -> None:
    record_counts(
        input_rows=report.metrics.records_received,
        output_rows=report.metrics.records_ingested,
        rejected_rows=0,
    )


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    landing_root: Path
    settings: IngestionSettings
    bronze_root: Path | None = None  # local runs (LocalBronzeSink)
    bronze_table: str | None = None  # Databricks (DeltaBronzeSink)
    observability_root: Path | None = None  # local runs (JsonLinesObservabilitySink)


def load_settings(path: Path, environment: str) -> IngestionConfig:
    """Landing root, Bronze target and ingestion settings of ``environment``."""
    section = load_environment(path, environment)
    landing_root = Path(section.pop("landing_root"))
    bronze_root = section.pop("bronze_root", None)
    bronze_table = section.pop("bronze_table", None)
    observability_root = section.pop("observability_root", None)
    return IngestionConfig(
        landing_root,
        IngestionSettings(**section),
        Path(bronze_root) if bronze_root else None,
        bronze_table,
        Path(observability_root) if observability_root else None,
    )


def build_incremental_ingestion(  # noqa: PLR0913 - keyword-only wiring options
    landing_root: Path,
    settings: IngestionSettings,
    bronze: BronzeSink,
    *,
    observer: ObservabilitySink,
    transport: Transport = urllib_transport,
    clock: Callable[[], datetime] | None = None,
) -> IncrementalIngestion:
    """Production wiring: HTTP client with the configured timeout and retries.

    ``transport`` defaults to the real network. ``clients.replay`` gives the recording and the
    replay transports (Volume fallback); a replay passes ``clock`` = the recording instant, so
    the run requests exactly the recorded URLs.
    """
    metrics = RequestMetrics()
    http = HttpClient(
        transport=transport,
        policy=RetryPolicy(max_attempts=settings.max_attempts),
        timeout_seconds=settings.timeout_seconds,
        metrics=metrics,
    )
    return IncrementalIngestion(
        sgs=SgsClient(http),
        ptax=PtaxClient(http),
        ibge=IbgeAgregadosClient(http),
        store=LandingStore(landing_root),
        bronze=bronze,
        checkpoints=WatermarkStore(landing_root),
        settings=settings,
        http_metrics=metrics,
        observer=observer,
        clock=clock or (lambda: datetime.now(UTC)),
    )
