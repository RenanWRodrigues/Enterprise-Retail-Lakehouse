"""CLI: incremental ingestion (official APIs -> raw landing -> local Bronze).

Example (from the repository root):
  PYTHONPATH="projects/brazilian-financial-lakehouse/src;shared/src" \
  python -m financial_lakehouse.ingestion --env local

Revisions older than the lookback window (docs/INCREMENTAL_STRATEGY.md):
  python -m financial_lakehouse.ingestion --env local --reprocess-from 2024-01-01

Record the API responses for an offline replay (Databricks without internet, docs/databricks):
  python -m financial_lakehouse.ingestion --env local --record-responses data/replay/financial

Exit code 1 when any indicator failed (the error is logged and in the run manifest).
"""

import argparse
import logging
import os
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

from financial_lakehouse.clients.http import urllib_transport
from financial_lakehouse.clients.replay import recording_transport
from financial_lakehouse.ingestion import (
    INDICATORS,
    IngestionFailedError,
    build_incremental_ingestion,
    indicator,
    load_settings,
)
from financial_lakehouse.ingestion.pipeline import IncrementalIngestion, IngestionReport
from financial_lakehouse.storage import LocalBronzeSink
from lakehouse_common.logging import setup_logging
from lakehouse_common.observability import JsonLinesObservabilitySink

DEFAULT_CONFIG = Path(__file__).parents[3] / "config" / "ingestion.toml"
logger = logging.getLogger("financial_lakehouse")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--env", default="local")
    parser.add_argument("--backfill-start", type=date.fromisoformat, help="override the start")
    parser.add_argument(
        "--reprocess-from",
        type=date.fromisoformat,
        help="also re-read the history since this date (late revisions; checkpoint kept)",
    )
    parser.add_argument(
        "--indicator",
        action="append",
        choices=[item.indicator_id for item in INDICATORS],
        help="limit to these indicator ids",
    )
    parser.add_argument(
        "--record-responses",
        type=Path,
        help="also save every API response here, for a later offline replay",
    )
    return parser


def _pipeline(
    parser: argparse.ArgumentParser, args: argparse.Namespace, now: datetime
) -> IncrementalIngestion:
    """The local pipeline of ``--env``; configuration problems are usage errors (exit 2)."""
    config = load_settings(args.config, args.env)
    if config.bronze_root is None or config.observability_root is None:
        parser.error(
            f"environment {args.env!r} needs bronze_root and observability_root "
            "(use the Databricks notebook)"
        )
    settings = config.settings
    if args.backfill_start:
        try:
            settings = replace(settings, backfill_start=args.backfill_start)
        except ValueError as invalid:
            parser.error(str(invalid))
    transport = (
        recording_transport(urllib_transport, args.record_responses, at=now)
        if args.record_responses
        else urllib_transport
    )
    return build_incremental_ingestion(
        config.landing_root,
        settings,
        LocalBronzeSink(config.bronze_root),
        observer=JsonLinesObservabilitySink(config.observability_root),
        transport=transport,
        clock=lambda: now,  # one instant per run: a recording replays with the same clock
    )


def _log_report(report: IngestionReport) -> None:
    """One line per indicator, then the request metrics (what an operator reads)."""
    for r in report.results:
        logger.info(
            "%-24s %-9s received=%-6d ingested=%-6d requests=%d/%d failed watermark=%s",
            r.indicator_id,
            r.status,
            r.records_received,
            r.records_ingested,
            r.requests,
            r.failed_requests,
            r.watermark,
        )
    logger.info("metrics: %s", report.metrics)


def main(argv: list[str] | None = None) -> int:
    """Run the ingestion; exit 1 when any indicator failed, 2 on a usage error."""
    parser = _parser()
    args = parser.parse_args(argv)
    setup_logging(os.environ)  # text by default; LAKEHOUSE_LOG_FORMAT=json; optional webhook
    pipeline = _pipeline(parser, args, datetime.now(UTC))
    selected = [indicator(i) for i in args.indicator] if args.indicator else list(INDICATORS)
    try:
        report = pipeline.run(selected, reprocess_from=args.reprocess_from)
    except IngestionFailedError as failure:
        report = failure.report
        logger.error("%s", failure)
    _log_report(report)
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
