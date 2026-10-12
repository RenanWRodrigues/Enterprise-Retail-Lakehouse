"""CLI: write the retail landing zone to a directory (local path or Databricks Volume).

Full batch (default, unchanged):
  python -m retail_lakehouse.synthetic --output data/landing/retail

Incremental batch of a plan (CDC: changed and new master data, new orders):
  python -m retail_lakehouse.synthetic --output data/landing/retail \
      --plan projects/enterprise-retail-lakehouse/config/incremental.toml --batch batch_002
"""

import argparse
import csv
import json
import logging
from dataclasses import replace
from pathlib import Path

from retail_lakehouse.synthetic import FileSink, generate_retail_dataset, load_config
from retail_lakehouse.synthetic.incremental import (
    CHANGE_COLUMNS,
    IncrementalSummary,
    change_rows,
    generate_incremental_batch,
    load_plan,
)

logger = logging.getLogger("retail_lakehouse.synthetic")
DEFAULT_CONFIG = Path(__file__).parents[3] / "config" / "data_generation.toml"
CHANGES_FILE = "_changes_{batch_id}.csv"
INCREMENT_FILE = "_increment_{batch_id}.json"


def _full(config_path: Path, output: Path) -> None:
    config = load_config(config_path)
    with FileSink(output, config=config) as sink:
        summary = generate_retail_dataset(config, sink)
    for dataset, count in summary.row_counts.items():
        logger.info("%-20s %9d rows", dataset, count)
    for anomaly_type, count in summary.anomaly_counts.items():
        logger.info("anomaly %-20s %7d", anomaly_type, count)


def _write_increment(output: Path, summary: IncrementalSummary) -> None:
    with (output / CHANGES_FILE.format(batch_id=summary.batch_id)).open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=CHANGE_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(change_rows(summary.changes))
    identity = {
        "batch_id": summary.batch_id,
        "batch_fingerprint": summary.batch_fingerprint,
        "window": list(summary.window),
        "changes": len(summary.changes),
    }
    (output / INCREMENT_FILE.format(batch_id=summary.batch_id)).write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _incremental(plan_path: Path, batch_id: str, output: Path) -> None:
    plan = load_plan(plan_path)
    config = replace(plan.base, batch_id=batch_id, anomalies=())  # file names and manifest
    with FileSink(output, config=config) as sink:
        summary = generate_incremental_batch(plan, batch_id, sink)
    _write_increment(output, summary)
    logger.info("batch %s (%s .. %s): %d changes, fingerprint %s", batch_id, *summary.window,
                len(summary.changes), summary.batch_fingerprint[:12])  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="TOML configuration")
    parser.add_argument("--output", type=Path, required=True, help="landing zone root directory")
    parser.add_argument("--plan", type=Path, help="incremental plan (TOML); requires --batch")
    parser.add_argument("--batch", help="batch id of the plan to generate")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    if (args.plan is None) != (args.batch is None):
        parser.error("--plan and --batch go together")
    if args.plan:
        _incremental(args.plan, args.batch, args.output)
    else:
        _full(args.config, args.output)
    logger.info("landing zone written to %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
