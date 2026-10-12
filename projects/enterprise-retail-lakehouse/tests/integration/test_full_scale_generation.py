"""Full-scale generation with the repository configuration (slow: run with -m integration)."""

import re
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from retail_lakehouse.synthetic import AnomalyRecord, generate_retail_dataset, load_config

CALENDAR_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
CONFIG_FILE = Path("projects/enterprise-retail-lakehouse/config/data_generation.toml")


class _ProfilingSink:
    """Counts rows and orders per month without keeping rows in memory."""

    def __init__(self) -> None:
        self.row_counts: Counter[str] = Counter()
        self.orders_per_month: Counter[str] = Counter()
        self.anomaly_counts: Counter[str] = Counter()

    def write(self, dataset: str, row: Mapping[str, str | None]) -> None:
        self.row_counts[dataset] += 1
        month = str(row["order_timestamp"])[:7] if dataset == "orders" else ""
        if CALENDAR_MONTH.match(month):  # malformed timestamps are intentional anomalies
            self.orders_per_month[month] += 1

    def record_anomaly(self, anomaly: AnomalyRecord) -> None:
        self.anomaly_counts[anomaly.anomaly_type.value] += 1


def test_full_scale_volumes_seasonality_and_anomalies(repo_root: Path) -> None:
    config = load_config(repo_root / CONFIG_FILE)
    sink = _ProfilingSink()

    summary = generate_retail_dataset(config, sink)

    counts = sink.row_counts
    assert counts["customers"] >= 50_000
    assert counts["products"] >= 2_000
    assert counts["stores"] >= 30
    assert counts["suppliers"] >= 100
    assert counts["orders"] >= 100_000
    assert counts["order_items"] >= 300_000
    assert summary.row_counts == dict(counts)

    months = sink.orders_per_month
    assert len(months) == 24
    assert months["2024-11"] > months["2024-02"] * 1.3
    assert months["2025-12"] > months["2025-03"] * 1.3

    assert set(sink.anomaly_counts) == {
        "duplicate_record",
        "null_value",
        "invalid_email",
        "negative_price",
        "invalid_quantity",
        "orphan_foreign_key",
        "duplicate_payment",
        "invalid_date",
    }
