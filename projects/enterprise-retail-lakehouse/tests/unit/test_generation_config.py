"""Generation configuration: defaults, TOML loading and validation."""

from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from retail_lakehouse.synthetic import (
    SUPPORTED_ANOMALIES,
    AnomalyRule,
    AnomalyType,
    GenerationConfig,
    load_config,
)

CONFIG_FILE = Path("projects/enterprise-retail-lakehouse/config/data_generation.toml")


def test_repository_config_meets_target_volumes(repo_root: Path) -> None:
    config = load_config(repo_root / CONFIG_FILE)

    assert config.customers >= 50_000
    assert config.products >= 2_000
    assert config.stores >= 30
    assert config.suppliers >= 100
    assert config.orders >= 100_000
    months = (config.end_date.year - config.start_date.year) * 12 + (
        config.end_date.month - config.start_date.month + 1
    )
    assert months == 24


def test_repository_config_enables_every_requested_anomaly_type(repo_root: Path) -> None:
    config = load_config(repo_root / CONFIG_FILE)
    enabled = {rule.anomaly_type for rule in config.anomalies if rule.rate > 0}
    assert enabled == set(AnomalyType)


def test_load_config_parses_anomaly_rules(tmp_path: Path) -> None:
    path = tmp_path / "gen.toml"
    path.write_text(
        """
[generation]
seed = 1
start_date = 2024-01-01
end_date = 2024-12-31
customers = 10
products = 5
stores = 2
suppliers = 2
orders = 20

[[anomalies]]
type = "null_value"
dataset = "customers"
column = "email"
rate = 0.25

[[anomalies]]
type = "duplicate_payment"
dataset = "payments"
rate = 0.1
""",
        encoding="utf-8",
    )

    config = load_config(path)

    assert (config.seed, config.start_date, config.orders) == (1, date(2024, 1, 1), 20)
    assert config.anomalies == (
        AnomalyRule(AnomalyType.NULL_VALUE, "customers", "email", 0.25),
        AnomalyRule(AnomalyType.DUPLICATE_PAYMENT, "payments", None, 0.1),
    )


def test_default_config_is_clean() -> None:
    assert GenerationConfig().anomalies == ()


@pytest.mark.parametrize(
    "changes",
    [
        {"end_date": date(2023, 12, 31)},
        {"customers": 0},
        {"orders": -1},
        {"stores": 1},  # needs >= 1 physical store + the online store
        {"customers": 9_000_000},  # orphan ids must stay outside the id range
    ],
)
def test_invalid_volumes_or_dates_are_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        replace(GenerationConfig(), **changes)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("anomaly_type", "dataset", "column", "rate"),
    [
        (AnomalyType.NULL_VALUE, "customers", "email", 1.5),  # rate outside [0, 1]
        (AnomalyType.NEGATIVE_PRICE, "customers", "email", 0.1),  # unsupported target
        (AnomalyType.DUPLICATE_PAYMENT, "orders", None, 0.1),  # unsupported target
    ],
)
def test_invalid_anomaly_rules_are_rejected(
    anomaly_type: AnomalyType, dataset: str, column: str | None, rate: float
) -> None:
    with pytest.raises(ValueError):
        AnomalyRule(anomaly_type, dataset, column, rate)


def test_duplicate_anomaly_rules_are_rejected() -> None:
    rule = AnomalyRule(AnomalyType.NULL_VALUE, "customers", "email", 0.1)
    with pytest.raises(ValueError, match="duplicate"):
        GenerationConfig(anomalies=(rule, rule))


def test_every_anomaly_type_has_at_least_one_supported_target() -> None:
    assert {anomaly_type for anomaly_type, _, _ in SUPPORTED_ANOMALIES} == set(AnomalyType)
