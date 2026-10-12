"""Ingestion CLI contract: exit code 0 on success, 1 when any indicator failed; settings applied."""

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from financial_lakehouse.clients.http import urllib_transport
from financial_lakehouse.clients.replay import recorded_at
from financial_lakehouse.ingestion import (
    INDICATORS,
    IndicatorResult,
    IngestionFailedError,
    IngestionReport,
    IngestionSettings,
)
from financial_lakehouse.ingestion import (
    __main__ as cli,
)

CONFIG = """
[local]
landing_root = "{root}/landing"
bronze_root = "{root}/bronze"
observability_root = "{root}/observability"
backfill_start = 2015-01-01

[databricks]
landing_root = "/Volumes/x/landing"
bronze_table = "x.bronze.t"
backfill_start = 2015-01-01
"""


class _Pipeline:
    def __init__(self, outcome: IngestionReport | Exception) -> None:
        self.outcome = outcome
        self.selected: list[Any] | None = None
        self.reprocess_from: date | None = None

    def run(
        self, indicators: list[Any] | None = None, *, reprocess_from: date | None = None
    ) -> IngestionReport:
        self.selected = indicators
        self.reprocess_from = reprocess_from
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.fixture
def config(tmp_path: Path) -> Path:
    path = tmp_path / "ingestion.toml"
    path.write_text(CONFIG.format(root=tmp_path.as_posix()), encoding="utf-8")
    return path


def _install(monkeypatch: pytest.MonkeyPatch, pipeline: _Pipeline) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def build(
        landing_root: Path,
        settings: IngestionSettings,
        bronze: object,
        *,
        observer: object,
        transport: object,
        clock: object,
    ) -> _Pipeline:
        seen.update(
            landing_root=landing_root,
            settings=settings,
            bronze=bronze,
            observer=observer,
            transport=transport,
            clock=clock,
        )
        return pipeline

    monkeypatch.setattr(cli, "build_incremental_ingestion", build)
    return seen


def test_success_exits_zero_and_applies_overrides(
    monkeypatch: pytest.MonkeyPatch, config: Path
) -> None:
    pipeline = _Pipeline(IngestionReport("R1", [IndicatorResult("ipca_monthly", "succeeded")]))
    seen = _install(monkeypatch, pipeline)

    code = cli.main(
        ["--config", str(config), "--backfill-start", "2024-01-01", "--indicator", "ipca_monthly"]
    )

    assert code == 0
    assert seen["settings"].backfill_start == date(2024, 1, 1)
    assert [i.indicator_id for i in pipeline.selected or []] == ["ipca_monthly"]
    assert pipeline.reprocess_from is None  # a normal run is incremental only


def test_reprocess_window_is_passed_to_the_run(
    monkeypatch: pytest.MonkeyPatch, config: Path
) -> None:
    pipeline = _Pipeline(IngestionReport("R1", [IndicatorResult("ipca_monthly", "succeeded")]))
    _install(monkeypatch, pipeline)

    assert cli.main(["--config", str(config), "--reprocess-from", "2023-01-01"]) == 0

    assert pipeline.reprocess_from == date(2023, 1, 1)
    assert len(pipeline.selected or []) == len(INDICATORS)


def test_a_failed_indicator_exits_one(monkeypatch: pytest.MonkeyPatch, config: Path) -> None:
    report = IngestionReport("R1", [IndicatorResult("selic_daily", "failed", error="Timeout")])
    _install(monkeypatch, _Pipeline(IngestionFailedError(report)))

    assert cli.main(["--config", str(config)]) == 1


def test_an_environment_without_local_bronze_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, config: Path
) -> None:
    _install(monkeypatch, _Pipeline(IngestionReport("R1")))
    with pytest.raises(SystemExit) as stopped:
        cli.main(["--config", str(config), "--env", "databricks"])
    assert stopped.value.code == 2  # argparse usage error


def test_unknown_indicator_is_a_usage_error_not_a_traceback(
    monkeypatch: pytest.MonkeyPatch, config: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _install(monkeypatch, _Pipeline(IngestionReport("R1")))
    with pytest.raises(SystemExit) as stopped:
        cli.main(["--config", str(config), "--indicator", "selic_meta"])
    assert stopped.value.code == 2
    assert "invalid choice: 'selic_meta'" in capsys.readouterr().err


def test_mid_month_backfill_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, config: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _install(monkeypatch, _Pipeline(IngestionReport("R1")))
    with pytest.raises(SystemExit) as stopped:
        cli.main(["--config", str(config), "--backfill-start", "2024-01-15"])
    assert stopped.value.code == 2
    assert "first day of a month" in capsys.readouterr().err


def test_the_network_is_used_unless_responses_are_recorded(
    monkeypatch: pytest.MonkeyPatch, config: Path, tmp_path: Path
) -> None:
    pipeline = _Pipeline(IngestionReport("R1", [IndicatorResult("ipca_monthly", "succeeded")]))
    plain = _install(monkeypatch, pipeline)
    assert cli.main(["--config", str(config)]) == 0
    assert plain["transport"] is urllib_transport

    recording = _install(monkeypatch, pipeline)
    replay_root = tmp_path / "replay"
    assert cli.main(["--config", str(config), "--record-responses", str(replay_root)]) == 0
    assert recording["transport"] is not urllib_transport
    assert recorded_at(replay_root) == recording["clock"]()  # replay with the same instant
