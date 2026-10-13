"""``python -m iot_lakehouse.synthetic``: arguments, configuration, exit codes, what it writes.

The Spark write itself (``write_landing``) needs Hadoop native helpers that Windows lacks and is
covered by ``tests/integration/test_iot_landing_write.py``. Here the session and the writer are
fakes, so these tests run anywhere without a JVM: they check what the CLI decides and passes.
Exit codes: 0 written, 1 the landing zone could not be written, 2 usage or configuration error.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from iot_lakehouse.synthetic import __main__ as cli
from iot_lakehouse.synthetic import generate_master_data, load_config
from lakehouse_common.observability import JsonLinesObservabilitySink

TINY = """\
[generation]
seed = {seed}
start = 2025-03-01T00:00:00Z
days = 2
sampling_interval_seconds = 600
equipment_per_type = 1
failure_rate_per_year = 6.0
preventive_interval_days = 1
batch_id = "initial"

[anomalies]
spike_rate = 0.01
impossible_rate = 0.005
duplicate_rate = 0.02
out_of_order_rate = 0.02
late_rate = 0.01
comm_outages_per_sensor_month = 1.0
drift_sensor_share = 0.5
lateness_threshold_minutes = 60
"""


@dataclass
class _Writer:
    """Stands in for ``write_landing``: records each call, optionally fails."""

    error: Exception | None = None
    calls: list[dict[str, Any]] = field(default_factory=list)

    def __call__(
        self, spark: object, config: Any, master: Any, root: Any, *, sink: Any
    ) -> dict[str, int]:
        self.calls.append({"spark": spark, "config": config, "master": master, "root": root,
                           "sink": sink})  # fmt: skip
        if self.error is not None:
            raise self.error
        return {"sensor_readings": 10, "equipment": 4}


class _Builder:
    def __init__(self, started: list[str]) -> None:
        self._started = started

    def appName(self, name: str) -> "_Builder":  # noqa: N802 - SparkSession.builder API
        self._started.append(name)
        return self

    def getOrCreate(self) -> str:  # noqa: N802 - SparkSession.builder API
        return "spark"


@pytest.fixture
def started(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """App names of the Spark sessions the CLI started (empty: no JVM was needed)."""
    names: list[str] = []
    monkeypatch.setattr(cli, "SparkSession", type("S", (), {"builder": _Builder(names)}))
    return names


@pytest.fixture
def writer(monkeypatch: pytest.MonkeyPatch) -> _Writer:
    fake = _Writer()
    monkeypatch.setattr(cli, "write_landing", fake)
    return fake


def _config(tmp_path: Path, seed: int = 11) -> Path:
    path = tmp_path / f"generation_{seed}.toml"
    path.write_text(TINY.format(seed=seed), encoding="utf-8")
    return path


def _exit_code(argv: list[str]) -> int:
    with pytest.raises(SystemExit) as stopped:
        cli.main(argv)
    return int(stopped.value.code or 0)


def test_valid_arguments_write_the_landing_zone_and_exit_zero(
    tmp_path: Path, started: list[str], writer: _Writer
) -> None:
    config, output = _config(tmp_path), tmp_path / "landing"

    assert cli.main(["--config", str(config), "--output", str(output)]) == 0

    assert started == ["iot-synthetic-sources"]
    [call] = writer.calls
    assert call["root"] == str(output)
    assert call["config"] == load_config(config)
    assert isinstance(call["sink"], JsonLinesObservabilitySink)  # pipeline_runs next to the data


def test_the_master_data_follows_the_seed(
    tmp_path: Path, started: list[str], writer: _Writer
) -> None:
    for seed in (11, 11, 12):
        cli.main(["--config", str(_config(tmp_path, seed)), "--output", str(tmp_path / "out")])

    same, again, other = (call["master"] for call in writer.calls)
    assert same == again == generate_master_data(load_config(_config(tmp_path, 11)))
    assert same != other


@pytest.mark.parametrize("argv", [[], ["--config", "x.toml"], ["--output", "x", "--unknown"]])
def test_invalid_arguments_are_usage_errors(
    argv: list[str], started: list[str], writer: _Writer
) -> None:
    assert _exit_code(argv) == 2
    assert started == []


def test_a_missing_configuration_fails_before_spark_starts(
    tmp_path: Path, started: list[str], writer: _Writer, capsys: pytest.CaptureFixture[str]
) -> None:
    code = _exit_code(["--config", str(tmp_path / "missing.toml"), "--output", str(tmp_path)])

    assert code == 2
    assert "missing.toml" in capsys.readouterr().err
    assert (started, writer.calls) == ([], [])


@pytest.mark.parametrize(
    "text",
    [
        "[generation\n",  # not TOML
        TINY.format(seed=11) + "unknown_key = 1\n",  # key not in the configuration
        TINY.format(seed=11).replace("days = 2", "days = 0"),  # value out of range
    ],
    ids=["not-toml", "unknown-key", "out-of-range"],
)
def test_an_invalid_configuration_fails_before_spark_starts(
    tmp_path: Path, started: list[str], writer: _Writer, text: str
) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text(text, encoding="utf-8")

    assert _exit_code(["--config", str(broken), "--output", str(tmp_path / "out")]) == 2
    assert (started, writer.calls) == ([], [])


def test_a_write_failure_exits_one_with_a_logged_error(
    tmp_path: Path, started: list[str], writer: _Writer, caplog: pytest.LogCaptureFixture
) -> None:
    writer.error = PermissionError("read-only volume")

    with caplog.at_level("ERROR", logger="iot_lakehouse.synthetic"):
        code = cli.main(["--config", str(_config(tmp_path)), "--output", str(tmp_path / "out")])

    assert code == 1
    assert any("could not write" in r.getMessage() for r in caplog.records)
