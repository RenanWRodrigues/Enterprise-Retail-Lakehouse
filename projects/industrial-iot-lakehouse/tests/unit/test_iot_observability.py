"""IoT landing generation is observed: one ``pipeline_runs`` row, also when it fails.

The Spark write is replaced by a fake (it needs Linux file writes); what is tested here is the
wiring: project, layer, counts, status and error. The real write is asserted in
``tests/integration/test_iot_landing_write.py``.
"""

from pathlib import Path
from typing import Any

import pytest

from iot_lakehouse.synthetic import IotGenerationConfig, writer
from lakehouse_common.observability import InMemoryObservabilitySink


@pytest.fixture
def runs() -> InMemoryObservabilitySink:
    return InMemoryObservabilitySink()


def _write(monkeypatch: pytest.MonkeyPatch, outcome: dict[str, int] | Exception) -> None:
    def fake(*args: Any) -> dict[str, int]:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(writer, "_write_landing", fake)


def test_successful_generation_is_recorded(
    monkeypatch: pytest.MonkeyPatch, runs: InMemoryObservabilitySink, tmp_path: Path
) -> None:
    _write(monkeypatch, {"equipment": 3, "sensors": 12, "sensor_readings": 1_000})

    counts = writer.write_landing(None, IotGenerationConfig(), None, tmp_path, sink=runs)  # type: ignore[arg-type]

    [run] = runs.runs
    assert counts["sensor_readings"] == 1_000
    assert (run.pipeline_name, run.project, run.layer, run.table) == (
        "iot_landing",
        "iot",
        "landing",
        tmp_path.as_posix(),
    )
    assert (run.status, run.output_rows, run.rejected_rows) == ("SUCCEEDED", 1_015, 0)


def test_failed_generation_is_recorded_and_raised(
    monkeypatch: pytest.MonkeyPatch, runs: InMemoryObservabilitySink, tmp_path: Path
) -> None:
    _write(monkeypatch, OSError("Volume is read-only"))

    with pytest.raises(OSError, match="read-only"):
        writer.write_landing(None, IotGenerationConfig(), None, tmp_path, sink=runs)  # type: ignore[arg-type]

    [run] = runs.runs
    assert (run.status, run.error_message, run.output_rows) == (
        "FAILED",
        "OSError: Volume is read-only",
        None,
    )
