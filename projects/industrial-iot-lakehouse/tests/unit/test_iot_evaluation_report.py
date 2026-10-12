"""Report rendering and the leakage guard of the detector module (pure Python)."""

import ast
from pathlib import Path
from typing import Any

from iot_lakehouse.evaluation.report import render_report

DETECTOR = Path("projects/industrial-iot-lakehouse/src/iot_lakehouse/evaluation/detector.py")


def _metrics(precision: Any) -> dict[str, Any]:
    confusion = {
        "tp": 0,
        "fp": 0,
        "tn": 0,
        "fn": 0,
        "precision": precision,
        "recall": None,
        "f1": None,
    }
    return {
        "failures": 0,
        "alert_events": 0,
        "event": confusion,
        "day": {**confusion, "false_positive_rate": None},
        "latency": {"median_lead_time_hours": None, "min_lead_time_hours": None},
    }


GENERATION = {"seed": "1", "start": "2025-01-01", "days": "2", "sampling_interval_seconds": "900"}
REPORT = {
    "parameters": {
        "generation": GENERATION,
        "thresholds": [0.1], "chosen_threshold": 0.1, "baseline_days": 7, "min_history_days": 3,
        "tolerance_before_hours": 0.0, "tolerance_after_hours": 24.0,
    },
    "dataset": {
        "readings_checksum": 42, "equipment": 2, "sensors": 4, "sensor_days": 8, "failures": 0,
        "drift_episodes": 0, "calibration_equipment": ["E1"], "evaluation_equipment": ["E2"],
    },
    "calibration_sweep": [{"threshold": 0.1, **_metrics(None)}],
    "evaluation": _metrics(None),
}  # fmt: skip


def test_undefined_metrics_are_reported_as_such_never_as_zero() -> None:
    text = render_report(REPORT, generated_at="2026-10-09 12:00")

    assert "| precision | n/a (undefined: zero denominator) |" in text
    assert "| false_positive_rate | n/a (undefined: zero denominator) |" in text
    assert "Readings checksum: `42`" in text and "**chosen: 0.1**" in text


def test_the_same_results_render_the_same_report() -> None:
    first = render_report(REPORT, generated_at="2026-10-09 12:00")

    assert render_report(REPORT, generated_at="2026-10-09 12:00") == first


def test_the_detector_imports_nothing_from_the_generator_or_the_ground_truth(
    repo_root: Path,
) -> None:
    tree = ast.parse((repo_root / DETECTOR).read_text(encoding="utf-8"))
    modules = {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }

    assert not {m for m in modules if "synthetic" in m or "ground_truth" in m or "matching" in m}
