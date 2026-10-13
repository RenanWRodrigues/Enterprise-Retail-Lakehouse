"""``python -m retail_lakehouse.synthetic``: exit codes, files written, determinism, errors.

Runs ``main()`` in-process on the smoke configuration (300 orders) and the incremental plan,
writing to ``tmp_path``. Exit codes: 0 written, 1 the landing zone could not be written,
2 usage or configuration error (argparse convention, as in the Financial ingestion CLI).
"""

import hashlib
import json
from pathlib import Path

import pytest

from retail_lakehouse.synthetic import __main__ as cli
from retail_lakehouse.synthetic.incremental import CHANGE_COLUMNS

CONFIG = Path("projects/enterprise-retail-lakehouse/config")
SOURCES = {"crm", "ecommerce", "erp", "logistics", "payments"}


@pytest.fixture
def smoke(repo_root: Path) -> Path:
    return repo_root / CONFIG / "data_generation_smoke.toml"


@pytest.fixture
def plan(repo_root: Path) -> Path:
    return repo_root / CONFIG / "incremental.toml"


def _tree(root: Path) -> dict[str, str]:
    """Relative path -> sha256 of every file under ``root``."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _exit_code(argv: list[str]) -> int:
    with pytest.raises(SystemExit) as stopped:
        cli.main(argv)
    return int(stopped.value.code or 0)


# --- full batch ----------------------------------------------------------------------------


def test_full_batch_writes_the_landing_zone_and_exits_zero(smoke: Path, tmp_path: Path) -> None:
    output = tmp_path / "new" / "landing"  # missing parents are created

    assert cli.main(["--config", str(smoke), "--output", str(output)]) == 0

    assert {p.name for p in output.iterdir() if p.is_dir()} == SOURCES
    assert (output / "_manifest_initial.json").is_file()
    assert (output / "_anomalies_initial.csv").is_file()
    manifest = json.loads((output / "_manifest_initial.json").read_text(encoding="utf-8"))
    assert manifest["config"]["orders"] == 300  # the smoke size, never the full volume


def test_same_seed_writes_byte_identical_files(smoke: Path, tmp_path: Path) -> None:
    cli.main(["--config", str(smoke), "--output", str(tmp_path / "a")])
    cli.main(["--config", str(smoke), "--output", str(tmp_path / "b")])

    assert _tree(tmp_path / "a") == _tree(tmp_path / "b")


def test_another_seed_writes_different_data(smoke: Path, tmp_path: Path) -> None:
    reseeded = tmp_path / "reseeded.toml"
    text = smoke.read_text(encoding="utf-8")
    assert "seed = 20240101" in text
    reseeded.write_text(text.replace("seed = 20240101", "seed = 7"), encoding="utf-8")

    cli.main(["--config", str(smoke), "--output", str(tmp_path / "a")])
    cli.main(["--config", str(reseeded), "--output", str(tmp_path / "b")])

    a, b = _tree(tmp_path / "a"), _tree(tmp_path / "b")
    assert a.keys() == b.keys()
    assert a != b


# --- incremental batch ---------------------------------------------------------------------


def test_incremental_batch_writes_its_files_and_identity(plan: Path, tmp_path: Path) -> None:
    output = tmp_path / "landing"

    assert cli.main(["--output", str(output), "--plan", str(plan), "--batch", "batch_002"]) == 0

    identity = json.loads((output / "_increment_batch_002.json").read_text(encoding="utf-8"))
    assert identity["batch_id"] == "batch_002"
    assert identity["changes"] > 0
    header = (output / "_changes_batch_002.csv").read_text(encoding="utf-8").splitlines()[0]
    assert header.split(",") == list(CHANGE_COLUMNS)
    assert (output / "_manifest_batch_002.json").is_file()


def test_incremental_batch_is_deterministic(plan: Path, tmp_path: Path) -> None:
    for name in ("a", "b"):
        cli.main(["--output", str(tmp_path / name), "--plan", str(plan), "--batch", "batch_003"])

    assert _tree(tmp_path / "a") == _tree(tmp_path / "b")


# --- usage and configuration errors: exit 2, one line, nothing written ----------------------


@pytest.mark.parametrize(
    "argv",
    [
        [],  # --output is required
        ["--output", "x", "--plan", "p.toml"],  # --plan without --batch
        ["--output", "x", "--batch", "batch_002"],  # --batch without --plan
        ["--output", "x", "--unknown"],
    ],
)
def test_invalid_arguments_are_usage_errors(argv: list[str]) -> None:
    assert _exit_code(argv) == 2


def test_a_missing_configuration_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "landing"

    code = _exit_code(["--config", str(tmp_path / "missing.toml"), "--output", str(output)])

    assert code == 2
    assert "missing.toml" in capsys.readouterr().err
    assert not output.exists()


def test_an_invalid_configuration_is_a_usage_error(tmp_path: Path) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text("seed = [\n", encoding="utf-8")

    assert _exit_code(["--config", str(broken), "--output", str(tmp_path / "out")]) == 2
    assert not (tmp_path / "out").exists()


def test_a_missing_plan_or_unknown_batch_is_a_usage_error(
    plan: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "landing"

    assert _exit_code(["--output", str(output), "--plan", str(tmp_path / "nope.toml"),
                       "--batch", "batch_002"]) == 2  # fmt: skip
    assert _exit_code(["--output", str(output), "--plan", str(plan), "--batch", "batch_999"]) == 2

    assert "batch_999" in capsys.readouterr().err
    assert not output.exists()


# --- write failure: exit 1, logged, no traceback ----------------------------------------------


def test_an_unwritable_output_exits_one_with_a_logged_error(
    smoke: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    blocker = tmp_path / "a_file"
    blocker.write_text("not a directory", encoding="utf-8")

    with caplog.at_level("ERROR", logger="retail_lakehouse.synthetic"):
        code = cli.main(["--config", str(smoke), "--output", str(blocker / "landing")])

    assert code == 1
    assert any("could not write" in r.getMessage() for r in caplog.records)
