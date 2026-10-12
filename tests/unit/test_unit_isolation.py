"""Unit tests are isolated: no network (remote Databricks, public APIs) and no Spark session."""

import socket
import urllib.request
from pathlib import Path

import pytest


def test_unit_tests_cannot_open_sockets() -> None:
    with pytest.raises(RuntimeError, match="must not open network connections"):
        socket.create_connection(("adb-0000000000000000.0.azuredatabricks.net", 443), timeout=1)


def test_unit_tests_cannot_call_http_apis() -> None:
    with pytest.raises((RuntimeError, OSError)):
        urllib.request.urlopen("https://api.bcb.gov.br/", timeout=1)


def test_unit_tests_do_not_import_databricks_sdks(repo_root: Path) -> None:
    unit_files = [
        *repo_root.glob("projects/*/tests/unit/*.py"),
        *repo_root.glob("shared/tests/unit/*.py"),
        *repo_root.glob("tests/unit/*.py"),
    ]
    offenders = [
        str(path.relative_to(repo_root))
        for path in unit_files
        if path.name != Path(__file__).name
        and any(
            token in path.read_text(encoding="utf-8")
            for token in ("import databricks", "from databricks", "databricks-connect")
        )
    ]
    assert offenders == []
