"""Reproducible installs: pyproject (intent) -> uv.lock (resolution) -> requirements (install).

``make install`` and the CI install ``requirements/dev.lock.txt`` with ``--require-hashes``;
``make lock-check`` (CI, needs uv) re-resolves. These checks need no uv and run everywhere:
the three files agree, every package is hashed, nothing local or secret leaks into them, and the
Databricks runtime still installs nothing (PySpark comes from the runtime).
"""

import re
import tomllib
from pathlib import Path

LOCK = Path("uv.lock")
EXPORT = Path("requirements/dev.lock.txt")
PIN = re.compile(r"^([A-Za-z0-9_.-]+)(?:\[[^\]]+\])?==([^;\s]+)")
LOCAL_OR_SECRET = re.compile(r"(?i)file:|[A-Z]:\\|/users/|/home/|token|password|://[^/\s]*@")


def _name(raw: str) -> str:
    return re.sub(r"[-_.]+", "-", raw).lower()


def _pyproject(repo_root: Path) -> dict[str, object]:
    return tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))


def _dev_pins(repo_root: Path) -> dict[str, str]:
    group = _pyproject(repo_root)["dependency-groups"]["dev"]  # type: ignore[index]
    return {_name(m.group(1)): m.group(2) for spec in group if (m := PIN.match(spec))}


def _locked(repo_root: Path) -> dict[str, str]:
    lock = tomllib.loads((repo_root / LOCK).read_text(encoding="utf-8"))
    return {_name(p["name"]): p["version"] for p in lock["package"] if "version" in p}


def _exported(repo_root: Path) -> dict[str, str]:
    lines = (repo_root / EXPORT).read_text(encoding="utf-8").splitlines()
    return {_name(m.group(1)): m.group(2) for line in lines if (m := PIN.match(line))}


def test_every_direct_pin_is_the_locked_and_the_installed_version(repo_root: Path) -> None:
    locked, exported = _locked(repo_root), _exported(repo_root)

    for name, version in _dev_pins(repo_root).items():
        assert (locked.get(name), exported.get(name)) == (version, version), name


def test_the_export_is_exactly_the_lock(repo_root: Path) -> None:
    locked = _locked(repo_root)
    locked.pop(_name(str(_pyproject(repo_root)["project"]["name"])))  # type: ignore[index]  # not installed

    assert _exported(repo_root) == locked


def test_every_exported_package_is_hashed(repo_root: Path) -> None:
    text = (repo_root / EXPORT).read_text(encoding="utf-8")
    entries = re.split(r"\n(?=[A-Za-z0-9])", text.split("\n", 2)[2])

    unhashed = [e.split()[0] for e in entries if PIN.match(e) and "--hash=sha256:" not in e]
    assert unhashed == []


def test_no_local_path_or_secret_in_the_lock_files(repo_root: Path) -> None:
    for path in (LOCK, EXPORT):
        lines = (repo_root / path).read_text(encoding="utf-8").splitlines()
        leaks = [line for line in lines if LOCAL_OR_SECRET.search(line)]
        assert leaks == [], path


def test_the_lock_targets_the_supported_python(repo_root: Path) -> None:
    lock = tomllib.loads((repo_root / LOCK).read_text(encoding="utf-8"))

    assert lock["requires-python"] == _pyproject(repo_root)["project"]["requires-python"]  # type: ignore[index]


def test_the_databricks_runtime_installs_nothing(repo_root: Path) -> None:
    """PySpark and Delta come from the runtime; installing them over it breaks the session."""
    assert _pyproject(repo_root)["project"]["dependencies"] == []  # type: ignore[index]
