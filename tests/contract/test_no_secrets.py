"""Lightweight secret scan over the repository's text files.

This is a safety net, not a replacement for a dedicated scanner
(see docs/SECURITY.md). It fails if anything resembling a credential is present.
"""

import re
from collections.abc import Iterator
from pathlib import Path

import pytest

EXCLUDED_DIRS = frozenset(
    {".git", ".venv", "venv", ".mypy_cache", ".ruff_cache", ".pytest_cache", "__pycache__"}
)
SCANNED_SUFFIXES = frozenset(
    {".py", ".sql", ".md", ".toml", ".json", ".yml", ".yaml", ".txt", ".cfg", ".ini", ".ipynb"}
    | {".sh", ".ps1"}
)
SCANNED_NAMES = frozenset({"Makefile", "Dockerfile", ".gitignore", ".dockerignore"})
ALLOWED_ENV_FILES = frozenset({".env.example"})
# Placeholders accepted as the password of a connection string in docs and tests.
_PLACEHOLDER = r"(?:password|pass|pwd|senha|\*+|<[^>@]*>|\{[^}@]*\}|\$\{?\w+\}?)"

SECRET_PATTERNS: dict[str, re.Pattern[str]] = {
    "databricks_token": re.compile(r"\bdapi[0-9a-f]{32}\b"),
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    "private_key_block": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    # Any name ending in a credential word: DATABRICKS_TOKEN, client_secret, db_password...
    "hardcoded_credential": re.compile(
        r"(?i)\b\w*(?:password|passwd|secret|api_key|token)\b\s*[:=]\s*['\"][^'\"\s<>{}]{8,}['\"]"
    ),
    "connection_string_password": re.compile(
        rf"\b[a-z][a-z0-9+.-]*://[^/\s:@'\"]+:(?!{_PLACEHOLDER}@)[^/\s@'\"{{}}<>]+@"
    ),
    "bearer_token": re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{20,}"),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    "slack_token": re.compile(r"\bxox[abpors]-[A-Za-z0-9-]{10,}"),
    "google_api_key": re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
}


def _iter_repository_files(repo_root: Path) -> Iterator[Path]:
    for path in repo_root.rglob("*"):
        relative_parts = path.relative_to(repo_root).parts
        if any(part in EXCLUDED_DIRS for part in relative_parts):
            continue
        if path.is_file():
            yield path


def _iter_text_files(repo_root: Path) -> Iterator[Path]:
    for path in _iter_repository_files(repo_root):
        if (
            path.suffix in SCANNED_SUFFIXES
            or path.name in SCANNED_NAMES
            or path.name.startswith(".env")
        ):
            yield path


@pytest.mark.parametrize("pattern_name", sorted(SECRET_PATTERNS))
def test_repository_contains_no_secret_like_values(repo_root: Path, pattern_name: str) -> None:
    pattern = SECRET_PATTERNS[pattern_name]
    offenders = [
        str(path.relative_to(repo_root))
        for path in _iter_text_files(repo_root)
        if pattern.search(path.read_text(encoding="utf-8", errors="ignore"))
    ]
    assert not offenders, f"possible {pattern_name} found in: {offenders}"


def test_no_env_file_is_present(repo_root: Path) -> None:
    env_files = [
        str(path.relative_to(repo_root))
        for path in _iter_repository_files(repo_root)
        if path.name.startswith(".env") and path.name not in ALLOWED_ENV_FILES
    ]
    assert not env_files, f".env files must never live in the repository: {env_files}"


@pytest.mark.parametrize(
    "sample",
    [
        "token = 'dapi" + "0123456789abcdef" * 2 + "'",
        "AWS_KEY=AKIA" + "ABCDEFGHIJKLMNOP",
        "password = '" + "hunter2hunter2" + "'",
        # Shapes added by the security audit (docs/SECURITY_AUDIT.md), assembled so the
        # source itself never matches.
        "DATABRICKS_TOKEN = '" + "abcdef0123456789" + "'",
        "postgresql://etl:" + "Sup3rS3cret" + "@db.example.invalid/x",
        "Authorization: Bearer " + "abcdefghij" * 3,
        "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12,
    ],
)
def test_secret_patterns_detect_known_shapes(sample: str) -> None:
    """Guard the scanner itself: obviously fake samples must be detected."""
    assert any(pattern.search(sample) for pattern in SECRET_PATTERNS.values())


@pytest.mark.parametrize(
    "placeholder",
    [
        "postgresql://user:password@host/db",
        "jdbc:postgresql://usuário:senha@host",
        "jdbc:postgresql://user:***@db/x",
        "https://user:<token>@host",
        "scheme://user:{FAKE}@host",
        "DATABRICKS_HOST=<your-workspace-url>",
        "token = '<DATABRICKS_TOKEN>'",
    ],
)
def test_placeholders_are_not_reported(placeholder: str) -> None:
    assert not any(pattern.search(placeholder) for pattern in SECRET_PATTERNS.values())
