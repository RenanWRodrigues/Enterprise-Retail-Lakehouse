"""Repository governance contract.

These tests protect the engineering foundation: required documentation,
the sections of CLAUDE.md, git hygiene and the guardrails in .claude/settings.json.
"""

import json
from pathlib import Path

import pytest

REQUIRED_DOCS = (
    "CLAUDE.md",
    "README.md",
    "docs/ARCHITECTURE.md",
    "docs/ENGINEERING_STANDARDS.md",
    "docs/TESTING_STRATEGY.md",
    "docs/DATA_QUALITY.md",
    "docs/SECURITY.md",
    "docs/ROADMAP.md",
)

REQUIRED_CLAUDE_SECTIONS = (
    "# Project Mission",
    "# Projects",
    "# Architecture Principles",
    "# Coding Rules",
    "# Notebook Rules",
    "# Testing Rules",
    "# Security Rules",
    "# Git Safety",
    "# Scope Discipline",
    "# Definition of Done",
    "# Completion Report",
)

COMPLETION_REPORT_HEADINGS = (
    "FILES CREATED",
    "FILES MODIFIED",
    "TESTS",
    "VALIDATION",
    "DECISIONS",
    "KNOWN LIMITATIONS",
    "NEXT STEP",
)

PROJECT_NAMES = (
    "Enterprise Retail Lakehouse",
    "Industrial IoT Lakehouse",
    "Brazilian Financial Data Lakehouse",
)

FORBIDDEN_COMMANDS = (
    "git reset --hard",
    "git clean -fd",
    "git checkout -- .",
    "git restore .",
    "git push --force",
    "rm -rf",
)

REQUIRED_GITIGNORE_PATTERNS = (".env", ".venv/", "__pycache__/", ".databrickscfg")


def _read(repo_root: Path, relative_path: str) -> str:
    return (repo_root / relative_path).read_text(encoding="utf-8")


@pytest.mark.parametrize("relative_path", REQUIRED_DOCS)
def test_required_document_exists_and_is_not_empty(repo_root: Path, relative_path: str) -> None:
    path = repo_root / relative_path
    assert path.is_file(), f"missing required document: {relative_path}"
    assert path.read_text(encoding="utf-8").strip(), f"empty document: {relative_path}"


@pytest.mark.parametrize("section", REQUIRED_CLAUDE_SECTIONS)
def test_claude_md_contains_required_section(repo_root: Path, section: str) -> None:
    lines = _read(repo_root, "CLAUDE.md").splitlines()
    assert section in lines, f"CLAUDE.md is missing section heading: {section!r}"


@pytest.mark.parametrize("heading", COMPLETION_REPORT_HEADINGS)
def test_claude_md_defines_completion_report_heading(repo_root: Path, heading: str) -> None:
    assert heading in _read(repo_root, "CLAUDE.md")


@pytest.mark.parametrize("project_name", PROJECT_NAMES)
def test_claude_md_lists_every_project(repo_root: Path, project_name: str) -> None:
    assert project_name in _read(repo_root, "CLAUDE.md")


@pytest.mark.parametrize("command", FORBIDDEN_COMMANDS)
def test_claude_md_forbids_destructive_command(repo_root: Path, command: str) -> None:
    assert command in _read(repo_root, "CLAUDE.md")


@pytest.mark.parametrize("pattern", REQUIRED_GITIGNORE_PATTERNS)
def test_gitignore_excludes_sensitive_and_generated_files(repo_root: Path, pattern: str) -> None:
    gitignore_lines = _read(repo_root, ".gitignore").splitlines()
    assert pattern in gitignore_lines, f".gitignore must contain {pattern!r}"


def test_claude_settings_deny_destructive_commands(repo_root: Path) -> None:
    settings = json.loads(_read(repo_root, ".claude/settings.json"))
    deny_rules = " ".join(settings["permissions"]["deny"])
    for command in FORBIDDEN_COMMANDS:
        assert command in deny_rules, f".claude/settings.json does not deny {command!r}"


def test_claude_settings_deny_reading_env_files(repo_root: Path) -> None:
    settings = json.loads(_read(repo_root, ".claude/settings.json"))
    assert any(".env" in rule for rule in settings["permissions"]["deny"])


def test_readme_has_no_unfilled_placeholders(repo_root: Path) -> None:
    """Regression (compliance audit): the README published an empty "results" section.

    A placeholder left in a presentation document reads as a claim with nothing behind it.
    """
    documents = [repo_root / "README.md", *sorted((repo_root / "docs").glob("*.md"))]
    unfilled = [
        path.relative_to(repo_root).as_posix()
        for path in documents
        if "<!-- RESULTS -->" in (text := path.read_text(encoding="utf-8")) or "<!-- filled" in text
    ]
    assert unfilled == []
