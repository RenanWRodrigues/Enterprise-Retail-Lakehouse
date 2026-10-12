"""Contract for the Claude Code skill library in .claude/skills/.

Each skill must exist, carry valid frontmatter, stay short and be referenced
from CLAUDE.md so the index never drifts from the library.
"""

import re
from pathlib import Path

import pytest

SKILLS_DIR = Path(".claude/skills")
REQUIRED_SKILLS = (
    "architecture",
    "databricks",
    "pyspark",
    "medallion",
    "data-quality",
    "testing",
    "dimensional-modeling",
    "api-ingestion",
    "documentation",
    "security",
)
MAX_SKILL_LINES = 150
MAX_DESCRIPTION_CHARS = 1024
SKILL_NAME_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
FRONTMATTER_DELIMITER = "---"


def _parse_frontmatter(text: str) -> dict[str, str]:
    lines = text.splitlines()
    assert lines and lines[0] == FRONTMATTER_DELIMITER, "SKILL.md must start with frontmatter"
    closing_index = lines.index(FRONTMATTER_DELIMITER, 1)
    fields: dict[str, str] = {}
    for line in lines[1:closing_index]:
        key, _, value = line.partition(":")
        fields[key.strip()] = value.strip()
    return fields


def _skill_text(repo_root: Path, skill: str) -> str:
    return (repo_root / SKILLS_DIR / skill / "SKILL.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("skill", REQUIRED_SKILLS)
def test_skill_file_exists(repo_root: Path, skill: str) -> None:
    assert (repo_root / SKILLS_DIR / skill / "SKILL.md").is_file()


@pytest.mark.parametrize("skill", REQUIRED_SKILLS)
def test_skill_frontmatter_is_valid(repo_root: Path, skill: str) -> None:
    fields = _parse_frontmatter(_skill_text(repo_root, skill))
    assert fields.get("name") == skill, "frontmatter name must match the directory name"
    assert SKILL_NAME_PATTERN.match(fields["name"])
    description = fields.get("description", "")
    assert description, "description drives skill selection and must not be empty"
    assert len(description) <= MAX_DESCRIPTION_CHARS


@pytest.mark.parametrize("skill", REQUIRED_SKILLS)
def test_skill_is_short(repo_root: Path, skill: str) -> None:
    line_count = len(_skill_text(repo_root, skill).splitlines())
    assert line_count <= MAX_SKILL_LINES, f"{skill} has {line_count} lines"


@pytest.mark.parametrize("skill", REQUIRED_SKILLS)
def test_claude_md_references_skill(repo_root: Path, skill: str) -> None:
    claude_md = (repo_root / "CLAUDE.md").read_text(encoding="utf-8")
    assert f"`{skill}`" in claude_md, f"CLAUDE.md must list the {skill!r} skill"


def test_no_unlisted_skills(repo_root: Path) -> None:
    present = {path.parent.name for path in (repo_root / SKILLS_DIR).glob("*/SKILL.md")}
    assert present == set(REQUIRED_SKILLS)
