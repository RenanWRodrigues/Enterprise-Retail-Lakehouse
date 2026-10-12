"""Test categories are derived from directories: ``<area>/tests/<category>/...``."""

from pathlib import Path
from typing import Final

TESTS_DIR_NAME: Final = "tests"
TEST_CATEGORIES: Final = frozenset(
    {"unit", "transformation", "data_quality", "contract", "integration"}
)


def category_of(path: Path) -> str | None:
    """Return the category directly below the closest ``tests`` directory, if any.

    ``path`` is expected relative to the repository root. Returns ``None`` when the
    file is not inside a recognised category directory.
    """
    parts = path.parts
    for index in range(len(parts) - 2, -1, -1):
        if parts[index] == TESTS_DIR_NAME:
            category = parts[index + 1]
            return category if category in TEST_CATEGORIES else None
    return None
