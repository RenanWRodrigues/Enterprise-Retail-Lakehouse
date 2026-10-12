from pathlib import Path

import pytest

from lakehouse_testing.markers import TEST_CATEGORIES, category_of


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    [
        ("projects/enterprise-retail-lakehouse/tests/unit/test_a.py", "unit"),
        ("projects/industrial-iot-lakehouse/tests/integration/nested/test_b.py", "integration"),
        ("shared/tests/data_quality/test_c.py", "data_quality"),
        ("tests/contract/test_d.py", "contract"),
        ("projects/brazilian-financial-lakehouse/tests/transformation/test_e.py", "transformation"),
    ],
)
def test_category_of_returns_directory_below_tests(relative_path: str, expected: str) -> None:
    assert category_of(Path(relative_path)) == expected


@pytest.mark.parametrize(
    "relative_path",
    [
        "projects/enterprise-retail-lakehouse/tests/test_uncategorized.py",
        "tests/smoke/test_unknown_category.py",
        "projects/enterprise-retail-lakehouse/src/retail_lakehouse/unit/test_misplaced.py",
    ],
)
def test_category_of_returns_none_outside_category_directories(relative_path: str) -> None:
    assert category_of(Path(relative_path)) is None


def test_categories_match_registered_pytest_markers(pytestconfig: pytest.Config) -> None:
    registered = {line.split(":")[0] for line in pytestconfig.getini("markers")}
    assert registered >= TEST_CATEGORIES
