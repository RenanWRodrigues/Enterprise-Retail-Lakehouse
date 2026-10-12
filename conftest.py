"""Repository-wide pytest configuration.

Tests live in ``<area>/tests/<category>/`` (``projects/<p>/tests``, ``shared/tests``)
and repository governance tests in ``tests/contract/``. Every test is tagged with
the marker of its category directory, so ``pytest -m unit`` selects all unit tests
across the monorepo without manual decorators, and ``-m "not integration"`` skips
the slow ones. A test outside a category directory is a collection error.
"""

import socket
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Final, NoReturn

import pytest

from lakehouse_testing.markers import category_of

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).parent


SPARK_FIXTURES: Final = frozenset({"spark", "delta_spark"})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Add the category marker derived from each test's directory.

    Unit tests must stay pure Python: one that requests a Spark fixture is a collection
    error (move it to ``transformation``/``data_quality``/``integration``).
    """
    uncategorized: list[str] = []
    spark_in_unit: list[str] = []
    for item in items:
        category = category_of(item.path.relative_to(REPO_ROOT))
        if category is None:
            uncategorized.append(item.nodeid)
            continue
        item.add_marker(getattr(pytest.mark, category))
        if category == "unit" and SPARK_FIXTURES & set(getattr(item, "fixturenames", ())):
            spark_in_unit.append(item.nodeid)
    if uncategorized:
        raise pytest.UsageError(
            "Tests must live in a <area>/tests/<category>/ directory: " + ", ".join(uncategorized)
        )
    if spark_in_unit:
        raise pytest.UsageError("Unit tests must not use Spark: " + ", ".join(spark_in_unit))


@pytest.fixture(autouse=True)
def _no_network_in_unit_tests(request: pytest.FixtureRequest) -> Iterator[None]:
    """Unit tests never reach the network (remote Databricks, public APIs): connecting fails."""
    if request.node.get_closest_marker("unit") is None:
        yield
        return

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        raise RuntimeError(f"unit tests must not open network connections: {args!r}")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(socket.socket, "connect", refuse)
        patch.setattr(socket.socket, "connect_ex", refuse)
        patch.setattr(socket, "create_connection", refuse)
        yield


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Absolute path of the repository root."""
    return REPO_ROOT


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator["SparkSession"]:
    """One local SparkSession for the whole test run (startup costs seconds).

    Imported lazily so suites that never request it (unit, contract) do not pay
    for the JVM. Tests must not rely on state left by other tests (temp views, conf).
    """
    from lakehouse_testing.spark import build_local_spark_session  # noqa: PLC0415 - lazy JVM

    session = build_local_spark_session(tmp_path_factory.mktemp("spark-warehouse"))
    yield session
    session.stop()


@pytest.fixture(scope="session")
def delta_spark(spark: "SparkSession") -> "SparkSession":
    """The shared session, when it has Delta Lake; otherwise the test is skipped.

    Delta needs Hadoop native helpers (winutils) on Windows, so Delta tests run in the
    Linux test container: see docs/TESTING_STRATEGY.md ("Docker").
    """
    from lakehouse_testing.spark import is_delta_enabled  # noqa: PLC0415 - lazy JVM

    if not is_delta_enabled(spark):
        pytest.skip("Delta Lake unavailable here (Windows without HADOOP_HOME): run in Docker")
    return spark
