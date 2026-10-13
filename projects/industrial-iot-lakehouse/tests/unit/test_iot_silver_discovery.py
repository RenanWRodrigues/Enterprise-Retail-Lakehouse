"""IoT Silver file planning: discovered Bronze files - processed + explicit reprocessing.

Discovery itself is tested in shared (``test_incremental_discovery.py``).
"""

import pytest

from iot_lakehouse.silver.pipeline import FILE_COLUMNS, plan_files
from lakehouse_common.incremental import Discovery, DiscoveryMode


def _found(*files: str) -> Discovery:
    return Discovery(tuple((f,) for f in files), DiscoveryMode.INCREMENTAL, 0, 2)


def test_discovery_reads_the_file_id() -> None:
    assert FILE_COLUMNS == ("_file_id",)


def test_new_files_are_planned_in_order() -> None:
    assert plan_files(_found("f3", "f2"), processed=set()) == ["f2", "f3"]


def test_processed_files_are_skipped() -> None:
    assert plan_files(_found("f2", "f3"), processed={"f1", "f2"}) == ["f3"]


def test_empty_bronze_plans_nothing() -> None:
    assert plan_files(Discovery((), DiscoveryMode.EMPTY, None, None), processed=set()) == []


def test_explicit_reprocessing_of_a_processed_file() -> None:
    assert plan_files(_found("f3"), processed={"f1"}, reprocess=["f1"]) == ["f1", "f3"]


def test_reprocessing_an_unknown_file_is_refused() -> None:
    with pytest.raises(ValueError, match="f9"):
        plan_files(_found("f3"), processed={"f1"}, reprocess=["f9"])
