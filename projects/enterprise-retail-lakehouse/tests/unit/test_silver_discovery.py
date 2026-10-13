"""Retail Silver batch planning: discovered batches - processed + explicit reprocessing.

Discovery itself (Delta log, checkpoint, fallbacks) is tested in shared
(``test_incremental_discovery.py``); here, what the Silver does with its result.
"""

from datetime import UTC, datetime

import pytest

from lakehouse_common.incremental import Discovery, DiscoveryMode
from retail_lakehouse.silver.pipeline import BATCH_COLUMNS, plan_batches

T1 = datetime(2026, 10, 1, tzinfo=UTC)
T2 = datetime(2026, 10, 2, tzinfo=UTC)
T3 = datetime(2026, 10, 3, tzinfo=UTC)


def _found(*units: tuple[str, datetime]) -> Discovery:
    return Discovery(tuple(units), DiscoveryMode.INCREMENTAL, 0, 3)


def test_discovery_reads_batch_id_and_ingestion_timestamp() -> None:
    assert BATCH_COLUMNS == ("_batch_id", "_ingestion_timestamp")


def test_new_batches_are_planned_oldest_first() -> None:
    plan = plan_batches(_found(("b3", T3), ("b2", T2)), processed={})

    assert plan == [("b2", T2), ("b3", T3)]


def test_batches_already_processed_are_skipped() -> None:
    """A previous run failed after b2: the range is read again, b2 is not redone."""
    plan = plan_batches(_found(("b2", T2), ("b3", T3)), processed={"b1": T1, "b2": T2})

    assert plan == [("b3", T3)]


def test_nothing_discovered_plans_nothing() -> None:
    assert plan_batches(Discovery((), DiscoveryMode.EMPTY, None, None), processed={}) == []


def test_explicit_reprocessing_uses_the_recorded_ingestion_time() -> None:
    plan = plan_batches(_found(("b3", T3)), processed={"b1": T1, "b2": T2}, reprocess=["b1"])

    assert plan == [("b1", T1), ("b3", T3)]


def test_reprocessing_a_batch_this_entity_never_had_is_ignored() -> None:
    """Incremental landings may lack an entity: the batch exists for others, not for this one."""
    plan = plan_batches(_found(("b3", T3)), processed={"b1": T1}, reprocess=["b9"])

    assert plan == [("b3", T3)]


def test_reprocess_requires_known_batches_across_entities() -> None:
    from retail_lakehouse.silver.pipeline import validate_reprocess  # noqa: PLC0415

    validate_reprocess(["b1"], known={"b1", "b2"})
    with pytest.raises(ValueError, match="b9"):
        validate_reprocess(["b1", "b9"], known={"b1", "b2"})
