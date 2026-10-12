"""Bronze sinks: insert-only merge by record key (idempotent), revisions kept, persistence."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from financial_lakehouse.storage import BronzeSink, InMemoryBronzeSink, LocalBronzeSink
from financial_lakehouse.transformation import BronzeRecord

TS = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)


def _record(value: str, reference: str = "01/02/2025") -> BronzeRecord:
    return BronzeRecord.create(
        source="bcb_sgs",
        series_id="189",
        indicator_id="igpm_monthly",
        reference=reference,
        reference_date=date(2025, 2, 1),
        value=value,
        attributes={},
        request_timestamp=TS,
        ingestion_timestamp=TS,
        run_id="R1",
        raw_file="raw.json",
        raw_sha256="abc",
    )


@pytest.fixture(params=["memory", "local"])
def sink(request: pytest.FixtureRequest, tmp_path: Path) -> BronzeSink:
    return InMemoryBronzeSink() if request.param == "memory" else LocalBronzeSink(tmp_path)


def test_merge_inserts_new_records_and_reports_how_many(sink: BronzeSink) -> None:
    assert sink.merge([_record("1.06"), _record("0.5", reference="01/03/2025")]) == 2
    assert sink.count() == 2


def test_merging_the_same_records_again_inserts_nothing(sink: BronzeSink) -> None:
    sink.merge([_record("1.06")])
    assert sink.merge([_record("1.06")]) == 0
    assert sink.count() == 1


def test_a_revised_value_is_appended_not_overwritten(sink: BronzeSink) -> None:
    sink.merge([_record("1.06")])
    assert sink.merge([_record("1.07")]) == 1
    assert sorted(r.value for r in sink.records()) == ["1.06", "1.07"]


def test_duplicates_inside_one_batch_are_inserted_once(sink: BronzeSink) -> None:
    assert sink.merge([_record("1.06"), _record("1.06")]) == 1


def test_empty_merge_is_a_no_op(sink: BronzeSink) -> None:
    assert sink.merge([]) == 0


def test_local_sink_is_idempotent_across_processes(tmp_path: Path) -> None:
    LocalBronzeSink(tmp_path).merge([_record("1.06")])
    again = LocalBronzeSink(tmp_path)
    assert again.merge([_record("1.06")]) == 0
    [stored] = again.records()
    assert stored == _record("1.06")


def test_a_value_returning_after_a_revision_is_stored_again(sink: BronzeSink) -> None:
    """Regression: A -> B -> A was dropped (A's content key existed); Silver kept B."""
    later = [
        replace(_record(v), ingestion_timestamp=TS + timedelta(days=d))
        for d, v in enumerate(["1.06", "1.07", "1.06", "1.06"])
    ]

    inserted = [sink.merge([record]) for record in later]

    assert inserted == [1, 1, 1, 0]  # the last one is a rerun of the revert
    assert sorted(r.value for r in sink.records()) == ["1.06", "1.06", "1.07"]
