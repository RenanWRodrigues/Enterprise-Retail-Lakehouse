"""Version rule of the Bronze MERGE (pure): what is a new version, what is a rerun."""

from datetime import UTC, date, datetime, timedelta

from financial_lakehouse.transformation import (
    BronzeRecord,
    content_key,
    latest_deliveries,
    new_versions,
    version_key,
)
from financial_lakehouse.transformation.bronze_records import record_key_of

T1 = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)
T2, T3 = T1 + timedelta(days=1), T1 + timedelta(days=2)


def _sgs(value: str, at: datetime = T1, reference: str = "01/02/2025") -> BronzeRecord:
    return BronzeRecord.create(
        source="bcb_sgs",
        series_id="189",
        indicator_id="igpm_monthly",
        reference=reference,
        reference_date=date(2025, 2, 1),
        value=value,
        attributes={},
        request_timestamp=at,
        ingestion_timestamp=at,
        run_id=f"{at:%Y%m%dT%H%M%SZ}",
        raw_file="raw.json",
        raw_sha256="abc",
    )


def _ptax(value: str, bulletin: str, buy: str = "6.40") -> BronzeRecord:
    return BronzeRecord.create(
        source="bcb_ptax",
        series_id="EUR",
        indicator_id="eur_brl_ptax",
        reference="2025-01-02 13:03:21.123",
        reference_date=date(2025, 1, 2),
        value=value,
        attributes={"cotacaoCompra": buy, "tipoBoletim": bulletin},
        request_timestamp=T1,
        ingestion_timestamp=T1,
        run_id="R1",
        raw_file="raw.json",
        raw_sha256="abc",
    )


def _store(*batches: list[BronzeRecord]) -> list[BronzeRecord]:
    """Apply each batch as the sinks do and return everything stored."""
    stored: list[BronzeRecord] = []
    for batch in batches:
        stored += new_versions(batch, latest_deliveries(stored))
    return stored


def test_first_version_keeps_the_plain_content_key() -> None:
    """Rows written before the version rule existed keep matching (no migration needed)."""
    [stored] = new_versions([_sgs("1.06")], {})
    assert stored.record_key == record_key_of("bcb_sgs", "189", "01/02/2025", "1.06", {})
    assert stored.record_key == content_key(stored)


def test_same_content_again_is_not_a_new_version() -> None:
    assert new_versions([_sgs("1.06", T2)], latest_deliveries([_sgs("1.06")])) == []


def test_a_changed_value_is_a_new_version() -> None:
    stored = _store([_sgs("1.06")], [_sgs("1.10", T2)])
    assert [r.value for r in stored] == ["1.06", "1.10"]


def test_a_value_returning_after_a_revision_gets_its_own_key() -> None:
    stored = _store([_sgs("1.06")], [_sgs("1.10", T2)], [_sgs("1.06", T3)])

    assert [r.value for r in stored] == ["1.06", "1.10", "1.06"]
    assert len({r.record_key for r in stored}) == 3
    [latest] = latest_deliveries(stored).values()
    assert [(r.value, r.ingestion_timestamp) for r in latest] == [("1.06", T3)]


def test_exact_duplicates_in_a_batch_collapse() -> None:
    assert len(new_versions([_sgs("1.06"), _sgs("1.06")], {})) == 1


def test_conflicting_values_in_one_delivery_are_kept_and_a_rerun_adds_nothing() -> None:
    """Two values for one key in one response: both stored (Silver quarantines the conflict),
    and repeating that response is still idempotent (the set of contents is compared).
    """
    stored = _store([_sgs("1.06"), _sgs("1.07")])
    assert new_versions([_sgs("1.07", T2), _sgs("1.06", T2)], latest_deliveries(stored)) == []


def test_only_keys_whose_delivery_changed_are_inserted() -> None:
    january = "01/01/2025"
    stored = _store([_sgs("1.06"), _sgs("0.27", reference=january)])

    new = new_versions(
        [_sgs("1.06", T2), _sgs("0.29", T2, reference=january)], latest_deliveries(stored)
    )

    assert [(r.reference, r.value) for r in new] == [(january, "0.29")]


def test_ptax_bulletins_of_the_same_instant_are_different_observations() -> None:
    opening, closing = _ptax("6.42", "Abertura"), _ptax("6.42", "Fechamento")
    assert version_key(opening) != version_key(closing)
    assert len(new_versions([opening, closing], {})) == 2


def test_a_change_in_any_delivered_field_is_a_new_version() -> None:
    """PTAX buy rate lives in the attributes: a revision of it alone is still captured."""
    stored = _store([_ptax("6.42", "Fechamento", buy="6.40")])
    revised = _ptax("6.42", "Fechamento", buy="6.41")
    assert new_versions([revised], latest_deliveries(stored)) != []


def test_latest_delivery_groups_by_run() -> None:
    stored = _store([_sgs("1.06"), _sgs("1.07")], [_sgs("1.10", T2)])
    [latest] = latest_deliveries(stored).values()
    assert [r.value for r in latest] == ["1.10"]
