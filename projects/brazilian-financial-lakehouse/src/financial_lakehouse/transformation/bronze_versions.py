"""Which incoming Bronze records are new *versions* of an observation (pure functions).

An observation is identified by its version key: source, series, reference and the attributes that
tell two observations of the same reference apart (PTAX bulletin type, IBGE location). A
*delivery* of a key is what one run stored for it (same ``ingestion_timestamp``).

Rule, per key: the incoming records are stored only if their contents differ from the latest
stored delivery of that key.

- same contents as the latest delivery (rerun, lookback overlap): nothing is inserted;
- different contents (revision, also a value *returning* after a revision, A -> B -> A): the whole
  incoming delivery is inserted, keyed with the fingerprint of the delivery it replaces, so it never
  collides with an older identical row and becomes the latest version Silver picks;
- no stored delivery (first version): the plain content key, as before this rule existed.

Precondition: every run has its own ``ingestion_timestamp`` (the run clock), which orders
deliveries the same way Silver's ``request_timestamp`` does.
"""

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from typing import Final

from financial_lakehouse.catalog import SourceKind
from financial_lakehouse.transformation.bronze_records import BronzeRecord, record_key_of

VersionKey = tuple[str, str, str, tuple[tuple[str, str], ...]]

IDENTITY_ATTRIBUTES: Final[Mapping[str, tuple[str, ...]]] = {
    SourceKind.BCB_SGS: (),
    SourceKind.BCB_PTAX: ("tipoBoletim",),  # several bulletins a day, one value each
    SourceKind.IBGE_AGREGADOS: ("localidade",),
}


def version_key(record: BronzeRecord) -> VersionKey:
    names = IDENTITY_ATTRIBUTES.get(record.source, ())
    identity = tuple((name, record.attributes.get(name, "")) for name in names)
    return (record.source, record.series_id, record.reference, identity)


def content_key(record: BronzeRecord) -> str:
    """Key of the content alone (no predecessor): equal for equal deliveries of an observation."""
    return record_key_of(
        record.source, record.series_id, record.reference, record.value, record.attributes
    )


def latest_deliveries(stored: Iterable[BronzeRecord]) -> dict[VersionKey, list[BronzeRecord]]:
    """Records of the most recent delivery of every version key in ``stored``."""
    groups: dict[VersionKey, list[BronzeRecord]] = {}
    for record in stored:
        groups.setdefault(version_key(record), []).append(record)
    latest: dict[VersionKey, list[BronzeRecord]] = {}
    for key, records in groups.items():
        newest = max(record.ingestion_timestamp for record in records)
        latest[key] = [record for record in records if record.ingestion_timestamp == newest]
    return latest


def _fingerprint(delivery: Sequence[BronzeRecord]) -> str:
    keys = "\n".join(sorted(record.record_key for record in delivery))
    return hashlib.sha256(keys.encode("utf-8")).hexdigest()


def new_versions(
    incoming: Sequence[BronzeRecord],
    stored_latest: Mapping[VersionKey, Sequence[BronzeRecord]],
) -> list[BronzeRecord]:
    """Incoming records to insert (in arrival order), re-keyed when they replace a delivery.

    Exact duplicates inside ``incoming`` collapse to their first occurrence.
    """
    deliveries: dict[VersionKey, dict[str, BronzeRecord]] = {}
    for record in incoming:
        deliveries.setdefault(version_key(record), {}).setdefault(content_key(record), record)
    inserts: list[BronzeRecord] = []
    for key, contents in deliveries.items():
        latest = stored_latest.get(key, ())
        if set(contents) == {content_key(record) for record in latest}:
            continue
        predecessor = _fingerprint(latest) if latest else None
        inserts.extend(
            replace(
                record,
                record_key=record_key_of(
                    record.source,
                    record.series_id,
                    record.reference,
                    record.value,
                    record.attributes,
                    predecessor=predecessor,
                ),
            )
            for record in contents.values()
        )
    return inserts
