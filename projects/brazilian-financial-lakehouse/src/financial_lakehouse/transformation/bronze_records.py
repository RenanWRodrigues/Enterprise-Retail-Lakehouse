"""Raw pages -> Bronze records (pure functions).

Bronze keeps the delivered ``value`` as text and the raw ``reference``; it adds
``reference_date`` (parsed per source) plus lineage (request/ingestion timestamps, run, raw file).
``record_key`` hashes the *content* (source, series, reference, value, attributes): re-reading the
same observation yields the same key (idempotent), a revised value yields a new key (history).
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Final, Self

from financial_lakehouse.catalog import Indicator, SourceKind
from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.transformation.raw_records import to_raw_observations

PERIOD_LENGTH: Final = 6  # IBGE ids: YYYYMM (monthly, rolling quarter) or YYYYQQ (quarterly)
MONTHS_PER_QUARTER: Final = 3
QUARTERS, MONTHS = range(1, 5), range(1, 13)


def record_key_of(  # noqa: PLR0913 - the content fields plus the optional predecessor
    source: str,
    series_id: str,
    reference: str,
    value: str,
    attributes: dict[str, str],
    *,
    predecessor: str | None = None,
) -> str:
    """Content hash: same observation -> same key (idempotent MERGE); a revised value -> new key.

    ``predecessor`` (fingerprint of the delivery this one replaces, see ``bronze_versions``) makes
    a value that comes *back* after a revision a new version instead of colliding with the first
    one. Without it the key is the plain content hash, so first versions keep their keys.
    """
    content: list[object] = [source, series_id, reference, value, sorted(attributes.items())]
    if predecessor is not None:
        content.append(predecessor)
    canonical = json.dumps(content, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BronzeRecord:
    record_key: str
    source: str
    series_id: str
    indicator_id: str
    reference: str
    reference_date: date
    value: str
    attributes: dict[str, str] = field(compare=True)
    request_timestamp: datetime | None = field(compare=False)
    ingestion_timestamp: datetime = field(compare=False)
    run_id: str = field(compare=False)
    raw_file: str = field(compare=False)
    raw_sha256: str = field(compare=False)

    @classmethod
    def create(  # noqa: PLR0913 - keyword-only constructor that derives the key
        cls,
        *,
        source: str,
        series_id: str,
        indicator_id: str,
        reference: str,
        reference_date: date,
        value: str,
        attributes: dict[str, str],
        request_timestamp: datetime | None,
        ingestion_timestamp: datetime,
        run_id: str,
        raw_file: str,
        raw_sha256: str,
    ) -> Self:
        return cls(
            record_key_of(source, series_id, reference, value, attributes),
            source,
            series_id,
            indicator_id,
            reference,
            reference_date,
            value,
            attributes,
            request_timestamp,
            ingestion_timestamp,
            run_id,
            raw_file,
            raw_sha256,
        )


def series_id_of(indicator: Indicator) -> str:
    """Official identifier: SGS code, PTAX currency, or IBGE agregado/variavel[/classificacao]."""
    if indicator.source is SourceKind.BCB_SGS:
        return str(indicator.series_code)
    if indicator.source is SourceKind.BCB_PTAX:
        return str(indicator.currency)
    parts = [str(indicator.agregado), str(indicator.variavel)]
    if indicator.classificacao:
        parts.append(indicator.classificacao)
    return "/".join(parts)


def _ibge_date(indicator: Indicator, period: str) -> date:
    if len(period) != PERIOD_LENGTH or not period.isdigit():
        raise ValueError(period)
    year, part = int(period[:4]), int(period[4:])
    if indicator.frequency == "quarterly":
        if part not in QUARTERS:
            raise ValueError(period)
        return date(year, (part - 1) * MONTHS_PER_QUARTER + 1, 1)
    if part not in MONTHS:
        raise ValueError(period)
    return date(year, part, 1)


def reference_date_of(indicator: Indicator, reference: str) -> date:
    """SGS ``dd/MM/yyyy``; PTAX bulletin day; IBGE first day of the month/quarter."""
    try:
        if indicator.source is SourceKind.BCB_SGS:
            return datetime.strptime(reference, "%d/%m/%Y").date()  # noqa: DTZ007 - a calendar date
        if indicator.source is SourceKind.BCB_PTAX:
            return date.fromisoformat(reference[:10])
        return _ibge_date(indicator, reference)
    except ValueError as error:
        raise ResponseSchemaError(
            f"{indicator.indicator_id}: unexpected reference {reference!r}"
        ) from error


def to_bronze_records(  # noqa: PLR0913 - keyword-only lineage fields
    indicator: Indicator,
    page: RawPage,
    *,
    run_id: str,
    ingestion_timestamp: datetime,
    raw_file: str,
    raw_sha256: str,
) -> list[BronzeRecord]:
    """One Bronze record per observation of ``page``, with lineage to the landed raw file."""
    series_id = series_id_of(indicator)
    return [
        BronzeRecord.create(
            source=str(indicator.source),
            series_id=series_id,
            indicator_id=indicator.indicator_id,
            reference=observation.reference,
            reference_date=reference_date_of(indicator, observation.reference),
            value=observation.value,
            attributes=observation.attributes,
            request_timestamp=page.requested_at,
            ingestion_timestamp=ingestion_timestamp,
            run_id=run_id,
            raw_file=raw_file,
            raw_sha256=raw_sha256,
        )
        for observation in to_raw_observations(indicator, page)
    ]
