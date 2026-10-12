"""Types shared by the source clients."""

from dataclasses import dataclass
from datetime import date, datetime


class ResponseSchemaError(Exception):
    """A response parsed as JSON but does not have the documented structure."""


@dataclass(frozen=True, slots=True)
class RawPage:
    """One API response: exact bytes (for the landing zone) + validated records (all text)."""

    source: str
    url: str
    body: bytes
    records: tuple[dict[str, str], ...]
    window_start: date | None = None
    window_end: date | None = None
    requested_at: datetime | None = None  # when the HTTP request was made
