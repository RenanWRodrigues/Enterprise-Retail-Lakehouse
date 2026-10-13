"""HTTP client robustness: truncated reads, corrupt compression, response size limits.

Error categories the caller can tell apart: transport (retried, then ``RetriesExhaustedError``
with ``last_status = None``), HTTP status (429/5xx retried, then ``last_status``; 4xx
``HttpStatusError`` at once), format (``ResponseFormatError``) and size
(``ResponseTooLargeError``), the last two never retried. No test touches the network: the real
transport is exercised against a patched ``urlopen``.
"""

import gzip
import http.client
import logging
import zlib
from collections.abc import Callable, Iterator
from datetime import date
from email.message import Message
from typing import Any

import pytest

from financial_lakehouse.clients.http import (
    DEFAULT_MAX_RESPONSE_BYTES,
    HttpClient,
    HttpError,
    HttpResponse,
    HttpStatusError,
    ResponseFormatError,
    ResponseTooLargeError,
    RetriesExhaustedError,
    RetryPolicy,
    decode_body,
    limited_urllib_transport,
    urllib_transport,
)
from financial_lakehouse.ingestion.pipeline import IngestionSettings

URL = "https://api.example.invalid/data"
Respond = Callable[..., HttpResponse]
BODY = b'[{"data": "02/01/2025", "valor": "6.1923"}]'


def _incomplete() -> http.client.IncompleteRead:
    return http.client.IncompleteRead(b'[{"data": "02/0', 30)


# --- retries: transport, HTTP status --------------------------------------------------------


def test_valid_response_is_returned_once(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.append(respond(200, BODY))

    assert http.get_json(URL) == [{"data": "02/01/2025", "valor": "6.1923"}]
    assert len(transport.calls) == 1


def test_a_transient_incomplete_read_is_retried(
    http: HttpClient, transport: Any, respond: Respond, sleeps: list[float]
) -> None:
    transport.outcomes.extend([_incomplete(), respond(200, BODY)])

    assert http.get(URL).body == BODY
    assert (len(transport.calls), len(sleeps)) == (2, 1)
    assert (http.metrics.failed_requests, http.metrics.successful_requests) == (1, 1)


def test_a_persistent_incomplete_read_stops_after_the_configured_attempts(
    http: HttpClient, transport: Any
) -> None:
    transport.outcomes.extend([_incomplete() for _ in range(10)])

    with pytest.raises(RetriesExhaustedError) as raised:
        http.get(URL)

    assert len(transport.calls) == 4  # policy of the `http` fixture: max_attempts = 4
    assert raised.value.attempts == 4
    assert raised.value.last_status is None  # transport failure, not an HTTP status
    assert "IncompleteRead" in raised.value.last_failure


@pytest.mark.parametrize("persistent", [False, True], ids=["recovers", "exhausted"])
def test_timeouts_are_retried_and_bounded(
    http: HttpClient, transport: Any, respond: Respond, persistent: bool
) -> None:
    timeouts = [TimeoutError("read timed out")] * (4 if persistent else 1)
    transport.outcomes.extend([*timeouts, respond(200, BODY)])

    if persistent:
        with pytest.raises(RetriesExhaustedError):
            http.get(URL)
    else:
        assert http.get(URL).status == 200
    assert len(transport.calls) == (len(timeouts) if persistent else 2)


@pytest.mark.parametrize("status", [429, 500])
def test_retryable_statuses_report_the_last_status_when_exhausted(
    http: HttpClient, transport: Any, respond: Respond, status: int
) -> None:
    transport.outcomes.extend([respond(status, headers={"Retry-After": "1"}) for _ in range(4)])

    with pytest.raises(RetriesExhaustedError) as raised:
        http.get(URL)

    assert (raised.value.last_status, len(transport.calls)) == (status, 4)


def test_not_found_is_not_retried(http: HttpClient, transport: Any, respond: Respond) -> None:
    transport.outcomes.extend([respond(404, b"not found"), respond(200)])

    with pytest.raises(HttpStatusError) as raised:
        http.get(URL)

    assert (raised.value.status, len(transport.calls)) == (404, 1)


def test_empty_body_is_a_response_but_not_json(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.extend([respond(200, b""), respond(200, b"")])

    assert http.get(URL).body == b""
    with pytest.raises(ResponseFormatError):
        http.get_json(URL)


# --- format: compression ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("encoding", "body"),
    [
        ("gzip", b"this is not gzip"),
        ("gzip", gzip.compress(BODY)[:-12]),  # truncated: no end-of-stream marker
        ("deflate", b"this is not deflate"),
        ("deflate", zlib.compress(BODY)[:-4]),
    ],
    ids=["gzip-corrupt", "gzip-truncated", "deflate-corrupt", "deflate-truncated"],
)
def test_broken_compression_is_a_format_error(encoding: str, body: bytes) -> None:
    with pytest.raises(ResponseFormatError, match=encoding):
        decode_body(body, {"Content-Encoding": encoding})


def test_multi_member_gzip_is_fully_decoded() -> None:
    body = gzip.compress(b"[1, ") + gzip.compress(b"2]")

    assert decode_body(body, {"Content-Encoding": "gzip"}) == b"[1, 2]"


def test_a_format_error_from_the_transport_is_not_retried(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.extend([ResponseFormatError("gzip body is corrupt"), respond(200, BODY)])

    with pytest.raises(ResponseFormatError):
        http.get(URL)

    assert len(transport.calls) == 1
    assert http.metrics.failed_requests == 1


# --- size limits -----------------------------------------------------------------------------


def test_the_default_limit_is_explicit() -> None:
    assert DEFAULT_MAX_RESPONSE_BYTES == 32 * 1024 * 1024


def test_decompressed_size_is_limited_too() -> None:
    """A small gzip body that expands past the limit (decompression bomb) is refused."""
    bomb = gzip.compress(b"0" * 10_000)

    with pytest.raises(ResponseTooLargeError):
        decode_body(bomb, {"Content-Encoding": "gzip"}, max_bytes=1_000)


def test_the_client_refuses_a_body_above_its_limit(transport: Any, respond: Respond) -> None:
    http = HttpClient(transport=transport, max_response_bytes=10, sleep=lambda _: None)
    transport.outcomes.extend([respond(200, b"x" * 11), respond(200, b"x" * 11)])

    with pytest.raises(ResponseTooLargeError) as raised:
        http.get(URL)

    assert (raised.value.limit, len(transport.calls)) == (10, 1)  # not retried
    assert isinstance(raised.value, HttpError)


class _FakeRaw:
    """What ``urlopen`` returns: status, headers, chunked ``read`` and a context manager."""

    def __init__(self, body: bytes, headers: dict[str, str], status: int = 200) -> None:
        self.status, self._body, self.reads = status, body, 0
        self.headers = Message()
        for key, value in headers.items():
            self.headers[key] = value

    def read(self, size: int = -1) -> bytes:
        self.reads += 1
        chunk, self._body = (
            (self._body, b"") if size < 0 else (self._body[:size], self._body[size:])
        )
        return chunk

    def __enter__(self) -> "_FakeRaw":
        return self

    def __exit__(self, *_: object) -> None:
        return None


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[_FakeRaw]]:
    """Patch ``urlopen``: each call serves the next queued ``_FakeRaw``."""
    queue: list[_FakeRaw] = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: queue.pop(0))
    yield queue


def test_real_transport_decodes_a_valid_gzip_body(served: list[_FakeRaw]) -> None:
    served.append(_FakeRaw(gzip.compress(BODY), {"Content-Encoding": "gzip"}))

    assert urllib_transport(URL, 5.0).body == BODY


def test_declared_length_above_the_limit_is_refused_before_reading(served: list[_FakeRaw]) -> None:
    raw = _FakeRaw(b"x" * 100, {"Content-Length": "100"})
    served.append(raw)

    with pytest.raises(ResponseTooLargeError):
        limited_urllib_transport(50)(URL, 5.0)
    assert raw.reads == 0


def test_streamed_body_is_cut_at_the_limit(served: list[_FakeRaw]) -> None:
    raw = _FakeRaw(b"x" * (3 * 64 * 1024), {})  # no Content-Length: chunked transfer
    served.append(raw)

    with pytest.raises(ResponseTooLargeError):
        limited_urllib_transport(64 * 1024)(URL, 5.0)
    assert raw.reads <= 2  # stops as soon as the limit is passed, never reads it all


def test_incomplete_read_from_the_real_transport_propagates(
    served: list[_FakeRaw], monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _FakeRaw(BODY, {"Content-Length": "500"})

    def broken(size: int = -1) -> bytes:
        raise _incomplete()

    monkeypatch.setattr(raw, "read", broken)
    served.append(raw)

    with pytest.raises(http.client.IncompleteRead):
        urllib_transport(URL, 5.0)


# --- logs ------------------------------------------------------------------------------------


def test_logs_mask_credentials_and_never_carry_the_payload(
    transport: Any, respond: Respond, caplog: pytest.LogCaptureFixture
) -> None:
    query_value = "abc123credential"
    http = HttpClient(transport=transport, policy=RetryPolicy(max_attempts=2), sleep=lambda _: None)
    transport.outcomes.extend([respond(503), respond(200, b'{"payload": "sensitive"}')])

    with caplog.at_level(logging.DEBUG, logger="financial_lakehouse.clients.http"):
        http.get(URL, {"token": query_value, "page": "2"})

    assert caplog.records
    assert query_value not in caplog.text
    assert "token=***" in caplog.text and "page=2" in caplog.text
    assert "sensitive" not in caplog.text


# --- configuration ---------------------------------------------------------------------------


def test_ingestion_settings_carry_a_validated_size_limit() -> None:
    start = date(2015, 1, 1)

    assert IngestionSettings(backfill_start=start).max_response_bytes == DEFAULT_MAX_RESPONSE_BYTES
    assert (
        IngestionSettings(backfill_start=start, max_response_bytes=1024).max_response_bytes == 1024
    )
    for invalid in ({"max_response_bytes": 0}, {"timeout_seconds": 0.0}):
        with pytest.raises(ValueError, match="positive"):
            IngestionSettings(backfill_start=start, **invalid)


def test_client_and_transport_reject_non_positive_limits() -> None:
    with pytest.raises(ValueError, match="positive"):
        HttpClient(max_response_bytes=0)
    with pytest.raises(ValueError, match="positive"):
        limited_urllib_transport(0)
