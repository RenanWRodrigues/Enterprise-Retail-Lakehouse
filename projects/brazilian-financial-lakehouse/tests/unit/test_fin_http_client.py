"""HTTP client: timeout, retry, exponential backoff, status validation, logging."""

import gzip
import logging
import urllib.error
import zlib
from collections.abc import Callable
from random import Random
from typing import Any

import pytest

from financial_lakehouse.clients.http import (
    HttpClient,
    HttpResponse,
    HttpStatusError,
    ResponseFormatError,
    RetriesExhaustedError,
    RetryPolicy,
    build_url,
    decode_body,
)

URL = "https://api.example.invalid/data"
Respond = Callable[..., HttpResponse]


def test_success_returns_the_response_without_retrying(
    http: HttpClient, transport: Any, respond: Respond, sleeps: list[float]
) -> None:
    transport.outcomes.append(respond(200, b'[{"a": 1}]'))

    assert http.get_json(URL) == [{"a": 1}]
    assert transport.calls == [URL]
    assert sleeps == []


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_are_retried(
    http: HttpClient, transport: Any, respond: Respond, status: int, sleeps: list[float]
) -> None:
    transport.outcomes.extend([respond(status), respond(200, b"[]")])

    assert http.get(URL).status == 200
    assert len(transport.calls) == 2
    assert len(sleeps) == 1


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("read timed out"),
        urllib.error.URLError("connection refused"),
        ConnectionResetError(),
    ],
)
def test_timeouts_and_connection_errors_are_retried(
    http: HttpClient, transport: Any, respond: Respond, error: Exception
) -> None:
    transport.outcomes.extend([error, respond(200)])
    assert http.get(URL).status == 200


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_client_errors_fail_immediately(
    http: HttpClient, transport: Any, respond: Respond, status: int
) -> None:
    transport.outcomes.append(respond(status, b'{"erro": "x"}'))

    with pytest.raises(HttpStatusError) as caught:
        http.get(URL)

    assert caught.value.status == status
    assert caught.value.body == b'{"erro": "x"}'
    assert len(transport.calls) == 1


def test_retries_are_bounded_and_report_the_last_failure(
    http: HttpClient, transport: Any, respond: Respond, sleeps: list[float]
) -> None:
    transport.outcomes.extend([respond(503)] * 4)

    with pytest.raises(RetriesExhaustedError) as caught:
        http.get(URL)

    assert caught.value.attempts == 4
    assert "503" in str(caught.value)
    assert len(sleeps) == 3  # no sleep after the last attempt


def test_backoff_is_exponential_capped_and_jittered() -> None:
    policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=8.0, jitter_ratio=0.25)
    rng = Random(42)
    delays = [policy.delay(attempt, rng) for attempt in range(1, 7)]

    nominal = [1, 2, 4, 8, 8, 8]
    for delay, expected in zip(delays, nominal, strict=True):
        assert expected * 0.75 <= delay <= min(expected * 1.25, 8.0)


def test_retry_after_header_is_honoured_and_capped(
    http: HttpClient, transport: Any, respond: Respond, sleeps: list[float]
) -> None:
    transport.outcomes.extend(
        [
            respond(429, headers={"Retry-After": "3"}),
            respond(429, headers={"Retry-After": "120"}),
            respond(200),
        ]
    )

    http.get(URL)

    assert sleeps == [3.0, 8.0]


def test_timeout_is_passed_to_the_transport(respond: Respond) -> None:
    seen: list[float] = []

    def transport(url: str, timeout: float) -> HttpResponse:
        seen.append(timeout)
        return respond(200)

    HttpClient(transport=transport, timeout_seconds=12.5, sleep=lambda _: None).get(URL)

    assert seen == [12.5]


def test_invalid_json_raises_a_format_error(
    http: HttpClient, transport: Any, respond: Respond
) -> None:
    transport.outcomes.append(respond(200, b"<html>maintenance</html>"))
    with pytest.raises(ResponseFormatError):
        http.get_json(URL)


def test_requests_and_retries_are_logged(
    http: HttpClient, transport: Any, respond: Respond, caplog: pytest.LogCaptureFixture
) -> None:
    transport.outcomes.extend([respond(503), respond(200, b"[]")])

    with caplog.at_level(logging.INFO, logger="financial_lakehouse.clients.http"):
        http.get(URL)

    messages = " | ".join(record.getMessage() for record in caplog.records)
    assert "503" in messages and "retry 1/3" in messages
    assert "200" in messages


def test_only_https_urls_are_allowed(http: HttpClient) -> None:
    with pytest.raises(ValueError, match="https"):
        http.get("http://api.example.invalid/data")


def test_build_url_keeps_api_specific_characters() -> None:
    url = build_url(
        "https://h.invalid/p", {"@moeda": "'EUR'", "localidades": "N1[all]", "$top": "10"}
    )
    assert url == "https://h.invalid/p?@moeda='EUR'&localidades=N1[all]&$top=10"


@pytest.mark.parametrize(
    ("encoding", "compress"),
    [
        ("gzip", gzip.compress),
        ("GZIP", gzip.compress),
        ("deflate", zlib.compress),
        ("", lambda b: b),
    ],
)
def test_transport_body_is_decompressed_by_content_encoding(
    encoding: str, compress: Callable[[bytes], bytes]
) -> None:
    """Regression: IBGE answers gzip without being asked (live run, 2026-10-06)."""
    body = b'[{"id": "202401"}]'
    headers = {"Content-Encoding": encoding} if encoding else {}
    assert decode_body(compress(body), headers) == body
