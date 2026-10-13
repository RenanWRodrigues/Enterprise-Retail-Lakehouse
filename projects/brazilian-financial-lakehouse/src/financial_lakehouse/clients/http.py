"""HTTP client for public APIs: timeout, retry with exponential backoff + jitter, status
validation, size limits and logging. Standard library only (``urllib``); the transport is
injectable, so tests never touch the network.

Error categories (all ``HttpError``): transport failures and 429/5xx are retried, then
``RetriesExhaustedError`` (``last_status`` None for transport, else the status); other statuses
raise ``HttpStatusError`` at once; ``ResponseFormatError`` (corrupt or truncated compression,
non-JSON body) and ``ResponseTooLargeError`` are never retried. Logs carry the URL with
secret-looking query values masked and the body size, never the body.
"""

import http.client
import json
import logging
import time
import urllib.error
import urllib.request
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from email.message import Message
from random import Random
from typing import Any, Final, Protocol
from urllib.parse import quote, urlsplit

from lakehouse_common.observability import mask_secrets

logger = logging.getLogger(__name__)

USER_AGENT: Final = "databricks-lakehouse-portfolio/0.1 (+public data ingestion)"
URL_SAFE_CHARACTERS: Final = "/'@$[]|(),=:"  # OData and IBGE syntax must reach the API verbatim
HTTP_OK_MIN, HTTP_OK_MAX = 200, 299
# Largest body accepted, compressed or decoded. The largest official payload recorded (a
# full SGS daily series window) is well under 1 MiB, so 32 MiB only stops runaway answers.
DEFAULT_MAX_RESPONSE_BYTES: Final = 32 * 1024 * 1024
READ_CHUNK_BYTES: Final = 64 * 1024
_GZIP_WBITS: Final = 16 + zlib.MAX_WBITS  # gzip container
_ZLIB_WBITS: Final = zlib.MAX_WBITS  # Content-Encoding: deflate = zlib stream (RFC 9110)


class HttpError(Exception):
    """Base error of the HTTP layer."""


class HttpStatusError(HttpError):
    """Non-retryable status (e.g. 4xx). Keeps status and body for the caller to inspect."""

    def __init__(
        self, url: str, status: int, body: bytes, requested_at: datetime | None = None
    ) -> None:
        super().__init__(f"GET {mask_secrets(url)} -> HTTP {status}")
        self.url, self.status, self.body, self.requested_at = url, status, body, requested_at


class RetriesExhaustedError(HttpError):
    """Every attempt failed transiently. ``last_status``: the last HTTP status (429/5xx), or
    None when the last attempt failed in transport (timeout, connection, truncated read)."""

    def __init__(
        self, url: str, attempts: int, last_failure: str, last_status: int | None = None
    ) -> None:
        super().__init__(
            f"GET {mask_secrets(url)} failed after {attempts} attempts (last: {last_failure})"
        )
        self.url, self.attempts, self.last_failure = url, attempts, last_failure
        self.last_status = last_status


class ResponseFormatError(HttpError):
    """The body is not what the API documents (e.g. HTML maintenance page instead of JSON,
    corrupt or truncated gzip/deflate)."""


class ResponseTooLargeError(HttpError):
    """The body (as received or once decompressed) exceeds the configured limit."""

    def __init__(self, limit: int, url: str | None = None) -> None:
        where = f"GET {mask_secrets(url)}: " if url else ""
        super().__init__(f"{where}response larger than {limit} bytes")
        self.limit, self.url = limit, url


@dataclass(frozen=True, slots=True)
class HttpResponse:
    url: str
    status: int
    headers: Mapping[str, str]
    body: bytes
    requested_at: datetime | None = None  # set by HttpClient when the attempt started

    def json(self) -> Any:  # noqa: ANN401 - JSON is untyped by nature
        try:
            return json.loads(self.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ResponseFormatError(f"{self.url}: response is not JSON") from error


Transport = Callable[[str, float], HttpResponse]


def _inflate(body: bytes, wbits: int, encoding: str, max_bytes: int, url: str | None) -> bytes:
    """Decompress every member, at most ``max_bytes`` of output; corrupt or cut = format error."""
    output = bytearray()
    data = body
    while data:
        stream = zlib.decompressobj(wbits)
        try:  # max_length >= 1 here (0 would mean unlimited)
            output += stream.decompress(data, max_bytes - len(output) + 1)
        except zlib.error as error:
            raise ResponseFormatError(f"{encoding} body is corrupt: {error}") from error
        if len(output) > max_bytes or stream.unconsumed_tail:
            raise ResponseTooLargeError(max_bytes, url)
        if not stream.eof:
            raise ResponseFormatError(f"{encoding} body is truncated")
        data = stream.unused_data if wbits == _GZIP_WBITS else b""  # gzip may be multi-member
    return bytes(output)


def decode_body(
    body: bytes,
    headers: Mapping[str, str],
    *,
    max_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    url: str | None = None,
) -> bytes:
    """Undo ``Content-Encoding``. IBGE sends gzip even when the client did not ask for it
    (observed 2026-10-06); ``urllib`` does not decompress on its own.

    Raises ``ResponseFormatError`` for corrupt or truncated compression and
    ``ResponseTooLargeError`` when the decoded body would exceed ``max_bytes``.
    """
    encoding = next((v for k, v in headers.items() if k.lower() == "content-encoding"), "")
    encoding = encoding.strip().lower()
    if encoding == "gzip":
        return _inflate(body, _GZIP_WBITS, encoding, max_bytes, url)
    if encoding == "deflate":
        return _inflate(body, _ZLIB_WBITS, encoding, max_bytes, url)
    return body


class _Readable(Protocol):
    headers: Message

    def read(self, size: int = -1, /) -> bytes: ...


def _read_limited(raw: _Readable, url: str, max_bytes: int) -> bytes:
    """Read in chunks and stop as soon as the body passes ``max_bytes`` (never all of it)."""
    declared = (raw.headers.get("Content-Length") or "").strip()
    if declared.isdigit() and int(declared) > max_bytes:
        raise ResponseTooLargeError(max_bytes, url)
    chunks: list[bytes] = []
    received = 0
    while chunk := raw.read(READ_CHUNK_BYTES):
        received += len(chunk)
        if received > max_bytes:
            raise ResponseTooLargeError(max_bytes, url)
        chunks.append(chunk)
    return b"".join(chunks)


def _fetch(url: str, timeout: float, max_bytes: int) -> HttpResponse:
    request = urllib.request.Request(  # noqa: S310 - scheme validated by HttpClient (https only)
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as raw:  # noqa: S310
            headers = dict(raw.headers.items())
            body = _read_limited(raw, url, max_bytes)
            return HttpResponse(
                url, raw.status, headers, decode_body(body, headers, max_bytes=max_bytes, url=url)
            )
    except urllib.error.HTTPError as error:
        message: Message = error.headers
        error_headers = dict(message.items())
        body = _read_limited(error, url, max_bytes)
        return HttpResponse(
            url,
            error.code,
            error_headers,
            decode_body(body, error_headers, max_bytes=max_bytes, url=url),
        )


def limited_urllib_transport(max_bytes: int) -> "Transport":
    """Real transport refusing bodies above ``max_bytes`` (received or decoded)."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")

    def transport(url: str, timeout: float) -> HttpResponse:
        return _fetch(url, timeout, max_bytes)

    return transport


def urllib_transport(url: str, timeout: float) -> HttpResponse:
    """Real transport (``DEFAULT_MAX_RESPONSE_BYTES``). HTTP error statuses are returned, not
    raised, for the client to decide; a truncated read raises ``http.client.IncompleteRead``."""
    return _fetch(url, timeout, DEFAULT_MAX_RESPONSE_BYTES)


def build_url(base: str, params: Mapping[str, str] | None = None) -> str:
    """Append ``params`` keeping the characters OData/IBGE syntax needs (``@ ' $ [ ] |``)."""
    if not params:
        return base
    query = "&".join(
        f"{quote(key, safe=URL_SAFE_CHARACTERS)}={quote(value, safe=URL_SAFE_CHARACTERS)}"
        for key, value in params.items()
    )
    return f"{base}?{query}"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    jitter_ratio: float = 0.25
    retry_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )

    def delay(self, attempt: int, rng: Random) -> float:
        """``base * 2**(attempt-1)``, capped, +/- jitter (still capped)."""
        nominal = min(self.max_delay_seconds, self.base_delay_seconds * float(2 ** (attempt - 1)))
        jittered = nominal * (1 + self.jitter_ratio * rng.uniform(-1.0, 1.0))
        return min(self.max_delay_seconds, max(0.0, jittered))


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
    return float(value) if value is not None and value.strip().isdigit() else None


# Retried: the GET is idempotent, so a timeout, a dropped connection or a body cut short
# (IncompleteRead: fewer bytes than Content-Length) can safely be asked again.
TRANSIENT_ERRORS: Final = (
    TimeoutError,
    ConnectionError,
    urllib.error.URLError,
    http.client.IncompleteRead,
)
# Never retried: the same request would get the same body.
BODY_ERRORS: Final = (ResponseFormatError, ResponseTooLargeError)


@dataclass
class RequestMetrics:
    """Counts every HTTP attempt (retries included). Failed = exception or non-2xx status."""

    requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    def record(self, *, success: bool) -> None:
        self.requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1


@dataclass(frozen=True, slots=True)
class _Retry:
    """A failed attempt worth repeating: why, the HTTP status (None: transport), Retry-After."""

    failure: str
    status: int | None = None
    wait: float | None = None


class HttpClient:
    def __init__(  # noqa: PLR0913 - keyword-only, all injectable for tests
        self,
        *,
        transport: Transport = urllib_transport,
        policy: RetryPolicy | None = None,
        timeout_seconds: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        rng: Random | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        metrics: RequestMetrics | None = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        if timeout_seconds <= 0 or max_response_bytes < 1:
            raise ValueError("timeout_seconds and max_response_bytes must be positive")
        self.metrics = metrics or RequestMetrics()
        self._clock = clock
        self._transport = transport
        self._policy = policy or RetryPolicy()
        self._timeout = timeout_seconds
        self._sleep = sleep
        self._rng = rng or Random()  # noqa: S311 - jitter for retry timing, not security
        self._max_bytes = max_response_bytes

    def get(self, url: str, params: Mapping[str, str] | None = None) -> HttpResponse:
        """GET with bounded retries on 429/5xx and transport failures (timeouts, connection,
        truncated reads). Raises ``HttpStatusError`` (other statuses, at once),
        ``ResponseFormatError`` / ``ResponseTooLargeError`` (never retried) or
        ``RetriesExhaustedError`` after ``policy.max_attempts`` attempts."""
        full_url = build_url(url, params)
        if urlsplit(full_url).scheme != "https":
            raise ValueError(f"only https URLs are allowed: {mask_secrets(full_url)}")
        shown = mask_secrets(full_url)  # what logs may carry
        attempts = self._policy.max_attempts
        retry = _Retry("no attempt")
        for attempt in range(1, attempts + 1):
            requested_at = self._clock()
            outcome = self._attempt(full_url, shown, attempt, requested_at)
            if isinstance(outcome, HttpResponse):
                return outcome
            retry = outcome
            if attempt == attempts:
                break
            self._wait(shown, retry, attempt, attempts)
        raise RetriesExhaustedError(full_url, attempts, retry.failure, retry.status)

    def _attempt(
        self, full_url: str, shown: str, attempt: int, requested_at: datetime
    ) -> HttpResponse | _Retry:
        """One request: the response, a retryable failure, or a non-retryable error raised."""
        try:
            response = self._transport(full_url, self._timeout)
        except TRANSIENT_ERRORS as error:
            self.metrics.record(success=False)
            return _Retry(f"{type(error).__name__}: {mask_secrets(str(error))}")
        except BODY_ERRORS as error:
            self.metrics.record(success=False)
            logger.warning("GET %s: %s (not retryable)", shown, type(error).__name__)
            raise
        if len(response.body) > self._max_bytes:
            self.metrics.record(success=False)
            logger.warning("GET %s: body above %d bytes (not retryable)", shown, self._max_bytes)
            raise ResponseTooLargeError(self._max_bytes, full_url)
        succeeded = HTTP_OK_MIN <= response.status <= HTTP_OK_MAX
        self.metrics.record(success=succeeded)
        if succeeded:
            logger.info(
                "GET %s -> %d (%d bytes, attempt %d)",
                shown,
                response.status,
                len(response.body),
                attempt,
            )
            return replace(response, requested_at=requested_at)
        if response.status not in self._policy.retry_statuses:
            logger.warning("GET %s -> %d (not retryable)", shown, response.status)
            raise HttpStatusError(full_url, response.status, response.body, requested_at)
        return _Retry(f"HTTP {response.status}", response.status, _retry_after(response.headers))

    def _wait(self, shown: str, retry: _Retry, attempt: int, attempts: int) -> None:
        delay = (
            min(retry.wait, self._policy.max_delay_seconds)
            if retry.wait is not None
            else self._policy.delay(attempt, self._rng)
        )
        logger.warning(
            "GET %s failed (%s); retry %d/%d in %.1fs",
            shown,
            retry.failure,
            attempt,
            attempts - 1,
            delay,
        )
        self._sleep(delay)

    def get_json(self, url: str, params: Mapping[str, str] | None = None) -> Any:  # noqa: ANN401
        return self.get(url, params).json()


def parse_json_text(response: HttpResponse) -> Any:  # noqa: ANN401 - JSON is untyped by nature
    """JSON where every number stays text: raw data keeps the exact decimals the API sent."""
    try:
        return json.loads(response.body, parse_float=str, parse_int=str)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResponseFormatError(f"{response.url}: response is not JSON") from error
