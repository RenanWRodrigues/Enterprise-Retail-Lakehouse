"""HTTP client for public APIs: timeout, retry with exponential backoff + jitter, status
validation and logging. Standard library only (``urllib``); the transport is injectable, so
tests never touch the network.
"""

import gzip
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
from typing import Any, Final
from urllib.parse import quote, urlsplit

logger = logging.getLogger(__name__)

USER_AGENT: Final = "databricks-lakehouse-portfolio/0.1 (+public data ingestion)"
URL_SAFE_CHARACTERS: Final = "/'@$[]|(),=:"  # OData and IBGE syntax must reach the API verbatim
HTTP_OK_MIN, HTTP_OK_MAX = 200, 299


class HttpError(Exception):
    """Base error of the HTTP layer."""


class HttpStatusError(HttpError):
    """Non-retryable status (e.g. 4xx). Keeps status and body for the caller to inspect."""

    def __init__(
        self, url: str, status: int, body: bytes, requested_at: datetime | None = None
    ) -> None:
        super().__init__(f"GET {url} -> HTTP {status}")
        self.url, self.status, self.body, self.requested_at = url, status, body, requested_at


class RetriesExhaustedError(HttpError):
    def __init__(self, url: str, attempts: int, last_failure: str) -> None:
        super().__init__(f"GET {url} failed after {attempts} attempts (last: {last_failure})")
        self.url, self.attempts, self.last_failure = url, attempts, last_failure


class ResponseFormatError(HttpError):
    """The body is not what the API documents (e.g. HTML maintenance page instead of JSON)."""


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


def decode_body(body: bytes, headers: Mapping[str, str]) -> bytes:
    """Undo ``Content-Encoding``. IBGE sends gzip even when the client did not ask for it
    (observed 2026-10-06); ``urllib`` does not decompress on its own."""
    encoding = next((v for k, v in headers.items() if k.lower() == "content-encoding"), "")
    encoding = encoding.strip().lower()
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        return zlib.decompress(body)
    return body


def urllib_transport(url: str, timeout: float) -> HttpResponse:
    """Real transport. HTTP error statuses are returned (not raised) for the client to decide."""
    request = urllib.request.Request(  # noqa: S310 - scheme validated by HttpClient (https only)
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as raw:  # noqa: S310
            headers = dict(raw.headers.items())
            return HttpResponse(url, raw.status, headers, decode_body(raw.read(), headers))
    except urllib.error.HTTPError as error:
        message: Message = error.headers
        error_headers = dict(message.items())
        return HttpResponse(
            url, error.code, error_headers, decode_body(error.read(), error_headers)
        )


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


TRANSIENT_ERRORS: Final = (TimeoutError, ConnectionError, urllib.error.URLError)


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
    ) -> None:
        self.metrics = metrics or RequestMetrics()
        self._clock = clock
        self._transport = transport
        self._policy = policy or RetryPolicy()
        self._timeout = timeout_seconds
        self._sleep = sleep
        self._rng = rng or Random()  # noqa: S311 - jitter for retry timing, not security

    def get(self, url: str, params: Mapping[str, str] | None = None) -> HttpResponse:
        """GET with retries on 429/5xx/timeouts; 4xx raise ``HttpStatusError`` immediately."""
        full_url = build_url(url, params)
        if urlsplit(full_url).scheme != "https":
            raise ValueError(f"only https URLs are allowed: {full_url}")
        attempts = self._policy.max_attempts
        last_failure = "no attempt"
        for attempt in range(1, attempts + 1):
            wait: float | None = None
            requested_at = self._clock()
            try:
                response = self._transport(full_url, self._timeout)
            except TRANSIENT_ERRORS as error:
                self.metrics.record(success=False)
                last_failure = f"{type(error).__name__}: {error}"
            else:
                succeeded = HTTP_OK_MIN <= response.status <= HTTP_OK_MAX
                self.metrics.record(success=succeeded)
                if succeeded:
                    logger.info(
                        "GET %s -> %d (%d bytes, attempt %d)",
                        full_url,
                        response.status,
                        len(response.body),
                        attempt,
                    )
                    return replace(response, requested_at=requested_at)
                if response.status not in self._policy.retry_statuses:
                    logger.warning("GET %s -> %d (not retryable)", full_url, response.status)
                    raise HttpStatusError(full_url, response.status, response.body, requested_at)
                last_failure = f"HTTP {response.status}"
                wait = _retry_after(response.headers)
            if attempt == attempts:
                break
            delay = (
                min(wait, self._policy.max_delay_seconds)
                if wait is not None
                else self._policy.delay(attempt, self._rng)
            )
            logger.warning(
                "GET %s failed (%s); retry %d/%d in %.1fs",
                full_url,
                last_failure,
                attempt,
                attempts - 1,
                delay,
            )
            self._sleep(delay)
        raise RetriesExhaustedError(full_url, attempts, last_failure)

    def get_json(self, url: str, params: Mapping[str, str] | None = None) -> Any:  # noqa: ANN401
        return self.get(url, params).json()


def parse_json_text(response: HttpResponse) -> Any:  # noqa: ANN401 - JSON is untyped by nature
    """JSON where every number stays text: raw data keeps the exact decimals the API sent."""
    try:
        return json.loads(response.body, parse_float=str, parse_int=str)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResponseFormatError(f"{response.url}: response is not JSON") from error
