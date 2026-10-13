"""Network preflight of the Financial APIs: can this environment reach each official host?

One small read-only GET per host (no retries, short timeout), after a separate DNS lookup, so a
failure says *where* it happened. Run it in the workspace (notebooks/setup/00_network_preflight.py)
before the Bronze ingestion; a local run only proves the local network.

Outcomes: ``reachable`` (2xx); ``dns_failure`` (the name does not resolve); ``network_blocked``
(connection refused, reset or unreachable: egress closed); ``timeout`` (ambiguous: a firewall that
silently drops packets, or a slow endpoint); ``tls_failure`` (an intercepting proxy or a broken
certificate chain); ``auth_required`` (401/403/407: these APIs need no credentials, so a proxy or
a block page answered); ``endpoint_unavailable`` (429/5xx: reached, but the API is down or
throttling); ``http_error`` (any other status, e.g. 404: the endpoint moved).
Nothing secret is printed: no headers, no cookies, no body; error texts pass ``mask_secrets``.
"""

import socket
import ssl
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final
from urllib.parse import urlsplit

from financial_lakehouse.clients.bcb_ptax import PTAX_URL
from financial_lakehouse.clients.bcb_sgs import SGS_URL
from financial_lakehouse.clients.http import USER_AGENT
from financial_lakehouse.clients.ibge import BASE_URL
from lakehouse_common.observability import mask_secrets

PROBE_TIMEOUT_SECONDS: Final = 10.0
HTTPS_PORT: Final = 443
MAX_PROBE_BYTES: Final = 64 * 1024  # read at most this much, then discard: no payload kept
HTTP_OK_MIN, HTTP_OK_MAX = 200, 299
AUTH_STATUSES: Final = frozenset({401, 403, 407})
UNAVAILABLE_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
MS_PER_SECOND: Final = 1000.0


class ProbeOutcome(StrEnum):
    REACHABLE = "reachable"
    DNS_FAILURE = "dns_failure"
    NETWORK_BLOCKED = "network_blocked"
    TIMEOUT = "timeout"
    TLS_FAILURE = "tls_failure"
    AUTH_REQUIRED = "auth_required"
    ENDPOINT_UNAVAILABLE = "endpoint_unavailable"
    HTTP_ERROR = "http_error"


@dataclass(frozen=True, slots=True)
class Endpoint:
    name: str
    url: str

    @property
    def host(self) -> str:
        return urlsplit(self.url).hostname or ""


# The smallest public request on each host the clients call (derived from their base URLs).
ENDPOINTS: Final = (
    Endpoint("bcb_sgs", f"{SGS_URL.format(code=432)}/ultimos/1?formato=json"),
    Endpoint("bcb_ptax", f"{PTAX_URL.split('CotacaoMoedaPeriodo')[0]}Moedas?$top=1&$format=json"),
    Endpoint("ibge_agregados", f"{BASE_URL}/1737/periodos"),
)


@dataclass(frozen=True, slots=True)
class ProbeResult:
    endpoint: Endpoint
    outcome: ProbeOutcome
    status: int | None  # HTTP status; None when no HTTP answer arrived
    addresses: int  # DNS answers for the host (0: did not resolve)
    elapsed_ms: float | None  # HTTPS request only (DNS excluded); None without an answer
    detail: str  # masked error text, or empty

    def as_row(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint.name,
            "host": self.endpoint.host,
            "outcome": self.outcome.value,
            "status": self.status,
            "dns_addresses": self.addresses,
            "elapsed_ms": self.elapsed_ms,
            "detail": self.detail,
        }


Resolve = Callable[[str, int], Sequence[object]]
Fetch = Callable[[str, float], int]


def _resolve(host: str, port: int) -> Sequence[object]:
    return socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)


def fetch_status(url: str, timeout: float) -> int:
    """GET ``url`` (https only); return the status, reading at most ``MAX_PROBE_BYTES``."""
    if not url.startswith("https://"):
        raise ValueError(f"only https URLs are probed: {url}")
    request = urllib.request.Request(  # noqa: S310 - https only (checked above)
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            response.read(MAX_PROBE_BYTES)  # the body proves the transfer; it is discarded
            return int(response.status)
    except urllib.error.HTTPError as error:  # an HTTP answer, not a transport failure
        return int(error.code)


def _transport_outcome(error: BaseException) -> ProbeOutcome:
    cause = error.reason if isinstance(error, urllib.error.URLError) else error
    if isinstance(cause, socket.gaierror):
        return ProbeOutcome.DNS_FAILURE
    if isinstance(cause, ssl.SSLError | ssl.CertificateError):
        return ProbeOutcome.TLS_FAILURE
    if isinstance(cause, TimeoutError | socket.timeout):
        return ProbeOutcome.TIMEOUT
    return ProbeOutcome.NETWORK_BLOCKED  # refused, reset, unreachable: egress is closed


def _status_outcome(status: int) -> ProbeOutcome:
    if HTTP_OK_MIN <= status <= HTTP_OK_MAX:
        return ProbeOutcome.REACHABLE
    if status in AUTH_STATUSES:
        return ProbeOutcome.AUTH_REQUIRED
    if status in UNAVAILABLE_STATUSES:
        return ProbeOutcome.ENDPOINT_UNAVAILABLE
    return ProbeOutcome.HTTP_ERROR


def _detail(error: BaseException) -> str:
    return mask_secrets(f"{type(error).__name__}: {error}")


def probe(
    endpoint: Endpoint,
    *,
    resolve: Resolve = _resolve,
    fetch: Fetch = fetch_status,
    clock: Callable[[], float] = time.monotonic,
    timeout: float = PROBE_TIMEOUT_SECONDS,
) -> ProbeResult:
    """DNS, then one HTTPS GET; never raises for a network failure (it is the result)."""
    try:
        addresses = len(resolve(endpoint.host, HTTPS_PORT))
    except OSError as error:  # socket.gaierror and friends
        return ProbeResult(endpoint, ProbeOutcome.DNS_FAILURE, None, 0, None, _detail(error))
    started = clock()
    try:
        status = fetch(endpoint.url, timeout)
    except OSError as error:  # URLError, timeouts, TLS and connection errors are OSErrors
        return ProbeResult(
            endpoint, _transport_outcome(error), None, addresses, None, _detail(error)
        )
    elapsed = round((clock() - started) * MS_PER_SECOND, 1)
    return ProbeResult(endpoint, _status_outcome(status), status, addresses, elapsed, "")


def probe_all(
    endpoints: Sequence[Endpoint] = ENDPOINTS,
    *,
    resolve: Resolve = _resolve,
    fetch: Fetch = fetch_status,
    clock: Callable[[], float] = time.monotonic,
    timeout: float = PROBE_TIMEOUT_SECONDS,
) -> list[ProbeResult]:
    """Every endpoint, in order; one failure does not stop the others."""
    return [probe(e, resolve=resolve, fetch=fetch, clock=clock, timeout=timeout) for e in endpoints]
