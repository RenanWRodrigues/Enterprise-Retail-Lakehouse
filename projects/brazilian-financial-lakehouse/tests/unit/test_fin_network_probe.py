"""Network preflight of the Financial APIs: each failure is classified, nothing touches the net.

DNS and the HTTPS request are injected, so every outcome is reproduced locally; the real run
happens in the workspace (notebooks/setup/00_network_preflight.py).
"""

import socket
import ssl
import urllib.error
from collections.abc import Callable, Iterator

import pytest

from financial_lakehouse.clients.bcb_ptax import PTAX_URL
from financial_lakehouse.clients.bcb_sgs import SGS_URL
from financial_lakehouse.clients.ibge import BASE_URL
from financial_lakehouse.clients.network_probe import (
    ENDPOINTS,
    Endpoint,
    ProbeOutcome,
    probe,
    probe_all,
)

SGS = Endpoint("bcb_sgs", "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1")


def _resolves(*_: object) -> list[object]:
    return [("AF_INET", None, None, "", ("200.218.0.1", 443))] * 2


def _ticks() -> Callable[[], float]:
    times: Iterator[float] = iter([10.0, 10.25])
    return lambda: next(times)


def _fetch(outcome: int | BaseException) -> Callable[[str, float], int]:
    def fetch(url: str, timeout: float) -> int:
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return fetch


def _probe(outcome: int | BaseException, **overrides: object) -> object:
    kwargs: dict[str, object] = {
        "resolve": _resolves,
        "fetch": _fetch(outcome),
        "clock": _ticks(),
        **overrides,
    }
    return probe(SGS, **kwargs)  # type: ignore[arg-type]


def test_endpoints_are_the_hosts_the_clients_use() -> None:
    hosts = {endpoint.host for endpoint in ENDPOINTS}

    assert (
        hosts
        == {
            SGS_URL.split("/")[2],
            PTAX_URL.split("/")[2],
            BASE_URL.split("/")[2],
        }
        == {"api.bcb.gov.br", "olinda.bcb.gov.br", "servicodados.ibge.gov.br"}
    )
    assert all(e.url.startswith("https://") for e in ENDPOINTS)


def test_a_2xx_answer_is_reachable_with_its_latency() -> None:
    result = _probe(200)

    assert result.outcome is ProbeOutcome.REACHABLE  # type: ignore[attr-defined]
    assert (result.status, result.addresses, result.elapsed_ms) == (200, 2, 250.0)  # type: ignore[attr-defined]


def test_a_name_that_does_not_resolve_is_a_dns_failure() -> None:
    def no_dns(*_: object) -> list[object]:
        raise socket.gaierror(-2, "Name or service not known")

    calls: list[str] = []

    def fetch(url: str, _timeout: float) -> int:
        calls.append(url)
        return 200

    result = _probe(200, resolve=no_dns, fetch=fetch)

    assert result.outcome is ProbeOutcome.DNS_FAILURE  # type: ignore[attr-defined]
    assert calls == []  # no HTTPS attempt without an address


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (ConnectionRefusedError(111, "Connection refused"), ProbeOutcome.NETWORK_BLOCKED),
        (
            urllib.error.URLError(OSError(101, "Network is unreachable")),
            ProbeOutcome.NETWORK_BLOCKED,
        ),
        (ConnectionResetError(104, "Connection reset by peer"), ProbeOutcome.NETWORK_BLOCKED),
        (TimeoutError("timed out"), ProbeOutcome.TIMEOUT),
        (urllib.error.URLError(TimeoutError("timed out")), ProbeOutcome.TIMEOUT),
        (
            urllib.error.URLError(ssl.SSLCertVerificationError("self-signed")),
            ProbeOutcome.TLS_FAILURE,
        ),
        (urllib.error.URLError(socket.gaierror(-3, "Temporary failure")), ProbeOutcome.DNS_FAILURE),
    ],
    ids=["refused", "unreachable", "reset", "timeout", "wrapped-timeout", "tls", "late-dns"],
)
def test_transport_failures_are_told_apart(error: BaseException, outcome: ProbeOutcome) -> None:
    result = _probe(error)

    assert (result.outcome, result.status) == (outcome, None)  # type: ignore[attr-defined]
    assert result.detail  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (401, ProbeOutcome.AUTH_REQUIRED),
        (403, ProbeOutcome.AUTH_REQUIRED),
        (407, ProbeOutcome.AUTH_REQUIRED),  # a proxy demanding credentials
        (429, ProbeOutcome.ENDPOINT_UNAVAILABLE),
        (503, ProbeOutcome.ENDPOINT_UNAVAILABLE),
        (404, ProbeOutcome.HTTP_ERROR),
    ],
)
def test_http_statuses_are_classified(status: int, outcome: ProbeOutcome) -> None:
    result = _probe(status)

    assert (result.outcome, result.status) == (outcome, status)  # type: ignore[attr-defined]


def test_details_never_carry_secrets() -> None:
    error = urllib.error.URLError(OSError("proxy rejected Authorization: Bearer abc.def.ghi"))

    result = _probe(error)

    assert "abc.def.ghi" not in result.detail  # type: ignore[attr-defined]


def test_every_endpoint_is_probed_even_after_a_failure() -> None:
    seen: list[str] = []

    def fetch(url: str, _timeout: float) -> int:
        seen.append(url)
        raise TimeoutError("timed out")

    results = probe_all(resolve=_resolves, fetch=fetch, clock=iter(range(100)).__next__)

    assert [r.endpoint.name for r in results] == [e.name for e in ENDPOINTS]
    assert seen == [e.url for e in ENDPOINTS]
    assert {r.outcome for r in results} == {ProbeOutcome.TIMEOUT}
