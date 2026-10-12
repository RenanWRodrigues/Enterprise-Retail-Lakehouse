"""Preflight checks that need no Spark: they report, never raise, and never print secrets."""

from types import SimpleNamespace
from typing import Any

from lakehouse_common.preflight import (
    OFFICIAL_APIS,
    PreflightResult,
    check_https,
    check_runtime,
)


def _spark(version: str = "4.0.0") -> Any:
    return SimpleNamespace(version=version)


def test_the_runtime_passes_from_python_3_12() -> None:
    assert check_runtime(_spark(), (3, 12)) == PreflightResult(
        "runtime", True, "Python 3.12; Spark 4.0.0"
    )


def test_an_older_python_is_reported_not_raised() -> None:
    result = check_runtime(_spark(), (3, 11))

    assert (result.ok, result.detail) == (False, "RuntimeError: Python 3.11 < required (3, 12)")


def test_outbound_https_lists_the_status_of_each_official_api() -> None:
    result = check_https(OFFICIAL_APIS, lambda url, timeout: 200)

    assert result.ok
    assert result.detail == (
        "api.bcb.gov.br=200, olinda.bcb.gov.br=200, servicodados.ibge.gov.br=200"
    )


def test_blocked_internet_is_a_failed_check_with_the_reason() -> None:
    def blocked(url: str, timeout: float) -> int:
        raise ConnectionError(f"egress denied to {url}")

    result = check_https(OFFICIAL_APIS, blocked)

    assert not result.ok and result.detail.startswith("ConnectionError: egress denied")


def test_a_token_in_an_error_is_masked() -> None:
    def leaky(url: str, timeout: float) -> int:
        raise PermissionError("rejected: token=" + "abc" + "def123 for " + url)

    result = check_https(["https://example.org/x"], leaky)

    assert "abcdef123" not in result.detail and "token=***" in result.detail


def test_only_https_is_checked() -> None:
    result = check_https(["http://example.org"], lambda url, timeout: 200)

    assert not result.ok and "only https" in result.detail
