"""Webhook alert delivery: optional, bounded retries, deduplicated, never breaks the pipeline."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import pytest

from lakehouse_common.logging import (
    ALERT_WEBHOOK_ENV,
    WebhookAlertHandler,
    install_alert_webhook,
)
from lakehouse_common.observability import InMemoryObservabilitySink, Layer, track_run
from lakehouse_common.quality import (
    CheckResult,
    CriticalQualityError,
    Dimension,
    Severity,
    Status,
)

URL = "https://hooks.example.invalid/alerts/T0K3N"


class FakePost:
    """Records every POST; answers the scripted statuses (or raises)."""

    def __init__(self, *outcomes: int | Exception) -> None:
        self.outcomes = list(outcomes)
        self.bodies: list[dict[str, Any]] = []

    def __call__(self, url: str, body: bytes, timeout: float) -> int:
        self.bodies.append(json.loads(body))
        outcome = self.outcomes.pop(0) if self.outcomes else 200
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _handler(post: FakePost, clock: Clock | None = None, **kwargs: Any) -> WebhookAlertHandler:
    return WebhookAlertHandler(
        URL, post=post, sleep=lambda _s: None, clock=clock or Clock(), **kwargs
    )


def _alert(handler: WebhookAlertHandler, message: str, level: int = logging.WARNING) -> None:
    handler.handle(logging.LogRecord("lakehouse.alerts", level, __file__, 1, message, None, None))


def test_an_alert_is_posted_as_json() -> None:
    post = FakePost()

    _alert(
        _handler(post), "[WARNING] retail_silver/silver_orders: unknown_customer failed 1/300 rows"
    )

    [body] = post.bodies
    assert body["level"] == "WARNING"
    assert body["event"].startswith("[WARNING] retail_silver")


def test_the_same_alert_is_sent_once_per_window() -> None:
    post, clock = FakePost(), Clock()
    handler = _handler(post, clock, dedup_seconds=3600)

    _alert(handler, "same")
    _alert(handler, "same")
    clock.now += 3601
    _alert(handler, "same")
    _alert(handler, "other")

    assert [b["event"] for b in post.bodies] == ["same", "same", "other"]


def test_an_unavailable_webhook_is_retried_then_given_up_silently(
    caplog: pytest.LogCaptureFixture,
) -> None:
    post = FakePost(503, ConnectionError("refused"), 503, 503)

    with caplog.at_level(logging.WARNING, logger="lakehouse.alerts.delivery"):
        _alert(_handler(post, max_attempts=3), "x")  # must not raise

    assert len(post.bodies) == 3
    assert "not delivered" in caplog.text
    assert "T0K3N" not in caplog.text  # the URL is a secret: never logged


def test_a_client_error_is_not_retried() -> None:
    post = FakePost(404)

    _alert(_handler(post, max_attempts=3), "x")

    assert len(post.bodies) == 1


def test_secrets_in_alerts_are_masked_before_sending() -> None:
    post = FakePost()

    _alert(_handler(post), "GET https://api.example.invalid/x?token=abc123 failed")

    assert "abc123" not in json.dumps(post.bodies)


def test_disabled_without_configuration() -> None:
    assert install_alert_webhook(environ={}) is None


def test_only_https_webhooks_are_accepted(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="lakehouse.alerts.delivery"):
        listener = install_alert_webhook(environ={ALERT_WEBHOOK_ENV: "http://plain.invalid/x"})

    assert listener is None
    assert "https" in caplog.text


def test_install_routes_lakehouse_alerts_through_a_background_queue() -> None:
    post = FakePost()
    listener = install_alert_webhook(environ={ALERT_WEBHOOK_ENV: URL}, post=post)
    assert listener is not None
    try:
        logging.getLogger("lakehouse.alerts").warning("queued alert")
    finally:
        listener.stop()  # drains the queue
        logging.getLogger("lakehouse.alerts").handlers.clear()

    assert [b["event"] for b in post.bodies] == ["queued alert"]


def test_a_failing_webhook_never_replaces_the_pipeline_error() -> None:
    post = FakePost(*[ConnectionError("down")] * 10)
    handler = _handler(post, max_attempts=2)
    alerts = logging.getLogger("lakehouse.alerts")
    alerts.addHandler(handler)
    critical = CheckResult(
        check_name="schema__orders", check_type="schema", table_name="silver_orders",
        column_name=None, dimension=Dimension.VALIDITY, status=Status.CRITICAL,
        severity=Severity.CRITICAL, rows_checked=1, failed_rows=1, failure_rate=1.0,
        execution_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )  # fmt: skip
    try:
        with pytest.raises(CriticalQualityError), track_run(
            pipeline_name="p", run_id="r", project="x", layer=Layer.SILVER, table="t",
            sink=InMemoryObservabilitySink(),
        ) as step:  # fmt: skip
            step.record_quality([critical])
    finally:
        alerts.removeHandler(handler)

    assert len(post.bodies) == 2  # attempted, failed, swallowed
