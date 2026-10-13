"""Operational alert channel: the ``lakehouse.alerts`` logger, optionally sent to a webhook.

The pipelines already log every WARNING/ERROR quality finding and every CRITICAL failure on
``lakehouse.alerts`` (``observability.runs``). ``install_alert_webhook`` adds delivery to an
HTTPS webhook whose URL comes from the environment (``LAKEHOUSE_ALERT_WEBHOOK_URL``; a secret,
e.g. from Databricks Secrets): never in code or config, never logged.

Delivery never affects the pipeline: the handler sits behind a ``QueueListener`` (a background
thread), retries are bounded (5xx, 429, transport errors; a 4xx is not retried), failures are
reported on ``lakehouse.alerts.delivery`` and swallowed, and the same alert text is sent at
most once per ``dedup_seconds``. Bodies are masked (``mask_secrets``).
"""

import atexit
import json
import logging
import queue
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from logging.handlers import QueueHandler, QueueListener
from typing import Final, Literal, TextIO

from lakehouse_common.logging.redaction import mask_secrets
from lakehouse_common.logging.structured import configure_logging

ALERT_LOGGER: Final = "lakehouse.alerts"
ALERT_WEBHOOK_ENV: Final = "LAKEHOUSE_ALERT_WEBHOOK_URL"
DEFAULT_ATTEMPTS: Final = 3
DEFAULT_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_DEDUP_SECONDS: Final = 3600.0
BACKOFF_SECONDS: Final = 1.0
RETRY_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
HTTP_OK_MIN, HTTP_OK_MAX = 200, 299

delivery_logger = logging.getLogger(f"{ALERT_LOGGER}.delivery")  # not propagated to alerts
delivery_logger.propagate = False
delivery_logger.addHandler(logging.StreamHandler())  # stderr: delivery problems stay visible

Post = Callable[[str, bytes, float], int]


def urllib_post(url: str, body: bytes, timeout: float) -> int:
    """POST a JSON body; the HTTP status (errors are returned, not raised)."""
    request = urllib.request.Request(  # noqa: S310 - https enforced by install_alert_webhook
        url, data=body, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return int(response.status)
    except urllib.error.HTTPError as error:
        return int(error.code)


class WebhookAlertHandler(logging.Handler):
    """Synchronous delivery of one record (run it behind a ``QueueListener``)."""

    def __init__(  # noqa: PLR0913 - keyword-only, injectable for tests
        self,
        url: str,
        *,
        post: Post = urllib_post,
        max_attempts: int = DEFAULT_ATTEMPTS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        dedup_seconds: float = DEFAULT_DEDUP_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(level=logging.WARNING)
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._url, self._post, self._attempts = url, post, max_attempts
        self._timeout, self._dedup, self._sleep, self._clock = timeout, dedup_seconds, sleep, clock
        self._sent: dict[str, float] = {}  # alert text -> last sent (monotonic)

    def emit(self, record: logging.LogRecord) -> None:
        event = mask_secrets(record.getMessage())
        now = self._clock()
        last = self._sent.get(event)
        if last is not None and now - last < self._dedup:
            return  # same alert within the window: already delivered
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "event": event,
            "run_id": getattr(record, "run_id", None),
            "pipeline": getattr(record, "pipeline", None),
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if self._deliver(body):
            self._sent[event] = now

    def _deliver(self, body: bytes) -> bool:
        failure = "no attempt"
        for attempt in range(1, self._attempts + 1):
            try:
                status = self._post(self._url, body, self._timeout)
            except OSError as error:  # timeouts and connection errors
                failure = type(error).__name__
            else:
                if HTTP_OK_MIN <= status <= HTTP_OK_MAX:
                    return True
                failure = f"HTTP {status}"
                if status not in RETRY_STATUSES:
                    break
            if attempt < self._attempts:
                self._sleep(BACKOFF_SECONDS * 2 ** (attempt - 1))
        delivery_logger.warning("alert not delivered to the webhook (%s)", failure)
        return False


def install_alert_webhook(
    *,
    environ: Mapping[str, str],
    post: Post = urllib_post,
    **handler_options: float,
) -> QueueListener | None:
    """Route ``lakehouse.alerts`` to the webhook in ``environ``; None when not configured.

    Returns the started ``QueueListener``: call ``stop()`` at the end of the job to drain it.
    An invalid URL disables delivery with a warning; it never stops the pipeline.
    """
    url = environ.get(ALERT_WEBHOOK_ENV, "").strip()
    if not url:
        return None
    if not url.startswith("https://"):
        delivery_logger.warning("%s ignored: only https webhooks are accepted", ALERT_WEBHOOK_ENV)
        return None
    records: queue.Queue[logging.LogRecord] = queue.Queue()
    handler = WebhookAlertHandler(url, post=post, **handler_options)  # type: ignore[arg-type]
    listener = _OnceListener(records, handler, respect_handler_level=True)
    logging.getLogger(ALERT_LOGGER).addHandler(QueueHandler(records))
    listener.start()
    atexit.register(listener.stop)  # drain pending alerts when the process ends
    return listener


class _OnceListener(QueueListener):
    """A listener whose ``stop`` may be called twice (explicitly and at exit)."""

    _stopped = False

    def stop(self) -> None:
        if not self._stopped:
            self._stopped = True
            super().stop()


LOG_FORMAT_ENV: Final = "LAKEHOUSE_LOG_FORMAT"  # "json" or "text"


def setup_logging(
    environ: Mapping[str, str],
    *,
    default_format: Literal["json", "text"] = "text",
    logger: logging.Logger | None = None,
    stream: TextIO | None = None,
) -> QueueListener | None:
    """Entry-point setup from the environment: log format and, if configured, the webhook.

    ``LAKEHOUSE_LOG_FORMAT`` = ``json`` or ``text`` (default ``default_format``; the CLIs keep
    ``text``, their previous output). Returns the webhook listener (``stop()`` it at the end).
    """
    chosen = environ.get(LOG_FORMAT_ENV, default_format).strip().lower()
    fmt: Literal["json", "text"] = "json" if chosen == "json" else "text"
    configure_logging(fmt=fmt, logger=logger, stream=stream)
    return install_alert_webhook(environ=environ)
