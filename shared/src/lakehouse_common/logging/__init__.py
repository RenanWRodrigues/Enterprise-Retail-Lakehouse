"""Structured logging, redaction and the operational alert channel, shared by the three projects.

Usage (a job or notebook entry point)::

    from lakehouse_common.logging import configure_logging, install_alert_webhook
    configure_logging()                                   # JSON lines on stderr
    listener = install_alert_webhook(environ=os.environ)  # optional, by environment

``observability.track_run`` correlates every log line of a run (project, pipeline, run_id).
docs/OBSERVABILITY.md §6.
"""

from lakehouse_common.logging.alerts import (
    ALERT_LOGGER,
    ALERT_WEBHOOK_ENV,
    LOG_FORMAT_ENV,
    WebhookAlertHandler,
    install_alert_webhook,
    setup_logging,
)
from lakehouse_common.logging.redaction import mask_secrets
from lakehouse_common.logging.structured import (
    CONTEXT_FIELDS,
    LOG_FIELDS,
    TEXT_FORMAT,
    ContextFilter,
    JsonFormatter,
    configure_logging,
    log_context,
)

__all__ = [
    "ALERT_LOGGER",
    "ALERT_WEBHOOK_ENV",
    "CONTEXT_FIELDS",
    "LOG_FIELDS",
    "LOG_FORMAT_ENV",
    "TEXT_FORMAT",
    "ContextFilter",
    "JsonFormatter",
    "WebhookAlertHandler",
    "configure_logging",
    "install_alert_webhook",
    "log_context",
    "mask_secrets",
    "setup_logging",
]
