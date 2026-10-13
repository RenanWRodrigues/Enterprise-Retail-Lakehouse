"""Structured logging on the standard ``logging`` module: one JSON object per line.

Fields (``LOG_FIELDS``): ``timestamp`` (UTC), ``level``, ``logger``, ``event`` (the message,
masked), ``project``, ``pipeline``, ``run_id``, ``batch_id``, ``stage``, ``duration_ms``,
``status``, ``error_type``; plus ``exception`` (masked, bounded) when there is one.

The correlation fields come from ``log_context`` (``contextvars``, so threads and tasks do not
mix), which ``observability.track_run`` opens for every run, or from ``extra=``. Any other
``extra`` key is dropped: payloads and personal data never reach a log line. Messages keep their
text, so readers of the previous plain-text logs see the same words.
"""

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Final, Literal, TextIO

from lakehouse_common.logging.redaction import mask_secrets

LOG_FIELDS: Final = (
    "timestamp",
    "level",
    "logger",
    "event",
    "project",
    "pipeline",
    "run_id",
    "batch_id",
    "stage",
    "duration_ms",
    "status",
    "error_type",
)
CONTEXT_FIELDS: Final = (
    "project", "pipeline", "run_id", "batch_id", "stage", "duration_ms", "status", "error_type",
)  # fmt: skip
TEXT_FORMAT: Final = "%(levelname)s %(name)s: %(message)s"  # the CLIs' format before JSON
MAX_EXCEPTION_LENGTH: Final = 4000
_HANDLER_NAME: Final = "lakehouse_common.logging"

_context: ContextVar[dict[str, object]] = ContextVar("lakehouse_log_context", default={})  # noqa: B039 - never mutated: replaced


_factory_installed: list[bool] = []  # once per process, chained to any existing factory


def _install_record_factory() -> None:
    """Stamp the current context on every record at creation, whatever handler reads it."""
    if _factory_installed:
        return
    previous = logging.getLogRecordFactory()

    def factory(*args: object, **kwargs: object) -> logging.LogRecord:
        record = previous(*args, **kwargs)
        for key, value in _context.get().items():
            setattr(record, key, value)
        return record

    logging.setLogRecordFactory(factory)
    _factory_installed.append(True)


@contextmanager
def log_context(**fields: object) -> Iterator[None]:
    """Add correlation fields (``CONTEXT_FIELDS``) to every log line inside the block."""
    unknown = sorted(set(fields) - set(CONTEXT_FIELDS))
    if unknown:
        raise ValueError(f"not a log context field: {unknown}")
    _install_record_factory()
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


class ContextFilter(logging.Filter):
    """Copies the current ``log_context`` onto each record (``extra=`` wins)."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = _context.get()
        line: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "event": mask_secrets(record.getMessage()),
        }
        for key in CONTEXT_FIELDS:
            line[key] = getattr(record, key, context.get(key))
        if record.exc_info and record.exc_info[0] is not None:
            line["error_type"] = line["error_type"] or record.exc_info[0].__name__
            text = self.formatException(record.exc_info)
            line["exception"] = mask_secrets(text)[-MAX_EXCEPTION_LENGTH:]
        return json.dumps(line, ensure_ascii=False, default=str)


def configure_logging(
    *,
    fmt: Literal["json", "text"] = "json",
    level: int = logging.INFO,
    stream: TextIO | None = None,
    logger: logging.Logger | None = None,
) -> logging.Handler:
    """Attach one stream handler (JSON or the previous text format) to ``logger`` (root).

    Idempotent: a second call returns the handler already installed instead of adding another.
    """
    target = logger or logging.getLogger()
    for handler in target.handlers:
        if handler.get_name() == _HANDLER_NAME:
            return handler
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(JsonFormatter() if fmt == "json" else logging.Formatter(TEXT_FORMAT))
    handler.addFilter(ContextFilter())
    target.addHandler(handler)
    target.setLevel(min(target.level or level, level))
    return handler
