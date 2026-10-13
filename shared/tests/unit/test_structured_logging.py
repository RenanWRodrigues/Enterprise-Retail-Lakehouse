"""Structured (JSON) logging: fields, correlation by run, redaction, compatibility."""

import io
import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest

from lakehouse_common.logging import (
    LOG_FIELDS,
    TEXT_FORMAT,
    JsonFormatter,
    configure_logging,
    log_context,
)
from lakehouse_common.observability import (
    InMemoryObservabilitySink,
    Layer,
    mask_secrets,
    track_run,
)

RUNS_LOGGER = "lakehouse_common.observability.runs"


@pytest.fixture
def captured() -> Iterator[tuple[logging.Logger, io.StringIO]]:
    """A logger whose records go through the JSON formatter into a buffer."""
    stream = io.StringIO()
    handler = configure_logging(fmt="json", stream=stream, logger=logging.getLogger("t"))
    logger = logging.getLogger("t.child")
    logger.setLevel(logging.DEBUG)
    yield logger, stream
    logging.getLogger("t").removeHandler(handler)


def _lines(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_every_line_is_json_with_every_field(captured: tuple[logging.Logger, io.StringIO]) -> None:
    logger, stream = captured

    logger.info("batch %s read", "b1")

    [line] = _lines(stream)
    assert set(LOG_FIELDS) <= set(line)
    assert LOG_FIELDS == (
        "timestamp", "level", "logger", "event", "project", "pipeline", "run_id",
        "batch_id", "stage", "duration_ms", "status", "error_type",
    )  # fmt: skip
    assert (line["level"], line["logger"], line["event"]) == ("INFO", "t.child", "batch b1 read")
    assert line["timestamp"].endswith("Z")  # UTC


def test_context_correlates_every_line_of_a_run(
    captured: tuple[logging.Logger, io.StringIO],
) -> None:
    logger, stream = captured

    with log_context(project="retail", pipeline="retail_silver", run_id="r-1"):
        with log_context(stage="merge", batch_id="b2"):
            logger.info("inner")
        logger.info("outer")
    logger.info("after")

    inner, outer, after = _lines(stream)
    assert (inner["run_id"], inner["stage"], inner["batch_id"]) == ("r-1", "merge", "b2")
    assert (outer["run_id"], outer["stage"]) == ("r-1", None)
    assert (after["run_id"], after["project"]) == (None, None)


def test_extra_fields_fill_the_standard_columns_only(
    captured: tuple[logging.Logger, io.StringIO],
) -> None:
    logger, stream = captured

    logger.info("done", extra={"status": "SUCCEEDED", "duration_ms": 12.5, "payload": {"cpf": "x"}})

    [line] = _lines(stream)
    assert (line["status"], line["duration_ms"]) == ("SUCCEEDED", 12.5)
    assert "payload" not in line  # arbitrary data never reaches the log


def test_exceptions_carry_their_type_masked(captured: tuple[logging.Logger, io.StringIO]) -> None:
    logger, stream = captured

    try:
        raise ValueError("connect failed: token=abc123")
    except ValueError:
        logger.exception("boom")

    [line] = _lines(stream)
    assert line["error_type"] == "ValueError"
    assert "abc123" not in json.dumps(line)
    assert "token=***" in line["exception"]


def test_secrets_and_emails_are_masked(captured: tuple[logging.Logger, io.StringIO]) -> None:
    logger, stream = captured

    logger.warning("retry with api_key=K9 for maria.silva@example.com")

    [line] = _lines(stream)
    assert "K9" not in line["event"] and "maria.silva" not in line["event"]
    assert mask_secrets("contact maria.silva@example.com") == "contact ***@***"


def test_run_logs_are_correlated_and_summarised(caplog: pytest.LogCaptureFixture) -> None:
    sink = InMemoryObservabilitySink()
    inner = logging.getLogger("retail_lakehouse.test")

    with caplog.at_level(logging.INFO), track_run(
        pipeline_name="retail_silver", run_id="r-7", project="retail",
        layer=Layer.SILVER, table="silver.silver_orders", sink=sink,
    ):  # fmt: skip
        inner.info("inside")

    inside = next(r for r in caplog.records if r.getMessage() == "inside")
    summary = next(r for r in caplog.records if r.name == RUNS_LOGGER)
    assert (inside.run_id, inside.pipeline, inside.project) == ("r-7", "retail_silver", "retail")  # type: ignore[attr-defined]
    assert (summary.status, summary.levelno) == ("SUCCEEDED", logging.INFO)  # type: ignore[attr-defined]
    assert summary.duration_ms >= 0  # type: ignore[attr-defined]


def test_a_failed_run_is_logged_as_an_error_with_its_type(caplog: pytest.LogCaptureFixture) -> None:
    sink = InMemoryObservabilitySink()

    with caplog.at_level(logging.INFO), pytest.raises(OSError), track_run(
        pipeline_name="iot_silver", run_id="r-8", project="iot",
        layer=Layer.SILVER, table="silver.readings", sink=sink,
    ):  # fmt: skip
        raise OSError("disk full")

    summary = next(r for r in caplog.records if r.name == RUNS_LOGGER)
    fields = (
        summary.levelno,
        getattr(summary, "status", None),
        getattr(summary, "error_type", None),
    )
    assert fields == (logging.ERROR, "FAILED", "OSError")
    assert getattr(summary, "run_id", None) == "r-8"
    assert "FAILED" in summary.getMessage()  # the text line consumers read is unchanged


def test_text_format_and_idempotent_setup_stay_compatible() -> None:
    stream = io.StringIO()
    target = logging.getLogger("compat")
    first = configure_logging(fmt="text", stream=stream, logger=target)
    second = configure_logging(fmt="text", stream=stream, logger=target)

    logging.getLogger("compat.x").warning("plain %s", "message")

    assert first is second  # no duplicate handler on a second call
    assert TEXT_FORMAT == "%(levelname)s %(name)s: %(message)s"  # the CLIs' previous format
    assert stream.getvalue() == "WARNING compat.x: plain message\n"
    target.removeHandler(first)


def test_the_formatter_is_usable_alone() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", None, None)

    assert json.loads(JsonFormatter().format(record))["event"] == "hello"


def test_setup_from_the_environment_selects_format_and_alerts() -> None:
    from lakehouse_common.logging import LOG_FORMAT_ENV, setup_logging  # noqa: PLC0415

    target = logging.getLogger("setup")
    text_listener = setup_logging({}, logger=target, stream=io.StringIO())
    [text_handler] = target.handlers
    target.removeHandler(text_handler)
    json_listener = setup_logging({LOG_FORMAT_ENV: "json"}, logger=target, stream=io.StringIO())
    [json_handler] = target.handlers
    target.removeHandler(json_handler)

    assert (text_listener, json_listener) == (None, None)  # no webhook configured
    assert not isinstance(text_handler.formatter, JsonFormatter)
    assert isinstance(json_handler.formatter, JsonFormatter)
