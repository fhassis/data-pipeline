import logging
import os
import sys
from typing import Any

from msgspec.json import encode as msgspec_encode
from opentelemetry import trace
from structlog import configure
from structlog.processors import (
    ExceptionRenderer,
    JSONRenderer,
    StackInfoRenderer,
    TimeStamper,
)
from structlog.stdlib import (
    BoundLogger,
    LoggerFactory,
    ProcessorFormatter,
    add_log_level,
    add_logger_name,
)


def _add_otel_context(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Inject OTel trace_id and span_id into each log record.

    No-op when there is no active span so it is safe to call unconditionally.
    The IDs use the standard 32-hex / 16-hex format that Grafana expects for
    log-to-trace correlation in Loki → Tempo.
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def configure_logging() -> None:
    """
    Configure structured JSON logging to stdout.

    Sets up the pipeline:
        structlog → stdlib → StreamHandler(stdout)

    Each log record emitted inside an active OTel span will carry
    ``trace_id`` and ``span_id`` fields, enabling log-to-trace correlation
    in Grafana (Loki → Tempo).  A log collector (Alloy / Promtail) is
    expected to scrape stdout and forward records to Loki.
    """

    # resolve the minimum log level from the env var, defaulting to INFO
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # processors shared between structlog-origin and stdlib-origin records
    # (FastStream internals and app_factory use stdlib loggers — this chain ensures
    # their records get the same enrichment as structlog ones)
    shared_processors = [
        # adds a "level" field (info, debug, error, ...)
        add_log_level,
        # adds a "logger" field (the logger name, e.g. "producer_worker")
        add_logger_name,
        # adds a "timestamp" field in ISO-8601 / UTC
        TimeStamper(fmt="iso"),
        # renders Python stack info when stack_info=True is passed
        StackInfoRenderer(),
        # injects trace_id / span_id when running inside an OTel span
        _add_otel_context,
    ]

    # structlog — enriches records then hands them off to the stdlib handler below
    configure(
        # processor chain: runs top-to-bottom on every log record
        processors=[
            *shared_processors,
            # flattens exception info into the event dict
            ExceptionRenderer(),
            # hands off to ProcessorFormatter so stdlib and structlog paths share a renderer
            ProcessorFormatter.wrap_for_formatter,
        ],
        # stdlib-compatible bound logger so FastStream / third-party libs can receive it as a drop-in logger
        wrapper_class=BoundLogger,
        # plain dict for per-logger bound context
        context_class=dict,
        # underlying logger is logging.getLogger(name) — integrates with stdlib
        logger_factory=LoggerFactory(),
    )

    # single formatter used by the stdlib StreamHandler — renders both structlog-origin
    # records (handed off via wrap_for_formatter) and foreign stdlib-origin records (like
    # FastStream's internal "app starting..." lines) as JSON
    formatter = ProcessorFormatter(
        processors=[
            # strips ProcessorFormatter's internal metadata keys before rendering
            ProcessorFormatter.remove_processors_meta,
            # final step: serialise the event dict to a JSON string.
            # msgspec is faster than stdlib json; decode bytes→str for stdlib logging compatibility
            JSONRenderer(serializer=lambda obj, **_: msgspec_encode(obj).decode()),
        ],
        # applied only to foreign (stdlib-origin) records so they get the same fields
        foreign_pre_chain=shared_processors,
    )

    # configure the stdlib root logger to actually emit at the requested level
    root = logging.getLogger()
    root.setLevel(level)

    # clear any pre-existing handlers so re-invocation does not duplicate output
    root.handlers.clear()

    # writes the structlog-formatted JSON line to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root.addHandler(handler)
