"""Airflow logging config that also ships every log line over OTLP.

Used via ``AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=otel_logging_config.LOGGING_CONFIG`` when ``OTEL_LOGS_EXPORTER=otlp``.

Airflow emits OTel traces and metrics natively, but not logs. Airflow 3 also leaves no room for a plain logging handler: its ``configure_logging`` replaces the root logger's handlers and the ``airflow`` logger's config, and most of its code (everything built on ``LoggingMixin``) logs through structlog, which writes straight to stdout without passing stdlib handlers.

What every line does pass through is the processor chain Airflow builds in ``structlog_processors``: structlog events run it directly, and stdlib records run it in the formatter of Airflow's console handler. Importing this module (which Airflow does to read ``LOGGING_CONFIG``, just before it configures logging) adds one processor to that chain. The processor hands each line to an OTel ``LoggingHandler``, which exports over OTLP using the standard ``OTEL_EXPORTER_OTLP_*`` env vars (on Azure Container Apps: the managed agent's gRPC endpoint). Lines logged inside a span carry its trace and span ids, so Datadog correlates logs with traces. A line logged elsewhere can name its span in ``trace_id`` and ``span_id`` fields instead.

Each record also gets a ``dd.service`` attribute set to ``OTEL_SERVICE_NAME``. Through the Container Apps managed agent, Datadog leaves a log's service empty: it keeps ``service.name`` only as a nested attribute. ``dd.service`` is what fills it.
"""
from __future__ import annotations

import functools
import logging
import os
import sys
import threading
from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

_LEVELS = {
    "critical": logging.CRITICAL,
    "fatal": logging.CRITICAL,
    "exception": logging.ERROR,
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}
# Event keys that structlog or Airflow use for themselves, rather than as log attributes.
_RESERVED = {"event", "level", "logger", "logger_name", "timestamp", "exc_info", "exception", "stack", "positional_args"}
# Fields a line can name its span in (see _named_span); exported as the line's ids, not as attributes.
_SPAN_FIELDS = {"trace_id", "span_id"}
# OTel's own logs go through the same chain; exporting them could loop.
_SKIP_LOGGERS = ("opentelemetry", "grpc")

_lock = threading.Lock()
_busy = threading.local()
_handler: logging.Handler | None = None


def _enabled() -> bool:
    return os.environ.get("OTEL_LOGS_EXPORTER", "").strip().lower() == "otlp"


def _otel_handler() -> logging.Handler:
    """An OTel LoggingHandler on the global SDK LoggerProvider, created on first use."""
    global _handler
    with _lock:
        if _handler is not None:
            return _handler
        from opentelemetry._logs import get_logger_provider, set_logger_provider
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

        provider = get_logger_provider()
        if not isinstance(provider, LoggerProvider):
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
            from opentelemetry.sdk.resources import Resource

            # Resource.create() picks up OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES.
            provider = LoggerProvider(resource=Resource.create())
            provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
            set_logger_provider(provider)
        _handler = LoggingHandler(level=logging.INFO, logger_provider=provider)
        _handler.addFilter(_add_dd_service)
        return _handler


def _add_dd_service(record: logging.LogRecord) -> bool:
    service = os.environ.get("OTEL_SERVICE_NAME")
    if service:
        record.__dict__["dd.service"] = service
    return True


def _exc_info(value):
    if value is True:
        return sys.exc_info()
    if isinstance(value, BaseException):
        return (type(value), value, value.__traceback__)
    if isinstance(value, tuple) and len(value) == 3:
        return value
    return None


def _named_span(event_dict):
    """The span a line names in ``trace_id`` / ``span_id`` fields (hex), or None.

    The OTel handler links a line to the span that's current when it's handled. A line about a span that has ended, or one exported by another process (Airflow 3's task supervisor exports its task's lines), can name the span instead.
    """
    trace_id, span_id = event_dict.get("trace_id"), event_dict.get("span_id")
    if not (isinstance(trace_id, str) and isinstance(span_id, str)):
        return None
    try:
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

        span_context = SpanContext(
            int(trace_id, 16), int(span_id, 16), is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED)
        )
    except (ImportError, ValueError):
        return None
    return NonRecordingSpan(span_context) if span_context.is_valid else None


def otel_processor(logger, method_name, event_dict):
    """structlog processor: export the line over OTLP, and pass it on unchanged."""
    name = str(event_dict.get("logger") or getattr(logger, "name", None) or "airflow")
    if getattr(_busy, "on", False) or name.startswith(_SKIP_LOGGERS):
        return event_dict
    level = _LEVELS.get(str(event_dict.get("level") or method_name).lower(), logging.INFO)
    if level < logging.INFO:
        return event_dict
    _busy.on = True
    try:
        source = event_dict.get("_record")  # set when the line came from a stdlib logger
        exc_info = _exc_info(event_dict.get("exc_info")) or (source.exc_info if source else None)
        record = logging.LogRecord(
            name,
            level,
            source.pathname if source else "",
            source.lineno if source else 0,
            str(event_dict.get("event", "")),
            None,
            exc_info,
            func=source.funcName if source else None,
        )
        span = _named_span(event_dict)
        skip = _RESERVED | _SPAN_FIELDS if span is not None else _RESERVED
        for key, value in event_dict.items():
            if key.startswith("_") or key in skip or key in record.__dict__:
                continue
            if isinstance(value, (str, bool, int, float)):
                record.__dict__[key] = value
        if span is None:
            _otel_handler().handle(record)
        else:
            from opentelemetry import context, trace

            token = context.attach(trace.set_span_in_context(span))
            try:
                _otel_handler().handle(record)
            finally:
                context.detach(token)
    except Exception:  # noqa: BLE001 - never break Airflow's logging
        pass
    finally:
        _busy.on = False
    return event_dict


def otel_handler() -> logging.Handler:
    """dictConfig factory for Airflow 2: the OTel LoggingHandler, or a no-op if it can't be built."""
    try:
        return _otel_handler()
    except Exception:  # noqa: BLE001 - never break Airflow's logging setup
        return logging.NullHandler()


def _install() -> None:
    """Add otel_processor to the processor chain Airflow's configure_logging is about to build."""
    try:
        from airflow._shared.logging import structlog as airflow_structlog
    except ImportError:
        # Airflow 2 has no structlog chain, and applies LOGGING_CONFIG as given: a plain handler on
        # root (component logs) and on the non-propagating airflow.task logger (task logs) does it.
        LOGGING_CONFIG["handlers"]["otel"] = {"()": f"{__name__}.otel_handler"}
        for logger in (LOGGING_CONFIG["root"], LOGGING_CONFIG["loggers"]["airflow.task"]):
            logger["handlers"] = [*logger["handlers"], "otel"]
        return
    original = airflow_structlog.structlog_processors
    if getattr(original, "_otel_wrapped", False):
        return

    @functools.wraps(original)
    def structlog_processors(*args, **kwargs):
        result = original(*args, **kwargs)
        # The original is cached, so this is the same list on every call: add the processor once.
        if otel_processor not in result[0]:
            result[0].append(otel_processor)
        return result

    structlog_processors._otel_wrapped = True
    airflow_structlog.structlog_processors = structlog_processors


if _enabled():
    _install()
