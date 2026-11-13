"""Airflow logging config that also ships log records over OTLP.

Used via ``AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=otel_logging_config.LOGGING_CONFIG``.

``opentelemetry-instrument`` can attach an OTel log handler to the root logger
(``OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED``), but Airflow then runs
``logging.config.dictConfig`` with its own config, which replaces the root
logger's handlers and silently drops it. Declaring the handler *in* Airflow's
config instead keeps it attached. Records carry the active trace/span ids, so
Datadog correlates them with traces.

The handler uses the global ``LoggerProvider`` that ``opentelemetry-instrument``
sets up when ``OTEL_LOGS_EXPORTER=otlp``; without one it is a no-op.
"""
from __future__ import annotations

import logging
from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG


def otel_handler() -> logging.Handler:
    """dictConfig factory for an OTel ``LoggingHandler`` on the global provider."""
    try:
        from opentelemetry._logs import get_logger_provider
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    except ImportError:  # OTel SDK not installed: keep Airflow's logging working
        return logging.NullHandler()
    provider = get_logger_provider()
    # Processes not started under opentelemetry-instrument (e.g. the image
    # entrypoint's `airflow db check`) only have the API's proxy provider, which
    # LoggingHandler can't use; fall back to a no-op handler there.
    if not isinstance(provider, LoggerProvider):
        return logging.NullHandler()
    return LoggingHandler(level=logging.INFO, logger_provider=provider)


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["otel"] = {"()": f"{__name__}.otel_handler"}
# Component logs (scheduler, triggerer, webserver) propagate to root; task logs
# go to the non-propagating "airflow.task" logger, so attach to both.
for _logger in (LOGGING_CONFIG["root"], LOGGING_CONFIG["loggers"]["airflow.task"]):
    _logger["handlers"] = [*_logger["handlers"], "otel"]
