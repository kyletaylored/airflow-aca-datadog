"""Airflow logging config that also ships log records over OTLP.

Used via ``AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=otel_logging_config.LOGGING_CONFIG``
when ``OTEL_LOGS_EXPORTER=otlp``.

Airflow emits OTel traces and metrics natively, but not logs. This adds an OTel
``LoggingHandler`` to Airflow's own logging config (root + ``airflow.task``).
Declaring it in Airflow's config, rather than attaching it to the root logger
directly, matters because Airflow's ``dictConfig`` replaces root handlers. The
handler exports over OTLP using the standard ``OTEL_EXPORTER_OTLP_*`` env vars
(on Azure Container Apps: the managed agent's gRPC endpoint). Records emitted
inside a span carry its trace/span ids, so Datadog correlates logs with traces.
"""
from __future__ import annotations

import logging
import os
import threading
from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

_lock = threading.Lock()


def _logger_provider():
    """The global SDK LoggerProvider, creating one if logs export is enabled."""
    from opentelemetry._logs import get_logger_provider, set_logger_provider
    from opentelemetry.sdk._logs import LoggerProvider

    with _lock:
        provider = get_logger_provider()
        if isinstance(provider, LoggerProvider):
            return provider
        if os.environ.get("OTEL_LOGS_EXPORTER", "").strip().lower() != "otlp":
            return None
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource

        # Resource.create() picks up OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES.
        provider = LoggerProvider(resource=Resource.create())
        provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
        set_logger_provider(provider)
        return provider


def otel_handler() -> logging.Handler:
    """dictConfig factory: an OTel LoggingHandler, or a no-op if logs export is off."""
    try:
        from opentelemetry.sdk._logs import LoggingHandler

        provider = _logger_provider()
    except Exception:  # noqa: BLE001 - never break Airflow's logging setup
        return logging.NullHandler()
    if provider is None:
        return logging.NullHandler()
    return LoggingHandler(level=logging.INFO, logger_provider=provider)


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["otel"] = {"()": f"{__name__}.otel_handler"}
# Component logs (scheduler, triggerer, api-server, dag-processor) propagate to
# root; task logs go to the non-propagating "airflow.task" logger.
for _logger in (LOGGING_CONFIG["root"], LOGGING_CONFIG["loggers"]["airflow.task"]):
    _logger["handlers"] = [*_logger["handlers"], "otel"]
