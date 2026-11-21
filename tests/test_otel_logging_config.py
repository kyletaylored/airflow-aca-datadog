"""Offline tests for the OTLP logging plugin (no collector, no network)."""
import importlib
import logging
import sys

import pytest

airflow_structlog = pytest.importorskip("airflow._shared.logging.structlog")
pytest.importorskip("opentelemetry.sdk._logs")


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.INFO)
        self.records = []
        self.spans = []

    def emit(self, record):
        from opentelemetry.trace import get_current_span

        self.records.append(record)
        # What the OTel handler would link the line to.
        self.spans.append(get_current_span().get_span_context())


@pytest.fixture
def plugin(monkeypatch):
    """The plugin, freshly imported with logs export on, and a handler that captures what it would export."""
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "airflow-test")
    current = airflow_structlog.structlog_processors
    # Unwrap only the plugin's own wrapper: functools.cache sets __wrapped__ too.
    original = current.__wrapped__ if getattr(current, "_otel_wrapped", False) else current
    # Start from Airflow's own chain builder, and put it back afterwards.
    monkeypatch.setattr(airflow_structlog, "structlog_processors", original)
    sys.modules.pop("otel_logging_config", None)
    module = importlib.import_module("otel_logging_config")
    capture = Capture()
    capture.addFilter(module._add_dd_service)
    monkeypatch.setattr(module, "_handler", capture)
    yield module, capture
    # The builder is cached and the plugin appends to its result, so drop that result too.
    if hasattr(original, "cache_clear"):
        original.cache_clear()
    sys.modules.pop("otel_logging_config", None)


def test_joins_airflows_processor_chain_once(plugin):
    module, _ = plugin
    shared, *_ = airflow_structlog.structlog_processors(False)
    airflow_structlog.structlog_processors(False)
    assert shared.count(module.otel_processor) == 1


def test_exports_a_structlog_line_with_its_attributes(plugin):
    module, capture = plugin
    event = {"event": "loaded 3 files", "level": "warning", "logger": "airflow.jobs", "dag_id": "contoso_orders", "timestamp": "now"}
    assert module.otel_processor(None, "warning", dict(event)) == event
    (record,) = capture.records
    assert (record.name, record.levelno, record.getMessage()) == ("airflow.jobs", logging.WARNING, "loaded 3 files")
    assert record.dag_id == "contoso_orders"
    assert record.__dict__["dd.service"] == "airflow-test"
    assert not hasattr(record, "timestamp")


def test_links_a_line_to_the_span_it_names(plugin):
    module, capture = plugin
    trace_id, span_id = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
    event = {"event": "dropped a column", "level": "info", "logger": "airflow.task", "trace_id": trace_id, "span_id": span_id}
    module.otel_processor(None, "info", dict(event))
    (record,), (span,) = capture.records, capture.spans
    assert (span.trace_id, span.span_id) == (int(trace_id, 16), int(span_id, 16))
    assert not hasattr(record, "trace_id")


def test_ignores_span_fields_that_arent_ids(plugin):
    module, capture = plugin
    module.otel_processor(None, "info", {"event": "x", "level": "info", "logger": "airflow.task", "trace_id": "not-hex", "span_id": "1"})
    (record,), (span,) = capture.records, capture.spans
    assert not span.is_valid
    assert record.trace_id == "not-hex"


def test_keeps_the_stdlib_records_source_location(plugin):
    module, capture = plugin
    source = logging.LogRecord("airflow.models", logging.INFO, "/x/dagbag.py", 42, "parsed", None, None, func="collect")
    module.otel_processor(None, "info", {"event": "parsed", "level": "info", "logger": "airflow.models", "_record": source})
    (record,) = capture.records
    assert (record.pathname, record.lineno, record.funcName) == ("/x/dagbag.py", 42, "collect")


@pytest.mark.parametrize(
    "event",
    [
        {"event": "debug detail", "level": "debug", "logger": "airflow.jobs"},
        {"event": "export failed", "level": "error", "logger": "opentelemetry.sdk._logs"},
    ],
)
def test_skips_debug_lines_and_otels_own_logs(plugin, event):
    module, capture = plugin
    module.otel_processor(None, event["level"], dict(event))
    assert capture.records == []
