"""Offline tests for the DAG-run -> OpenTelemetry trace listener (in-memory exporter)."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import otel_dag_run_traces as odt
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def tracer_and_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def _dag_run(state="success", **kw):
    return SimpleNamespace(
        dag_id="contoso_sales", run_id="manual__1", run_type="manual", state=state,
        start_date=T0, end_date=T0 + timedelta(minutes=5), queued_at=T0, external_trigger=True,
        **kw,
    )


def _ti(task_id, state="success", start=0, end=60, map_index=-1):
    return SimpleNamespace(
        task_id=task_id, state=state, map_index=map_index, try_number=1,
        operator="AzureContainerAppsJobOperator", hostname="sched-0", queued_dttm=T0,
        duration=None if start is None else float(end - start),
        start_date=None if start is None else T0 + timedelta(seconds=start),
        end_date=None if end is None else T0 + timedelta(seconds=end),
    )


def _by_name(exporter):
    return {s.name: s for s in exporter.get_finished_spans()}


def test_root_and_task_spans_share_trace_and_timing(tracer_and_exporter):
    tracer, exporter = tracer_and_exporter
    odt.emit_dag_run_trace(_dag_run(), [_ti("extract", end=30), _ti("load", start=30, end=90)], tracer)

    spans = _by_name(exporter)
    root, extract, load = spans["contoso_sales"], spans["extract"], spans["load"]
    assert root.parent is None
    for child in (extract, load):
        assert child.parent.span_id == root.context.span_id
        assert child.context.trace_id == root.context.trace_id
    assert root.start_time == int(T0.timestamp() * 1e9)
    assert load.end_time - load.start_time == 60 * 1_000_000_000
    assert root.attributes["operation.name"] == "airflow.dag_run"
    assert load.attributes["resource.name"] == "contoso_sales.load"
    assert "airflow.map_index" not in load.attributes  # -1 = unmapped, omitted


def test_failures_mark_error_and_unstarted_tasks_are_skipped(tracer_and_exporter):
    tracer, exporter = tracer_and_exporter
    tis = [_ti("boom", state="failed"), _ti("never", state="upstream_failed", start=None, end=None),
           _ti("shard", map_index=3)]
    odt.emit_dag_run_trace(_dag_run(state="failed"), tis, tracer)

    spans = _by_name(exporter)
    assert set(spans) == {"contoso_sales", "boom", "shard"}
    assert spans["contoso_sales"].status.status_code is StatusCode.ERROR
    assert spans["boom"].status.status_code is StatusCode.ERROR
    assert spans["shard"].attributes["airflow.map_index"] == 3


def test_listener_never_raises(monkeypatch):
    def explode(*_a, **_k):
        raise RuntimeError("db down")

    monkeypatch.setattr(odt, "_task_instances", explode)
    odt._DagRunTraceListener().on_dag_run_success(_dag_run(), "ok")  # must not raise


@pytest.mark.parametrize("value,expected", [("true", True), ("1", True), ("", False), ("no", False)])
def test_enable_flag(monkeypatch, value, expected):
    monkeypatch.setenv(odt.ENABLE_ENV, value)
    assert odt._enabled() is expected


def test_task_completion_flushes_spans(monkeypatch):
    calls = []
    monkeypatch.setattr(odt.trace, "get_tracer_provider",
                        lambda: SimpleNamespace(force_flush=lambda ms: calls.append(ms)))
    listener = odt._DagRunTraceListener()
    listener.on_task_instance_success(task_instance=None)
    listener.on_task_instance_failed(task_instance=None)
    assert calls == [2000, 2000]
