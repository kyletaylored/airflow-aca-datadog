"""Offline tests for the DAG-run -> distributed OpenTelemetry trace plugin."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import otel_dag_run_traces as odt
import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
DAG, RUN = "contoso_sales", "scheduled__1"


@pytest.fixture()
def provider_and_exporter():
    exporter = InMemorySpanExporter()
    return odt.build_tracer_provider(SimpleSpanProcessor(exporter)), exporter


def _dag_run(state="success"):
    return SimpleNamespace(
        dag_id=DAG, run_id=RUN, run_type="scheduled", state=state,
        start_date=T0, end_date=T0 + timedelta(minutes=5), queued_at=T0, external_trigger=False,
    )


def _ti(task_id, state="success", start=0, end=60, map_index=-1, try_number=1):
    return SimpleNamespace(
        dag_id=DAG, run_id=RUN, task_id=task_id, state=state, map_index=map_index,
        try_number=try_number, operator="AzureContainerAppsJobOperator", hostname="sched-0",
        queued_dttm=T0, duration=None if start is None else float(end - start),
        start_date=None if start is None else T0 + timedelta(seconds=start),
        end_date=None if end is None else T0 + timedelta(seconds=end),
    )


def _spans(exporter):
    return {(s.name, s.attributes.get("airflow.try_number")): s for s in exporter.get_finished_spans()}


def test_ids_are_derived_so_every_process_agrees(provider_and_exporter):
    provider, exporter = provider_and_exporter
    ti = _ti("register_store", map_index=2)
    odt.emit_dag_run_trace(_dag_run(), [ti], provider)

    spans = _spans(exporter)
    root, task = spans[(DAG, None)], spans[("register_store", 1)]
    # The scheduler-emitted task span has exactly the id the task process and
    # the triggerer attach as their parent.
    expected = odt.task_span_context(ti)
    assert task.context.trace_id == expected.trace_id == root.context.trace_id
    assert task.context.span_id == expected.span_id
    assert root.context.span_id == odt.root_span_id_for(DAG, RUN)
    assert task.parent.span_id == root.context.span_id
    assert root.parent is None
    assert task.attributes["airflow.map_index"] == 2
    assert root.attributes["operation.name"] == "airflow.dag_run"


def test_each_retry_gets_its_own_span_and_failures_are_errors(provider_and_exporter):
    provider, exporter = provider_and_exporter
    attempts = [
        _ti("register", state="failed", start=0, end=10, try_number=1),   # TaskInstanceHistory
        _ti("register", state="success", start=30, end=60, try_number=2),  # current TI
        _ti("never", state="upstream_failed", start=None, end=None),
    ]
    odt.emit_dag_run_trace(_dag_run(state="success"), attempts, provider)

    spans = _spans(exporter)
    assert set(spans) == {(DAG, None), ("register", 1), ("register", 2)}
    assert spans[("register", 1)].status.status_code is StatusCode.ERROR
    assert spans[("register", 2)].status.status_code is StatusCode.UNSET
    assert spans[("register", 1)].context.span_id != spans[("register", 2)].context.span_id
    assert spans[("register", 2)].end_time - spans[("register", 2)].start_time == 30 * 10**9


def test_attach_task_context_makes_task_span_the_parent(monkeypatch):
    monkeypatch.setenv(odt.ENABLE_ENV, "true")
    ti = _ti("run_aca_job")
    token = odt.attach_task_context(ti)
    try:
        current = trace.get_current_span().get_span_context()
        assert current == odt.task_span_context(ti)
    finally:
        otel_context.detach(token)


def test_attach_is_a_noop_when_disabled(monkeypatch):
    monkeypatch.setenv(odt.ENABLE_ENV, "")
    assert odt.attach_task_context(_ti("x")) is None


def test_emit_without_an_sdk_provider_is_a_noop():
    # No opentelemetry-instrument here: the global provider is the API proxy.
    odt.emit_dag_run_trace(_dag_run(), [_ti("x")])  # must not raise


def test_listener_never_raises(monkeypatch):
    def explode(*_a, **_k):
        raise RuntimeError("db down")

    monkeypatch.setattr(odt, "_task_attempts", explode)
    odt._DagRunTraceListener().on_dag_run_success(_dag_run(), "ok")


def test_task_completion_flushes_spans(monkeypatch):
    calls = []
    monkeypatch.setattr(odt.trace, "get_tracer_provider",
                        lambda: SimpleNamespace(force_flush=lambda ms: calls.append(ms)))
    listener = odt._DagRunTraceListener()
    listener.on_task_instance_success(task_instance=None)
    listener.on_task_instance_failed(task_instance=None)
    assert calls == [2000, 2000]


@pytest.mark.parametrize("value,expected", [("true", True), ("1", True), ("", False), ("no", False)])
def test_enable_flag(monkeypatch, value, expected):
    monkeypatch.setenv(odt.ENABLE_ENV, value)
    assert odt._enabled() is expected
