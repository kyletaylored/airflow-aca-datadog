"""Emit one OpenTelemetry trace per DAG run: a root span plus a child span per task.

Why this exists instead of Airflow's built-in ``[traces]`` exporter:
Airflow 2.10's native tracer hard-codes the OTLP/**HTTP** exporter, while the
Azure Container Apps *managed* OpenTelemetry agent only accepts OTLP/**gRPC**.
This listener instead uses the process-wide ``TracerProvider`` that
``opentelemetry-instrument`` builds from the ``OTEL_*`` env vars ACA injects
(gRPC), so spans flow through the managed agent to Datadog.

The trace is emitted from the scheduler when a DAG run finishes, using the
recorded start/end timestamps. Doing it in one long-lived process avoids the
LocalExecutor task processes exiting via ``os._exit()`` before a span batch is
flushed, and needs no cross-process trace-context plumbing.

Enabled only when ``AIRFLOW_OTEL_DAG_RUN_TRACES`` is truthy. If no OTel SDK is
configured the OpenTelemetry API is a no-op, so this is always safe to load.
"""
from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

log = logging.getLogger(__name__)

ENABLE_ENV = "AIRFLOW_OTEL_DAG_RUN_TRACES"
TRACER_NAME = "airflow.dag_run_traces"
_FAILED_STATES = {"failed", "upstream_failed"}


def _enabled() -> bool:
    return os.environ.get(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _ns(dt: datetime | None) -> int | None:
    return int(dt.timestamp() * 1_000_000_000) if dt else None


def _enum_str(value: Any) -> str | None:
    """Airflow states/run types are str-Enums; return their plain value."""
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _state(obj: Any) -> str:
    return _enum_str(getattr(obj, "state", None)) or "unknown"


def _attrs(**kwargs: Any) -> dict[str, Any]:
    """Drop ``None`` values; OTel attributes must be str/bool/int/float."""
    out: dict[str, Any] = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        out[key] = value if isinstance(value, (str, bool, int, float)) else str(value)
    return out


def emit_dag_run_trace(
    dag_run: Any,
    task_instances: Iterable[Any],
    tracer: trace.Tracer | None = None,
) -> None:
    """Record ``dag_run`` and its ``task_instances`` as one trace."""
    tracer = tracer or trace.get_tracer(TRACER_NAME)
    now = datetime.now(timezone.utc)
    run_start = dag_run.start_date or dag_run.queued_at or now
    run_end = dag_run.end_date or now
    run_state = _state(dag_run)

    root = tracer.start_span(
        dag_run.dag_id,
        kind=SpanKind.INTERNAL,
        start_time=_ns(run_start),
        attributes=_attrs(**{
            # Datadog maps these to the span's operation and resource names.
            "operation.name": "airflow.dag_run",
            "resource.name": dag_run.dag_id,
            "airflow.dag_id": dag_run.dag_id,
            "airflow.run_id": dag_run.run_id,
            "airflow.run_type": _enum_str(getattr(dag_run, "run_type", None)),
            "airflow.dag_run.state": run_state,
            "airflow.dag_run.queued_at": getattr(dag_run, "queued_at", None),
            "airflow.dag_run.external_trigger": getattr(dag_run, "external_trigger", None),
        }),
    )
    ctx = trace.set_span_in_context(root)

    for ti in task_instances:
        if ti.start_date is None:
            # Never started (e.g. skipped / upstream_failed): nothing to time.
            continue
        ti_state = _state(ti)
        map_index = getattr(ti, "map_index", -1)
        span = tracer.start_span(
            ti.task_id,
            context=ctx,
            kind=SpanKind.INTERNAL,
            start_time=_ns(ti.start_date),
            attributes=_attrs(**{
                "operation.name": "airflow.task",
                "resource.name": f"{dag_run.dag_id}.{ti.task_id}",
                "airflow.dag_id": dag_run.dag_id,
                "airflow.run_id": dag_run.run_id,
                "airflow.task_id": ti.task_id,
                "airflow.map_index": map_index if map_index is not None and map_index >= 0 else None,
                "airflow.try_number": getattr(ti, "try_number", None),
                "airflow.task.state": ti_state,
                "airflow.operator": getattr(ti, "operator", None),
                "airflow.hostname": getattr(ti, "hostname", None) or None,
                "airflow.queued_at": getattr(ti, "queued_dttm", None),
                "airflow.duration_s": getattr(ti, "duration", None),
            }),
        )
        if ti_state in _FAILED_STATES:
            span.set_status(Status(StatusCode.ERROR, f"task {ti_state}"))
        span.end(end_time=_ns(ti.end_date or run_end))

    if run_state == "failed":
        root.set_status(Status(StatusCode.ERROR, "dag run failed"))
    root.end(end_time=_ns(run_end))


def _task_instances(dag_run: Any) -> list[Any]:
    """Read TIs on the scheduler's *own* session.

    ``dag_run.get_task_instances()`` without a session would go through
    Airflow's scoped session and could commit/close the scheduler's in-flight
    transaction, so reuse the session the DagRun is already bound to.
    """
    from sqlalchemy.orm import object_session

    session = object_session(dag_run)
    if session is None:
        return []
    return list(dag_run.get_task_instances(session=session))


def _flush() -> None:
    """Export buffered spans now.

    LocalExecutor task processes end with ``os._exit()``, which skips the SDK's
    atexit flush, so spans the task produced (e.g. the operator's ARM calls via
    the ``requests`` instrumentation) would otherwise be dropped.
    """
    force_flush = getattr(trace.get_tracer_provider(), "force_flush", None)
    if force_flush is not None:
        force_flush(2000)


class _DagRunTraceListener:
    # --- task process: flush before os._exit() ---
    @hookimpl
    def on_task_instance_success(self, task_instance):  # noqa: ARG002
        self._safe(_flush)

    @hookimpl
    def on_task_instance_failed(self, task_instance):  # noqa: ARG002
        self._safe(_flush)

    # --- scheduler: one trace per finished DAG run ---
    @hookimpl
    def on_dag_run_success(self, dag_run, msg):  # noqa: ARG002
        self._emit(dag_run)

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg):  # noqa: ARG002
        self._emit(dag_run)

    def _emit(self, dag_run) -> None:
        self._safe(lambda: emit_dag_run_trace(dag_run, _task_instances(dag_run)))

    @staticmethod
    def _safe(fn) -> None:
        # Never let tracing break the scheduler loop or a task.
        try:
            fn()
        except Exception:  # noqa: BLE001
            log.warning("OpenTelemetry DAG-run tracing hook failed", exc_info=True)


class OtelDagRunTracesPlugin(AirflowPlugin):
    name = "otel_dag_run_traces"
    listeners = [_DagRunTraceListener()] if _enabled() else []
