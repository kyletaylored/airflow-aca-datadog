"""One distributed OpenTelemetry trace per DAG run.

    dag_run (root, scheduler)
    ├─ task attempt (one span per try, incl. retries)
    │  ├─ HTTP calls made *inside* the task process (ARM start-job, MSI token, ...)
    │  └─ HTTP calls made by the task's deferred trigger in the triggerer (polling)
    └─ ...

Why not Airflow's built-in ``[traces]``: Airflow 2.10 hard-codes the OTLP/HTTP
exporter, and the Azure Container Apps managed OTel agent accepts gRPC only.

How the pieces join up without passing context between processes: trace and
span ids are *derived* from ``dag_id``/``run_id``/``task_id``/``map_index``/
``try_number``. So every process computes the same ids:

* the task process (``on_task_instance_running``) and the triggerer
  (``attach_task_context`` from the trigger) make the task span the active
  parent, so the ``requests`` auto-instrumentation nests its spans under it;
* the scheduler emits the root and task spans with exactly those ids once the
  run finishes, from recorded timestamps (one long-lived process, so nothing is
  lost to LocalExecutor task processes exiting via ``os._exit()``).

Enabled only when ``AIRFLOW_OTEL_DAG_RUN_TRACES`` is truthy, and only does work
when ``opentelemetry-instrument`` installed an SDK tracer provider.
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
)

log = logging.getLogger(__name__)

ENABLE_ENV = "AIRFLOW_OTEL_DAG_RUN_TRACES"
TRACER_NAME = "airflow.dag_run_traces"
_FAILED_STATES = {"failed", "upstream_failed"}


def _enabled() -> bool:
    return os.environ.get(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


# --- deterministic ids -------------------------------------------------------

def _hash_id(bits: int, *parts: Any) -> int:
    digest = hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(digest[: bits // 8], "big") or 1  # 0 is an invalid id


def trace_id_for(dag_id: str, run_id: str) -> int:
    return _hash_id(128, "trace", dag_id, run_id)


def root_span_id_for(dag_id: str, run_id: str) -> int:
    return _hash_id(64, "dag_run", dag_id, run_id)


def task_span_id_for(dag_id: str, run_id: str, task_id: str, map_index: int, try_number: int) -> int:
    return _hash_id(64, "task", dag_id, run_id, task_id, map_index, try_number)


def task_span_context(ti: Any) -> SpanContext:
    """The (remote) span context of ``ti``'s current attempt."""
    map_index = getattr(ti, "map_index", -1)
    map_index = -1 if map_index is None else map_index
    return SpanContext(
        trace_id=trace_id_for(ti.dag_id, ti.run_id),
        span_id=task_span_id_for(ti.dag_id, ti.run_id, ti.task_id, map_index, ti.try_number),
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )


def attach_task_context(ti: Any) -> object | None:
    """Make ``ti``'s task span the active parent in this context (task process or trigger).

    Returns the context token, or ``None`` when tracing is off. Never raises.
    """
    if ti is None or not _enabled():
        return None
    try:
        span = NonRecordingSpan(task_span_context(ti))
        return otel_context.attach(trace.set_span_in_context(span))
    except Exception:  # noqa: BLE001
        log.debug("Could not attach task trace context", exc_info=True)
        return None


# --- emitting spans with preset ids ------------------------------------------

class _PresetIdGenerator:
    """Hands out preset ids for the next span (falls back to random)."""

    def __init__(self) -> None:
        from opentelemetry.sdk.trace.id_generator import RandomIdGenerator

        self._random = RandomIdGenerator()
        self.next_trace_id: int | None = None
        self.next_span_id: int | None = None

    def generate_trace_id(self) -> int:
        value, self.next_trace_id = self.next_trace_id, None
        return value or self._random.generate_trace_id()

    def generate_span_id(self) -> int:
        value, self.next_span_id = self.next_span_id, None
        return value or self._random.generate_span_id()


_provider_lock = threading.Lock()
_provider: Any = None


def build_tracer_provider(span_processor: Any = None) -> Any:
    """A TracerProvider whose span ids can be preset.

    Uses the same resource (``OTEL_SERVICE_NAME``/``OTEL_RESOURCE_ATTRIBUTES``)
    and OTLP exporter env config as ``opentelemetry-instrument``.
    """
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider(
        resource=Resource.create(), id_generator=_PresetIdGenerator(), shutdown_on_exit=False
    )
    if span_processor is None:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        span_processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(span_processor)
    return provider


def _default_provider() -> Any:
    """Lazily built in the scheduler; ``None`` when no OTel SDK is active."""
    global _provider
    from opentelemetry.sdk.trace import TracerProvider

    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        return None
    with _provider_lock:
        if _provider is None:
            _provider = build_tracer_provider()
        return _provider


# --- span construction -------------------------------------------------------

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


def emit_dag_run_trace(dag_run: Any, attempts: Iterable[Any], provider: Any = None) -> None:
    """Record ``dag_run`` and every task attempt as one trace with derived ids."""
    provider = provider or _default_provider()
    if provider is None:
        return
    tracer = provider.get_tracer(TRACER_NAME)
    ids: _PresetIdGenerator = provider.id_generator
    dag_id, run_id = dag_run.dag_id, dag_run.run_id

    now = datetime.now(timezone.utc)
    run_end = dag_run.end_date or now
    run_state = _state(dag_run)

    with _provider_lock:
        ids.next_trace_id = trace_id_for(dag_id, run_id)
        ids.next_span_id = root_span_id_for(dag_id, run_id)
        root = tracer.start_span(
            dag_id,
            kind=SpanKind.INTERNAL,
            start_time=_ns(dag_run.start_date or dag_run.queued_at or now),
            attributes=_attrs(**{
                # Datadog maps these to the span's operation and resource names.
                "operation.name": "airflow.dag_run",
                "resource.name": dag_id,
                "airflow.dag_id": dag_id,
                "airflow.run_id": run_id,
                "airflow.run_type": _enum_str(getattr(dag_run, "run_type", None)),
                "airflow.dag_run.state": run_state,
                "airflow.dag_run.queued_at": getattr(dag_run, "queued_at", None),
                "airflow.dag_run.external_trigger": getattr(dag_run, "external_trigger", None),
            }),
        )
        parent = trace.set_span_in_context(root)

        for ti in attempts:
            if ti.start_date is None:
                continue  # never started (skipped / upstream_failed)
            ti_state = _state(ti)
            map_index = getattr(ti, "map_index", -1)
            map_index = -1 if map_index is None else map_index
            ids.next_span_id = task_span_id_for(dag_id, run_id, ti.task_id, map_index, ti.try_number)
            span = tracer.start_span(
                ti.task_id,
                context=parent,
                kind=SpanKind.INTERNAL,
                start_time=_ns(ti.start_date),
                attributes=_attrs(**{
                    "operation.name": "airflow.task",
                    "resource.name": f"{dag_id}.{ti.task_id}",
                    "airflow.dag_id": dag_id,
                    "airflow.run_id": run_id,
                    "airflow.task_id": ti.task_id,
                    "airflow.map_index": map_index if map_index >= 0 else None,
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


def _task_attempts(dag_run: Any) -> list[Any]:
    """Current TIs plus earlier tries (TaskInstanceHistory, Airflow 2.10+).

    Reads on the scheduler's *own* session: ``get_task_instances()`` without a
    session would go through Airflow's scoped session and could commit/close
    the scheduler's in-flight transaction.
    """
    from sqlalchemy.orm import object_session

    session = object_session(dag_run)
    if session is None:
        return []
    attempts = list(dag_run.get_task_instances(session=session))
    try:
        from airflow.models.taskinstancehistory import TaskInstanceHistory

        attempts += (
            session.query(TaskInstanceHistory)
            .filter(
                TaskInstanceHistory.dag_id == dag_run.dag_id,
                TaskInstanceHistory.run_id == dag_run.run_id,
            )
            .all()
        )
    except ImportError:  # Airflow < 2.10: only the latest try is known
        pass
    return attempts


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
    # --- task process ---
    @hookimpl
    def on_task_instance_running(self, task_instance):
        # The context stays attached for the rest of this task process.
        attach_task_context(task_instance)

    @hookimpl
    def on_task_instance_success(self, task_instance):  # noqa: ARG002
        self._safe(_flush)

    @hookimpl
    def on_task_instance_failed(self, task_instance):  # noqa: ARG002
        self._safe(_flush)

    # --- scheduler: emit the trace once the run is finished ---
    @hookimpl
    def on_dag_run_success(self, dag_run, msg):  # noqa: ARG002
        self._emit(dag_run)

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg):  # noqa: ARG002
        self._emit(dag_run)

    def _emit(self, dag_run) -> None:
        self._safe(lambda: emit_dag_run_trace(dag_run, _task_attempts(dag_run)))

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
