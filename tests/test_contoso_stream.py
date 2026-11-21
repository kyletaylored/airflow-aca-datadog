"""The order stream: storefront and processor helpers, and the DAG's landing load.

No Kafka, storage account or database is needed; the clients are fakes.
"""
from __future__ import annotations

import json
import os
import random
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

_ROOT = os.path.dirname(os.path.dirname(__file__))
for _path in (os.path.join(_ROOT, "services"), os.path.join(_ROOT, "airflow", "dags")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import contoso_stream as stream  # noqa: E402
import order_processor  # noqa: E402
from contoso_dw import landing  # noqa: E402
from contoso_dw import sql as dw  # noqa: E402

# --- names that must agree for lineage to join -----------------------------------


def test_processor_and_dag_name_the_landing_zone_the_same_way():
    processor = stream.landing_dataset("stdlabc", "landing")
    dag = landing.landing_dataset("stdlabc", "landing")
    assert (processor["namespace"], processor["name"]) == dag
    assert dag == ("abfss://landing@stdlabc.dfs.core.windows.net", "orders")


def test_postgres_names_match_the_postgres_provider():
    assert landing.postgres_dataset("db.example.com", None, "contoso_dw", "raw", "orders") == (
        "postgres://db.example.com:5432",
        "contoso_dw.raw.orders",
    )


def test_kafka_dataset_uses_the_first_bootstrap_server():
    assert stream.kafka_dataset("ns.servicebus.windows.net:9093", "orders") == {
        "namespace": "kafka://ns.servicebus.windows.net:9093",
        "name": "orders",
    }


def test_stores_match_the_warehouse():
    assert stream.STORES == dw.STORES
    assert stream.CUSTOMER_POOL == dw.CUSTOMER_POOL


# --- services --------------------------------------------------------------------


def test_kafka_config_for_event_hubs():
    config = stream.kafka_config(
        {"KAFKA_BOOTSTRAP_SERVERS": "ns.servicebus.windows.net:9093", "KAFKA_CONNECTION_STRING": "Endpoint=sb://x"}
    )
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "$ConnectionString"
    assert config["sasl.password"] == "Endpoint=sb://x"


def test_orders_are_valid_and_parse_back():
    rng = random.Random(7)
    order = stream.make_order(rng, datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc))
    assert order["store"] in stream.STORES
    assert 1 <= int(order["customer_id"][1:]) <= stream.CUSTOMER_POOL
    assert order_processor.parse_order(json.dumps(order).encode()) == order


@pytest.mark.parametrize(
    "raw",
    [None, b"", b"not json", b"[1, 2]", json.dumps({"order_id": "x"}).encode()],
)
def test_processor_rejects_unusable_messages(raw):
    assert order_processor.parse_order(raw) is None


def test_blob_names_are_partitioned_by_day_and_hour():
    now = datetime(2026, 11, 14, 5, 7, 9, tzinfo=timezone.utc)
    assert order_processor.blob_name(now, "0123456789ab") == (
        "orders/dt=2026-11-14/hour=05/batch-20261114T050709-01234567.json"
    )


def test_ndjson_round_trips_through_the_dag_parser():
    orders = [stream.make_order(random.Random(i)) for i in range(3)]
    assert landing.parse_batch(order_processor.to_ndjson(orders)) == orders


def test_run_events_carry_lineage_and_the_row_count():
    common = {
        "job_namespace": "env",
        "job_name": order_processor.JOB_NAME,
        "run_id": "6f1c5b2e-0000-4000-8000-000000000000",
        "inputs": [stream.kafka_dataset("ns:9093", "orders")],
        "outputs": [stream.landing_dataset("acct", "landing")],
    }
    now = datetime(2026, 11, 14, tzinfo=timezone.utc)
    start = stream.run_event("START", event_time=now, **common)
    complete = stream.run_event("COMPLETE", event_time=now, output_rows=12, **common)
    assert start["job"]["name"] == "order_processor.land_orders"
    assert "outputFacets" not in start["outputs"][0]
    assert complete["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] == 12
    assert complete["run"]["runId"] == start["run"]["runId"]


def test_lineage_is_only_sent_with_datadog_settings():
    assert stream.lineage_endpoint({}) is None
    url, headers = stream.lineage_endpoint({"DD_API_KEY": "k", "DD_SITE": "us3.datadoghq.com"})
    assert url == "https://data-obs-intake.us3.datadoghq.com/api/v1/lineage"
    assert headers == {"DD-API-KEY": "k"}
    assert stream.send_lineage({}, {}) is False


# --- the DAG's landing load --------------------------------------------------------


RAW_ORDERS = {
    "order_id", "customer_id", "store", "channel", "items", "amount_cents", "currency",
    "ordered_at", "_batch_id", "_loaded_at",
}


class _Cursor:
    """Records statements, and keeps raw.orders' columns the way Postgres would."""

    def __init__(self, ingested, columns, ignore=(), fail=()):
        self.statements, self.rows, self._ingested = [], [], ingested
        self.columns = set(columns)
        self._ignore, self._fail = set(ignore), set(fail)
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if statement in self._fail:
            raise RuntimeError("must be owner of table orders")
        if statement == dw.RAW_ORDERS_COLUMNS:
            self._result = [(c,) for c in sorted(self.columns)]
        if statement in self._ignore:
            return
        if statement == dw.ADD_PROMO_CODE:
            self.columns.add("promo_code")
        if statement == dw.DROP_PROMO_CODE:
            self.columns.discard("promo_code")
        if statement == dw.RESTORE_AMOUNT_COLUMN and "amount_minor" in self.columns:
            self.columns = self.columns - {"amount_minor"} | {"amount_cents"}
        if statement == dw.BREAK_AMOUNT_COLUMN:
            self.columns = self.columns - {"amount_cents"} | {"amount_minor"}
        if statement.startswith("SELECT name FROM raw._ingested_files"):
            self._result = [(n,) for n in self._ingested]
        if statement.startswith("INSERT INTO raw._ingested_files"):
            self._ingested.add(params[0])

    def executemany(self, statement, rows):
        self.rows.extend(rows)

    def fetchall(self):
        return self._result


class _Conn:
    def __init__(self, ingested=(), columns=RAW_ORDERS, **cursor):
        self.cur = _Cursor(set(ingested), columns, **cursor)
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True


class _Container:
    def __init__(self, blobs):
        self.blobs = blobs

    def list_blobs(self, name_starts_with=""):
        return [SimpleNamespace(name=n) for n in self.blobs if n.startswith(name_starts_with)]

    def download_blob(self, name):
        return SimpleNamespace(readall=lambda: self.blobs[name])


def _blobs(count=2, per_blob=50):
    rng = random.Random(1)
    return {
        f"orders/dt=2026-11-14/hour=05/batch-{i}.json": order_processor.to_ndjson(
            [stream.make_order(rng) for _ in range(per_blob)]
        )
        for i in range(count)
    }


def test_load_only_takes_new_blobs():
    blobs = _blobs(count=3)
    first = sorted(blobs)[0]
    conn = _Conn(ingested=[first])
    stats, changes = landing.load_landed_orders(conn, _Container(blobs), landing.Chaos(), random.Random(0))
    assert stats == {"blobs": 2, "orders": 100, "waiting": 0}
    assert len(conn.cur.rows) == 100
    assert {row[-1] for row in conn.cur.rows} == set(sorted(blobs)[1:])
    assert conn.committed
    # schema_drift unset: the column is left as it is, and there's nothing to log.
    assert (dw.DROP_PROMO_CODE, None) not in conn.cur.statements
    assert (dw.ADD_PROMO_CODE, None) not in conn.cur.statements
    assert changes == []


def test_load_ignores_adls_directory_entries():
    blobs = {"orders/dt=2026-11-14": b"", "orders/dt=2026-11-14/hour=05": b"", **_blobs(count=1)}
    conn = _Conn()
    stats, _ = landing.load_landed_orders(conn, _Container(blobs), landing.Chaos(), random.Random(0))
    assert stats == {"blobs": 1, "orders": 50, "waiting": 0}


def test_skip_load_leaves_the_backlog():
    conn = _Conn()
    stats, _ = landing.load_landed_orders(
        conn, _Container(_blobs()), landing.Chaos(skip_load=True), random.Random(0)
    )
    assert stats == {"blobs": 0, "orders": 0, "waiting": 2}
    assert conn.cur.rows == []


def test_chaos_on_load():
    rng = random.Random(3)
    orders = [stream.make_order(rng) for _ in range(1000)]
    nulled = landing.apply_chaos(orders, landing.Chaos(null_customers=True), random.Random(0))
    assert 200 < sum(o["customer_id"] is None for o in nulled) < 400
    dropped = landing.apply_chaos(orders, landing.Chaos(volume_drop=True), random.Random(0))
    assert 20 < len(dropped) < 90


def _describe(changes):
    return [c.describe() for c in changes]


def test_schema_drift_on_adds_the_column():
    conn = _Conn()
    _, changes = landing.load_landed_orders(conn, _Container({}), landing.Chaos(schema_drift=True))
    assert (dw.ADD_PROMO_CODE, None) in conn.cur.statements
    assert "promo_code" in conn.cur.columns
    assert _describe(changes) == ["schema_drift on: added the promo_code column to raw.orders"]
    assert conn.committed


def test_schema_drift_off_drops_the_column():
    conn = _Conn(columns=RAW_ORDERS | {"promo_code"})
    _, changes = landing.load_landed_orders(conn, _Container({}), landing.Chaos(schema_drift=False))
    assert (dw.DROP_PROMO_CODE, None) in conn.cur.statements
    assert "promo_code" not in conn.cur.columns
    assert _describe(changes) == ["schema_drift off: dropped the promo_code column from raw.orders"]


@pytest.mark.parametrize(
    ("columns", "drift", "line"),
    [
        (RAW_ORDERS | {"promo_code"}, True, "schema_drift on: raw.orders already had the promo_code column, so nothing changed"),
        (RAW_ORDERS, False, "schema_drift off: raw.orders had no promo_code column to drop, so nothing changed"),
    ],
)
def test_schema_drift_says_when_it_changed_nothing(columns, drift, line):
    _, changes = landing.load_landed_orders(_Conn(columns=columns), _Container({}), landing.Chaos(schema_drift=drift))
    assert _describe(changes) == [line]


def test_a_statement_that_changes_nothing_fails_the_load():
    # Say ADD COLUMN IF NOT EXISTS ran against another table: no error, but no column either.
    conn = _Conn(ignore=[dw.ADD_PROMO_CODE])
    with pytest.raises(landing.SchemaChangeFailed, match="should have a promo_code column"):
        landing.load_landed_orders(conn, _Container(_blobs(count=1)), landing.Chaos(schema_drift=True))
    assert not conn.committed
    assert conn.cur.rows == []


def test_a_statement_that_errors_fails_the_load():
    conn = _Conn(columns=RAW_ORDERS | {"promo_code"}, fail=[dw.DROP_PROMO_CODE])
    with pytest.raises(landing.SchemaChangeFailed, match="the ALTER TABLE failed: must be owner") as failed:
        landing.load_landed_orders(conn, _Container({}), landing.Chaos(schema_drift=False))
    assert isinstance(failed.value.__cause__, RuntimeError)
    assert not conn.committed


def test_chaos_from_params_keeps_schema_drift_unset():
    assert landing.Chaos.from_params({}).schema_drift is None
    assert landing.Chaos.from_params({"schema_drift": False}).schema_drift is False
    assert landing.Chaos.from_params({"schema_drift": True}).schema_drift is True


def test_load_restores_the_amount_column_first():
    conn = _Conn(columns=RAW_ORDERS - {"amount_cents"} | {"amount_minor"})
    _, changes = landing.load_landed_orders(
        conn, _Container(_blobs(count=1)), landing.Chaos(), random.Random(0)
    )
    statements = [s for s, _ in conn.cur.statements]
    assert statements.index(dw.RESTORE_AMOUNT_COLUMN) < statements.index(
        "SELECT name FROM raw._ingested_files"
    )
    assert dw.BREAK_AMOUNT_COLUMN not in statements
    assert _describe(changes) == ["breaking_schema cleanup: renamed raw.orders.amount_minor to amount_cents"]


def test_breaking_schema_renames_the_column_after_the_load():
    conn = _Conn()
    _, changes = landing.load_landed_orders(
        conn, _Container(_blobs(count=1)), landing.Chaos(breaking_schema=True), random.Random(0)
    )
    statements = [s for s, _ in conn.cur.statements]
    assert statements.index(dw.BREAK_AMOUNT_COLUMN) > statements.index(dw.PURGE_OLD_ORDERS)
    assert len(conn.cur.rows) == 50
    assert conn.committed
    assert _describe(changes) == ["breaking_schema on: renamed raw.orders.amount_cents to amount_minor"]


def test_each_alter_table_gets_a_span_that_the_log_line_names(monkeypatch):
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.trace import StatusCode, format_span_id

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(landing, "_tracer", lambda: provider.get_tracer("test"))

    _, (change,) = landing.load_landed_orders(_Conn(), _Container({}), landing.Chaos(schema_drift=True))
    drift, restore = exporter.get_finished_spans()
    assert drift.name == "ALTER TABLE raw.orders"
    assert drift.attributes["db.statement"] == dw.ADD_PROMO_CODE
    assert drift.attributes["contoso.schema.outcome"] == "added"
    assert {k: v for k, v in drift.attributes.items() if k.startswith("contoso.column")} == {
        "contoso.column.name": "promo_code",
        "contoso.column.wanted": "present",
        "contoso.column.before": "absent",
        "contoso.column.after": "present",
    }
    # No attribute is both a value and the parent of others, which Datadog can't keep.
    names = set(drift.attributes)
    assert not any(other.startswith(name + ".") for name in names for other in names)
    assert restore.attributes["contoso.schema.outcome"] == "unchanged"
    assert change.span_id == format_span_id(drift.context.span_id)

    exporter.clear()
    with pytest.raises(landing.SchemaChangeFailed) as failed:
        landing.load_landed_orders(_Conn(ignore=[dw.ADD_PROMO_CODE]), _Container({}), landing.Chaos(schema_drift=True))
    (span,) = exporter.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["contoso.schema.outcome"] == "failed"
    assert failed.value.span_id == format_span_id(span.context.span_id)


def test_chaos_reads_dag_params():
    assert landing.Chaos.from_params({"null_customers": True, "interval_minutes": 15}) == landing.Chaos(
        null_customers=True
    )
