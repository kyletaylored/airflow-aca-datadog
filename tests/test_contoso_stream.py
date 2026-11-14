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


class _Cursor:
    def __init__(self, ingested):
        self.statements, self.rows, self._ingested = [], [], ingested
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if statement.startswith("SELECT name FROM raw._ingested_files"):
            self._result = [(n,) for n in self._ingested]
        if statement.startswith("INSERT INTO raw._ingested_files"):
            self._ingested.add(params[0])

    def executemany(self, statement, rows):
        self.rows.extend(rows)

    def fetchall(self):
        return self._result


class _Conn:
    def __init__(self, ingested=()):
        self.cur = _Cursor(set(ingested))
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
    stats = landing.load_landed_orders(conn, _Container(blobs), landing.Chaos(), random.Random(0))
    assert stats == {"blobs": 2, "orders": 100, "waiting": 0}
    assert len(conn.cur.rows) == 100
    assert {row[-1] for row in conn.cur.rows} == set(sorted(blobs)[1:])
    assert conn.committed
    assert (dw.DROP_PROMO_CODE, None) in conn.cur.statements


def test_load_ignores_adls_directory_entries():
    blobs = {"orders/dt=2026-11-14": b"", "orders/dt=2026-11-14/hour=05": b"", **_blobs(count=1)}
    conn = _Conn()
    stats = landing.load_landed_orders(conn, _Container(blobs), landing.Chaos(), random.Random(0))
    assert stats == {"blobs": 1, "orders": 50, "waiting": 0}


def test_skip_load_leaves_the_backlog():
    conn = _Conn()
    stats = landing.load_landed_orders(
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


def test_schema_drift_on_load():
    conn = _Conn()
    landing.load_landed_orders(conn, _Container({}), landing.Chaos(schema_drift=True))
    assert (dw.ADD_PROMO_CODE, None) in conn.cur.statements


def test_chaos_reads_dag_params():
    assert landing.Chaos.from_params({"null_customers": True, "interval_minutes": 15}) == landing.Chaos(
        null_customers=True
    )
