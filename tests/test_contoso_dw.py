"""The contoso_dw demo pipeline: SQL, chaos switches, lineage, and the bootstrap.

No database is needed. Lineage is checked with openlineage-sql, the same parser
Airflow's OpenLineage provider uses, when it's installed.
"""
from __future__ import annotations

import importlib
import os
import sys
import types

import pytest

_DAGS = os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow", "dags")
if _DAGS not in sys.path:
    sys.path.insert(0, _DAGS)

from contoso_dw import sql as dw  # noqa: E402

jinja2 = pytest.importorskip("jinja2")

PARAMS = {
    "interval_minutes": 15,
    "null_customers": False,
    "volume_drop": False,
    "skip_load": False,
    "schema_drift": False,
}


def render(statements: list[str], **chaos) -> list[str]:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    params = {**PARAMS, **chaos}
    return [env.from_string(s).render(params=params, run_id="manual__t") for s in statements]


def test_ingest_without_chaos():
    alter, insert, _cleanup = render(dw.INGEST_ORDERS)
    assert "DROP COLUMN IF EXISTS promo_code" in alter
    assert "WHEN false AND random() < 0.3" in insert
    assert "(200 + floor(random() * 200))::int" in insert
    assert "interval '15 minutes'" in insert
    assert "md5('manual__t' || '-' || n)" in insert


@pytest.mark.parametrize(
    ("chaos", "expected"),
    [
        ({"null_customers": True}, "WHEN true AND random() < 0.3"),
        ({"volume_drop": True}, "* 0.05)::int)"),
        ({"skip_load": True}, "generate_series(\n            1, 0"),
    ],
)
def test_chaos_switches_change_the_load(chaos, expected):
    insert = render(dw.INGEST_ORDERS, **chaos)[1]
    assert expected in insert


def test_schema_drift_adds_the_column():
    alter = render(dw.INGEST_ORDERS, schema_drift=True)[0]
    assert alter.strip() == "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS promo_code text"


def test_datadog_grants_cover_every_schema():
    grants = dw.datadog_grants("contoso_dw")
    assert "GRANT pg_monitor TO datadog" in grants
    assert "GRANT CONNECT ON DATABASE contoso_dw TO datadog" in grants
    for schema in dw.SCHEMAS:
        assert f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO datadog" in grants
        assert (
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO datadog"
            in grants
        )


# Each task's input and output tables, as OpenLineage will report them.
EXPECTED_LINEAGE = {
    "INGEST_ORDERS": (set(), {"raw.orders"}),
    "INGEST_CUSTOMERS": (set(), {"raw.customers"}),
    "STAGING_ORDERS": ({"raw.orders"}, {"staging.orders"}),
    "STAGING_CUSTOMERS": ({"raw.customers"}, {"staging.customers"}),
    "MART_DAILY_STORE_SALES": ({"staging.orders"}, {"marts.daily_store_sales"}),
    "MART_CUSTOMER_VALUE": (
        {"staging.orders", "staging.customers"},
        {"marts.customer_value"},
    ),
    "MART_HOURLY_ORDER_VOLUME": ({"staging.orders"}, {"marts.hourly_order_volume"}),
}


@pytest.mark.parametrize("name", sorted(EXPECTED_LINEAGE))
def test_openlineage_reads_the_right_tables(name):
    ol_sql = pytest.importorskip("openlineage_sql")
    result = ol_sql.parse(render(getattr(dw, name)), dialect="postgres", default_schema="public")
    assert not result.errors
    inputs = {f"{t.schema}.{t.name}" for t in result.in_tables}
    outputs = {f"{t.schema}.{t.name}" for t in result.out_tables}
    assert (inputs, outputs) == EXPECTED_LINEAGE[name]


# --- bootstrap ------------------------------------------------------------------


class _Composed:
    def __init__(self, template, args):
        self.template, self.args = template, args


def _fake_pg_module():
    """A stand-in for psycopg2.sql / psycopg.sql that records composition."""
    pg = types.ModuleType("fakepg")
    pg_sql = types.ModuleType("fakepg.sql")

    class SQL:
        def __init__(self, template):
            self.template = template

        def format(self, *args):
            return _Composed(self.template, args)

    pg_sql.SQL = SQL
    pg_sql.Identifier = lambda v: ("ident", v)
    pg_sql.Literal = lambda v: ("literal", v)
    pg.sql = pg_sql
    return pg, pg_sql


class _Cursor:
    def __init__(self, role_exists):
        self.executed, self._role_exists, self._last = [], role_exists, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self.executed.append(statement)
        self._last = statement

    def fetchone(self):
        return (1,) if self._role_exists else None


@pytest.fixture
def bootstrap(monkeypatch):
    pg, pg_sql = _fake_pg_module()
    monkeypatch.setitem(sys.modules, "fakepg", pg)
    monkeypatch.setitem(sys.modules, "fakepg.sql", pg_sql)
    monkeypatch.delenv("AIRFLOW_CONN_CONTOSO_DW", raising=False)
    pytest.importorskip("airflow.sdk")
    return importlib.import_module("contoso_dw_bootstrap_dag")


def _conn(role_exists):
    cursor = _Cursor(role_exists)
    conn_cls = type("Connection", (), {"__module__": "fakepg.extensions"})
    conn = conn_cls()
    conn.cursor = lambda: cursor
    conn.committed = False

    def commit():
        conn.committed = True

    conn.commit = commit
    return conn, cursor


def test_bootstrap_creates_the_role_and_keeps_the_password_out_of_plain_sql(bootstrap):
    conn, cursor = _conn(role_exists=False)
    bootstrap.setup_warehouse(conn, "contoso_dw", "s3cret")

    plain = [s for s in cursor.executed if isinstance(s, str)]
    assert "CREATE ROLE datadog LOGIN" in plain
    assert not any("s3cret" in s for s in plain)
    (alter,) = [s for s in cursor.executed if isinstance(s, _Composed)]
    assert alter.args == (("ident", "datadog"), ("literal", "s3cret"))
    assert "GRANT pg_monitor TO datadog" in plain
    assert any("CREATE TABLE IF NOT EXISTS marts.customer_value" in s for s in plain)
    assert conn.committed


def test_bootstrap_reuses_an_existing_role(bootstrap):
    conn, cursor = _conn(role_exists=True)
    bootstrap.setup_warehouse(conn, "contoso_dw", "s3cret")
    assert "CREATE ROLE datadog LOGIN" not in cursor.executed


# --- DAG shape (needs Airflow and the common SQL provider) -----------------------


def test_contoso_orders_dag_shape(monkeypatch):
    pytest.importorskip("airflow.providers.common.sql.operators.sql")
    monkeypatch.setenv("AIRFLOW_CONN_CONTOSO_DW", "postgresql://u:p@db.example.com:5432/contoso_dw")
    sys.modules.pop("contoso_orders_dag", None)
    module = importlib.import_module("contoso_orders_dag")
    dag = next(v for v in vars(module).values() if isinstance(v, module.DAG))

    def upstream(task_id):
        return set(dag.get_task(task_id).upstream_task_ids)

    assert upstream("staging_orders") == {"ingest_orders"}
    assert upstream("staging_customers") == {"ingest_customers"}
    assert upstream("check_staging") == {"staging_orders", "staging_customers"}
    for mart in ("mart_daily_store_sales", "mart_customer_value", "mart_hourly_order_volume"):
        assert upstream(mart) == {"check_staging"}


def test_dags_are_skipped_without_the_pipeline(monkeypatch):
    pytest.importorskip("airflow.sdk")
    monkeypatch.delenv("AIRFLOW_CONN_CONTOSO_DW", raising=False)
    for name in ("contoso_orders_dag", "contoso_dw_bootstrap_dag"):
        sys.modules.pop(name, None)
        module = importlib.import_module(name)
        assert not [v for v in vars(module).values() if isinstance(v, module.DAG)]
