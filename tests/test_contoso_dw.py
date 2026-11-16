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
    "breaking_schema": False,
    "refund_bug": False,
}


def render(statements: list[str], **chaos) -> list[str]:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    params = {**PARAMS, **chaos}
    return [env.from_string(s).render(params=params, run_id="manual__t") for s in statements]


def test_ingest_without_chaos():
    alter, restore, insert, cleanup = render(dw.INGEST_ORDERS)
    assert "DROP COLUMN IF EXISTS promo_code" in alter
    assert "RENAME COLUMN amount_minor TO amount_cents" in restore
    assert cleanup == dw.PURGE_OLD_ORDERS
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
    insert = render(dw.INGEST_ORDERS, **chaos)[2]
    assert expected in insert


def test_schema_drift_adds_the_column():
    alter = render(dw.INGEST_ORDERS, schema_drift=True)[0]
    assert alter.strip() == "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS promo_code text"


def test_breaking_schema_renames_the_column_after_the_load():
    cleanup = render(dw.INGEST_ORDERS, breaking_schema=True)[-1]
    assert cleanup == dw.PURGE_OLD_ORDERS + ";\n" + dw.BREAK_AMOUNT_COLUMN


def test_staging_reads_the_column_breaking_schema_renames():
    # The rename only breaks staging_orders if staging still reads amount_cents.
    assert "amount_cents" in render(dw.STAGING_ORDERS)[1]
    assert "amount_cents" in dw.BREAK_AMOUNT_COLUMN


def test_refund_bug_only_changes_the_store_sales_mart():
    normal = render(dw.MART_DAILY_STORE_SALES)[1]
    buggy = render(dw.MART_DAILY_STORE_SALES, refund_bug=True)[1]
    assert "sum(amount_usd) AS revenue_usd," in normal and "CASE" not in normal
    assert (
        "sum(CASE WHEN store = 'Denver' THEN -amount_usd ELSE amount_usd END) AS revenue_usd,"
        in buggy
    )
    assert "Denver" in dw.STORES
    # staging must stay clean, or check_staging would stop the run before the marts.
    assert render(dw.STAGING_ORDERS, refund_bug=True) == render(dw.STAGING_ORDERS)


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


@pytest.mark.parametrize(
    ("name", "chaos"),
    [(name, {}) for name in sorted(EXPECTED_LINEAGE)]
    + [("INGEST_ORDERS", {"breaking_schema": True}), ("MART_DAILY_STORE_SALES", {"refund_bug": True})],
)
def test_openlineage_reads_the_right_tables(name, chaos):
    ol_sql = pytest.importorskip("openlineage_sql")
    result = ol_sql.parse(render(getattr(dw, name), **chaos), dialect="postgres", default_schema="public")
    # openlineage-sql can't parse PL/pgSQL DO blocks (INGEST_ORDERS restores the
    # amount column with one). The provider still reports the other statements'
    # tables, so only those errors are allowed.
    assert not [e for e in result.errors if not e.origin_statement.lstrip().startswith("DO $$")]
    inputs = {f"{t.schema}.{t.name}" for t in result.in_tables}
    outputs = {f"{t.schema}.{t.name}" for t in result.out_tables}
    assert (inputs, outputs) == EXPECTED_LINEAGE[name]


# Each table's columns as written, and a few column edges the demo relies on:
# scenario 2 follows raw.orders.amount_cents down to the revenue columns.
EXPECTED_COLUMNS = {
    "STAGING_ORDERS": (
        {"order_id", "customer_id", "store", "channel", "items", "amount_usd", "ordered_at", "order_date"},
        {"amount_usd": "orders.amount_cents", "order_date": "orders.ordered_at"},
    ),
    "STAGING_CUSTOMERS": (
        {"customer_id", "email", "region", "signed_up_at"},
        {"email": "customers.email"},
    ),
    "MART_DAILY_STORE_SALES": (
        {"sales_date", "store", "revenue_usd", "last_order_at"},
        {"revenue_usd": "orders.amount_usd", "sales_date": "orders.order_date"},
    ),
    "MART_CUSTOMER_VALUE": (
        {"customer_id", "region", "revenue_usd", "first_order_at", "last_order_at"},
        {"revenue_usd": "orders.amount_usd"},
    ),
    "MART_HOURLY_ORDER_VOLUME": (
        {"order_hour", "store"},
        {"order_hour": "orders.ordered_at"},
    ),
}


@pytest.mark.parametrize(
    ("name", "chaos"),
    [(name, {}) for name in sorted(EXPECTED_COLUMNS)] + [("MART_DAILY_STORE_SALES", {"refund_bug": True})],
)
def test_column_lineage_keeps_the_target_names(name, chaos):
    # Columns with no input columns (count(*), now()) have no lineage, so only the
    # ones read from a table are listed.
    ol_sql = pytest.importorskip("openlineage_sql")
    result = ol_sql.parse(render(getattr(dw, name), **chaos), dialect="postgres", default_schema="public")
    lineage = {
        c.descendant.name: {f"{x.origin.name}.{x.name}" for x in c.lineage if x.origin}
        for c in result.column_lineage
    }
    columns, edges = EXPECTED_COLUMNS[name]
    assert set(lineage) == columns
    for column, source in edges.items():
        assert source in lineage[column]


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


def test_bootstrap_leaves_metabase_out_unless_asked(bootstrap):
    conn, cursor = _conn(role_exists=False)
    bootstrap.setup_warehouse(conn, "contoso_dw", "s3cret")
    assert not any("metabase" in s for s in cursor.executed if isinstance(s, str))


def test_bootstrap_creates_the_metabase_role_with_marts_access_only(bootstrap):
    conn, cursor = _conn(role_exists=False)
    bootstrap.setup_warehouse(conn, "contoso_dw", "s3cret", "mb-s3cret")

    plain = [s for s in cursor.executed if isinstance(s, str)]
    assert "CREATE ROLE metabase LOGIN" in plain
    assert not any("mb-s3cret" in s for s in plain)
    passwords = [s.args for s in cursor.executed if isinstance(s, _Composed)]
    assert (("ident", "metabase"), ("literal", "mb-s3cret")) in passwords
    # Taking the role comes first: the admin can't hand it the database otherwise.
    assert plain.index("GRANT metabase TO CURRENT_USER") < plain.index(
        "ALTER DATABASE metabase OWNER TO metabase"
    )
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase" in plain
    assert not any(f"SCHEMA {s} TO metabase" in g for s in ("raw", "staging") for g in plain)


def test_metabase_database_gets_citext_and_its_public_schema(bootstrap):
    conn, cursor = _conn(role_exists=True)
    bootstrap.setup_metabase_database(conn)
    assert cursor.executed == [
        "CREATE EXTENSION IF NOT EXISTS citext",
        "ALTER SCHEMA public OWNER TO metabase",
    ]
    assert conn.committed


def test_dbm_setup_follows_datadogs_azure_steps(bootstrap):
    conn, cursor = _conn(role_exists=True)
    bootstrap.setup_dbm_database(conn)
    assert cursor.executed[:4] == [
        "CREATE SCHEMA IF NOT EXISTS datadog",
        "GRANT USAGE ON SCHEMA datadog TO datadog",
        "GRANT USAGE ON SCHEMA public TO datadog",
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
    ]
    (function,) = cursor.executed[4:]
    assert "CREATE OR REPLACE FUNCTION datadog.explain_statement(" in function
    assert "SET TRANSACTION READ ONLY" in function
    assert "SECURITY DEFINER" in function
    assert conn.committed


def test_dbm_covers_every_database_but_azures_own():
    assert "datallowconn AND NOT datistemplate" in dw.LIST_DATABASES
    assert "NOT IN ('azure_maintenance', 'azure_sys')" in dw.LIST_DATABASES


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
