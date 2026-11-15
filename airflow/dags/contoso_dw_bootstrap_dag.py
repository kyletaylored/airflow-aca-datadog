"""One-time setup of the contoso_dw warehouse database.

Creates the medallion schemas and tables (``raw``, ``staging``, ``marts``), seeds
the customer pool, and creates the read-only ``datadog`` role that the Datadog
Agent uses for Data Observability. When Metabase is deployed, it also creates the
``metabase`` role, which owns Metabase's own database and reads the marts. It runs
once, as soon as it's deployed (``@once``), and is safe to trigger again, for
example after rotating ``DD_POSTGRES_PASSWORD``, or after turning Metabase on in an
existing deployment.

The statements run on a raw database cursor rather than through a SQL operator,
so the role's password never appears in task logs or in OpenLineage's SQL facet.

Only defined when the data pipeline is deployed (``AIRFLOW_CONN_CONTOSO_DW`` set).
"""
from __future__ import annotations

import importlib
import os
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.decorators import task
    from airflow.models.dag import DAG

from contoso_dw import sql as dw

CONN_ID = "contoso_dw"
PASSWORD_ENV = "AIRFLOW_VAR_DATADOG_POSTGRES_PASSWORD"
# Set only when Metabase is deployed (ENABLE_METABASE).
METABASE_PASSWORD_ENV = "AIRFLOW_VAR_METABASE_POSTGRES_PASSWORD"


def _set_role(cur, pg_sql, role: str, password: str) -> None:
    """Create ``role`` if it's missing, and set its password either way."""
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    if cur.fetchone() is None:
        cur.execute(f"CREATE ROLE {role} LOGIN")
    cur.execute(
        pg_sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
            pg_sql.Identifier(role), pg_sql.Literal(password)
        )
    )


def setup_warehouse(
    conn, database: str, datadog_password: str, metabase_password: str | None = None
) -> None:
    """Create the schemas, tables, seed data and the datadog role on ``conn``.

    With ``metabase_password``, also the metabase role and its grants. The metabase
    database itself needs ``setup_metabase_database`` as well.
    """
    # psycopg2 or psycopg 3, whichever the hook returned; both have sql.Literal.
    pg_sql = importlib.import_module(type(conn).__module__.split(".")[0] + ".sql")
    with conn.cursor() as cur:
        for statement in [*dw.CREATE_TABLES, dw.upsert_customers("1")]:
            cur.execute(statement)

        _set_role(cur, pg_sql, dw.DATADOG_ROLE, datadog_password)
        for statement in dw.datadog_grants(database):
            cur.execute(statement)

        if metabase_password:
            _set_role(cur, pg_sql, dw.METABASE_ROLE, metabase_password)
            for statement in dw.metabase_grants(database):
                cur.execute(statement)
    conn.commit()


def setup_metabase_database(conn) -> None:
    """Get the metabase database ready for Metabase's own migrations (``conn`` is in it)."""
    with conn.cursor() as cur:
        for statement in dw.METABASE_DATABASE_SETUP:
            cur.execute(statement)
    conn.commit()


if os.environ.get("AIRFLOW_CONN_CONTOSO_DW"):
    with DAG(
        dag_id="contoso_dw_bootstrap",
        description="Create the contoso_dw schemas, tables and the datadog role.",
        start_date=datetime(2026, 1, 1),
        schedule="@once",
        catchup=False,
        is_paused_upon_creation=False,
        tags=["contoso", "data-observability", "setup"],
    ) as dag:

        @task
        def bootstrap_warehouse() -> None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            password = os.environ.get(PASSWORD_ENV)
            if not password:
                raise ValueError(f"{PASSWORD_ENV} is not set; deploy with DD_POSTGRES_PASSWORD.")
            metabase_password = os.environ.get(METABASE_PASSWORD_ENV)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                setup_warehouse(
                    conn, hook.get_connection(CONN_ID).schema, password, metabase_password
                )
            finally:
                conn.close()
            if metabase_password:
                # Same server and admin, in Metabase's own database.
                conn = PostgresHook(postgres_conn_id=CONN_ID, database=dw.METABASE_DATABASE).get_conn()
                try:
                    setup_metabase_database(conn)
                finally:
                    conn.close()

        bootstrap_warehouse()
