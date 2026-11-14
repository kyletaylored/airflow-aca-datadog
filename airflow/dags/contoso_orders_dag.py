"""Contoso orders: a small medallion pipeline in Postgres (raw -> staging -> marts).

The story:
    "Orders from every Contoso store land in raw tables. Every 15 minutes we
     clean them into staging, check them, and rebuild the marts that finance
     and the store managers read."

Every task is SQL run with SQLExecuteQueryOperator (or SQLCheckOperator), so
OpenLineage reads each task's input and output tables from the statements. In
Datadog, Data Jobs Monitoring shows the DAG and its tasks, and Data
Observability shows the same tables in its catalog, joined in one lineage graph:

    raw.orders ──► staging.orders ──┬─► marts.daily_store_sales
    raw.customers ► staging.customers ┴─► marts.customer_value
                                       └─► marts.hourly_order_volume

The Datadog Agent's Postgres check runs the data quality monitors on those
tables. Trigger with chaos params to break something on purpose:
    null_customers  30% of orders arrive without a customer (null-ratio monitor)
    volume_drop     5% of the usual orders (row-count monitor)
    skip_load       no new orders (freshness monitor)
    schema_drift    raw.orders gains a promo_code column (schema change)

Only defined when the data pipeline is deployed (``AIRFLOW_CONN_CONTOSO_DW`` set).
Run ``contoso_dw_bootstrap`` first; the deploy does that automatically (``@once``).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import DAG, Param
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.models.dag import DAG
    from airflow.models.param import Param

from contoso_dw import sql as dw

CONN_ID = "contoso_dw"


def _chaos(title: str, description: str) -> Param:
    return Param(default=False, type="boolean", title=title, description=description)


if os.environ.get("AIRFLOW_CONN_CONTOSO_DW"):
    from airflow.providers.common.sql.operators.sql import (
        SQLCheckOperator,
        SQLExecuteQueryOperator,
    )

    with DAG(
        dag_id="contoso_orders",
        description="Contoso orders medallion in Postgres: raw -> staging -> marts.",
        start_date=datetime(2026, 1, 1),
        schedule=os.environ.get("AIRFLOW_ORDERS_SCHEDULE", "*/15 * * * *") or None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args={
            # Also covers the first run racing contoso_dw_bootstrap after a deploy.
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
        },
        tags=["contoso", "data-observability", "postgres", "demo"],
        params={
            "interval_minutes": Param(
                default=15,
                type="integer",
                minimum=1,
                title="Minutes of orders per run",
                description="Order timestamps spread over this many minutes before now.",
            ),
            "null_customers": _chaos("Null customers", "30% of orders have no customer_id."),
            "volume_drop": _chaos("Volume drop", "Load only 5% of the usual orders."),
            "skip_load": _chaos("Skip the load", "Load no orders at all."),
            "schema_drift": _chaos("Schema drift", "Add a promo_code column to raw.orders."),
        },
    ) as dag:

        def sql_task(task_id: str, statements: list[str]) -> SQLExecuteQueryOperator:
            return SQLExecuteQueryOperator(task_id=task_id, conn_id=CONN_ID, sql=statements)

        ingest_orders = sql_task("ingest_orders", dw.INGEST_ORDERS)
        ingest_customers = sql_task("ingest_customers", dw.INGEST_CUSTOMERS)
        staging_orders = sql_task("staging_orders", dw.STAGING_ORDERS)
        staging_customers = sql_task("staging_customers", dw.STAGING_CUSTOMERS)
        check_staging = SQLCheckOperator(
            task_id="check_staging", conn_id=CONN_ID, sql=dw.CHECK_STAGING
        )
        marts = [
            sql_task("mart_daily_store_sales", dw.MART_DAILY_STORE_SALES),
            sql_task("mart_customer_value", dw.MART_CUSTOMER_VALUE),
            sql_task("mart_hourly_order_volume", dw.MART_HOURLY_ORDER_VOLUME),
        ]

        ingest_orders >> staging_orders
        ingest_customers >> staging_customers
        [staging_orders, staging_customers] >> check_staging >> marts
