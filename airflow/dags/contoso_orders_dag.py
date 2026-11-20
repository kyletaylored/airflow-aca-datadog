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
    schema_drift    raw.orders gains a promo_code column (schema change). It
                    stays until a run with schema_drift set to false; runs
                    that leave it unset, like the scheduled ones, don't touch it
    breaking_schema raw.orders.amount_cents is renamed after the load, so
                    staging_orders fails (Job monitor) and the marts go stale
    refund_bug      the store sales mart counts every Denver order as a refund
                    (negative-revenue and revenue anomaly monitors)
    memory_pressure the memory_pressure task holds memory in the scheduler's
                    container, up to 85% of its limit, for 10 minutes (the
                    scheduler memory monitor)

Where the orders come from:
    stream mode    when the order stream is deployed (``CONTOSO_LANDING_ACCOUNT``
                   set), ingest_orders loads the batches the order processor landed
                   in ADLS, so lineage starts at the storefront and Event Hubs.
    generate mode  otherwise, ingest_orders makes up the interval's orders in SQL.
The chaos switches work the same in both modes.

Only defined when the data pipeline is deployed (``AIRFLOW_CONN_CONTOSO_DW`` set).
Run ``contoso_dw_bootstrap`` first; the deploy does that automatically (``@once``).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import DAG, Param, task
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.decorators import task
    from airflow.models.dag import DAG
    from airflow.models.param import Param

from contoso_dw import sql as dw

CONN_ID = "contoso_dw"
LANDING_ACCOUNT = os.environ.get("CONTOSO_LANDING_ACCOUNT")
LANDING_CONTAINER = os.environ.get("CONTOSO_LANDING_CONTAINER", "landing")


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
            # Unset by default, so scheduled runs leave the column alone and a schema
            # change lasts long enough for Datadog's schema metadata collection to see it.
            "schema_drift": Param(
                default=None,
                type=["null", "boolean"],
                title="Schema drift",
                description=(
                    "On: add a promo_code column to raw.orders. Off: drop it. "
                    "Unset (scheduled runs): leave raw.orders as it is."
                ),
            ),
            "breaking_schema": _chaos(
                "Breaking schema change",
                "Rename raw.orders.amount_cents after the load, so staging_orders fails.",
            ),
            "refund_bug": _chaos(
                "Refund bug", "The store sales mart counts every Denver order as a refund."
            ),
            "memory_pressure": _chaos(
                "Memory pressure",
                "Hold memory in the scheduler's container, up to 85% of its limit, for 10 minutes.",
            ),
        },
    ) as dag:

        def sql_task(task_id: str, statements: list[str]) -> SQLExecuteQueryOperator:
            return SQLExecuteQueryOperator(task_id=task_id, conn_id=CONN_ID, sql=statements)

        if LANDING_ACCOUNT:
            from contoso_dw.landing_operator import LoadLandedOrdersOperator

            ingest_orders = LoadLandedOrdersOperator(
                task_id="ingest_orders",
                conn_id=CONN_ID,
                landing_account=LANDING_ACCOUNT,
                landing_container=LANDING_CONTAINER,
            )
        else:
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

        # With LocalExecutor this runs in the scheduler's container, next to every other
        # task. It finishes at once unless the memory_pressure switch is on.
        @task(task_id="memory_pressure", retries=0)
        def memory_pressure(**context):
            if context["params"].get("memory_pressure"):
                from contoso_dw.memory_pressure import hold_memory

                hold_memory()

        memory_pressure()

        ingest_orders >> staging_orders
        ingest_customers >> staging_customers
        [staging_orders, staging_customers] >> check_staging >> marts
