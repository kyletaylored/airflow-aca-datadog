"""Contoso schema flip: raw.orders gains a promo_code column for 2 hours in every 6.

The story:
    "The storefront team keeps shipping a promo_code field before the warehouse has
     agreed to it, then rolling it back."

At 00:00, 06:00, 12:00 and 18:00 UTC, flip_promo_code adds the column; 2 hours
later, it drops it. Datadog's schema change monitor and the raw.orders column
contract alert on each change and recover on the next, with no one triggering a
run. The column is nullable and nothing reads it, so the rest of the pipeline is
unaffected. The change is the same one contoso_orders' schema_drift switch makes,
with the same span and log line, and the switch still forces either state until
the next flip.

Set ``AIRFLOW_SCHEMA_FLIP_SCHEDULE`` to another cron, or to an empty string to only
run it by hand. Only defined when the data pipeline is deployed
(``AIRFLOW_CONN_CONTOSO_DW`` set).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import DAG
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.models.dag import DAG

from contoso_dw import schema_flip

CONN_ID = "contoso_dw"

if os.environ.get("AIRFLOW_CONN_CONTOSO_DW"):
    from contoso_dw.landing_operator import FlipPromoCodeOperator

    with DAG(
        dag_id="contoso_schema_flip",
        description="Add raw.orders' promo_code column for 2 hours in every 6.",
        start_date=datetime(2026, 1, 1),
        schedule=os.environ.get("AIRFLOW_SCHEMA_FLIP_SCHEDULE", schema_flip.SCHEDULE) or None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args={
            # Covers an ALTER TABLE that times out waiting for a contoso_orders load.
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
        },
        tags=["contoso", "data-observability", "postgres", "demo"],
    ) as dag:
        FlipPromoCodeOperator(task_id="flip_promo_code", conn_id=CONN_ID)
