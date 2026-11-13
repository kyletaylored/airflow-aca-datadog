"""Contoso daily sales rollup, orchestrated over Azure Container Apps Jobs.

The story (say this out loud to a customer):
    "Contoso has a handful of stores. Every night we want ONE number:
     total sales across all stores. Each store reports its own sales as a
     separate ACA Job execution, all running in parallel. When they're all
     in, a final job adds them into the company total."

Why this needs an orchestrator (and plain ACA Jobs can't do it alone):
    * dependency ordering    -> finalize only runs after every store is in
    * parallel fan-out       -> one ACA Job execution per store, at once
    * per-task retries       -> if a store's register crashes, retry just
                                that store; the nightly total still lands

Zero build, nothing to host:
    Each task runs the stock public image ``python:3.12-slim`` on YOUR ACA
    Job and hands it a tiny inline program via a command override. No custom
    image, no registry, no secrets baked in. The same operator/DAG works in
    both templates (bring-your-own Airflow, or Airflow hosted on ACA).

Drop this file into ``airflow/dags/`` of either template repo and trigger
``contoso_sales`` (the demo scripts do this for you).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

try:
    from operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )
except ModuleNotFoundError:  # repo root on PYTHONPATH (tests / local)
    from plugins.operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )

# Stock image every task runs on. Overridden per execution by the operator,
# so the ACA Job's own base image is irrelevant.
DEMO_IMAGE = "python:3.12-slim"

# The entire "store" workload, inline. Pure stdlib so python:slim runs it as-is.
# No Jinja braces ({{ }}) here, so Airflow templating passes it through untouched.
CONTOSO_PY = (
    "import os, sys, time, hashlib\n"
    "def sales(store, date):\n"
    "    h = hashlib.sha256((store + '|' + date).encode()).hexdigest()\n"
    "    return 1000 + int(h[:6], 16) % 9000\n"
    "mode = os.environ.get('MODE', 'store')\n"
    "date = os.environ.get('BUSINESS_DATE', 'today')\n"
    "try_num = sys.argv[1] if len(sys.argv) > 1 else '1'\n"
    "if mode == 'finalize':\n"
    "    stores = [s for s in os.environ.get('STORE_LIST', '').split('|') if s]\n"
    "    total = 0\n"
    "    print('=' * 54)\n"
    "    print('CONTOSO  daily sales rollup   ' + date)\n"
    "    print('-' * 54)\n"
    "    for s in stores:\n"
    "        v = sales(s, date); total += v\n"
    "        print('  {:<24} ${:>9,}'.format(s, v))\n"
    "    print('-' * 54)\n"
    "    print('  TOTAL across {} stores:   ${:,}'.format(len(stores), total))\n"
    "    print('=' * 54)\n"
    "    sys.exit(0)\n"
    "store = os.environ.get('STORE_NAME', 'Unknown Store')\n"
    "amount = sales(store, date)\n"
    "print(store + ' is tallying its registers for ' + date + ' ...')\n"
    "time.sleep(int(os.environ.get('WORK_SECONDS', '4')))\n"
    "if os.environ.get('CHAOS') == '1' and store == os.environ.get('CHAOS_STORE', ''):\n"
    "    if try_num == '1':\n"
    "        sys.stderr.write('!! ' + store + ' register crashed before reporting (attempt 1).\\n')\n"
    "        sys.exit(1)\n"
    "    print(store + ' register recovered on retry (attempt ' + try_num + ').')\n"
    "print('{} sold ${:,} today.'.format(store, amount))\n"
    "sys.exit(0)\n"
)

RUN_PROGRAM = ["python", "-c", CONTOSO_PY]

DEFAULT_STORES = [
    "Seattle",
    "Portland",
    "San Francisco",
    "Denver",
    "Austin",
    "Chicago",
    "New York",
]

with DAG(
    dag_id="contoso_sales",
    description="Contoso nightly sales total: fan out one ACA Job per store, then sum.",
    start_date=datetime(2026, 1, 1),
    # Manual by default; the azd deploy sets AIRFLOW_DEMO_SCHEDULE (e.g. */5 * * * *).
    schedule=os.environ.get("AIRFLOW_DEMO_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
    },
    tags=["aca", "jobs", "contoso", "demo"],
    params={
        "stores": Param(
            default=DEFAULT_STORES,
            type="array",
            items={"type": "string"},
            title="Stores reporting tonight",
            description="Each store runs as its own parallel ACA Job execution.",
        ),
        "business_date": Param(
            default="",
            type=["null", "string"],
            title="Business date (optional)",
            description="Label for the run. Blank uses the Airflow logical date.",
        ),
        "work_seconds": Param(
            default=4,
            type="integer",
            minimum=0,
            maximum=120,
            title="Seconds each store 'works'",
            description="Just makes the parallel fan-out visible on stage.",
        ),
        "chaos": Param(
            default=False,
            type="boolean",
            title="Chaos: make one store flaky",
            description="Force one store's register to crash so Airflow retries it.",
        ),
        "chaos_store": Param(
            default="Denver",
            type="string",
            title="Which store misbehaves",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="open_for_business")

    @task
    def build_store_runs(params: dict | None = None, **context) -> list[dict[str, str]]:
        """One env-var bundle per store: the fan-out shards."""
        p = params or {}
        stores = p.get("stores") or DEFAULT_STORES
        date = p.get("business_date") or context["ds"]
        chaos = "1" if p.get("chaos") else "0"
        return [
            {
                "MODE": "store",
                "STORE_NAME": s,
                "BUSINESS_DATE": date,
                "WORK_SECONDS": str(p.get("work_seconds", 4)),
                "CHAOS": chaos,
                "CHAOS_STORE": str(p.get("chaos_store", "")),
            }
            for s in stores
        ]

    @task
    def build_finalize_env(params: dict | None = None, **context) -> dict[str, str]:
        """Env for the roll-up job: the list of stores + the date."""
        p = params or {}
        stores = p.get("stores") or DEFAULT_STORES
        date = p.get("business_date") or context["ds"]
        return {
            "MODE": "finalize",
            "STORE_LIST": "|".join(stores),
            "BUSINESS_DATE": date,
        }

    store_runs = build_store_runs()

    # Fan out: one real ACA Job execution per store, in parallel.
    # deferrable=True frees the Airflow worker slot while each job runs,
    # so fan-out width is bounded by ACA, not by Airflow workers.
    report_store = AzureContainerAppsJobOperator.partial(
        task_id="report_store_sales",
        subscription_id="{{ var.value.azure_subscription_id }}",
        resource_group="{{ var.value.aca_resource_group }}",
        job_name="{{ var.value.aca_job_name }}",
        image=DEMO_IMAGE,
        command=RUN_PROGRAM,
        args=["{{ ti.try_number }}"],
        deferrable=True,
        poll_interval_seconds=10,
        execution_timeout_seconds=1800,
    ).expand(env_vars=store_runs)

    # Roll up: runs only after every store has reported.
    finalize = AzureContainerAppsJobOperator(
        task_id="roll_up_company_total",
        subscription_id="{{ var.value.azure_subscription_id }}",
        resource_group="{{ var.value.aca_resource_group }}",
        job_name="{{ var.value.aca_job_name }}",
        image=DEMO_IMAGE,
        command=RUN_PROGRAM,
        args=[],
        env_vars=build_finalize_env(),
        deferrable=True,
        poll_interval_seconds=10,
        execution_timeout_seconds=1800,
    )

    start >> store_runs >> report_store >> finalize
