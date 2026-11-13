from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, Param, task
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.decorators import task
    from airflow.models.dag import DAG
    from airflow.models.param import Param
    from airflow.operators.empty import EmptyOperator

try:
    from operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )
except ModuleNotFoundError:
    from plugins.operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )


# Production-style pipeline:
#   start -> make_shards -> [shard 0 .. shard N-1] -> finalize
#
# This is the orchestration pattern ACA Jobs can't express on its own:
# dependency-aware ordering, parallel fan-out, and per-task retries. Each shard
# runs as a real ACA Job execution. `deferrable=True` frees the Airflow worker
# slot while each job runs, so the fan-out width is bounded by ACA, not by the
# number of Airflow workers. The number of shards is set per run via config.
with DAG(
    dag_id="aca_jobs_pipeline",
    description="Dependency + parallel fan-out + retries over ACA Jobs (deferrable).",
    start_date=datetime(2026, 1, 1),
    # Manual by default; the azd deploy sets AIRFLOW_DEMO_SCHEDULE (e.g. */5 * * * *).
    schedule=os.environ.get("AIRFLOW_DEMO_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["aca", "jobs", "pipeline"],
    params={
        "shards": Param(
            default=5,
            type="integer",
            minimum=1,
            maximum=200,
            title="Number of parallel shards",
            description="How many ACA Job executions to fan out in parallel.",
        ),
        "command": Param(
            default=None,
            type=["null", "array"],
            items={"type": "string"},
            title="Command override (optional)",
            description='Entrypoint to run for every shard, e.g. ["python", "main.py"].',
        ),
        "args": Param(
            default=None,
            type=["null", "array"],
            items={"type": "string"},
            title="Args override (optional)",
            description='Arguments passed to every shard, e.g. ["--mode", "batch"].',
        ),
        "env": Param(
            default=None,
            type=["null", "object"],
            title="Extra environment variables (optional)",
            description='Env vars applied to every shard (merged with each shard\'s '
            'SHARD_INDEX / SHARD_TOTAL), e.g. {"DATASET": "sales-2026"}.',
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    finalize = EmptyOperator(task_id="finalize")

    @task
    def make_shards(params: dict | None = None) -> list[dict[str, str]]:
        params = params or {}
        n = int(params.get("shards", 5))
        extra_env = params.get("env") or {}
        shard_env = []
        for i in range(n):
            env = {str(k): str(v) for k, v in extra_env.items()}
            env["SHARD_INDEX"] = str(i)
            env["SHARD_TOTAL"] = str(n)
            shard_env.append(env)
        return shard_env

    shards = make_shards()

    run_shards = AzureContainerAppsJobOperator.partial(
        task_id="run_shard",
        subscription_id="{{ var.value.azure_subscription_id }}",
        resource_group="{{ var.value.aca_resource_group }}",
        job_name="{{ var.value.aca_job_name }}",
        command="{{ params.command }}",
        args="{{ params.args }}",
        azure_conn_id="{{ var.value.get('azure_conn_id', '') }}",
        deferrable=True,
        poll_interval_seconds=15,
        execution_timeout_seconds=3600,
    ).expand(env_vars=shards)

    start >> shards >> run_shards >> finalize
