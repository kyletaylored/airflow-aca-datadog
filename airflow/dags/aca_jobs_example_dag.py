from __future__ import annotations

import os
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import DAG, Param
except ImportError:  # pragma: no cover - Airflow 2.x (local docker-compose stack)
    from airflow.models.param import Param

    from airflow import DAG

try:
    # In a real Airflow deployment the `plugins/` directory is on sys.path,
    # so the operator is imported as `operators....`.
    from operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )
except ModuleNotFoundError:
    # Fallback when running with the repository root on PYTHONPATH.
    from plugins.operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )


# This DAG is meant to be reusable: a customer triggers it with a config form
# (Airflow UI "Trigger DAG w/ config", or `airflow dags trigger --conf '{...}'`)
# and specifies which ACA Job to run and what arguments to run it with — no code
# edits required. Blank fields fall back to Airflow Variables / the job's own
# defaults.
with DAG(
    dag_id="aca_jobs_example",
    description="Run any ACA Job from Airflow — fill in the form to specify your workload.",
    start_date=datetime(2026, 1, 1),
    # Manual by default; the azd deploy sets AIRFLOW_DEMO_SCHEDULE (e.g. */5 * * * *).
    schedule=os.environ.get("AIRFLOW_DEMO_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["aca", "jobs"],
    params={
        "job_name": Param(
            default="",
            type=["null", "string"],
            title="ACA Job name",
            description="Name of the ACA Job to run. Leave blank to use the "
            "'aca_job_name' Airflow Variable.",
        ),
        "command": Param(
            default=None,
            type=["null", "array"],
            items={"type": "string"},
            title="Command override (optional)",
            description='Entrypoint to run instead of the image default, e.g. '
            '["python", "main.py"].',
        ),
        "args": Param(
            default=None,
            type=["null", "array"],
            items={"type": "string"},
            title="Args override (optional)",
            description='Arguments passed to the command, e.g. ["--batch-size", "100"].',
        ),
        "env": Param(
            default=None,
            type=["null", "object"],
            title="Environment variables (optional)",
            description='Extra environment variables as a JSON object, e.g. '
            '{"BATCH_SIZE": "100", "MODE": "nightly"}.',
        ),
    },
) as dag:
    run_aca_job = AzureContainerAppsJobOperator(
        task_id="run_aca_job",
        subscription_id="{{ var.value.azure_subscription_id }}",
        resource_group="{{ var.value.aca_resource_group }}",
        job_name="{{ params.job_name or var.value.aca_job_name }}",
        command="{{ params.command }}",
        args="{{ params.args }}",
        env_vars="{{ params.env }}",
        azure_conn_id="{{ var.value.get('azure_conn_id', '') }}",
        poll_interval_seconds=15,
        execution_timeout_seconds=3600,
    )
