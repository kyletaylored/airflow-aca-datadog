"""Async trigger for deferrable ACA Job execution polling.

When the operator runs with `deferrable=True`, it starts the ACA Job execution
and then hands off to this trigger, which polls for completion inside Airflow's
async *triggerer* process. This frees the worker slot for the entire job
duration, so a single Airflow deployment can have thousands of ACA Job
executions in flight without pinning a worker per job.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

try:
    from operators import aca_jobs_common as aca
except ModuleNotFoundError:  # pragma: no cover - import fallback
    from plugins.operators import aca_jobs_common as aca


class AzureContainerAppsJobTrigger(BaseTrigger):
    """Polls an ACA Job execution to a terminal state, asynchronously."""

    def __init__(
        self,
        *,
        job_ref: dict[str, str],
        execution_name: str,
        api_version: str = aca.DEFAULT_API_VERSION,
        poll_interval_seconds: int = 15,
        timeout_seconds: int = 60 * 60,
        auth: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.job_ref = job_ref
        self.execution_name = execution_name
        self.api_version = api_version
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds
        self.auth = auth or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "triggers.azure_container_apps_job_trigger.AzureContainerAppsJobTrigger",
            {
                "job_ref": self.job_ref,
                "execution_name": self.execution_name,
                "api_version": self.api_version,
                "poll_interval_seconds": self.poll_interval_seconds,
                "timeout_seconds": self.timeout_seconds,
                "auth": self.auth,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        job_ref = aca.ACAJobRef.from_dict(self.job_ref)
        token = await asyncio.to_thread(aca.get_token, self.auth, None)
        deadline = time.monotonic() + self.timeout_seconds

        while True:
            if time.monotonic() > deadline:
                yield TriggerEvent(
                    {
                        "status": "timeout",
                        "execution_name": self.execution_name,
                        "message": (
                            f"Execution '{self.execution_name}' did not complete "
                            f"before timeout ({self.timeout_seconds}s)."
                        ),
                    }
                )
                return

            try:
                state = await asyncio.to_thread(
                    aca.get_execution_state,
                    job_ref,
                    self.execution_name,
                    token,
                    self.api_version,
                    None,
                )
            except aca.ACATokenExpired:
                # Long jobs can outlive the ARM token; refresh and keep polling.
                token = await asyncio.to_thread(aca.get_token, self.auth, None)
                continue

            if state in aca.TERMINAL_SUCCESS:
                yield TriggerEvent(
                    {"status": "success", "execution_name": self.execution_name}
                )
                return
            if state in aca.TERMINAL_FAILURE:
                yield TriggerEvent(
                    {
                        "status": "failed",
                        "execution_name": self.execution_name,
                        "message": (
                            f"Execution '{self.execution_name}' ended in terminal "
                            f"failure state: {state}"
                        ),
                    }
                )
                return

            await asyncio.sleep(self.poll_interval_seconds)
