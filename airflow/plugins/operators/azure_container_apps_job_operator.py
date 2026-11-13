from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow.exceptions import AirflowException

try:  # Airflow 3 Task SDK
    from airflow.sdk import BaseOperator
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.models import BaseOperator

try:
    from operators import aca_jobs_common as aca
except ModuleNotFoundError:  # pragma: no cover - import fallback
    from plugins.operators import aca_jobs_common as aca

try:
    from triggers.azure_container_apps_job_trigger import (
        AzureContainerAppsJobTrigger,
    )
except ModuleNotFoundError:  # pragma: no cover - import fallback
    from plugins.triggers.azure_container_apps_job_trigger import (
        AzureContainerAppsJobTrigger,
    )


# Re-export for backwards compatibility with earlier imports.
ACAJobRef = aca.ACAJobRef
DEFAULT_API_VERSION = aca.DEFAULT_API_VERSION
OPERATOR_USER_AGENT = aca.OPERATOR_USER_AGENT


def _default_deferrable() -> bool:
    try:
        from airflow.configuration import conf

        return conf.getboolean("operators", "default_deferrable", fallback=False)
    except Exception:  # pragma: no cover - configuration not available
        return False


class AzureContainerAppsJobOperator(BaseOperator):
    """Runs an Azure Container Apps Job execution using the ARM Jobs API.

    Set ``deferrable=True`` to free the Airflow worker slot while the job runs:
    the operator starts the execution, then defers polling to Airflow's async
    triggerer. This is what lets one Airflow deployment orchestrate thousands of
    concurrent ACA Job executions without pinning a worker per job.

    **Authentication.** The easiest, most portable option is ``azure_conn_id``,
    pointing at an Airflow Connection that holds a service principal
    (client id/secret/tenant) or a user-assigned managed identity client id. This
    works in any Airflow — Azure, AWS, or on-prem. If you omit it, the operator
    falls back to the ``AZURE_ACCESS_TOKEN`` env var (demo) and then to
    ``DefaultAzureCredential`` (managed identity / az login). ``subscription_id``
    and ``resource_group`` may also be stored in the connection's *extra* so DAGs
    need even less inline config.

    **Lineage.** When the OpenLineage provider is installed, each execution gets
    ``OPENLINEAGE_PARENT_ID`` and ``OPENLINEAGE_ROOT_PARENT_ID`` (the task's and
    DAG run's ``{namespace}/{job_name}/{run_id}``). A job that emits OpenLineage
    events with them as its parent shows up under this task in Datadog. Turn it
    off with ``pass_openlineage_parent=False``; it is skipped with
    ``job_start_body``, which is sent as-is.
    """

    template_fields = (
        "subscription_id",
        "resource_group",
        "job_name",
        "job_resource_id",
        "job_start_body",
        "image",
        "command",
        "args",
        "env_vars",
        "cpu",
        "memory",
        "azure_conn_id",
        "managed_identity_client_id",
    )

    def __init__(
        self,
        *,
        subscription_id: str | None = None,
        resource_group: str | None = None,
        job_name: str | None = None,
        job_resource_id: str | None = None,
        image: str | None = None,
        command: list[str] | None = None,
        args: list[str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: str | None = None,
        job_start_body: dict[str, Any] | None = None,
        api_version: str = aca.DEFAULT_API_VERSION,
        poll_interval_seconds: int = 15,
        execution_timeout_seconds: int = 60 * 60,
        deferrable: bool | None = None,
        azure_conn_id: str | None = None,
        managed_identity_client_id: str | None = None,
        pass_openlineage_parent: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.job_name = job_name
        self.job_resource_id = job_resource_id
        self.image = image
        self.command = command
        self.args = args
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.job_start_body = job_start_body
        self.api_version = api_version
        self.poll_interval_seconds = poll_interval_seconds
        self.execution_timeout_seconds = execution_timeout_seconds
        self.deferrable = _default_deferrable() if deferrable is None else deferrable
        self.azure_conn_id = azure_conn_id
        self.managed_identity_client_id = managed_identity_client_id
        self.pass_openlineage_parent = pass_openlineage_parent

    def _auth(self) -> dict[str, Any]:
        """Serializable auth config shared by the operator and its trigger."""
        return {
            "conn_id": self.azure_conn_id,
            "managed_identity_client_id": self.managed_identity_client_id,
        }

    def execute(self, context: dict[str, Any]) -> str:
        job_ref = self._resolve_job_ref()
        auth = self._auth()
        token = aca.get_token(auth, self.log)
        if self.pass_openlineage_parent and not self.job_start_body:
            lineage_env = aca.openlineage_parent_env(context["ti"], self.log)
            # The DAG's own env_vars win, so a job can be pointed elsewhere.
            self.env_vars = {**lineage_env, **(self.env_vars or {})} or None
        start_body = self._build_start_body(job_ref, token)
        execution_name = aca.start_execution(
            job_ref, token, start_body, self.api_version, self.log
        )

        if self.deferrable:
            self.defer(
                trigger=AzureContainerAppsJobTrigger(
                    job_ref=job_ref.to_dict(),
                    execution_name=execution_name,
                    api_version=self.api_version,
                    poll_interval_seconds=self.poll_interval_seconds,
                    timeout_seconds=self.execution_timeout_seconds,
                    auth=auth,
                ),
                method_name="execute_complete",
            )

        self._wait_for_terminal_state(job_ref, execution_name, token)
        return execution_name

    def execute_complete(
        self,
        context: dict[str, Any],
        event: Mapping[str, Any],
    ) -> str:
        status = event.get("status")
        execution_name = event.get("execution_name")
        if status == "success":
            self.log.info("Execution '%s' succeeded", execution_name)
            return execution_name  # type: ignore[return-value]
        raise AirflowException(
            event.get("message")
            or f"Execution '{execution_name}' did not succeed (status={status})."
        )

    def _resolve_job_ref(self) -> aca.ACAJobRef:
        if self.job_resource_id:
            return aca.ACAJobRef.from_resource_id(self.job_resource_id)

        subscription_id = self.subscription_id
        resource_group = self.resource_group
        if not (subscription_id and resource_group):
            # Allow customers to store these once in the connection's extra.
            defaults = aca.connection_defaults(self.azure_conn_id)
            subscription_id = subscription_id or defaults.get("subscription_id")
            resource_group = resource_group or defaults.get("resource_group")

        if not (subscription_id and resource_group and self.job_name):
            raise AirflowException(
                "Provide either job_resource_id or subscription_id + resource_group "
                "+ job_name (subscription_id/resource_group may also come from the "
                "Airflow connection's extra)."
            )
        return aca.ACAJobRef(
            subscription_id=subscription_id,
            resource_group=resource_group,
            job_name=self.job_name,
        )

    def _build_start_body(self, job_ref: aca.ACAJobRef, token: str) -> dict[str, Any]:
        """Build the body for the ACA Jobs ``/start`` call.

        Precedence:
        1. An explicit ``job_start_body`` (power users) is used as-is.
        2. If any friendly override is set (image/command/args/env_vars/cpu/memory),
           fetch the job's existing container definition and apply only the
           requested changes on top of it, so the customer specifies just what
           they want for this run.
        3. Otherwise send an empty body, which runs the job with its defaults.
        """
        if self.job_start_body:
            return self.job_start_body

        has_overrides = any(
            value is not None
            for value in (
                self.image,
                self.command,
                self.args,
                self.env_vars,
                self.cpu,
                self.memory,
            )
        )
        if not has_overrides:
            return {}

        base_container = self._get_base_container(job_ref, token)
        container: dict[str, Any] = dict(base_container)

        if not container.get("name"):
            container["name"] = self.job_name or job_ref.job_name
        if self.image is not None:
            container["image"] = self.image
        if not container.get("image"):
            raise AirflowException(
                "Could not determine the container image for the override; "
                "pass image=... or remove the override arguments."
            )
        if self.command is not None:
            container["command"] = self.command
        if self.args is not None:
            container["args"] = self.args
        if self.env_vars:
            container["env"] = self._merge_env(base_container.get("env"), self.env_vars)
        if self.cpu is not None or self.memory is not None:
            resources = dict(base_container.get("resources") or {})
            if self.cpu is not None:
                resources["cpu"] = self.cpu
            if self.memory is not None:
                resources["memory"] = self.memory
            container["resources"] = resources

        return {"containers": [container]}

    def _get_base_container(self, job_ref: aca.ACAJobRef, token: str) -> dict[str, Any]:
        url = f"{job_ref.base_url}?api-version={self.api_version}"
        response = aca.request_with_retry("GET", url, token, log=self.log)
        if response.status_code >= 300:
            raise AirflowException(
                "Failed to read ACA Job definition (needed to apply your "
                f"overrides): status={response.status_code}, body={response.text}"
            )
        payload = aca.safe_json(response)
        containers = (
            payload.get("properties", {})
            .get("template", {})
            .get("containers", [])
        )
        return dict(containers[0]) if containers else {}

    @staticmethod
    def _merge_env(
        base_env: list[dict[str, Any]] | None,
        overrides: Mapping[str, Any],
    ) -> list[dict[str, str]]:
        env_map: dict[str, dict[str, str]] = {}
        for entry in base_env or []:
            name = entry.get("name")
            if name:
                env_map[name] = entry
        for name, value in overrides.items():
            env_map[name] = {"name": name, "value": str(value)}
        return list(env_map.values())

    def _wait_for_terminal_state(
        self,
        job_ref: aca.ACAJobRef,
        execution_name: str,
        token: str,
    ) -> None:
        import time

        deadline = time.monotonic() + self.execution_timeout_seconds
        while time.monotonic() < deadline:
            try:
                state = aca.get_execution_state(
                    job_ref, execution_name, token, self.api_version, self.log
                )
            except aca.ACATokenExpired:
                token = aca.get_token(self._auth(), self.log)
                continue

            if state in aca.TERMINAL_SUCCESS:
                self.log.info("Execution '%s' succeeded", execution_name)
                return
            if state in aca.TERMINAL_FAILURE:
                raise AirflowException(
                    f"Execution '{execution_name}' ended in terminal failure state: {state}"
                )
            self.log.info("Execution '%s' current state=%s", execution_name, state)
            time.sleep(self.poll_interval_seconds)

        raise AirflowException(
            f"Execution '{execution_name}' did not complete before timeout "
            f"({self.execution_timeout_seconds}s)"
        )
