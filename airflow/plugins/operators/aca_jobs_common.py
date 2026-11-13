"""Shared helpers for the Azure Container Apps Jobs integration.

Used by both the operator (worker side) and the trigger (triggerer side) so the
HTTP/auth/poll logic lives in one place. Hardened for production fan-out:
retries with exponential backoff on throttling (429) and transient 5xx errors,
honoring the ARM `Retry-After` header.
"""
from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

import requests
from airflow.exceptions import AirflowException


def _get_connection(conn_id: str):
    """Look up an Airflow Connection (Task SDK on Airflow 3, BaseHook on 2.x)."""
    try:
        from airflow.sdk import BaseHook
    except ImportError:  # pragma: no cover - Airflow 2.x
        from airflow.hooks.base import BaseHook
    return BaseHook.get_connection(conn_id)

DEFAULT_API_VERSION = "2024-03-01"

# Identifies executions started by this operator on the ARM Jobs API, so the
# ACA platform can attribute job executions to Airflow and adoption of this
# integration can be measured (see docs/why-airflow-on-aca-jobs.md).
OPERATOR_USER_AGENT = "airflow-on-aca-jobs-operator/0.1"

# Transient statuses worth retrying: ARM throttling (429) and gateway/5xx.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

TERMINAL_SUCCESS = {"Succeeded"}
TERMINAL_FAILURE = {"Failed", "Canceled"}


class ACATokenExpired(Exception):
    """Raised on HTTP 401 so callers can refresh the ARM token and retry."""


@dataclass(frozen=True)
class ACAJobRef:
    subscription_id: str
    resource_group: str
    job_name: str

    @property
    def base_url(self) -> str:
        return (
            "https://management.azure.com/subscriptions/"
            f"{self.subscription_id}/resourceGroups/{self.resource_group}"
            f"/providers/Microsoft.App/jobs/{self.job_name}"
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "subscription_id": self.subscription_id,
            "resource_group": self.resource_group,
            "job_name": self.job_name,
        }

    @staticmethod
    def from_dict(data: Mapping[str, str]) -> ACAJobRef:
        return ACAJobRef(
            subscription_id=data["subscription_id"],
            resource_group=data["resource_group"],
            job_name=data["job_name"],
        )

    @staticmethod
    def from_resource_id(resource_id: str) -> ACAJobRef:
        pattern = (
            r"^/subscriptions/(?P<subscription_id>[^/]+)/resourceGroups/"
            r"(?P<resource_group>[^/]+)/providers/Microsoft\.App/jobs/"
            r"(?P<job_name>[^/]+)$"
        )
        match = re.match(pattern, resource_id, flags=re.IGNORECASE)
        if not match:
            raise AirflowException(
                "job_resource_id must match "
                "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/jobs/<job>"
            )
        parts = match.groupdict()
        return ACAJobRef(
            subscription_id=parts["subscription_id"],
            resource_group=parts["resource_group"],
            job_name=parts["job_name"],
        )


def headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": OPERATOR_USER_AGENT,
    }


ARM_SCOPE = "https://management.azure.com/.default"


def get_token(auth: Mapping[str, Any] | None = None, log: Any = None) -> str:
    """Return an ARM bearer token using whatever auth the customer configured.

    Resolution order (first match wins):

    1. **Airflow Connection** (``auth["conn_id"]``) — the easy, portable path that
       works in *any* Airflow (Azure, AWS, on-prem). The connection can hold a
       service principal (client id/secret/tenant), a pre-fetched ``access_token``,
       or a user-assigned managed identity client id. See ``token_from_connection``.
    2. **Pre-fetched token** in the ``AZURE_ACCESS_TOKEN`` env var (handy for the
       local demo / quick tests; short-lived ~1h).
    3. **Ambient credentials** via ``DefaultAzureCredential`` (managed identity,
       ``AZURE_*`` env vars, az login, …). Pass ``managed_identity_client_id`` to
       select a specific user-assigned identity.
    """
    auth = dict(auth or {})
    conn_id = auth.get("conn_id")
    managed_identity_client_id = auth.get("managed_identity_client_id")

    if conn_id:
        if log:
            log.info("Authenticating to Azure via Airflow connection '%s'.", conn_id)
        return token_from_connection(conn_id, managed_identity_client_id, log)

    env_token = os.environ.get("AZURE_ACCESS_TOKEN")
    if env_token:
        if log:
            log.info("Using ARM token from AZURE_ACCESS_TOKEN environment variable.")
        return env_token

    if log:
        log.info("Authenticating to Azure via DefaultAzureCredential.")
    return _default_credential_token(managed_identity_client_id)


def token_from_connection(
    conn_id: str,
    managed_identity_client_id: str | None = None,
    log: Any = None,
) -> str:
    """Mint an ARM token from an Airflow Connection.

    Recognised fields (login/password or matching keys in the JSON *extra*):

    | Purpose                         | Connection field |
    |---------------------------------|------------------|
    | Service principal client id     | ``login`` (or extra ``client_id``) |
    | Service principal client secret | ``password`` (or extra ``client_secret``) |
    | Azure AD tenant id              | extra ``tenant_id`` |
    | Pre-fetched ARM token           | extra ``access_token`` |
    | User-assigned MI client id      | extra ``managed_identity_client_id`` |

    Tenant/subscription/resource-group can also be stored in the extra so DAGs
    need even less inline config (see ``connection_defaults``).
    """
    conn = _get_connection(conn_id)
    extra = _conn_extra(conn)

    access_token = extra.get("access_token")
    if access_token:
        return str(access_token)

    client_id = conn.login or extra.get("client_id") or extra.get("clientId")
    client_secret = conn.password or extra.get("client_secret") or extra.get("clientSecret")
    tenant_id = extra.get("tenant_id") or extra.get("tenantId")
    mi_client_id = (
        managed_identity_client_id
        or extra.get("managed_identity_client_id")
        or extra.get("managedIdentityClientId")
    )

    if client_id and client_secret and tenant_id:
        from azure.identity import ClientSecretCredential

        credential = ClientSecretCredential(
            tenant_id=str(tenant_id),
            client_id=str(client_id),
            client_secret=str(client_secret),
        )
        return credential.get_token(ARM_SCOPE).token

    # No full service principal in the connection: fall back to ambient creds,
    # honoring a user-assigned managed identity if one was supplied.
    if log:
        log.info(
            "Connection '%s' has no full service principal; using "
            "DefaultAzureCredential.",
            conn_id,
        )
    return _default_credential_token(mi_client_id or client_id)


def connection_defaults(conn_id: str | None) -> dict[str, str]:
    """Pull optional subscription_id/resource_group defaults from a connection.

    Lets customers store these once in the connection's *extra* instead of
    repeating them in every DAG. Returns an empty dict when unavailable.
    """
    if not conn_id:
        return {}
    try:
        extra = _conn_extra(_get_connection(conn_id))
    except Exception:  # pragma: no cover - connection missing/unreadable
        return {}
    out: dict[str, str] = {}
    for key in ("subscription_id", "subscriptionId"):
        if extra.get(key):
            out["subscription_id"] = str(extra[key])
            break
    for key in ("resource_group", "resourceGroup"):
        if extra.get(key):
            out["resource_group"] = str(extra[key])
            break
    return out


def _conn_extra(conn: Any) -> dict[str, Any]:
    try:
        extra = conn.extra_dejson
        return dict(extra) if isinstance(extra, dict) else {}
    except Exception:  # pragma: no cover - malformed extra
        return {}


def _default_credential_token(managed_identity_client_id: str | None = None) -> str:
    from azure.identity import DefaultAzureCredential

    kwargs: dict[str, Any] = {}
    if managed_identity_client_id:
        kwargs["managed_identity_client_id"] = str(managed_identity_client_id)
    credential = DefaultAzureCredential(**kwargs)
    return credential.get_token(ARM_SCOPE).token


def request_with_retry(
    method: str,
    url: str,
    token: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: int = 30,
    max_retries: int = 6,
    backoff_base: float = 1.0,
    backoff_cap: float = 60.0,
    log: Any = None,
    sleep: Callable[[float], None] = time.sleep,
) -> requests.Response:
    """Issue an ARM request, retrying transient throttling/5xx with backoff.

    Honors the `Retry-After` response header when present. Raises
    ACATokenExpired on 401 so the caller can refresh the token.
    """
    last_response: requests.Response | None = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers(token),
                json=json_body,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise AirflowException(f"ARM request failed after retries: {exc}") from exc
            delay = min(backoff_base * (2 ** attempt), backoff_cap)
            if log:
                log.warning("ARM request error (%s); retry in %.1fs", exc, delay)
            sleep(delay)
            continue

        if response.status_code == 401:
            raise ACATokenExpired("ARM returned 401 (token expired or invalid).")

        if response.status_code in RETRYABLE_STATUS and attempt < max_retries:
            delay = _retry_delay(response, attempt, backoff_base, backoff_cap)
            if log:
                log.warning(
                    "ARM %s on %s; retry %d/%d in %.1fs",
                    response.status_code, url, attempt + 1, max_retries, delay,
                )
            last_response = response
            sleep(delay)
            continue

        return response

    return last_response  # type: ignore[return-value]


def _retry_delay(
    response: requests.Response,
    attempt: int,
    backoff_base: float,
    backoff_cap: float,
) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), backoff_cap)
        except ValueError:
            pass
    return min(backoff_base * (2 ** attempt), backoff_cap)


def start_execution(
    job_ref: ACAJobRef,
    token: str,
    start_body: dict[str, Any],
    api_version: str,
    log: Any = None,
) -> str:
    url = f"{job_ref.base_url}/start?api-version={api_version}"
    response = request_with_retry(
        "POST", url, token, json_body=start_body, log=log
    )
    if response.status_code >= 300:
        raise AirflowException(
            "Failed to start ACA Job execution: "
            f"status={response.status_code}, body={response.text}"
        )
    payload = safe_json(response)
    execution_name = _extract_execution_name(payload, response.headers)
    if not execution_name:
        raise AirflowException(
            "ACA start response did not include execution name in body or headers."
        )
    if log:
        log.info("Started ACA Job execution '%s'", execution_name)
    return execution_name


def get_execution_state(
    job_ref: ACAJobRef,
    execution_name: str,
    token: str,
    api_version: str,
    log: Any = None,
) -> str:
    url = (
        f"{job_ref.base_url}/executions/{execution_name}"
        f"?api-version={api_version}"
    )
    response = request_with_retry("GET", url, token, log=log)
    if response.status_code >= 300:
        raise AirflowException(
            "Failed to fetch ACA Job execution state: "
            f"status={response.status_code}, body={response.text}"
        )
    payload = safe_json(response)
    state = payload.get("properties", {}).get("status")
    if not state:
        raise AirflowException(f"Execution status missing from response: {payload}")
    return state


def safe_json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
        if not isinstance(data, dict):
            raise AirflowException(
                f"Expected JSON object response, got: {type(data).__name__}"
            )
        return data
    except (json.JSONDecodeError, ValueError) as exc:
        raise AirflowException(f"Invalid JSON response: {response.text}") from exc


def _extract_execution_name(
    payload: Mapping[str, Any],
    response_headers: Mapping[str, str],
) -> str | None:
    execution_name = payload.get("name")
    if isinstance(execution_name, str) and execution_name:
        return execution_name

    location = response_headers.get("Location") or response_headers.get("location")
    if not location:
        return None

    match = re.search(r"/executions/([^/?]+)", location)
    if not match:
        return None
    return match.group(1)


# --- OpenTelemetry: a client span per HTTP call ---------------------------------
# Every ARM call (start / poll) and the managed-identity token fetch go through
# `requests`, so instrumenting it puts each call under Airflow's own `worker.*`
# (operator) or `trigger.*` (triggerer) span, which are current while
# execute()/run() runs. No-op unless opentelemetry-instrumentation-requests is
# installed; spans go to whatever global tracer provider Airflow configured.

_ARM_NAME_SEGMENTS = re.compile(r"/(jobs|executions)/[^/]+", re.IGNORECASE)


def arm_resource_name(method: str, url: str) -> str | None:
    """Low-cardinality name for an ARM call, e.g. ``POST Microsoft.App/jobs/{job}/start``.

    Returns None for non-ARM URLs so they keep the default span naming.
    """
    path = url.split("?", 1)[0]
    if "management.azure.com" not in path or "/providers/" not in path:
        return None
    provider_path = path.split("/providers/", 1)[1]
    templated = _ARM_NAME_SEGMENTS.sub(
        lambda m: f"/{m.group(1)}/{{{m.group(1).lower().rstrip('s')}}}", provider_path
    )
    return f"{method.upper()} {templated}"


def _name_arm_span(span: Any, request: Any) -> None:
    if not span.is_recording():
        return
    method, url = request.method or "", request.url or ""
    name = arm_resource_name(method, url)
    if name is None and url.split("?", 1)[0].endswith("/msi/token"):
        name = f"{method.upper()} managed identity token"
    if name:
        # Datadog uses `resource.name` as the span's resource (otherwise just "POST").
        span.set_attribute("resource.name", name)


# OpenLineage's Datadog transport also uses `requests`, but its listener emits
# outside the task span, so each event would become a stray single-span trace.
_EXCLUDED_URLS = r"data-obs-intake\."


def instrument_requests() -> bool:
    """Turn on `requests` OTel instrumentation once per process; True if active."""
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
    except ImportError:
        return False
    instrumentor = RequestsInstrumentor()
    if not instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.instrument(request_hook=_name_arm_span, excluded_urls=_EXCLUDED_URLS)
    return True


instrument_requests()


# --- OpenLineage: hand the task's run to the job --------------------------------
# A job that emits its own OpenLineage events with these as its ParentRunFacet
# shows up nested under the Airflow task in Datadog (Data Jobs Monitoring and the
# trace view), the same way the Databricks provider links Databricks runs. The
# format is '{namespace}/{job_name}/{run_id}', the one dbt-ol reads and the
# provider's lineage_parent_id / lineage_root_parent_id macros produce.

OPENLINEAGE_PARENT_ENV = "OPENLINEAGE_PARENT_ID"
OPENLINEAGE_ROOT_PARENT_ENV = "OPENLINEAGE_ROOT_PARENT_ID"


def openlineage_parent_env(task_instance: Any, log: Any = None) -> dict[str, str]:
    """Env vars identifying this task's OpenLineage run; {} without the provider."""
    try:
        from airflow.providers.openlineage.plugins.macros import (
            lineage_parent_id,
            lineage_root_parent_id,
        )
    except ImportError:
        return {}
    try:
        return {
            OPENLINEAGE_PARENT_ENV: lineage_parent_id(task_instance),
            OPENLINEAGE_ROOT_PARENT_ENV: lineage_root_parent_id(task_instance),
        }
    except Exception as exc:  # lineage must never fail the task
        if log:
            log.warning("Not passing OpenLineage parent run to the job: %s", exc)
        return {}
