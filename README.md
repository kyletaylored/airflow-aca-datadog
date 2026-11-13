# Airflow hosted on Azure Container Apps

[![CI](https://github.com/hetvip2/airflow-hosted-on-aca/actions/workflows/ci.yml/badge.svg)](https://github.com/hetvip2/airflow-hosted-on-aca/actions/workflows/ci.yml)

A self-contained [Azure Developer CLI (`azd`)](https://learn.microsoft.com/azure/developer/azure-developer-cli/)
template that **hosts Apache Airflow on Azure Container Apps** and uses it to
orchestrate **Azure Container Apps Jobs**. You run one command, `azd up`, and get
a working Airflow (web + scheduler + triggerer) plus a sample ACA Job for Airflow
to drive — no servers to manage, scale-to-zero job execution, per-execution billing.

> **Two ways to combine Airflow + ACA Jobs**
> - **This repo (you host Airflow):** deploy the whole Airflow control plane on
>   Azure Container Apps. Good when you don't already run Airflow and want a
>   turnkey orchestrator next to your jobs.
> - **[`airflow-on-aca-jobs`](https://github.com/hetvip2/airflow-on-aca-jobs) (host nothing):**
>   drop the same operator into an Airflow you *already* run (Managed Airflow,
>   MWAA, self-hosted) and point it at ACA Jobs. Lowest ownership.
>
> Both share the exact same operator/trigger code, so you can start here and
> graduate to host-nothing later with zero DAG changes.

---

## Architecture

```
                         Azure Container Apps environment
   ┌───────────────────────────────────────────────────────────────┐
   │                                                                 │
   │   airflow-web        airflow-scheduler      airflow-triggerer   │
   │   (ingress :8080)    (LocalExecutor)        (deferrable ops)    │
   │        │                   │                      │             │
   │        └──────── Postgres Flexible Server (metadata DB) ────────┤
   │                                                                 │
   │   DAGs + plugins  ◄──  Azure Files share (mounted /shared)      │
   │                                                                 │
   │   user-assigned managed identity  ──► ARM Jobs API ──►  ACA Job │
   │   (Contributor on the resource group)                  (sample) │
   └───────────────────────────────────────────────────────────────┘
```

- **Airflow** runs as three Container Apps using **LocalExecutor** (tasks run in
  the scheduler process — simple, no Celery/Redis to operate).
- **DAGs and plugins** are delivered to a mounted **Azure Files** share, so you
  update workflows by re-uploading files — no image rebuild.
- A **user-assigned managed identity** (Contributor on the resource group) lets
  the operator call the ARM Jobs API with **no secrets in Airflow**.
- A **sample ACA Job** is deployed as the "muscle" Airflow drives out of the box.

---

## Quick start (local, ~5 minutes)

Run the exact same Airflow stack on your laptop with Docker before touching Azure.

```bash
git clone https://github.com/hetvip2/airflow-hosted-on-aca
cd airflow-hosted-on-aca

cp .env.example .env        # fill in your ACA Job + auth (see below)
docker compose up --build
```

Open <http://localhost:8080> (user `airflow` / password `airflow`), unpause
`aca_jobs_example`, and **Trigger DAG w/ config** to run your ACA Job.

For a local demo, the quickest auth is a short-lived ARM token:

```bash
az account get-access-token --resource https://management.azure.com --query accessToken -o tsv
# paste into AZURE_ACCESS_TOKEN in .env
```

---

## Deploy to Azure (`azd up`)

### Prerequisites

- [Azure Developer CLI (`azd`)](https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd)
- [Azure CLI (`az`)](https://learn.microsoft.com/cli/azure/install-azure-cli) (used by the post-deploy upload hook)
- An Azure subscription and `azd auth login`
- **Permissions:** the deploy creates a **role assignment** (grants the managed
  identity Contributor so it can start jobs), which requires you to have **Owner**
  or **User Access Administrator** on the target scope. Plain **Contributor is not
  enough** to assign roles. On locked-down or internal subscriptions this step
  often fails — see [Permissions & role assignment](#permissions--role-assignment)
  for the one-flag workaround.

### Steps

```bash
azd env new my-airflow

# Required secrets (or azd will prompt):
# URL-safe (it is embedded in the SQLAlchemy URL) and meets Azure complexity rules:
azd env set POSTGRES_ADMIN_PASSWORD "Pg1-$(openssl rand -hex 16)"
azd env set AIRFLOW_ADMIN_PASSWORD  "$(openssl rand -base64 18)"
azd env set AIRFLOW_SECRET_KEY      "$(openssl rand -hex 32)"   # keeps logins valid across restarts

azd up
```

`azd up` will:

1. Provision the infrastructure in `infra/main.bicep` (Log Analytics, ACA
   environment, managed identity + role assignment, Postgres, storage + file
   share, the three Airflow apps, and the sample ACA Job).
2. Run the **postprovision hook** (`scripts/upload-dags.*`) to upload `airflow/dags`
   and `airflow/plugins` to the file share.

When it finishes, `azd` prints the Airflow web URL (also `azd env get-values`
→ `AIRFLOW_WEB_URL`). Log in with `airflow` / your `AIRFLOW_ADMIN_PASSWORD`.

### Updating DAGs / plugins later

Edit files under `airflow/`, then re-run the upload hook:

```bash
azd hooks run postprovision       # or: ./scripts/upload-dags.sh
```

Airflow picks up the changes within a minute — no redeploy needed.

### Configurable parameters

| azd env variable | Default | Purpose |
| --- | --- | --- |
| `AZURE_LOCATION` | _(prompted)_ | Region (Postgres Flexible Server must be available there) |
| `POSTGRES_ADMIN_PASSWORD` | _(required)_ | Postgres admin password |
| `AIRFLOW_ADMIN_PASSWORD` | _(required)_ | Airflow web UI admin password |
| `NAME_PREFIX` | `aflo` | Prefix for resource names |
| `POSTGRES_ADMIN_USER` | `airflowadmin` | Postgres admin login |
| `POSTGRES_SKU_NAME` / `POSTGRES_SKU_TIER` | `Standard_B1ms` / `Burstable` | Postgres size (B1ms is the smallest managed option) |
| `POSTGRES_ZONE` | _(Azure picks)_ | Pin an availability zone (`1`/`2`/`3`) if creation fails with `SkuNotAvailable` |
| `POSTGRES_LOCATION` | _(same as `AZURE_LOCATION`)_ | Put only Postgres in another region if the SKU isn't offered in yours |
| `AIRFLOW_ADMIN_USER` | `airflow` | Airflow web UI admin username |
| `AIRFLOW_SECRET_KEY` | _(random per start)_ | Webserver `secret_key`; set it so sessions/CSRF survive restarts and new revisions |
| `ASSIGN_JOBS_ROLE` | `true` | Set to `false` to skip the identity's role assignment when you lack Owner/UAA (see below) |
| `DEMO_DAG_SCHEDULE` | `*/5 * * * *` | Schedule for the bundled DAGs (set to an empty string for manual-only) |
| `RESOURCE_TAGS` | _(empty)_ | Extra tags on every resource, e.g. `team=data,owner=me` (merged with the resource group's tags) |
| `DD_API_KEY` | _(empty)_ | Datadog API key. Setting it enables OTel traces + Data Jobs Monitoring (see [Datadog](#datadog-opentelemetry-traces--data-jobs-monitoring)) |
| `DD_SITE` | `datadoghq.com` | Datadog site (`us3.datadoghq.com`, `us5.datadoghq.com`, `datadoghq.eu`, `ap1.datadoghq.com`, …) |
| `DD_OTEL_SIGNALS` | `traces,metrics,logs` | Which OTel signals the managed agent sends to Datadog |
| `DD_ENV` | `airflow-aca` | Datadog `env` tag on traces and the OpenLineage namespace |

---

## Datadog: OpenTelemetry traces + Data Jobs Monitoring

Set a Datadog API key before `azd up` and the deploy wires up two things:

```bash
azd env set DD_API_KEY "<your-datadog-api-key>"
azd env set DD_SITE    "datadoghq.com"     # your Datadog site
azd env set DD_ENV     "airflow-aca"       # env tag / OpenLineage namespace
azd up
```

**1. Traces via the ACA managed OpenTelemetry agent.** The Container Apps
environment's [managed OTel agent](https://learn.microsoft.com/azure/container-apps/opentelemetry-agents)
is configured with a Datadog destination for traces. Each Airflow component runs
under `opentelemetry-instrument`, which picks up the OTLP/gRPC endpoint ACA
injects. You get:

- `airflow-webserver`: Flask request spans (UI + REST API; `/health` excluded).
- `airflow-scheduler`: **one trace per DAG run**, with a root span (`airflow.dag_run`) and
  a child span per task (`airflow.task`) carrying dag/run/task ids, try number, and
  state (`airflow/plugins/otel_dag_run_traces.py`). The operator's ARM API calls
  appear as `requests` spans.
- `airflow-triggerer`: the deferred job-polling ARM calls.

> Airflow 2.10's built-in `[traces]` exporter isn't used because it is hard-coded
> to OTLP/**HTTP**, and the managed agent only accepts OTLP/**gRPC**. For the same
> reason the web app runs gunicorn directly: `airflow webserver` spawns gunicorn as
> a subprocess, which `opentelemetry-instrument` intentionally leaves uninstrumented.

**2. OpenLineage → [Data Jobs Monitoring](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/).**
At startup the OpenLineage provider is upgraded from the image's 1.11.0 to
`2.8.0` (Datadog's recommended version for Airflow 2.10) and configured with the
native `datadog` transport (`OPENLINEAGE__TRANSPORT__TYPE=datadog`, `DD_API_KEY`,
`DD_SITE`, `OPENLINEAGE_NAMESPACE=$DD_ENV`). DAG and task runs appear under
**Data Observability → Jobs Monitoring** with `env:$DD_ENV`.

Leave `DD_API_KEY` empty to deploy without any of this. The managed agent has no
Key Vault integration, so the key is passed as a secure deployment parameter, and
the apps reference it as a Container Apps secret.

---

## Permissions & role assignment

The template gives the Airflow managed identity permission to start/read ACA Jobs
by creating a **Contributor role assignment** scoped to the resource group.
Creating any role assignment requires the deployer to hold **Owner** or **User
Access Administrator** — **Contributor alone cannot assign roles**. This is the
most common reason `azd up` fails on internal or policy-restricted subscriptions.

Symptoms if you don't have the rights:

- `AuthorizationFailed` / `does not have authorization to perform action
  'Microsoft.Authorization/roleAssignments/write'`, or
- a policy denial on role assignments.

**You don't know your role?** Check it:

```bash
# Your role on the subscription (or scope to the RG with --scope)
az role assignment list --assignee $(az ad signed-in-user show --query id -o tsv) \
  --query "[].roleDefinitionName" -o tsv
```

If you see `Owner` or `User Access Administrator`, the default deploy works as-is.
If you only see `Contributor` (or nothing), use one of the options below.

### Option A — deploy without the role, have an admin grant it (works on any sub)

Skip the role assignment so the whole stack still deploys, then have someone with
Owner/UAA grant the role once:

```bash
azd env set ASSIGN_JOBS_ROLE false
azd up
```

When it finishes, the deployment prints a ready-to-run command (also visible via
`azd env get-values` → `GRANT_ROLE_COMMAND`). An admin runs it once:

```bash
# Printed as the grantRoleCommand output — an Owner/UAA runs this:
az role assignment create \
  --assignee-object-id <airflowIdentityPrincipalId> \
  --assignee-principal-type ServicePrincipal \
  --role Contributor \
  --scope /subscriptions/<sub>/resourceGroups/<rg>
```

Airflow can start jobs the moment the assignment exists — no redeploy needed.

> **Least privilege:** you can grant a narrower built-in role instead of
> Contributor. `Container Apps Jobs Operator` (or `Azure Container Apps Contributor`)
> is enough to start/read job executions. Swap `--role` accordingly.

### Option B — elevate first (if you have eligible access via PIM)

On subscriptions where you're an **eligible** Owner/UAA via Privileged Identity
Management, activate the role first, then deploy normally:

1. Azure portal → **Privileged Identity Management** → activate **Owner** or
   **User Access Administrator** on the target scope.
2. `azd up` (leave `ASSIGN_JOBS_ROLE=true`).

### Note for internal / enterprise subscriptions

Internal Microsoft (and many enterprise) subscriptions frequently (a) grant users
only Contributor, and/or (b) block role assignments via Azure Policy, and/or (c)
restrict public networking and regions. Option A above sidesteps (a)/(b) entirely
by decoupling the role grant from the deploy. For (c), deploy into a resource
group/region your subscription allows; the demo defaults use public networking and
can be locked down for production (see [Notes & caveats](#notes--caveats)).

---

## Running your own jobs

The included DAGs are **parameterized** — you specify your workload at trigger
time, no code changes. The operator reads your job's existing definition and
applies only the overrides you pass, so blank fields keep the job's defaults.

### Single job — `aca_jobs_example`

In the Airflow UI, open `aca_jobs_example` → **Trigger DAG w/ config** and fill
the form:

| Field | What it does | Example |
|-------|--------------|---------|
| **ACA Job name** | Which job to run (blank = the `aca_job_name` Variable) | `nightly-etl` |
| **Command override** | Replace the image entrypoint | `["python", "main.py"]` |
| **Args override** | Arguments for the command | `["--batch-size", "100"]` |
| **Environment variables** | Extra env vars for this run | `{"MODE": "nightly"}` |

Or from the CLI / REST API (for automation and scheduling):

```bash
airflow dags trigger aca_jobs_example \
  --conf '{"job_name":"nightly-etl","command":["python","main.py"],"args":["--batch-size","100"],"env":{"MODE":"nightly"}}'
```

### Parallel fan-out — `aca_jobs_pipeline`

Run the same workload across N parallel ACA Job executions (each shard gets
`SHARD_INDEX` / `SHARD_TOTAL` plus any env you pass):

```bash
airflow dags trigger aca_jobs_pipeline \
  --conf '{"shards":50,"command":["python","worker.py"],"env":{"DATASET":"sales-2026"}}'
```

This is the orchestration ACA cron can't express on its own: dependency-aware
ordering, parallel fan-out, and per-task retries. With `deferrable=True` (the
pipeline default) each execution frees its Airflow worker slot while ACA runs, so
fan-out width is bounded by ACA — not by your Airflow worker count.

### Targeting your own job

To target *your* job instead of the bundled sample, either pass `job_name` at
trigger time or set the Airflow Variables `azure_subscription_id`,
`aca_resource_group`, `aca_job_name` (the infra wires these automatically for the
sample job).

### Authentication options

The operator resolves credentials in this order:

1. **Airflow Connection** (`azure_conn_id`) — service principal, pre-fetched
   token, or managed identity stored as an Airflow Connection. **Auto-refreshes**,
   so use this (or option 3) for long-running pipelines.
2. **`AZURE_ACCESS_TOKEN`** env var — a short-lived (~1h) ARM token. Handy for a
   quick local demo, but it does **not** refresh — long jobs or large fan-outs
   that outlive the token will fail mid-run. Don't use it for production.
3. **`DefaultAzureCredential`** — on Azure this transparently uses the
   **user-assigned managed identity** (no secrets, auto-refreshes). Locally it
   falls back to your `az login` / environment credentials.

On the `azd`-deployed stack, option 3 is automatic via the managed identity — the
recommended production path with no secrets to manage.

---

## Integrating an external platform

A common scenario: the customer **doesn't already run an orchestrator**, so this
template *becomes* their orchestration layer — and an existing external system
(their app, data platform, or SaaS) needs to kick off the work or react to it.
Airflow is built for exactly this. The external system never has to "be" Airflow;
it just triggers a DAG or exchanges data with one. Three standard patterns:

### 1. Trigger Airflow from outside via its REST API

Any external system can start a workflow with an HTTPS call to the hosted Airflow.
The DAG then fans the work out to ACA Jobs:

```bash
curl -X POST "https://<your-airflow-web-url>/api/v1/dags/aca_jobs_example/dagRuns" \
  -H "Content-Type: application/json" \
  -u "<user>:<password>" \
  -d '{"conf": {"job_name": "nightly-etl", "env": {"DATASET": "sales-2026"}}}'
```

This is the most common integration: the customer's platform passes its arguments
in `conf`, and Airflow orchestrates the ACA Job(s). (Airflow's
[stable REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
also lists runs, fetches status, and reads logs, so the caller can poll results.)

### 2. Event-driven trigger

Let an Azure event start the pipeline — e.g. a file landing in Blob Storage or a
message on a bus. Wire **Event Grid → Logic App / Azure Function → Airflow REST
API** (pattern 1), so a customer's upstream system triggers a run with no manual
step. The same REST endpoint above is the integration point.

### 3. Integrate inside the DAG

Put the external system *in the pipeline* as tasks: a DAG can read from an
external queue/storage/API, run the ACA Job, then write results back or send a
webhook/notification on completion. This keeps the integration as code next to the
orchestration, version-controlled with the rest of the DAG.

> In every pattern: **external platform = the trigger / data source**, **this
> hosted Airflow = the orchestrator**, **ACA Jobs = the execution.** Both the
> orchestration and the job execution stay on Azure; the customer's existing
> system simply plugs in.

> The template ships the orchestration core (Airflow + operator + sample job) and
> exposes Airflow's REST API for integration. A connector to a *specific* external
> product is a small, customer-specific add-on (a REST call or a DAG task), not a
> change to this architecture.

---

## Project structure

```
airflow-hosted-on-aca/
├─ airflow/
│  ├─ dags/                 # aca_jobs_example, aca_jobs_pipeline, contoso_sales (demo)
│  ├─ plugins/
│  │  ├─ operators/         # AzureContainerAppsJobOperator + ARM/auth helpers
│  │  ├─ triggers/          # async trigger for deferrable execution
│  │  └─ otel_dag_run_traces.py  # DAG run -> OTel trace listener (Datadog)
│  └─ requirements.txt      # azure-identity, requests
├─ infra/
│  ├─ main.bicep            # all Azure resources
│  └─ main.parameters.json  # azd → bicep parameter mapping
├─ scripts/
│  ├─ upload-dags.sh / .ps1 # postprovision: upload DAGs+plugins to the share
├─ tests/                   # 43 offline unit tests (mocked ARM, no Azure)
├─ Dockerfile               # stock apache/airflow + operator + DAGs (local stack)
├─ docker-compose.yml       # local Postgres + web + scheduler + triggerer
├─ azure.yaml               # azd template definition + hooks
└─ pyproject.toml           # pip-installable + pytest/ruff config
```

---

## Testing

The operator/trigger have **offline unit tests** (ARM calls mocked — no Azure
needed):

```bash
pip install -e ".[test]"
pytest
```

CI runs these on Python 3.10 and 3.11 and validates that the Bicep compiles.

> Airflow does not run natively on Windows. To run the tests on Windows, use the
> Docker image (`docker run --rm -v "${PWD}:/work" -w /work apache/airflow:2.10.2
> bash -lc "pip install -q pytest ruff && pytest"`).

---

## Validated end-to-end

This template was tested against a **live** Azure Container Apps Job with the
local stack above — single jobs with custom arguments and an 8-shard parallel
pipeline — across three independent runs:

| Validation run | Single job (custom args) | Parallel pipeline | Azure status |
|----------------|--------------------------|-------------------|--------------|
| Run A | ✅ success | ✅ 8 shards | Succeeded |
| Run B | ✅ success | ✅ 8 shards | Succeeded |
| Run C | ✅ success | ✅ 8 shards | Succeeded |

All runs confirmed that per-run `command` / `args` / `env` overrides reach the
live ACA execution, deferral frees worker slots while jobs run, and ARM tokens
refresh on `401` mid-poll. The runs collectively drove **100+ successful ACA Job
executions** concurrently with no throttling errors.

### Live `azd up` deployment

The full `azd up` path was also run against a live subscription (North Central US,
Contributor-only account using `ASSIGN_JOBS_ROLE=false`). Confirmed:

- ✅ `azd up` provisions the whole stack green: Log Analytics, ACA environment,
  managed identity, Postgres Flexible Server, storage + file share, the three
  Airflow apps, and the sample ACA Job.
- ✅ The `postprovision` hook uploads DAGs + plugins to the file share.
- ✅ Airflow `/health` returns `200` with metadatabase, scheduler, and triggerer
  all healthy.
- ✅ **Zero DAG import errors**; the operator plugin and all DAGs load on the
  deployed stack.
- ✅ The stable REST API accepts basic auth, so an external system can trigger a
  DAG run (validated by POSTing to `/api/v1/dags/.../dagRuns`).

> Starting an actual job additionally requires the managed identity's role
> assignment (see [Permissions & role assignment](#permissions--role-assignment)).
> With `ASSIGN_JOBS_ROLE=false`, grant it once via the printed `grantRoleCommand`;
> the operator's job-start/poll path itself is covered by the live runs above.

---

## Notes & caveats

- **Executor:** LocalExecutor runs tasks **inside the scheduler process** — simple
  and great up to moderate concurrency (validated to ~50 concurrent ACA Job
  executions). For large fan-outs (hundreds–thousands of shards), switch to
  CeleryExecutor/KubernetesExecutor; the operator/DAG code is unchanged. Because
  the operator is **deferrable**, in-flight executions are bounded by ACA, not by
  Airflow worker count — but the scheduler still needs enough parallelism to
  *start* them.
- **Auth for long runs:** use an Airflow Connection or managed identity (both
  auto-refresh). The static `AZURE_ACCESS_TOKEN` is demo-only — see
  [Authentication options](#authentication-options).
- **Networking:** this template uses public ACA ingress and "allow Azure
  services" on Postgres for a fast start. For production, add a VNet + private
  endpoints (the [`n8n-on-aca`](https://github.com/simonjj/n8n-on-aca) template is a good private-networking reference).
- **Least privilege:** the managed identity gets `Contributor` on the resource
  group for simplicity. Scope it to a custom role limited to
  `Microsoft.App/jobs/*/start` + read for production.
- **Task logs in the UI:** with LocalExecutor across separate Container Apps, the
  web app can't always reach the scheduler replica that served a task's log, so
  individual task logs may not render in the UI (the run state is still accurate).
  For durable, UI-visible logs, configure Airflow **remote logging** to Azure Blob
  Storage (`AIRFLOW__LOGGING__REMOTE_LOGGING=true` + a blob connection), or use
  Celery/Kubernetes executors.
- The Bicep is validated by `az bicep build` in CI. A live `azd up` deploy has been
  run manually (see [Live `azd up` deployment](#live-azd-up-deployment)) but is not
  run in CI because it incurs Azure costs.

## License

Apache-2.0 — see [LICENSE](LICENSE).
