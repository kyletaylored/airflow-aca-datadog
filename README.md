# Airflow hosted on Azure Container Apps

[![CI](https://github.com/hetvip2/airflow-hosted-on-aca/actions/workflows/ci.yml/badge.svg)](https://github.com/hetvip2/airflow-hosted-on-aca/actions/workflows/ci.yml)

A self-contained [Azure Developer CLI (`azd`)](https://learn.microsoft.com/azure/developer/azure-developer-cli/)
template that **hosts Apache Airflow on Azure Container Apps** and uses it to
orchestrate **Azure Container Apps Jobs**. You run one command, `azd up`, and get
a working Airflow 3 (api-server + scheduler + triggerer + dag-processor) plus a sample ACA Job for Airflow
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

- **Airflow 3** runs as four Container Apps (api-server, scheduler, triggerer,
  dag-processor) using **LocalExecutor** (tasks run in
  the scheduler process — simple, no Celery/Redis to operate).
- **DAGs and plugins** are delivered to a mounted **Azure Files** share, so you
  update workflows by re-uploading files — no image rebuild.
- A **user-assigned managed identity** (Contributor on the resource group) lets
  the operator call the ARM Jobs API with **no secrets in Airflow**.
- A **sample ACA Job** is deployed as the "muscle" Airflow drives out of the box.

---

## Quick start (local, ~5 minutes)

Run the DAGs and operator on your laptop with Docker before touching Azure. The
local stack still uses Airflow 2.10 (the code keeps Airflow 2 fallbacks); the
Azure deploy runs Airflow 3.4.

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
   share, the four Airflow apps, and the sample ACA Job).
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
| `AIRFLOW_SECRET_KEY` | _(random per start)_ | API server `secret_key` and JWT secret; set it so sessions/CSRF survive restarts and new revisions |
| `ASSIGN_JOBS_ROLE` | `true` | Set to `false` to skip the identity's role assignment when you lack Owner/UAA (see below) |
| `DEMO_DAG_SCHEDULE` | `*/5 * * * *` | Schedule for the bundled DAGs (set to an empty string for manual-only) |
| `RESOURCE_TAGS` | _(empty)_ | Extra tags on every resource, e.g. `team=data,owner=me` (merged with the resource group's tags) |
| `DD_API_KEY` | _(empty)_ | Datadog API key. Setting it enables OTel traces + Data Jobs Monitoring (see [Datadog](#datadog-opentelemetry-traces--data-jobs-monitoring)) |
| `DD_SITE` | `datadoghq.com` | Datadog site (`us3.datadoghq.com`, `us5.datadoghq.com`, `datadoghq.eu`, `ap1.datadoghq.com`, …) |
| `TELEMETRY_SIGNALS` | `traces,metrics,logs` | This template's setting (not a Datadog or OTel variable): which OTel signals Airflow emits and the managed agent sends to Datadog |
| `DD_ENV` | `airflow-aca` | Datadog `env` tag on traces and the OpenLineage namespace |

---

## Datadog: OpenTelemetry traces + Data Jobs Monitoring

### The minimum, for any Airflow 3 deployment

There are two independent pieces, and each one is only environment variables. The
official `apache/airflow` 3.x image already includes the OpenLineage provider and
the OpenTelemetry SDK, so neither needs an install. **Start with Data Jobs
Monitoring:** on its own it gives you a trace per DAG run in Datadog.

| | Data Jobs Monitoring (OpenLineage) | APM (OpenTelemetry) |
|---|---|---|
| **You get** | Every DAG run and task run: status, duration, retries, failures, and dataset lineage. Also a trace per DAG run (DAG → tasks → jobs they launched). | Airflow's internals: the scheduler and the task process, plus calls tasks make (here, Azure ARM). Optionally Airflow metrics and logs carrying trace ids. |
| **Set it on** | Scheduler and wherever tasks run (workers; with `LocalExecutor`, the scheduler) | Each component you want to see |
| **Needs** | A Datadog API key | An OTLP endpoint that forwards to Datadog |

**Data Jobs Monitoring.** Set these four variables ([Datadog guide](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/)):

```bash
OPENLINEAGE__TRANSPORT__TYPE=datadog
DD_API_KEY=<datadog-api-key>     # from a secret
DD_SITE=datadoghq.com            # your Datadog site
OPENLINEAGE_NAMESPACE=<env>      # shows up as the env tag
```

The `datadog` transport needs `apache-airflow-providers-openlineage` 2.7.3+ (the
3.x image's is newer). On an older provider, set
`OPENLINEAGE_URL=https://data-obs-intake.<site>` and `OPENLINEAGE_API_KEY=<key>`
instead of the first three.

**APM.** Turn on Airflow's native tracing and name each component:

```bash
AIRFLOW__TRACES__OTEL_ON=True
OTEL_SERVICE_NAME=airflow-scheduler                # per component
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>
```

Then give Airflow an OTLP endpoint:

- **On Azure Container Apps:** enable the environment's
  [managed OpenTelemetry agent](https://learn.microsoft.com/azure/container-apps/opentelemetry-agents)
  with a Datadog destination (site and API key). ACA injects the endpoint into every
  app. Don't set any `OTEL_EXPORTER_OTLP_*` variable yourself: if you set even one,
  ACA stops injecting all of them. Extend `OTEL_RESOURCE_ATTRIBUTES` as
  [Datadog's ACA managed-agent guide](https://docs.datadoghq.com/serverless/azure_container_apps/otel_managed_agent/)
  describes, so Datadog ties each service to its container app:
  `cloud.provider=azure,cloud.platform=azure.container_apps,cloud.resource_id=<app resource id>,cloud.region=<region>,deployment.environment.name=<env>,service.version=<version>`.
  Keep `deployment.environment=<env>` in the list too: the managed agent sets the
  Datadog `env` tag only from that key.
- **Anywhere else:** point `OTEL_EXPORTER_OTLP_ENDPOINT` at a Datadog Agent with
  OTLP ingest turned on (for example `http://<agent>:4317`).

Everything else in this template is optional on top of that: Airflow metrics, logs,
Azure API spans, and ACA Jobs reporting their own runs. All of it is described below.

### OpenLineage (DJM) vs OpenTelemetry (APM), for a pipeline team

They are not the same data. OpenLineage reports **what your pipelines did**: one
event when each DAG run, task run, or launched job starts and ends, with its status,
timing, configuration and datasets. OpenTelemetry reports **how Airflow did the
work**: fine-grained spans inside Airflow's own processes, plus Airflow's metrics and
logs. In Datadog, DJM's services are your DAGs (`contoso_sales`); APM's services are
Airflow components (`airflow-aca-scheduler`). They arrive as separate traces, so tie
them together by DAG ID, run ID and task ID.

| Question a pipeline owner asks | Data Jobs Monitoring (OpenLineage) | APM (OpenTelemetry) |
|---|---|---|
| Did last night's run succeed? Which task failed, and why? | ✅ Status of each DAG run and task, try number, error message and stack trace | Partly: errors on the task's span |
| Is it slower than usual, or late? | ✅ Run and task duration history per DAG, monitors on job status and duration | Task-process timing only |
| What did it read and write? What is downstream of a broken run? | ✅ Dataset lineage (from operators that report it, and from jobs like `contoso_store_job`) | ❌ |
| What did a task launch (ACA Job, Databricks, Spark), and how did that go? | ✅ Child job runs nested under the task, when the job reports its parent | ❌ (unless the job is OTel-instrumented too) |
| Which operator, owner, tags, params, schedule, run type? | ✅ Attached to every run as tags | Some, as span attributes |
| Where did the time go *inside* a task: queueing, the scheduler, an API call? | ❌ | ✅ Scheduler/worker/triggerer spans and HTTP client spans (here: each Azure ARM call, with latency and status code) |
| Is Airflow itself healthy (heartbeats, executor and pool slots, DAG parse time)? | ❌ | ✅ Airflow's OTel metrics |
| Task logs next to the run | When logs are collected with `source:airflow` | ✅ Logs carrying trace ids (this template's log handler) |

**Rule of thumb:** the team that owns the DAGs starts with Data Jobs Monitoring,
because it is organized around DAGs and data. APM is for the team that runs
Airflow itself, or for when a slow task's cause sits inside Airflow or in the
services a task calls.

### Airflow 2 vs Airflow 3

This template started on Airflow 2.10 and now runs 3.4. The Data Jobs Monitoring
setup is the same on both
([Datadog's Kubernetes tab](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/?tab=kubernetes)
is what we followed, since ACA apps are configured the same way: env vars on the
scheduler and workers). The OpenTelemetry setup is where they differ.

| | Airflow 2 (2.7 to 2.10) | Airflow 3 |
|---|---|---|
| **OpenLineage provider** | Install and pin it on the scheduler and workers yourself (we used `apache-airflow-providers-openlineage==2.8.0`, Datadog's pick for 2.10). The `datadog` transport needs 2.7.3+. | Included in the official image (3.4.0 ships 2.21). Nothing to install. |
| **DJM variables** | The same four. On 2.7/2.8 with the `OPENLINEAGE_URL` form, also set `AIRFLOW__OPENLINEAGE__CONFIG_PATH=""` and `AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS=""`. | The same four. |
| **Trace export** | Airflow's own OTel exports only to its `[traces] otel_host`/`otel_port` settings over OTLP/HTTP, and ignores the standard `OTEL_EXPORTER_OTLP_*` variables that ACA injects (gRPC). | Reads the standard `OTEL_EXPORTER_OTLP_*` variables and defaults to gRPC, so the endpoint ACA injects just works. |
| **Service name** | `AIRFLOW__TRACES__OTEL_SERVICE` | `OTEL_SERVICE_NAME` |
| **What it took on ACA** | Startup pip installs of the OTel SDK and instrumentations, running every component under `opentelemetry-instrument`, a gunicorn workaround for the webserver, and a custom plugin to build DAG-run traces (see this repo's history before commit `a1410cd`). | `AIRFLOW__TRACES__OTEL_ON=True` (and `AIRFLOW__METRICS__OTEL_ON=True` for metrics). |
| **Components** | webserver, scheduler, triggerer | api-server, scheduler, triggerer, dag-processor (each reports as its own service) |

If you are still on Airflow 2, start with Data Jobs Monitoring only. It needs just
the provider and four variables, and it already gives you a trace per DAG run.

### What this template sets up

Set a Datadog API key before `azd up` and the deploy wires up two things:

```bash
azd env set DD_API_KEY "<your-datadog-api-key>"
azd env set DD_SITE    "datadoghq.com"     # your Datadog site
azd env set DD_ENV     "airflow-aca"       # env tag / OpenLineage namespace
azd up
```

**1. Traces, metrics and logs via the ACA managed OpenTelemetry agent.** The
Container Apps environment's [managed OTel agent](https://learn.microsoft.com/azure/container-apps/opentelemetry-agents)
is configured with a Datadog destination, and ACA injects its OTLP/gRPC endpoint
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`) into every app.
Airflow 3 reads those standard variables natively, so the deploy only sets:

- **Traces:** `AIRFLOW__TRACES__OTEL_ON=True`
  ([Airflow traces](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/traces.html)):
  one trace per DAG run with a span per task, propagated from the scheduler to the
  task process.
- **Azure API calls:** the scheduler and triggerer also install
  `opentelemetry-instrumentation-requests` at startup, and the operator plugin turns
  it on. Each ARM call (start the job, poll its execution) and managed-identity token
  fetch becomes a client span under the task's `worker.*` / `trigger.*` span, with
  resources like `POST Microsoft.App/jobs/{job}/start`. For production, bake the
  package into a custom image instead of installing it at startup.
- **Metrics:** `AIRFLOW__METRICS__OTEL_ON=True`: scheduler heartbeat, executor
  and pool slots, DAG/task durations, DAG parsing.
- **Logs:** Airflow has no native OTel logs, so `plugins/otel_logging_config.py`
  adds an OTLP log handler to Airflow's logging config. Records emitted inside a
  span carry its trace id, so Datadog links logs and traces.

Each component reports as its own service: `airflow-aca-api-server`,
`airflow-aca-scheduler`, `airflow-aca-triggerer`, `airflow-aca-dag-processor`
(`env:$DD_ENV`). The template's `TELEMETRY_SIGNALS` setting (default
`traces,metrics,logs`) picks the signals.

**2. OpenLineage → [Data Jobs Monitoring](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/).**
The image's OpenLineage provider (2.21) supports Datadog's native transport, so
the deploy only sets `OPENLINEAGE__TRANSPORT__TYPE=datadog`, `DD_API_KEY`, `DD_SITE`
and `OPENLINEAGE_NAMESPACE=$DD_ENV`. DAG and task runs appear under
**Data Observability → Jobs Monitoring** with `env:$DD_ENV`.

**3. ACA Job runs nested under the task that started them.** The operator passes
each execution `OPENLINEAGE_PARENT_ID` and `OPENLINEAGE_ROOT_PARENT_ID` (the task's
and DAG run's `{namespace}/{job_name}/{run_id}`, the format dbt-ol also reads).
A job that sends its own OpenLineage START/COMPLETE events, with those as its
parent run, appears in Datadog under that Airflow task, the same way Databricks
runs do. Its input and output datasets join the lineage graph. The sample ACA Job
gets `DD_API_KEY` (as a job secret), `DD_SITE` and `OPENLINEAGE_NAMESPACE` for
this. [`airflow/dags/jobs/contoso_store_job.py`](airflow/dags/jobs/contoso_store_job.py)
is a stdlib-only example: in your own job you can do the same with the
`openlineage-python` client and `OPENLINEAGE__TRANSPORT__TYPE=datadog`. Pass
`pass_openlineage_parent=False` to the operator to turn this off.

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
AIRFLOW=https://<your-airflow-web-url>
# Airflow 3 REST API (v2) uses bearer tokens: exchange the username/password once.
TOKEN=$(curl -s -X POST "$AIRFLOW/auth/token" -H "Content-Type: application/json" \
  -d '{"username": "<user>", "password": "<password>"}' | jq -r .access_token)

curl -X POST "$AIRFLOW/api/v2/dags/aca_jobs_example/dagRuns" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"logical_date": null, "conf": {"job_name": "nightly-etl", "env": {"DATASET": "sales-2026"}}}'
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
│  │  └─ jobs/              # code the contoso_sales ACA Jobs run (reports OpenLineage)
│  ├─ plugins/
│  │  ├─ operators/         # AzureContainerAppsJobOperator + ARM/auth helpers
│  │  ├─ triggers/          # async trigger for deferrable execution
│  │  └─ otel_logging_config.py  # OTLP log handler for Airflow's logging (Datadog)
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
