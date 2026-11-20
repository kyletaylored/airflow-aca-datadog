# Datadog for Airflow on Azure Container Apps

[![CI](https://github.com/kyletaylored/airflow-aca-datadog/actions/workflows/ci.yml/badge.svg)](https://github.com/kyletaylored/airflow-aca-datadog/actions/workflows/ci.yml)

A working example of monitoring Apache Airflow 3 on Azure Container Apps (ACA) with Datadog:

- **Data Jobs Monitoring** through OpenLineage: every DAG run, task, and the ACA Job runs those tasks launch, with dataset lineage.
- **APM** through OpenTelemetry and the ACA managed OTel agent: Airflow's own traces and metrics, plus a span for every Azure API call a task makes.

The Airflow-on-ACA deployment itself (the `azd` template, the ACA Jobs operator, managed-identity auth) comes from [hetvip2/airflow-hosted-on-aca](https://github.com/hetvip2/airflow-hosted-on-aca). See that repo for the Airflow details. This repo adds the Datadog instrumentation and demo DAGs that show it off.

**Contents:** [Architecture](#architecture) · [Minimum configuration](#minimum-configuration) · [DJM and APM compared](#data-jobs-monitoring-and-apm-compared) · [Airflow 2 and 3](#airflow-2-and-airflow-3-differences) · [Reference configuration](#reference-configuration) · [Deployment](#deployment) · [Known issues](#known-issues) · [Repository layout](#repository-layout)

---

## Architecture

### How the components call each other

```mermaid
flowchart LR
    subgraph env["Azure Container Apps environment"]
        dagp["dag-processor<br/>Container App"]
        subgraph schedApp["scheduler · Container App"]
            sched["scheduler loop"]
            task["task process<br/>LocalExecutor"]
        end
        api["api-server<br/>Container App"]
        trig["triggerer<br/>Container App"]
        job["ACA Job execution<br/>Container Apps Job"]
    end
    arm["Azure Resource Manager<br/>management.azure.com"]

    dagp -- "1 parsed DAGs" --> sched
    sched -- "2 starts the task" --> task
    task -- "3 Execution API" --> api
    task -- "4 start job" --> arm
    arm -- "5 runs" --> job
    task -- "6 defers" --> trig
    trig -- "7 poll execution" --> arm
    trig -- "8 job finished,<br/>resume the task" --> sched
```

Each box is a separate app in one Container Apps environment. For a DAG run of `contoso_sales`:

1. The **dag-processor** parses the DAG files and stores them in the metadata database, where the **scheduler** picks them up.
2. The scheduler creates the DAG run. With `LocalExecutor`, it starts each task as a process inside the scheduler's own container.
3. The task process reports its state, and reads variables and XComs, through the **api-server**'s Execution API.
4. The ACA Jobs operator asks Azure Resource Manager to start an execution of the ACA Job.
5. ARM starts the execution as a **Container Apps Job**.
6. The task defers, which hands the wait to the **triggerer** and frees the task's slot.
7. The triggerer polls ARM for the execution's status.
8. When the execution finishes, the trigger's event (through the metadata database) makes the scheduler resume the task, which records the result.

The scheduler and triggerer authenticate to ARM with a user-assigned managed identity that has Contributor on the resource group. All four apps share a PostgreSQL Flexible Server metadata database and mount the DAGs and plugins from an Azure Files share at `/opt/airflow/shared`.

Datadog's trace map for a `contoso_sales` run shows this same path. It goes from `airflow-aca-scheduler` to `airflow-aca-api-server`, then to the scheduler's task spans and `airflow-aca-triggerer`, and from both of those to `management.azure.com`. The `localhost` calls next to them are managed-identity token requests: ACA serves its identity endpoint inside each container.

### Telemetry to Datadog

```mermaid
flowchart LR
    subgraph env["Azure Container Apps environment"]
        apps["Airflow apps<br/>api-server, scheduler,<br/>triggerer, dag-processor"]
        job["ACA Job executions"]
        agent["Managed OpenTelemetry agent"]
    end

    subgraph dd["Datadog"]
        apm["APM<br/>traces, metrics"]
        djm["Data Jobs Monitoring<br/>DAG, task and job runs, lineage"]
    end

    apps -- "OTLP over gRPC" --> agent
    agent -- "Datadog destination" --> apm
    apps -- "OpenLineage events" --> djm
    job -- "OpenLineage events<br/>with the parent task" --> djm

    classDef datadog fill:#632ca6,stroke:#632ca6,color:#ffffff
    class apm,djm datadog
```

Telemetry reaches Datadog by two independent paths:

- **APM:** every Airflow app exports OTLP to the environment's managed OpenTelemetry agent, which forwards traces, metrics and logs to Datadog.
- **Data Jobs Monitoring:** the OpenLineage listener on the scheduler, where tasks run, and each ACA Job execution post OpenLineage events straight to Datadog's intake. The agent isn't involved.

---

## Minimum configuration

Two steps, and both are configuration only: no code changes, no custom image, no Datadog Agent. Each step works on its own. **Start with step 1:** it covers what pipeline owners ask about, and it already gives you a trace per DAG run.

### Step 1: Data Jobs Monitoring

Set four environment variables on the scheduler, and on your workers if you have any. With `LocalExecutor`, tasks run in the scheduler, so the scheduler alone is enough ([Datadog guide](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/)).

```bash
OPENLINEAGE__TRANSPORT__TYPE=datadog
DD_API_KEY=<datadog-api-key>     # from a secret
DD_SITE=datadoghq.com            # your Datadog site
OPENLINEAGE_NAMESPACE=<env>      # shows up as the env tag
```

**You get:** every DAG run and task run in Data Jobs Monitoring, with status, duration, retries, errors and dataset lineage, plus a trace per DAG run.

### Step 2: APM

**2a. Send the environment's telemetry to Datadog.** Turn on the Container Apps environment's [managed OpenTelemetry agent](https://learn.microsoft.com/azure/container-apps/opentelemetry-agents) with a Datadog destination. It's one command:

```bash
az containerapp env telemetry data-dog set -g <resource-group> -n <environment> \
  --site datadoghq.com --key <datadog-api-key> --enable-open-telemetry-traces true
```

**2b. Turn on Airflow's tracing.** Set these on each Airflow app (api-server, scheduler, triggerer, dag-processor), with each app's own service name:

```bash
AIRFLOW__TRACES__OTEL_ON=True
OTEL_SERVICE_NAME=airflow-scheduler                # per app
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>
```

ACA injects the agent's endpoint into every app. **Don't set any `OTEL_EXPORTER_OTLP_*` variable yourself:** in this demo, setting only `OTEL_EXPORTER_OTLP_PROTOCOL` left every app without an endpoint, and nothing was exported (see [Known issues](#known-issues)). Outside ACA, set `OTEL_EXPORTER_OTLP_ENDPOINT` to a Datadog Agent with OTLP ingest turned on (for example `http://<agent>:4317`) instead of doing 2a.

**You get:** one APM service per Airflow component, with traces of the scheduler, the task process and the triggerer.

### What you don't need

- **No install on Airflow 3.** The official `apache/airflow` 3.x image already includes the OpenLineage provider and the OpenTelemetry SDK. Airflow 2 needs a bit more; see [Airflow 2 and Airflow 3 differences](#airflow-2-and-airflow-3-differences).
- **No DAG or operator changes.** Both work at the Airflow level.
- **No Datadog Agent.** Data Jobs Monitoring posts straight to Datadog's intake, and APM uses ACA's managed agent. (The optional [data pipeline demo](docs/data-pipeline-demo.md) adds an Agent for Postgres monitoring, which neither step needs.)

### Optional extras

Everything else in this repo builds on the two steps. Add any of these only when you want what it gives you:

| Add | What it gives you | How |
|---|---|---|
| Airflow metrics | Scheduler heartbeats, executor and pool slots, DAG parse times | `AIRFLOW__METRICS__OTEL_ON=True`, and `--enable-open-telemetry-metrics true` in 2a |
| Azure API spans | A span for each Azure call a task makes, with latency and status | `opentelemetry-instrumentation-requests` (see [Additional instrumentation](#additional-instrumentation)) |
| ACA Job runs under their task | Each job run nested under the Airflow task that started it in Data Jobs Monitoring, with its datasets in the lineage | This repo's ACA Jobs operator passes the parent IDs; the job sends its own OpenLineage events |
| Services linked to their container apps | Datadog ties each APM service to its container app | Extend `OTEL_RESOURCE_ATTRIBUTES` as [Datadog's ACA guide](https://docs.datadoghq.com/serverless/azure_container_apps/otel_managed_agent/) describes: `cloud.provider=azure,cloud.platform=azure.container_apps,cloud.resource_id=<app resource id>,cloud.region=<region>,deployment.environment.name=<env>,service.version=<version>`. Keep `deployment.environment=<env>` too: the managed agent sets Datadog's `env` tag only from that key. |

The `datadog` OpenLineage transport needs `apache-airflow-providers-openlineage` 2.7.3+ (the Airflow 3 image's is newer). On an older provider, replace the first three variables of step 1 with `OPENLINEAGE_URL=https://data-obs-intake.<site>` and `OPENLINEAGE_API_KEY=<key>`.

---

## Data Jobs Monitoring and APM compared

They are not the same data. OpenLineage reports **what your pipelines did**: one event when each DAG run, task run, or launched job starts and ends, with its status, timing, configuration and datasets. OpenTelemetry reports **how Airflow did the work**: fine-grained spans inside Airflow's own processes, plus Airflow's metrics and logs. In Datadog, DJM's services are your DAGs (`contoso_sales`); APM's services are Airflow components (`airflow-aca-scheduler`). They arrive as separate traces, so tie them together by DAG ID, run ID and task ID.

| Question a pipeline owner asks | Data Jobs Monitoring (OpenLineage) | APM (OpenTelemetry) |
|---|---|---|
| Did last night's run succeed? Which task failed, and why? | ✅ Status of each DAG run and task, try number, error message and stack trace | Partly: errors on the task's span |
| Is it slower than usual, or late? | ✅ Run and task duration history per DAG, monitors on job status and duration | Task-process timing only |
| What did it read and write? What is downstream of a broken run? | ✅ Dataset lineage (from operators that report it, and from jobs like `contoso_store_job`) | ❌ |
| What did a task launch (ACA Job, Databricks, Spark), and how did that go? | ✅ Child job runs nested under the task, when the job reports its parent | ❌ (unless the job is OTel-instrumented too) |
| Which operator, owner, tags, params, schedule, run type? | ✅ Attached to every run as tags | Some, as span attributes |
| Where did the time go *inside* a task: queueing, the scheduler, an API call? | ❌ | ✅ Scheduler/worker/triggerer spans and HTTP client spans (here: each Azure ARM call, with latency and status code) |
| Is Airflow itself healthy (heartbeats, executor and pool slots, DAG parse time)? | ❌ | ✅ Airflow's OTel metrics |
| Task logs next to the run | When logs are collected with `source:airflow` | Airflow's logs with trace ids, through this repo's logging plugin and the managed agent |

**Rule of thumb:** the team that owns the DAGs starts with Data Jobs Monitoring, because it is organized around DAGs and data. APM is for the team that runs Airflow itself, or for when a slow task's cause sits inside Airflow or in the services a task calls.

---

## Airflow 2 and Airflow 3 differences

This repo started on Airflow 2.10 and now runs 3.4. The Data Jobs Monitoring setup is the same on both ([Datadog's Kubernetes tab](https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/?tab=kubernetes) is what we followed, since ACA apps are configured the same way: env vars on the scheduler and workers). The OpenTelemetry setup is where they differ.

| | Airflow 2 (2.7 to 2.10) | Airflow 3 |
|---|---|---|
| **OpenLineage provider** | Install and pin it on the scheduler and workers yourself (we used `apache-airflow-providers-openlineage==2.8.0`, Datadog's pick for 2.10). The `datadog` transport needs 2.7.3+. | Included in the official image (3.4.0 ships 2.21). Nothing to install. |
| **DJM variables** | The same four. On 2.7/2.8 with the `OPENLINEAGE_URL` form, also set `AIRFLOW__OPENLINEAGE__CONFIG_PATH=""` and `AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS=""`. | The same four. |
| **Trace export** | Airflow's own OTel exports only to its `[traces] otel_host`/`otel_port` settings over OTLP/HTTP, and ignores the standard `OTEL_EXPORTER_OTLP_*` variables that ACA injects (gRPC). | Reads the standard `OTEL_EXPORTER_OTLP_*` variables and defaults to gRPC, so the endpoint ACA injects just works. |
| **Service name** | `AIRFLOW__TRACES__OTEL_SERVICE` | `OTEL_SERVICE_NAME` |
| **What it took on ACA** | Startup pip installs of the OTel SDK and instrumentations, running every component under `opentelemetry-instrument`, a gunicorn workaround for the webserver, and a custom plugin to build DAG-run traces (see this repo's history before commit `a1410cd`). | `AIRFLOW__TRACES__OTEL_ON=True` (and `AIRFLOW__METRICS__OTEL_ON=True` for metrics). |
| **Components** | webserver, scheduler, triggerer | api-server, scheduler, triggerer, dag-processor (each reports as its own service) |

If you are still on Airflow 2, start with Data Jobs Monitoring only. It needs just the provider and four variables, and it already gives you a trace per DAG run.

---

## Reference configuration

Everything below is defined in [`infra/main.bicep`](infra/main.bicep) and turns on when `DD_API_KEY` is set. Leave the key empty to deploy plain Airflow. The settings live in five places:

| Location | What it holds | Defined in |
|---|---|---|
| Container Apps environment | The managed OTel agent and its Datadog destination | `openTelemetryConfiguration` on `managedEnvironment` |
| Container app env vars | Per-app settings. The API key comes from the app's `dd-api-key` secret | `datadogEnvVars`, `otelServiceEnv()`, `httpTracingEnv` |
| ACA Job template env | What every job execution starts with. The API key comes from the job's `dd-api-key` secret | `jobLineageEnv` on `sampleJob` |
| Job execution overrides | Values added to one execution when a task starts it | The operator plugin, at run time |
| Azure Files share | Plugin code that some variables point to | [`airflow/plugins/`](airflow/plugins) |

The Airflow apps are the api-server, scheduler, triggerer and dag-processor.

### Data Jobs Monitoring (OpenLineage) settings

Data Jobs Monitoring doesn't use the OTel agent. Each OpenLineage client posts straight to `data-obs-intake.<site>`, so everything here is an environment variable.

| Variable | Value | Set on | Where it's set |
|---|---|---|---|
| `OPENLINEAGE__TRANSPORT__TYPE` | `datadog` | Airflow apps | Container app env var |
| `DD_API_KEY` | `dd-api-key` secret | Airflow apps, sample ACA Job | Container app or job secret, referenced by the env var (`secretRef`) |
| `DD_SITE` | `$DD_SITE` | Airflow apps, sample ACA Job | Container app env var, job template env |
| `OPENLINEAGE_NAMESPACE` | `$DD_ENV` (becomes the `env` tag) | Airflow apps, sample ACA Job | Container app env var, job template env |
| `AIRFLOW__API__BASE_URL` | `https://<prefix>-web.<environment default domain>` (the api-server's public URL) | Airflow apps | Container app env var |
| `OPENLINEAGE_PARENT_ID` | The task's run: `{namespace}/{dag_id}.{task_id}/{run_id}` | Each ACA Job execution | Job execution override, added by the operator |
| `OPENLINEAGE_ROOT_PARENT_ID` | The DAG run: `{namespace}/{dag_id}/{run_id}` | Each ACA Job execution | Job execution override, added by the operator |

Only the scheduler needs the first four variables, because tasks run there with `LocalExecutor`. The other apps also get them because all four apps share one env list. `AIRFLOW__API__BASE_URL` is Airflow's `[api] base_url`. Airflow builds each task event's `log_url` from it, and Data Jobs Monitoring's **Airflow UI** links use that host. Without it they point at `http://localhost:8080`. The ACA Job doesn't read `OPENLINEAGE__TRANSPORT__TYPE`: the demo job's own code sends the events. Pass `pass_openlineage_parent=False` to the operator to stop sending the two parent IDs.

### APM (OpenTelemetry) settings

**Container Apps environment (not environment variables).** The environment's `openTelemetryConfiguration` gives the managed agent a Datadog destination (site and API key). It routes traces to that destination, plus metrics and logs if `TELEMETRY_SIGNALS` includes them. The agent can't read Key Vault or Container Apps secrets, so the key is passed in as a secure deployment parameter and stored on the environment.

**Injected by ACA.** ACA adds `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_PROTOCOL` (gRPC) to every app and job, pointing at the agent. They aren't in the Bicep. Don't set either one yourself (see [Known issues](#known-issues)).

**Container app env vars:**

| Variable | Value | Set on | For |
|---|---|---|---|
| `AIRFLOW__TRACES__OTEL_ON` | `True` | Airflow apps | Traces |
| `AIRFLOW__METRICS__OTEL_ON` | `True` | Airflow apps, when metrics are on | Metrics |
| `AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS` | `otel_logging_config.LOGGING_CONFIG` | Airflow apps, when logs are on | Logs. The logging plugin is code on the Azure Files share |
| `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` | `otlp`, or `none` for a signal that's turned off | Airflow apps | Which signals to send |
| `OTEL_SERVICE_NAME` | `airflow-aca-<component>` | Each app, with its own value | One service per component |
| `OTEL_RESOURCE_ATTRIBUTES` | `cloud.provider`, `cloud.platform`, `cloud.resource_id`, `cloud.region`, `deployment.environment`, `deployment.environment.name`, `service.version`, `service.namespace` | Each app, with its own value | Ties each service to its container app |
| `_PIP_ADDITIONAL_REQUIREMENTS` | `opentelemetry-instrumentation-requests==0.66b2` | Scheduler, triggerer | Azure API spans |

The sample ACA Job sends no OpenTelemetry. ACA injects the endpoint into it too, but the job's code doesn't use it.

### Additional instrumentation

- **Logs.** Airflow has no native OTel logs, and Airflow 3 leaves no room for a plain logging handler: it resets the root and `airflow` loggers' handlers, and most of its code logs through structlog, which skips stdlib handlers. So [`plugins/otel_logging_config.py`](airflow/plugins/otel_logging_config.py) adds a processor to the structlog chain that both structlog and stdlib lines pass through, and exports each line over OTLP. Lines logged inside a span carry its trace id, and each line gets `dd.service` so Datadog can set its service (see [Known issues](#known-issues)). On Airflow 2 it adds a plain OTLP handler instead.
- **Azure API spans.** The scheduler and triggerer install `opentelemetry-instrumentation-requests` at startup (`_PIP_ADDITIONAL_REQUIREMENTS`, pinned to the image's OTel SDK), and the operator plugin turns it on. Each ARM call (start the job, poll its execution) and managed-identity token fetch becomes a client span under the task's `worker.*` / `trigger.*` span, with resources like `POST Microsoft.App/jobs/{job}/start`. For production, bake the package into a custom image instead.
- **ACA Job runs nested under the task that started them.** `OPENLINEAGE_PARENT_ID` and `OPENLINEAGE_ROOT_PARENT_ID` use the same format dbt-ol reads. A job that sends its own OpenLineage START and COMPLETE events with those IDs as its parent shows up in Datadog under that Airflow task, the way Databricks runs do. Its datasets also join the lineage graph. [`airflow/dags/jobs/contoso_store_job.py`](airflow/dags/jobs/contoso_store_job.py) is an example that uses only the standard library. In your own job, the `openlineage-python` client with `OPENLINEAGE__TRANSPORT__TYPE=datadog` does the same thing.
- **The job's datasets on the Airflow task too.** Datadog files the job's runs under a parent job of their own, named after the task, rather than under the Airflow task, so the job's datasets don't reach the DAG's node in the lineage graph. Name them on the operator with `openlineage_inputs` and `openlineage_outputs` (lists of `{"namespace": ..., "name": ...}`) and the task's OpenLineage events carry them as well. `contoso_sales` does this, so each store dataset has two producers in the graph: the task and the job run it started.

---

## Demo DAGs

| DAG | Shows |
|---|---|
| `contoso_sales` | The main demo. Fans out one ACA Job run per store, gates on every store reporting, then rolls up a company total. Each job run reports its own lineage, so DJM shows the store runs nested under their tasks and lineage from each store's sales dataset into the total. The tasks report the same datasets, so the DAG's node in the lineage graph connects to them. Trigger with `{"chaos": true}` to make one store fail once and recover on retry. |
| `contoso_orders` | Optional, with a one-time `contoso_dw_bootstrap` DAG that creates its tables. A Postgres medallion (`raw` → `staging` → `marts`) built with SQL tasks, so OpenLineage reports table-level lineage and Data Observability monitors the same tables. With the optional order stream, it loads orders that a storefront sent through Event Hubs, so lineage starts at the Kafka topic. See [the data pipeline demo](docs/data-pipeline-demo.md), which also has the Contoso team's story and five demo scenarios. |

`contoso_sales` runs every 5 minutes by default (`DEMO_DAG_SCHEDULE`), so Datadog has data shortly after the deploy. `contoso_orders` runs every 15 minutes, and exists only when the data pipeline is turned on (`ENABLE_DATA_PIPELINE=true`).

The [persona site](https://kyletaylored.com/airflow-aca-datadog/) teaches the same pipeline by role: one module for each of the five people who run it, with the problem in their part and the Datadog tools that find the cause. Its source is in [`site/`](site/).

---

## Deployment

You need the [Azure Developer CLI](https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd), the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli), an Azure subscription, and a Datadog API key.

```bash
azd env new airflow-datadog

azd env set POSTGRES_ADMIN_PASSWORD "Pg1-$(openssl rand -hex 16)"
azd env set AIRFLOW_ADMIN_PASSWORD  "$(openssl rand -base64 18)"
azd env set AIRFLOW_SECRET_KEY      "$(openssl rand -hex 32)"

azd env set DD_API_KEY "<your-datadog-api-key>"
azd env set DD_SITE    "datadoghq.com"     # your Datadog site
azd env set DD_ENV     "airflow-aca"       # env tag and OpenLineage namespace

azd up
```

`azd up` provisions everything, then its postprovision hook uploads `airflow/dags` and `airflow/plugins` to the Azure Files share the apps mount. After editing DAGs or plugins, run `azd hooks run postprovision` again; Airflow picks them up within a minute.

The deploy assigns the Airflow managed identity a role on the resource group, which needs Owner or User Access Administrator. With only Contributor, set `azd env set ASSIGN_JOBS_ROLE false` and have an admin run the printed `grantRoleCommand` (details in the [source repo](https://github.com/hetvip2/airflow-hosted-on-aca#permissions--role-assignment)).

| azd env variable | Default | Purpose |
| --- | --- | --- |
| `DD_API_KEY` | _(empty)_ | Datadog API key. Setting it turns on everything above |
| `DD_SITE` | `datadoghq.com` | Datadog site (`us3.datadoghq.com`, `datadoghq.eu`, `ap1.datadoghq.com`, …) |
| `DD_ENV` | `airflow-aca` | Datadog `env` tag and the OpenLineage namespace |
| `TELEMETRY_SIGNALS` | `traces,metrics,logs` | This repo's setting (not a Datadog or OTel variable): which OTel signals Airflow emits and the agent forwards |
| `DEMO_DAG_SCHEDULE` | `*/5 * * * *` | Schedule for `contoso_sales` (empty string for manual-only) |
| `RESOURCE_TAGS` | _(empty)_ | Extra tags on every resource, e.g. `team=data,owner=me` |
| `ASSIGN_JOBS_ROLE` | `true` | `false` skips the managed identity's role assignment |
| `ENABLE_DATA_PIPELINE`, `DD_POSTGRES_PASSWORD` | `false`, _(empty)_ | The optional [data pipeline demo](docs/data-pipeline-demo.md) and the password for its Agent's Postgres role |
| `ENABLE_ORDER_STREAM` | `false` | The pipeline's optional order stream: a storefront, Event Hubs and an order processor (needs `ENABLE_DATA_PIPELINE`) |
| `ENABLE_METABASE`, `METABASE_DB_PASSWORD` | `false`, _(empty)_ | The pipeline's optional Metabase, whose dashboards appear in Datadog lineage below the marts, and the password for its Postgres role (needs `ENABLE_DATA_PIPELINE`) |
| `POSTGRES_MAX_CONNECTIONS` | _(empty)_ | Postgres `max_connections`; empty leaves the server's value. Metabase needs `Standard_B2s` and `200` |
| `AZURE_LOCATION`, `NAME_PREFIX`, `POSTGRES_*`, `AIRFLOW_ADMIN_*`, `AIRFLOW_SECRET_KEY` | | Airflow deployment settings; see [`infra/main.parameters.json`](infra/main.parameters.json) and the [source repo](https://github.com/hetvip2/airflow-hosted-on-aca#configurable-parameters) |

### Continuous deployment

[`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) can deploy every merge to `main`: infra changes run `azd provision`, and DAG or plugin changes only upload the files. It runs the CI checks first, signs in to Azure with OpenID Connect, and is off until you turn it on. Setup is in [docs/continuous-deployment.md](docs/continuous-deployment.md).

---

## Known issues

- **Logs from the managed agent have no service unless they carry `dd.service`.** Datadog keeps the `service.name` resource attribute only as a nested attribute, so the log's service stays empty and `service:` searches miss it. The logging plugin sets `dd.service` from `OTEL_SERVICE_NAME`. For logs from other OTel SDKs, set the same attribute, or add a service remapper on `service.name` to a Datadog log pipeline.
- **Setting `OTEL_EXPORTER_OTLP_PROTOCOL` stopped all export.** In this demo, setting just `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` on the Airflow apps left them without an endpoint, and nothing was exported until it was removed. Microsoft's docs say a variable you set only overrides the injected one; we haven't tested the other variables.
- **`env:none` on spans.** The managed agent maps Datadog `env` only from `deployment.environment`, not from `deployment.environment.name` alone. Set both.
- **OpenLineage's own HTTP calls.** With `requests` instrumentation on, the OpenLineage transport's POSTs to `data-obs-intake.*` would each become a stray single-span trace (its listener runs outside the task span), so the plugin excludes that host.
- **ARM `403 The request is blocked`.** Consumption ACA environments share outbound IPs, and ARM's edge once blocked the one the scheduler used. A new revision moved it to another IP. If you need a stable outbound IP, put the environment on a VNet with a NAT gateway.
- **Manually triggered Airflow 3 runs have no logical date**, so `ds` is missing from the task context. The demo DAG falls back to the run's `run_after` date.

---

## Repository layout

```
├─ airflow/
│  ├─ dags/                 # the demo DAGs
│  │  ├─ jobs/              # code the contoso_sales ACA Jobs run (reports OpenLineage)
│  │  └─ contoso_dw/        # SQL and loader for the optional contoso_orders pipeline
│  └─ plugins/
│     ├─ operators/         # ACA Jobs operator, ARM helpers, requests instrumentation
│     ├─ triggers/          # async trigger for deferrable execution
│     └─ otel_logging_config.py  # OTLP log handler for Airflow's logging
├─ infra/                   # main.bicep: ACA environment + managed OTel agent, Airflow apps,
│                           #   sample job, and the optional data pipeline
├─ docs/                    # the optional data pipeline demo, continuous deployment
├─ services/                # storefront and order processor for its order stream
├─ metabase/                # the pipeline's optional Metabase dashboards
├─ site/                    # persona site (Astro + DRUIDS), published to GitHub Pages
├─ scripts/                 # postprovision upload to the share, Metabase seed
├─ .github/workflows/       # CI, continuous deployment, the site
├─ tests/                   # offline unit tests (no Azure, no Datadog)
└─ entity.datadog.yaml      # Software Catalog definitions: each service linked to its code
```

The `Dockerfile` and `docker-compose.yml` local stack come from the source repo and still run Airflow 2.10 without Datadog.

## Testing

```bash
pip install -r airflow/requirements.txt pytest openlineage-sql==1.55.0
pytest
```

`openlineage-sql` is the parser Airflow's OpenLineage provider uses; without it the `contoso_dw` lineage tests skip. CI runs the same tests, lints with `ruff`, and checks that the Bicep compiles.

## License and attribution

Apache-2.0, see [LICENSE](LICENSE). This project is derived from [hetvip2/airflow-hosted-on-aca](https://github.com/hetvip2/airflow-hosted-on-aca) (Copyright (c) Microsoft Corporation), which provides the Airflow-on-ACA template, operator, and trigger.
