# Plan: an Azure-only data pipeline for Data Observability lineage and quality

Goal: a simplified, all-Azure version of the Shopist lineage demo. Orders flow from a
producer, through a queue and a landing zone, into a Postgres medallion
(`raw` → `staging` → `marts`) built by an Airflow DAG. Datadog shows the following:

- **Lineage** from the producer to the marts, with the Airflow tasks between them.
- **Quality monitors** on the Postgres tables, with "upstream issue" badges when
  something breaks.
- **Jobs Monitoring** for the DAG, plus APM / DSM for the services.

Phases 1 and 2 have no BI layer. Phase 3 adds a self-hosted Metabase, plus a story
for the demo: who runs each part of the pipeline, and how Datadog helps each of them.

**Scope.** This pipeline supports the repo's main topic; it doesn't replace it. The
README and guide stay about instrumenting Airflow on ACA with OpenLineage (DJM) and
OpenTelemetry (APM). The pipeline gives those signals a realistic workload: real
tables for OpenLineage to report, and a real upstream for the trace map. So:

- Its docs go in a separate page (`docs/data-pipeline-demo.md`). The README gets a
  short pointer, plus one line in "Demo DAGs".
- Its infra sits behind an `enableDataPipeline` parameter that defaults to `false`.
  The core template deploys exactly as today, and the demo environment turns it on.
- Nothing in it changes how Airflow itself is instrumented. Airflow stays on the
  managed OTel agent and the native `datadog` OpenLineage transport.

```
contoso-storefront ──► Event Hubs "orders" ──► order-processor ──► Blob "landing/orders/…"
  (Container App,        (Kafka endpoint)       (Container App)            │
   dd-trace + DSM)                                                         ▼
                                     Airflow DAG contoso_orders (scheduler task process)
                                       load_raw ─► staging_* ─► quality gate ─► marts_*
                                                                           │
                                     Postgres Flexible Server, database contoso_dw
                                       raw.orders, raw.customers
                                       staging.orders, staging.customers
                                       marts.daily_store_sales, marts.customer_value,
                                       marts.hourly_order_volume
                                                                           ▲
                                     aflo-dd-agent (Container App, Datadog Agent ≥ 7.81)
                                       Postgres check with data_observability: enabled
                                       Remote Configuration → runs the quality monitors
                                       APM/DSM intake for the two services
```

## Status of the prerequisites

| Prerequisite | Status |
|---|---|
| Data Observability for PostgreSQL (Preview) | Enabled on the org, per Kyle |
| Storage Management for Azure Blob (Preview) | Enabled on the org, per Kyle. It needs the Datadog Azure integration to use an **App Registration**, not the Azure Marketplace (Datadog ISV) integration. The subscription has both: a Marketplace resource (`ese-tola-azure-isv`) and app registrations such as `datadog-azure-ese-tola-sandbox`. Check which one the US3 org's Azure tile uses before phase 2. Storage Management is a nice-to-have, so it doesn't block anything. |
| Remote Configuration | Must be on in three places: for the org, on the API key the Agent uses (`DD_API_KEY` in the `azure` environment), and on the Agent. It's the Agent default from 7.47. |
| Postgres superuser | The Flexible Server admin (`airflowadmin`, in `azure_pg_admin`) can create roles and grant `pg_monitor`. |

## Design decisions

1. **Same server, new database.** Add `contoso_dw` to the existing Flexible Server
   (Postgres 16, B1ms) as a Bicep `flexibleServers/databases` resource. Airflow's
   `airflow` database stays separate, and the `datadog` role never gets `SELECT` on it.
   B1ms handles demo volume: a few thousand rows an hour, about 50 connections.
2. **One Datadog Agent, as a Container App.** Use `datadog/agent:7.81+`, with
   `minReplicas = maxReplicas = 1`. Only one Agent may own the Postgres check, or the
   monitors run twice. It gets internal HTTP ingress on target port 8126, so dd-trace
   services reach it at `http://aflo-dd-agent`. A Consumption environment without a
   VNet has no TCP ingress, but trace and DSM payloads are HTTP. Settings:
   `DD_HOSTNAME=aflo-dd-agent`, `DD_APM_NON_LOCAL_TRAFFIC=true`, `DD_ENV`, `DD_SITE`,
   and `DD_API_KEY` as a secret. Airflow stays on the managed OTel agent; this
   Agent is only for the Postgres check and the dd-trace services. The two phase 2
   services use dd-trace because Data Streams Monitoring needs a Datadog SDK;
   OpenTelemetry can't produce DSM pathways. The docs should say so, since it's the
   one place the demo steps outside OTel.
3. **Lineage names must match.** Datadog's OpenLineage convention for Postgres is
   namespace `postgres://{host}:{port}` and name `{database}.{schema}.{table}`. The
   Airflow connection and the Agent's `host:` both use the server FQDN
   (`aflo-pg-….postgres.database.azure.com`) and port 5432. Then the DAG's datasets
   and the catalog's tables are the same lineage nodes.
4. **SQL steps use `SQLExecuteQueryOperator`.** Its OpenLineage SQL parser reports
   input and output tables automatically. Only the Python load task needs
   hand-written inlets and outlets. The `apache/airflow:3.4.0` image already ships the
   Postgres provider, so there's nothing to install.
5. **No container registry, as today.** The two services run on `python:3.12-slim`.
   They pip-install `ddtrace confluent-kafka azure-storage-blob` at startup and run
   code from the Azure Files share, the same pattern as the ACA Job. (Production
   would build an image.)
6. **Secrets.** One new secret, `DD_POSTGRES_PASSWORD`, for the `datadog` role. It goes
   in the azd env, the GitHub `azure` environment, and the Agent's and Airflow's
   Container Apps secrets. The Event Hubs and storage connection strings come from
   Bicep (`listKeys`), so they are never set by hand. The deploy pipeline can't
   assign roles, so everything uses keys rather than managed-identity role
   assignments.

## Phase 1: Postgres medallion, Agent and quality monitors

The DAG generates its own orders in this phase, so phase 1 stands alone.

### 1a. Spikes (do first, about half a day)

- **S1, the lineage join:** one `SQLExecuteQueryOperator` writes
  `contoso_dw.raw.orders`. Confirm that the DJM dataset node and the Postgres
  catalog table resolve to the same entity.
- **S2, the Agent on ACA:**
  - The Postgres check reports.
  - `datadog-agent status` shows Remote Configuration `API Key: Authorized`.
  - A test dd-trace span reaches APM through `http://aflo-dd-agent`.
  - **Check config:** decide how the Agent gets its config without a plaintext
    password on the share. Option (a): a startup wrapper writes
    `conf.d/postgres.d/conf.yaml` from env vars, then execs the Agent's entrypoint.
    Option (b): the Agent's built-in secret backend. Pick whichever works in S2.

### 1b. Infra (`infra/main.bicep`)

- **The `contoso_dw` database.**
- **The `aflo-dd-agent` Container App:**
  - 0.5 vCPU, 1 GiB
  - internal ingress on 8126
  - secrets `dd-api-key` and `dd-postgres-password`
  - env as in decision 2
  - the Postgres check config, using the approach S2 picked
- **`AIRFLOW_CONN_CONTOSO_DW` on the Airflow apps:**
  `postgresql://<admin>:<pw>@<fqdn>:5432/contoso_dw?sslmode=require`, built from the
  existing Postgres secret. Add `DD_POSTGRES_PASSWORD` for the bootstrap DAG.
- **Parameters:** `datadogPostgresPassword` (secure) and `enableDataPipeline`
  (default `false`; azd env `ENABLE_DATA_PIPELINE`, set to `true` for the demo
  environment). Everything in 1b and phase 2 is conditional on it.

### 1c. DAGs (`airflow/dags/`)

**`contoso_dw_bootstrap`:** manual and idempotent. It uses the admin connection to:

- create the `raw`, `staging` and `marts` schemas;
- create the `datadog` role with `pg_monitor`, `GRANT SELECT ON pg_stat_database`,
  `USAGE` and `SELECT` on the three schemas, and `ALTER DEFAULT PRIVILEGES` for the
  table-owning role.

It runs once after the first deploy. It can also run as a pre-step of
`contoso_orders`.

**`contoso_orders`:** every 15 minutes (`DEMO_DAG_SCHEDULE`-style variable).

| Task | Phase 1 | Lineage |
|---|---|---|
| `ingest_orders` | Generates 200 to 400 orders for the interval across the 7 Contoso stores (Seattle … New York) and inserts them into `raw.orders`. Upserts new customers into `raw.customers`. | Python task with hand-set outlets |
| `staging_orders` | Deduplicates, casts types, drops test orders, and computes `amount_usd` | SQL, automatic |
| `staging_customers` | Normalizes email and region | SQL, automatic |
| `check_staging` | `SQLCheckOperator`: `staging.orders` not empty, no negative amounts | SQL, automatic |
| `mart_daily_store_sales` | Date × store: orders, revenue | SQL, automatic |
| `mart_customer_value` | Lifetime orders and revenue per customer | SQL, automatic |
| `mart_hourly_order_volume` | Orders per hour (a nice row-count signal) | SQL, automatic |

**Chaos.** Use DAG params, with defaults off:

| Param | Effect | Monitor it trips |
|---|---|---|
| `null_customers` | 30% of orders have a `NULL` `customer_id` | Null ratio on `staging.orders.customer_id` |
| `volume_drop` | Only 5% of normal volume | Row count on `raw.orders` |
| `schema_drift` | Adds or renames a column in `raw.orders` | Schema change on the table |
| `skip_load` | No new rows | Freshness, as custom SQL on `marts.daily_store_sales` |

### 1d. Monitors (Data Observability → table → Monitors)

| Table | Monitor | ID (2026-11-14) |
|---|---|---|
| `raw.orders` | Row count, anomaly detection | 29737308: never recorded a value |
| `staging.orders` | Null ratio on `customer_id`, anomaly detection | 29737310 |
| `staging.orders` | Custom SQL freshness on `max(ordered_at)`, alert above 45 minutes | 29737311 |
| `marts.daily_store_sales` | Custom SQL freshness: `SELECT EXTRACT(EPOCH FROM now() - max(updated_at))/60 AS dd_value`, alert above 45 minutes | 29744949 |
| `marts.daily_store_sales` | Custom SQL: revenue must be ≥ 0 | 29737312 |

All of them are drafts, so none notifies. Only 29737310, 29737311 and 29737312 have
recorded a value, once each, around 05:58 on 2026-11-14; no monitor has run a second
time. The Agent is healthy (Postgres check OK, Remote Configuration authorized), so
this is waiting on the Data Observability team. Changing the model type, the query,
the schedule (`crontab_override`), creating fresh monitors and restarting the Agent
all made no difference.

Create them in the UI first. If the monitors API supports this monitor type, script
them later. Allow about an hour after the Agent starts for the first metadata sync.

### 1e. Docs and tests

- **`docs/data-pipeline-demo.md`:** the medallion, the Agent, the monitors, the
  chaos switches, and how to turn the pipeline on.
- **README:** a short pointer to that page, and one line for `contoso_orders` in
  "Demo DAGs". The README's diagrams and the minimum-setup sections stay as they are.
- **Unit tests:**
  - the order generator, which is deterministic with a seed;
  - the chaos switches;
  - the hand-set lineage objects of the load task;
  - the SQL files, which must parse.

**Phase 1 is done when:**

- the `contoso_dw` tables are in the catalog;
- lineage runs `raw` → `staging` → `marts`, with the `contoso_orders` tasks between
  them;
- the four monitors have data;
- each chaos switch trips its monitor, and the downstream marts show as affected.

## Phase 2: Streaming front and blob landing

**Queue choice.** For open-source languages, Data Streams Monitoring supports Kafka and
RabbitMQ. It supports Azure Service Bus only from .NET. The options:

- **Event Hubs' Kafka endpoint, with Python services.** This is the choice. It's
  managed, and Kafka is the closest match to the Shopist demo.
- **RabbitMQ in ACA:** would need TCP between apps. A Consumption environment without
  a custom VNet has no TCP ingress, so this is out.
- **Service Bus with .NET services:** works, but it would add a second language to
  the repo for one hop.

### 2a. Spikes

- **S3, blob visibility:** Datadog documents Blob Storage dataset resolution for Azure
  Data Factory, but not for custom OpenLineage events. Send one test event with
  output `wasbs://landing@<account>.blob.core.windows.net` (name
  `orders/dt=…`), and check whether it shows as a catalog or lineage node. Also check
  whether Storage Management's view of the account links to it.
  **If it doesn't show up:** the processor writes straight to `raw.orders`, and the
  blob hop is dropped.
- **S4, DSM over Event Hubs:** confirm that dd-trace-py's Kafka integration
  (confluent-kafka, `DD_DATA_STREAMS_ENABLED=true`) shows the
  `contoso-storefront → orders → order-processor` pathway against Event Hubs' Kafka
  endpoint.

### 2b. Infra

- **Event Hubs namespace:** Standard tier, 1 throughput unit (Basic has no Kafka
  endpoint). It has an event hub named `orders` (2 partitions, 1-day retention), a
  consumer group `order-processor`, and two policies: send-only for the storefront
  and listen-only for the processor.
- **A `landing` blob container** in the existing storage account (StorageV2, flat
  namespace).
- **The `contoso-storefront` Container App:** 0.25 vCPU, no ingress.
  - It produces orders at `ORDERS_PER_MINUTE`, with the same chaos knobs exposed as env
    vars.
  - Settings: `DD_SERVICE=contoso-storefront`, `DD_DATA_STREAMS_ENABLED=true`,
    `DD_TRACE_AGENT_URL=http://aflo-dd-agent`.
  - Kafka: `SASL_SSL` / `PLAIN` to `<ns>.servicebus.windows.net:9093`, with user
    `$ConnectionString`.
- **The `order-processor` Container App:** 0.25 vCPU.
  - It consumes, validates, and every 60 seconds writes an NDJSON batch to
    `landing/orders/dt=YYYY-MM-DD/hour=HH/batch-<ts>.json`.
  - It sends an OpenLineage event per batch: input is Kafka `orders`, output is the
    blob prefix. These sit next to its DSM spans.
- **Storage Management:** run the script that Datadog's Enable buckets page generates
  for the storage account, from Cloud Shell. It creates the inventory policy, the
  `datadog-storage-monitoring` container, and a Storage Blob Data Reader role for
  Datadog's App Registration. This is a role assignment, so you run it, not the
  pipeline. Data arrives 24 to 48 hours later.

### 2c. DAG changes

- **`ingest_orders` loads blobs instead of generating orders.** It lists blobs under
  `landing/orders/` that aren't in `raw._ingested_files`, `COPY`s them into
  `raw.orders` with `_source_file` and `_loaded_at`, and records them. Inlets are the
  blob prefix; the outlet is `contoso_dw.raw.orders`.
- **A `source` param:** `stream` (default once phase 2 ships) or `generate`, so the
  phase 1 path keeps working without the streaming apps.

**Phase 2 is done when** lineage runs storefront → `orders` topic → order-processor →
landing blob (or straight to `raw.orders` if S3 fails) → `contoso_orders` → marts.
The DSM view should also show producer and consumer latency.

## Status, 2026-11-14

Phases 1 and 2 shipped in PRs #7 and #8, and Deploy now runs CI first (PR #9).
What's still open against the "done when" lists:

- **Monitors:** see 1d. The row-count monitor on `raw.orders` has never run, and no
  monitor has run twice, so no chaos run has tripped a monitor yet. The chaos effects
  themselves are verified in the tables.
- **Affected marts:** not checked yet, for the same reason.
- **Lineage gaps:** both are on Datadog's side; our events are correct.
  - `ingest_customers` reports `contoso_dw.raw.customers` as its output, but Datadog
    draws no edge for it. It's the only task with outputs and no inputs.
  - The `orders` topic exists twice: once from OpenLineage and once from DSM. The
    lineage map shows the OpenLineage one, so its producer and consumer panel is
    empty. DSM itself shows the storefront → order-processor pathway.
- **The storefront** isn't a lineage node, because it doesn't send OpenLineage events.
  Lineage starts at the topic.

## Phase 3: the Contoso story, Metabase, and one monitor of each type

**Status:** built except Metabase (see 3e) and the Job monitor, which the API
wouldn't accept, so it gets created in the UI. The narrative, team, scenarios and
run sheet are in `docs/data-pipeline-demo.md`. Monitors don't record values yet; see
1d.

### 3a. Narrative

> Contoso Outfitters is a global e-commerce retailer of outdoor and travel gear. It
> sells through its website, its mobile app and its own stores. This demo follows its
> North American business: orders from seven stores and about 2,000 customers.
>
> Every order the storefront takes goes onto Azure Event Hubs. An order processor
> lands the orders in batches in Azure Data Lake Storage. Apache Airflow, running on
> Azure Container Apps, loads them every 15 minutes into a Postgres warehouse: `raw`,
> then cleaned `staging` tables, then `marts` tables for reporting. The business reads
> those marts in Metabase dashboards. Datadog watches each step: APM and Data Streams
> Monitoring for the services, Data Jobs Monitoring for Airflow, and Data
> Observability for the tables, their lineage, and the dashboards that depend on them.

The data already matches this: stores in seven US cities, five regions, and web,
mobile and store channels.

### 3b. The team

| Person | Role | Owns |
|---|---|---|
| Dave | Application engineer | The storefront and the order processor, up to the files in the landing zone |
| Rajesh | Data architect | The warehouse design: the schemas and tables in `raw`, `staging` and `marts`, and what each column means |
| Emily | Data engineer | The Airflow pipelines that move data from the landing zone through `raw` and `staging` |
| Priya (new) | Analytics engineer | The `marts` transformations: the SQL that turns clean orders into business metrics |
| John | BI lead | Metabase and its dashboards, and the people who read them |

**Roles we could add later, but don't need now:**
- **Data platform engineer:** owns the infrastructure, meaning Airflow on ACA, the
  Datadog Agent, and Postgres. In this demo, Emily covers it.
- **Data steward:** owns data definitions and access. That's governance, and Datadog
  plays a small part in it.

### 3c. Scenarios

Each scenario is a chaos switch on the `contoso_orders` DAG (a run param), so the
presenter can trigger it on demand. Each has the same arc: the issue starts in one
person's area, Datadog alerts, the owner finds the cause, and the fix is outlined but
not performed.

| # | Who | What breaks | Switch | What Datadog shows |
|---|---|---|---|---|
| 1 | Dave | A storefront release stops sending `customer_id` on some orders. | `null_customers` (exists) | Nullness anomaly on `staging.orders.customer_id` alerts. Lineage traces it upstream through the landing zone to the topic and the storefront. Downstream, `marts.customer_value` shows as affected. |
| 2 | Rajesh | A schema change renames `raw.orders.amount_cents` to `amount_minor`, without updating the staging SQL. | `breaking_schema` (new) | The `staging_orders` task fails, and the Job monitor alerts. Source to Target shows `staging.orders` falling behind `raw.orders`. Column-level lineage shows every downstream column that depends on the renamed one. |
| 3 | Emily | A batch never gets loaded, and later the volume drops. | `skip_load`, `volume_drop` (exist) | Freshness on `staging.orders` and `marts.daily_store_sales` alerts. DJM shows which run skipped the load. |
| 4 | Priya | A refund-handling change makes some orders count as negative revenue. | `refund_bug` (new) | The negative-revenue threshold monitor alerts, and the anomaly monitor on total daily revenue flags the dip. |
| 5 | John | Each upstream issue reaches his reports. | none: a consequence of 1 to 4 | The marts tables his reports read show as affected in lineage, so he can see which numbers are wrong or stale, and whose issue it is. With a Metabase license, his dashboards would appear in lineage below the marts (see 3e). |

**Bits AI in each scenario.** Datadog's Bits AI docs don't describe Data Observability
use yet, so treat these steps as to-verify:
- **Bits Investigation:** run from the alert in scenarios 1 to 4 to find the root
  cause. No Preview label in the docs.
- **Bits Chat:** for John, "which dashboards read `marts.daily_store_sales`?", and for
  Rajesh, "what depends on `raw.orders.amount_cents`?".
- **Bits Code:** in scenarios 2 and 4, propose the SQL fix as a pull request. No
  Preview label.
- **In Preview, request access first:**
  - Bits Remediation, for the next steps after an investigation.
  - Bits Data Analysis, for John to ask questions of the marts without SQL.
  - Bits Detection, to suggest monitors for tables that have none.

### 3d. Monitors: one of each type

| Type | Monitor | Scenario | Notes |
|---|---|---|---|
| Data quality, threshold | Negative revenue on `marts.daily_store_sales` (29737312). Freshness on `staging.orders` (29737311) and `marts.daily_store_sales` (29744949). | 3, 4 | These exist already. |
| Data quality, anomaly | Nullness on `staging.orders.customer_id` (29737310). New: daily revenue sum on `marts.daily_store_sales.revenue_usd`. | 1, 4 | Anomaly detection needs some history before it alerts. |
| Job | Failed runs of the `contoso_orders` DAG | 2 | To create in the UI. The API takes `data-jobs alert` with a `run_query` variable (`job_type: airflow.dag`), but rejected every query string we tried. |
| Source to target | Custom SQL counts, `raw.orders` (non-test) against `staging.orders`, alert below -50 | 2 | Created, 29745880. In Preview. |
| Schema change | Skipped | none | Only Snowflake, BigQuery, Databricks and Redshift are supported, not Postgres. |

All of these depend on the open issue in 1d: monitors need to run more than once.

### 3e. Metabase on ACA

- **App:** `aflo-metabase`, running the `metabase/metabase` image, about 1 vCPU and
  2 GiB, with external ingress on port 3000. Behind a new `enableMetabase` parameter
  that defaults to `false`, like the other optional pieces.
- **Metabase's own database:** a new `metabase` database on the same Postgres server.
- **What it reads:** `contoso_dw`, as a read-only `metabase` role on `marts` only.
- **Content:** a small seed script that creates two dashboards through Metabase's API:
  "Store sales" on `daily_store_sales`, and "Customer value" on `customer_value`.
- **Datadog:** the Metabase integration, which uses the Metabase URL and an API key.
  It adds Metabase logs, and lineage from the marts tables to cards and dashboards.

**Spike result (2026-11-14): on hold until there's a Pro or Enterprise license.**
Open-source Metabase (`metabase/metabase:v0.65.4.4`) ran on ACA, built by hand, not in
Bicep. The Datadog tile accepted the API key: its `GET /api/database` calls returned
200. It then stopped with "Cannot access 'Internal Metabase Database'… ensure you have
Metabase Pro or Enterprise plan." That database is Metabase's Usage analytics source,
which only Pro and Enterprise create, so no Postgres grant can supply it. No Metabase
entities reached the catalog. Self-hosted Pro uses the same image plus a license token
in `MB_PREMIUM_EMBEDDING_TOKEN`.

What the spike needed on Azure Postgres, for whenever this goes into Bicep:
- **`public` schema:** in the `metabase` database it belongs to `azure_pg_admin`, so
  the `metabase` role has to own it, not just the database.
- **`azure.extensions`:** must allow `CITEXT`, and `citext` has to be created in the
  `metabase` database.
- **Connection pools:** Metabase's default pools took 13 of the 35 connections B1ms
  allows ordinary roles. The pools are capped with
  `MB_APPLICATION_DB_MAX_CONNECTION_POOL_SIZE=4` and
  `MB_JDBC_DATA_WAREHOUSE_MAX_CONNECTION_POOL_SIZE=3`, and the server is now
  `Standard_B2s`.
- **`max_connections`:** set by hand to 200. Azure kept the B1ms value of 50 after the
  SKU change.
- **`JAVA_OPTS=-Xmx1536m`:** without it, the JVM saw only about 500 MB of the 2 GiB
  container.

**Until then, John's scenario changes.** He owns the `marts` tables and the reports on
them, and his view is downstream impact on the marts in lineage and monitors. There
are no dashboard nodes.

### 3f. Order of work

1. ~~The Metabase spike~~: done; on hold, see 3e.
2. ~~The two new chaos switches~~: done.
3. The new monitors: revenue anomaly (29745883) and Source to Target (29745880)
   created; the Job monitor is still to create in the UI.
4. Metabase in Bicep and the seed script, once there's a license.
5. ~~The narrative, team, scenarios and run sheet~~: done.

## Rough monthly cost added (centralus, list prices, always-on)

| Item | Approx. |
|---|---|
| `aflo-dd-agent` (0.5 vCPU, 1 GiB) | $25–35 |
| `contoso-storefront` + `order-processor` (0.25 vCPU each) | $20–30 |
| Event Hubs Standard, 1 TU | ~$22 |
| Postgres | No change (same server); a few hundred MB of storage |
| `aflo-metabase` (1 vCPU, 2 GiB), phase 3 | $40–60 |

Scale the two services to 0 replicas between demos to save most of the phase 2
cost.

## Decisions on the earlier open questions

These are defaults, chosen so work can start. Change any of them later.

1. **The Azure integration type** only matters for Storage Management, which is
   optional. Check it in the Datadog Azure tile before phase 2, as described above.
2. **Keep `contoso_sales`.** It's the README's example of ACA Job runs nested under
   Airflow tasks, which is core to the guide. `contoso_orders` is the warehouse
   example. The two DAGs stay independent.
3. **Run every 15 minutes, set by a variable.** It gives the monitors enough points
   within a demo session. Hourly (to match Shopist) is a one-line change.
