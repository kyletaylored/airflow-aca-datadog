// What each Datadog product needs in this demo, as short as it can be. Every
// setting here is what infra/main.bicep and airflow/dags/contoso_dw/ deploy;
// change them together. Values in angle brackets are the reader's own.

import type { ToolId } from "./tools";

export interface Snippet {
  label: string;
  code: string;
  // The same setting made in the Azure portal, as clicks. Names in backticks are set as code.
  portal?: string[];
  // The code is the values to enter, so the portal view shows it below the clicks.
  values?: boolean;
}

export interface ConfigStep {
  id: string;
  name: string;
  // The product's badge, when it has one on the tools page.
  tool?: ToolId;
  gives: string;
  where: string;
  snippets: Snippet[];
  // Steps that are clicks rather than settings.
  steps?: string[];
  note?: string;
  docs: string;
  // Where this repo sets it up.
  source: { label: string; path: string };
}

export interface ConfigGroup {
  title: string;
  intro: string;
  steps: ConfigStep[];
}

const REPO = "https://github.com/kyletaylored/airflow-aca-datadog/blob/main/";

export const configGroups: ConfigGroup[] = [
  {
    title: "Airflow on Azure Container Apps",
    intro:
      "Configuration only: no code changes, no custom image and no Datadog Agent. Start with Data Jobs Monitoring; it already gives you a trace per DAG run.",
    steps: [
      {
        id: "djm",
        name: "Data Jobs Monitoring",
        tool: "djm",
        gives:
          "Every DAG run and task run, with status, duration, retries, errors and dataset lineage, plus a trace per DAG run.",
        where:
          "Four environment variables on the scheduler, and on any workers. With LocalExecutor, tasks run in the scheduler, so the scheduler alone is enough.",
        snippets: [
          {
            label: "Scheduler environment",
            values: true,
            code: `OPENLINEAGE__TRANSPORT__TYPE=datadog
DD_API_KEY=<datadog-api-key>     # from a secret
DD_SITE=<datadog-site>           # datadoghq.com, us3.datadoghq.com, …
OPENLINEAGE_NAMESPACE=<env>      # becomes the env tag`,
            portal: [
              "Open the scheduler's container app. Under **Settings → Secrets**, add a secret named `dd-api-key` that holds your Datadog API key.",
              "Select **App → Containers**. If the app has more than one container, pick the scheduler in **Container**, then open the **Environment variables** tab.",
              "Select **Add** for each variable below. `OPENLINEAGE__TRANSPORT__TYPE`, `DD_SITE` and `OPENLINEAGE_NAMESPACE` use the source **Manual entry**; `DD_API_KEY` uses **Reference a secret**, pointing at `dd-api-key`.",
              "Save. Container Apps deploys the change as a new revision, so you don't create one yourself.",
            ],
          },
        ],
        note: "The Airflow 3 image already includes the OpenLineage provider. On Airflow 2, install apache-airflow-providers-openlineage 2.7.3 or later.",
        docs: "https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/",
        source: { label: "datadogEnvVars in infra/main.bicep", path: "infra/main.bicep" },
      },
      {
        id: "apm",
        name: "APM",
        tool: "apm",
        gives:
          "One service per Airflow component (api-server, scheduler, triggerer, dag-processor), with traces of the scheduler, the task process and the triggerer, Airflow's metrics, and its logs linked to those traces.",
        where:
          "The Container Apps environment's managed OpenTelemetry agent, plus a few variables on each Airflow app.",
        snippets: [
          {
            label: "Give the environment a Datadog destination",
            code: `az containerapp env telemetry data-dog set \\
  -g <resource-group> -n <environment> \\
  --site <datadog-site> --key <datadog-api-key> \\
  --enable-open-telemetry-traces true \\
  --enable-open-telemetry-metrics true
# the CLI has no logs flag: select Logs in the portal, or set
# logsConfiguration in Bicep`,
            portal: [
              "Open the Container Apps environment and select **Monitoring → OTel endpoints**.",
              "On the **Datadog** row, select **Update**. Set **Site** to your Datadog site and paste your API key into **Key**.",
              "Select **Logs**, **Traces** and **Metrics**, then save.",
              "The row's status changes to **Configured**. To change the settings later, leave **Key** blank to keep the existing key.",
            ],
          },
          {
            label: "Each Airflow app, with its own service name",
            values: true,
            code: `AIRFLOW__TRACES__OTEL_ON=True
AIRFLOW__METRICS__OTEL_ON=True
OTEL_SERVICE_NAME=airflow-scheduler      # per app
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>
# logs, through this repo's logging plugin:
OTEL_LOGS_EXPORTER=otlp
AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=otel_logging_config.LOGGING_CONFIG`,
            portal: [
              "Open each Airflow container app in turn: api-server, scheduler, triggerer and dag-processor.",
              "Select **App → Containers**, then open the **Environment variables** tab.",
              "Select **Add** for each variable below, as **Manual entry**, with that app's own `OTEL_SERVICE_NAME`.",
              "Save. Container Apps deploys the change as a new revision.",
            ],
          },
        ],
        note: "Don't set any OTEL_EXPORTER_OTLP_* variable: Container Apps injects the agent's endpoint, and in this demo setting just OTEL_EXPORTER_OTLP_PROTOCOL stopped all export. Airflow 3 can't send its logs over OTLP on its own; the logging plugin in airflow/plugins/otel_logging_config.py does it.",
        docs: "https://docs.datadoghq.com/serverless/azure_container_apps/otel_managed_agent/",
        source: { label: "otelServiceEnv() in infra/main.bicep", path: "infra/main.bicep" },
      },
    ],
  },
  {
    title: "The warehouse and the order stream",
    intro:
      "These need a Datadog Agent. The demo runs one as a single-replica container app that both the Postgres check and the stream services use.",
    steps: [
      {
        id: "dataObservability",
        name: "Data Observability",
        tool: "dataObservability",
        gives:
          "A catalog of the warehouse tables, and data quality monitors on them: freshness, row counts, nulls and custom SQL.",
        where:
          "The Agent's Postgres check (Agent 7.81 or later), and a read-only datadog role in the database.",
        snippets: [
          {
            label: "conf.d/postgres.d/conf.yaml",
            code: `instances:
  - host: <postgres-host>      # the FQDN Airflow's connection uses
    port: 5432
    username: datadog
    password: <password>
    dbname: <database>
    ssl: require
    collect_schemas:
      enabled: true
    relations:
      - relation_regex: '.*'
    data_observability:
      enabled: true`,
          },
          {
            label: "Agent environment",
            code: `DD_API_KEY=<datadog-api-key>
DD_SITE=<datadog-site>
DD_REMOTE_CONFIGURATION_ENABLED=true   # how the monitors' queries arrive`,
          },
          {
            label: "In the database, as an admin, for each schema",
            code: `CREATE ROLE datadog LOGIN PASSWORD '<password>';
GRANT pg_monitor TO datadog;
GRANT SELECT ON pg_stat_database TO datadog;
GRANT USAGE ON SCHEMA <schema> TO datadog;
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO datadog;
ALTER DEFAULT PRIVILEGES IN SCHEMA <schema>
  GRANT SELECT ON TABLES TO datadog;`,
          },
        ],
        note: "Run one Agent replica, so each monitor runs once. Using the same host as Airflow's connection makes lineage and the catalog name the same tables.",
        docs: "https://docs.datadoghq.com/data_observability/quality_monitoring/transactional_databases/postgres/",
        source: { label: "ddAgent in infra/main.bicep", path: "infra/main.bicep" },
      },
      {
        id: "dbm",
        name: "Database Monitoring",
        tool: "dbm",
        gives: "Query metrics, samples and explain plans for every database on the server, plus each table's schema.",
        where: "The same Postgres check, a few server settings, and a datadog schema in each database.",
        snippets: [
          {
            label: "Add to the same check instance",
            code: `    dbm: true
    database_autodiscovery:
      enabled: true
    azure:
      deployment_type: flexible_server
      fully_qualified_domain_name: <postgres-host>`,
          },
          {
            label: "Flexible Server parameters",
            values: true,
            code: `azure.extensions                  = PG_STAT_STATEMENTS
pg_stat_statements.track          = ALL
pg_stat_statements.max            = 10000
pg_stat_statements.track_utility  = off
track_activity_query_size         = 4096
track_io_timing                   = on`,
            portal: [
              "Open the Azure Database for PostgreSQL flexible server and select **Settings → Server parameters**.",
              "Search for each parameter below and set its value. For `azure.extensions`, select `PG_STAT_STATEMENTS` in the list.",
              "Select **Save**. When the portal asks, restart the server, or restart it later from **Overview → Restart**.",
            ],
          },
          {
            label: "In each database",
            code: `CREATE SCHEMA datadog;
GRANT USAGE ON SCHEMA datadog TO datadog;
GRANT USAGE ON SCHEMA public TO datadog;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- plus datadog.explain_statement, from Datadog's guide`,
          },
        ],
        note: "pg_stat_statements.max and track_activity_query_size apply only after the server restarts.",
        docs: "https://docs.datadoghq.com/database_monitoring/setup_postgres/azure/",
        source: { label: "contoso_dw/sql.py", path: "airflow/dags/contoso_dw/sql.py" },
      },
      {
        id: "dsm",
        name: "Data Streams Monitoring",
        tool: "dsm",
        gives:
          "The pathway from producer to topic to consumer, with throughput, lag and end-to-end latency, plus APM traces of both services.",
        where: "dd-trace in the producer and the consumer, sending to the Agent. The Kafka client needs no code changes.",
        snippets: [
          {
            label: "Run each service under dd-trace",
            code: `pip install ddtrace
ddtrace-run python storefront.py`,
          },
          {
            label: "Each service's environment",
            code: `DD_SERVICE=contoso-storefront           # per service
DD_ENV=<env>
DD_DATA_STREAMS_ENABLED=true
DD_TRACE_REMOVE_INTEGRATION_SERVICE_NAMES_ENABLED=true
DD_TRACE_AGENT_URL=http://<agent-app>     # internal ingress on 8126`,
          },
          {
            label: "Agent environment",
            code: `DD_APM_ENABLED=true
DD_APM_NON_LOCAL_TRAFFIC=true`,
          },
        ],
        docs: "https://docs.datadoghq.com/data_streams/setup/language/python/",
        source: { label: "streamServiceEnv() in infra/main.bicep", path: "infra/main.bicep" },
      },
    ],
  },
  {
    title: "Optional",
    intro: "Each of these adds context to what the steps above collect.",
    steps: [
      {
        id: "metabase",
        name: "Metabase lineage",
        gives: "Lineage that continues past the marts to the Metabase questions and dashboards that read them.",
        where: "Datadog's Metabase integration. Nothing to install; it needs Data Observability's warehouse connection.",
        snippets: [],
        steps: [
          "In Metabase, create an API key in the Administrators group (Admin settings → Authentication → API keys).",
          "In Datadog, open the Metabase integration tile and add the Metabase URL and that key.",
        ],
        docs: "https://docs.datadoghq.com/data_observability/quality_monitoring/business_intelligence/metabase/",
        source: { label: "docs/data-pipeline-demo.md", path: "docs/data-pipeline-demo.md" },
      },
      {
        id: "sourceCode",
        name: "Source Code Integration",
        gives: "Each service linked to its repository and deployed commit, so Datadog shows the code behind a span or stack trace.",
        where: "Resource attributes on the OpenTelemetry services and two variables on the dd-trace services. Needs Datadog's GitHub integration.",
        snippets: [
          {
            label: "Airflow apps: append to OTEL_RESOURCE_ATTRIBUTES",
            code: `vcs.repository.url.full=github.com/<owner>/<repo>,
vcs.ref.head.revision=<commit-sha>,
git.repository_url=github.com/<owner>/<repo>,
git.commit.sha=<commit-sha>`,
          },
          {
            label: "dd-trace services",
            code: `DD_GIT_REPOSITORY_URL=github.com/<owner>/<repo>
DD_GIT_COMMIT_SHA=<commit-sha>`,
          },
        ],
        note: "Through the managed agent, Datadog reads only the git.* names, so set both pairs.",
        docs: "https://docs.datadoghq.com/source_code/",
        source: { label: "otelGitAttributes in infra/main.bicep", path: "infra/main.bicep" },
      },
    ],
  },
];

export const sourceUrl = (path: string) => REPO + path;

export const notNeeded = [
  "No install on Airflow 3: the official image includes the OpenLineage provider and the OpenTelemetry SDK.",
  "No DAG or operator changes: both Airflow integrations work at the Airflow level.",
  "No Datadog Agent for Airflow: Data Jobs Monitoring posts straight to Datadog, and APM uses Container Apps' managed agent. The Agent is only for the warehouse and the order stream.",
];
