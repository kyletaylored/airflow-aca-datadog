// The Datadog products the scenarios use, in the order the site lists them.

export type ToolId =
  | "djm"
  | "dataObservability"
  | "apm"
  | "dsm"
  | "dbm"
  | "infra"
  | "bits";

export interface Tool {
  id: ToolId;
  name: string;
  // What a reader asks when they reach for it.
  question: string;
  summary: string;
  // What this demo sends it, in one line.
  inThisDemo: string;
  docs: string;
}

export const tools: Tool[] = [
  {
    id: "djm",
    name: "Data Jobs Monitoring",
    question: "Did the pipeline run, and which task failed?",
    summary:
      "Every DAG run and task run, with status, duration, retries, the error and its stack trace, and the datasets each task read and wrote.",
    inThisDemo:
      "Airflow's OpenLineage provider sends each run of contoso_orders directly to Datadog, without the Agent.",
    docs: "https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/",
  },
  {
    id: "dataObservability",
    name: "Data Observability",
    question: "Is the data correct, and what does a faulty table affect?",
    summary:
      "A catalog of the warehouse tables, data quality monitors on them (freshness, volume, nulls, custom SQL), and lineage from the Kafka topic through to the reports.",
    inThisDemo:
      "The Datadog Agent reads the contoso_dw tables, and lineage joins Airflow's OpenLineage events with the Agent's view of the tables.",
    docs: "https://docs.datadoghq.com/data_observability/",
  },
  {
    id: "apm",
    name: "APM",
    question: "Which service is degraded, and where is the time spent?",
    summary:
      "Traces from the storefront and the order processor, and from Airflow's own components: the scheduler, the task process and the triggerer.",
    inThisDemo:
      "The stream services use dd-trace. Airflow exports OpenTelemetry to the Container Apps managed agent.",
    docs: "https://docs.datadoghq.com/tracing/",
  },
  {
    id: "dsm",
    name: "Data Streams Monitoring",
    question: "Are messages being delivered?",
    summary:
      "The pathway from producer to topic to consumer, with throughput, lag and end-to-end latency.",
    inThisDemo: "The storefront → orders topic → order-processor pathway on Event Hubs.",
    docs: "https://docs.datadoghq.com/data_streams/",
  },
  {
    id: "dbm",
    name: "Database Monitoring",
    question: "Is the database itself performing well?",
    summary:
      "Query metrics, samples and explain plans for every database on the Postgres server, plus each table's schema.",
    inThisDemo: "The same Agent check that feeds Data Observability, with dbm enabled.",
    docs: "https://docs.datadoghq.com/database_monitoring/",
  },
  {
    id: "infra",
    name: "Infrastructure Monitoring",
    question: "Does the platform have the resources the work needs?",
    summary:
      "CPU, memory, replicas and restarts for every container app, so a slow or failing task can be traced to the container it runs in.",
    inThisDemo:
      "Datadog's Azure integration reads each container app's metrics from Azure Monitor, a few minutes behind.",
    docs: "https://docs.datadoghq.com/integrations/azure_container_apps/",
  },
  {
    id: "bits",
    name: "Bits AI",
    question: "Can Datadog perform the initial investigation?",
    summary:
      "Bits Investigation starts from an alert, Bits Chat answers questions about telemetry, and Bits Code proposes a fix as a pull request.",
    inThisDemo:
      "The Bits AI documentation does not yet cover Data Observability, so test each prompt before presenting it.",
    docs: "https://docs.datadoghq.com/bits_ai/",
  },
];

export const toolById = Object.fromEntries(tools.map((t) => [t.id, t])) as Record<ToolId, Tool>;
