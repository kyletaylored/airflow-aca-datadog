// The Datadog products the scenarios use, in the order the site lists them.

export type ToolId =
  | "djm"
  | "dataObservability"
  | "apm"
  | "dsm"
  | "dbm"
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
      "Airflow's OpenLineage provider sends each run of contoso_orders straight to Datadog. No Agent is involved.",
    docs: "https://docs.datadoghq.com/data_observability/jobs_monitoring/airflow/",
  },
  {
    id: "dataObservability",
    name: "Data Observability",
    question: "Is the data right, and what does a bad table affect?",
    summary:
      "A catalog of the warehouse tables, data quality monitors on them (freshness, volume, nulls, custom SQL), and lineage from the Kafka topic through to the reports.",
    inThisDemo:
      "The Datadog Agent reads the contoso_dw tables, and lineage joins Airflow's OpenLineage events with the Agent's view of the tables.",
    docs: "https://docs.datadoghq.com/data_observability/",
  },
  {
    id: "apm",
    name: "APM",
    question: "Which service is misbehaving, and where does the time go?",
    summary:
      "Traces from the storefront and the order processor, and from Airflow's own components: the scheduler, the task process and the triggerer.",
    inThisDemo:
      "The stream services use dd-trace. Airflow exports OpenTelemetry to the Container Apps managed agent.",
    docs: "https://docs.datadoghq.com/tracing/",
  },
  {
    id: "dsm",
    name: "Data Streams Monitoring",
    question: "Are the messages getting through?",
    summary:
      "The pathway from producer to topic to consumer, with throughput, lag and end-to-end latency.",
    inThisDemo: "The storefront → orders topic → order-processor pathway on Event Hubs.",
    docs: "https://docs.datadoghq.com/data_streams/",
  },
  {
    id: "dbm",
    name: "Database Monitoring",
    question: "How is the database itself doing?",
    summary:
      "Query metrics, samples and explain plans for every database on the Postgres server, plus each table's schema.",
    inThisDemo: "The same Agent check that feeds Data Observability, with dbm turned on.",
    docs: "https://docs.datadoghq.com/database_monitoring/",
  },
  {
    id: "bits",
    name: "Bits AI",
    question: "Can Datadog do the first pass of the investigation?",
    summary:
      "Bits Investigation starts from an alert, Bits Chat answers questions about your telemetry, and Bits Code proposes a fix as a pull request.",
    inThisDemo:
      "Datadog's Bits AI docs don't cover Data Observability yet, so try each prompt before you present it.",
    docs: "https://docs.datadoghq.com/bits_ai/",
  },
];

export const toolById = Object.fromEntries(tools.map((t) => [t.id, t])) as Record<ToolId, Tool>;
