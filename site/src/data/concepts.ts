// The concept lessons, in reading order, and a short glossary they share. Each lesson's objectives
// say what the learner can do after it: two or three statements, each starting with a verb.

export const conceptLessons = [
  {
    slug: "terminology",
    title: "Observability and data engineering terms",
    objectives: [
      "Translate common observability terms into their data engineering equivalents, and the reverse.",
      "Name the five stages of the data engineering lifecycle, and place each component of the demo in one of them.",
    ],
    summary:
      "The terms both fields use for the same ideas, mapped in both directions, and the five stages of the data engineering lifecycle that the course uses to place each component.",
  },
  {
    slug: "why-pipelines",
    title: "The case for a distributed pipeline",
    objectives: [
      "Compare a single shared database with a pipeline in which each team owns one step.",
      "List the benefits of the additional components, and their costs.",
      "Explain why a distributed pipeline makes failures harder to see.",
    ],
    summary:
      "A single shared database compared with a pipeline in which each team owns one step, and the benefits and costs of the additional components.",
  },
  {
    slug: "medallion",
    title: "Medallion architecture",
    objectives: [
      "Describe the bronze, silver and gold layers, and the demo tables in each.",
      "Explain what layering makes possible: rebuilding from raw data, containing problems, and serving separate audiences.",
      "Name the five data quality dimensions, and the demo monitors that cover each one.",
      "Distinguish ETL from ELT.",
    ],
    summary:
      "Bronze, silver and gold: raw data retained as it arrived, cleaned data, and business-ready tables. The correspondence to this demo's raw, staging and marts schemas, the five data quality dimensions, and a comparison of ETL and ELT.",
  },
  {
    slug: "landscape",
    title: "The data tool landscape",
    objectives: [
      "Group the common data products by the function each performs.",
      "Identify the equivalent of a product on each major cloud provider.",
    ],
    summary:
      "Airflow, dbt, Fivetran, Snowflake, Kafka, Power BI and approximately twenty other products, grouped by function, with each cloud provider's equivalent shown side by side.",
  },
  {
    slug: "lineage-and-traces",
    title: "Lineage events and traces",
    objectives: [
      "Distinguish what OpenLineage reports from what OpenTelemetry reports.",
      "Choose between Data Jobs Monitoring and APM for a given question about a pipeline.",
    ],
    summary:
      "The two kinds of telemetry Airflow sends to Datadog: OpenLineage events for Data Jobs Monitoring, which describe what each pipeline did, and OpenTelemetry traces for APM, which describe how Airflow did the work.",
  },
] as const;

// "Concept 2 of 5": where a lesson sits in the reading order.
export function conceptPosition(slug: (typeof conceptLessons)[number]["slug"]): string {
  return `Concept ${conceptLessons.findIndex((l) => l.slug === slug) + 1} of ${conceptLessons.length}`;
}

export const glossary: { term: string; meaning: string }[] = [
  { term: "OLTP", meaning: "Online transaction processing: the application's database, tuned for a high volume of small, fast reads and writes." },
  { term: "OLAP", meaning: "Online analytical processing: storage and compute tuned for large scans and aggregates over historical data." },
  { term: "ETL and ELT", meaning: "Extract, transform, load (data is cleaned before it is stored) compared with extract, load, transform (data is stored raw, then transformed inside the warehouse)." },
  { term: "Batch and streaming", meaning: "Processing data in scheduled chunks (every 15 minutes, nightly) compared with continuously, event by event." },
  { term: "CDC", meaning: "Change data capture: reading a database's change log to copy inserts, updates and deletes as they happen, without querying the tables." },
  { term: "Data lake", meaning: "Low-cost object storage (S3, ADLS, Google Cloud Storage) that holds files in any format, readable by any compatible engine." },
  { term: "Lakehouse", meaning: "A data lake with open table formats (Delta Lake, Apache Iceberg) that add transactions, schemas and warehouse-style performance." },
  { term: "Data contract", meaning: "An agreement between the team producing data and the teams using it: which fields, which types, what they mean, and how changes are announced." },
  { term: "Schema on read", meaning: "Data is stored as received and a structure is applied when it is read, as in a landing zone. Schema on write enforces the structure at load time, as in a warehouse table." },
  { term: "Semantic layer", meaning: "Metric and join definitions maintained in one place, so that every dashboard computes revenue consistently." },
  { term: "Reverse ETL", meaning: "Syncing modeled data from the warehouse back into operational tools such as a CRM." },
  { term: "Lineage", meaning: "The record of which datasets each dataset is derived from, and which jobs produced it; used to trace an incorrect figure to its cause." },
  { term: "Data quality dimensions", meaning: "Freshness, volume, schema, distribution and lineage: the questions data quality monitoring asks of a table. The medallion lesson maps each to the demo's monitors." },
  { term: "Data governance", meaning: "The policies and controls for who may access which data, how sensitive fields such as email addresses are protected, how long data is retained, and how ownership and lineage are recorded. In the data engineering lifecycle it applies to every stage rather than being a stage of its own." },
  { term: "OpenLineage", meaning: "An open standard for reporting job runs and the datasets they read and write. Airflow, dbt and Spark emit it, and Data Jobs Monitoring reads it." },
  { term: "OpenTelemetry", meaning: "An open standard for traces, metrics and logs from running software. Airflow emits it for its own processes, and APM reads the traces." },
  { term: "Trace and span", meaning: "A trace records one request or unit of work; each span within it records one step, such as a function call or an HTTP request, with its start time and duration." },
];
