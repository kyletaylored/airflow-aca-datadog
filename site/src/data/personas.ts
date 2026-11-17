// The five people who run Contoso's order pipeline, and the scenario each one
// owns. Written from docs/data-pipeline-demo.md ("The story" and "Demo
// scenarios") and the code in services/ and airflow/dags/; keep them in step.

import type { Owner, PersonaDiagram } from "./pipeline";
import type { ToolId } from "./tools";

export interface Step {
  tool: ToolId;
  text: string;
}

export interface Question {
  prompt: string;
  options: string[];
  answer: number;
  why: string;
}

export interface StackItem {
  // What kind of thing it is: "Language", "Azure service", "Library"…
  kind: string;
  name: string;
  detail: string;
}

export interface Example {
  label: string;
  code: string;
}

export interface Persona {
  slug: Owner;
  name: string;
  role: string;
  owns: string;
  summary: string;
  // DiceBear "notionists" seed for the face.
  avatarSeed: string;
  stack: StackItem[];
  handoff: {
    starts: string;
    ends: string;
    contract: string;
    examples?: Example[];
  };
  responsibilities: string[];
  problem: {
    title: string;
    story: string;
    // How the demo makes it happen, when that differs from the story.
    inDemo?: string;
    // The lab instructions, when they aren't "trigger contoso_orders with <trigger>".
    lab?: string;
    trigger?: string;
    wait: string;
  };
  diagram: PersonaDiagram;
  detect: {
    intro: string;
    monitors: string[];
  };
  investigate: Step[];
  bits: string;
  resolve: string;
  tools: ToolId[];
  quiz: Question[];
}

const ORDER_MESSAGE = `{
  "order_id": "4f1c2a7e9b3d4c58a1e6f0b2d9c7e3a1",
  "customer_id": "C01234",
  "store": "Denver",
  "channel": "web",
  "items": 3,
  "amount_cents": 4599,
  "currency": "USD",
  "ordered_at": "2026-11-16T14:27:05+00:00"
}`;

const BATCH_PATH = `landing/                    the ADLS Gen2 container
  orders/
    dt=2026-11-16/          the UTC date the batch landed
      hour=14/              the UTC hour
        batch-20261116T143000-1a2b3c4d.json
        batch-20261116T143500-9e8f7a6b.json

One file every 5 minutes. Each line of a file is one order message.`;

export const personas: Persona[] = [
  {
    slug: "dave",
    name: "Dave",
    role: "Application engineer",
    owns: "The storefront and the order processor, up to the files in the landing zone",
    summary:
      "Dave builds the services that accept orders and pass them to the data team. When the data is incorrect, Dave needs to determine quickly whether a release caused the problem.",
    avatarSeed: "dave-4",
    stack: [
      { kind: "Language", name: "Python 3.12", detail: "Both services, on the stock python:3.12-slim image." },
      {
        kind: "Azure service",
        name: "Azure Container Apps",
        detail: "contoso-storefront and order-processor each run as a Container App.",
      },
      {
        kind: "Azure service",
        name: "Azure Event Hubs",
        detail: "Standard tier, used through its Kafka endpoint. The orders topic carries one message per order.",
      },
      {
        kind: "Library",
        name: "confluent-kafka",
        detail: "The Kafka producer in the storefront and the consumer in the order processor.",
      },
      {
        kind: "Azure service",
        name: "Azure Data Lake Storage Gen2",
        detail: "The landing container, written with the azure-storage-blob SDK.",
      },
      {
        kind: "Datadog",
        name: "dd-trace with Data Streams Monitoring",
        detail: "Traces both services and follows each message from producer to consumer.",
      },
      {
        kind: "Lineage",
        name: "OpenLineage",
        detail: "The order processor reports each landed batch, so lineage begins at the topic.",
      },
    ],
    handoff: {
      starts:
        "At checkout. The storefront immediately publishes the order to the orders topic as one JSON message, keyed by store.",
      ends:
        "When the order processor writes a batch file to the landing zone, every 5 minutes. Emily's ingest_orders task then loads the file into the warehouse.",
      contract:
        "Dave owns the order message: its fields and their meaning. The processor rejects any message that lacks a required field, but customer_id is not a required field, so an order without a customer passes through unchecked.",
      examples: [
        { label: "One message on the orders topic", code: ORDER_MESSAGE },
        { label: "Where the batches land", code: BATCH_PATH },
      ],
    },
    responsibilities: [
      "Release the storefront and the order processor.",
      "Keep the order message's fields populated and their meaning stable for every downstream team.",
      "Ensure that every order is published from checkout to the orders topic.",
      "Write complete batches to the landing zone, without losing or duplicating any order.",
    ],
    problem: {
      title: "A storefront release drops customer IDs",
      story:
        "A new storefront release stops sending customer_id on approximately 30% of orders. No errors occur: the orders still reach the topic, land in the lake and load into the warehouse. However, staging.orders accumulates nulls, and the customer value mart silently omits those orders, because it joins on the customer.",
      inDemo:
        "The null_customers parameter clears customer_id on 30% of the orders as they load, simulating the faulty release.",
      trigger: "null_customers",
      wait: "One run of approximately 2 minutes, then up to 15 minutes for the monitor's next check",
    },
    diagram: {
      focus: ["storefront", "topic", "processor", "landing"],
      marks: [
        { node: "storefront", kind: "problem", label: "Release drops customer_id" },
        { node: "staging", kind: "alert", label: "customer_id nulls" },
        { node: "marts", kind: "impact", label: "customer_value loses orders" },
      ],
    },
    detect: {
      intro: "No step fails, so the alert originates from the data itself, further down the pipeline in staging.orders.",
      monitors: ["customerNulls"],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "From the monitor, open the table's lineage and trace it upstream: staging.orders ← staging_orders ← raw.orders ← ingest_orders ← the landing zone ← the orders topic.",
      },
      {
        tool: "dataObservability",
        text: "Downstream, marts.customer_value is marked as affected: it omits the orders that lack a customer.",
      },
      { tool: "apm", text: "Open contoso-storefront, the service that published the orders." },
      {
        tool: "dsm",
        text: "The pathway from the storefront to order-processor remains healthy, so the problem lies in the data rather than its delivery.",
      },
    ],
    bits: "Run Bits Investigation from the alert and ask which upstream service produced the null values.",
    resolve: "Dave rolls back the storefront release, and the next scheduled load is clean.",
    tools: ["dataObservability", "apm", "dsm", "bits"],
    quiz: [
      {
        prompt: "Data Streams Monitoring shows that the storefront → order-processor pathway is healthy. What does this indicate to Dave?",
        options: [
          "The orders were delivered; the problem lies in their contents",
          "The order processor dropped messages",
          "Event Hubs is throttling the storefront",
        ],
        answer: 0,
        why: "A healthy pathway means messages were delivered on time. The nulls were already present in the orders the storefront published.",
      },
      {
        prompt: "Why did the order processor accept the orders without a customer?",
        options: [
          "It only validates orders on weekends",
          "customer_id is not one of its required fields",
          "It forwards rejected orders to staging regardless",
        ],
        answer: 1,
        why: "The processor drops a message only when a required field is missing, and customer_id is not a required field.",
      },
    ],
  },
  {
    slug: "rajesh",
    name: "Rajesh",
    role: "Data architect",
    owns: "The warehouse design: the schemas and tables, and what each column means",
    summary:
      "Rajesh determines the structure of the warehouse. A rename that appears harmless in raw can break every table built on it, so Rajesh needs to see what depends on a column before changing it.",
    avatarSeed: "rajesh-9",
    stack: [
      {
        kind: "Database",
        name: "PostgreSQL 16",
        detail: "Azure Database for PostgreSQL Flexible Server. The warehouse is the contoso_dw database.",
      },
      {
        kind: "Data model",
        name: "Medallion layers",
        detail: "raw holds orders as they landed, staging holds them cleaned and typed, and marts holds the business metrics.",
      },
      {
        kind: "Schema as code",
        name: "SQL DDL in the repo",
        detail: "Every table is defined in airflow/dags/contoso_dw/sql.py and created by the contoso_dw_bootstrap DAG.",
      },
      {
        kind: "Access",
        name: "Postgres roles",
        detail: "The datadog role can read all three layers; the metabase role can read only the marts.",
      },
      {
        kind: "Datadog",
        name: "Data Observability",
        detail: "The catalog of every table, and column-level lineage from raw to the reports.",
      },
      {
        kind: "Datadog",
        name: "Database Monitoring",
        detail: "Each table's schema, and query metrics and explain plans for the whole server.",
      },
    ],
    handoff: {
      starts: "Rajesh does not operate a step of the pipeline. Every step reads from and writes to tables that Rajesh designed.",
      ends: "At release. Rajesh decides how a column changes, and which teams must adapt to the change.",
      contract:
        "The column contract: raw.orders.amount_cents holds whole cents, staging.orders.amount_usd holds dollars, and revenue_usd in marts.daily_store_sales and marts.customer_value is summed from amount_usd. Renaming any of these columns breaks every consumer that reads it.",
    },
    responsibilities: [
      "Design the raw, staging and marts schemas and their tables.",
      "Define the meaning and type of each column.",
      "Approve schema changes, and assess the impact of each change before release.",
    ],
    problem: {
      title: "A schema change breaks staging",
      story:
        "raw.orders.amount_cents is renamed to amount_minor. The load still succeeds, but the staging_orders task reads amount_cents. The task fails, retries twice and fails again, so none of the mart tasks run and every report becomes stale.",
      trigger: "breaking_schema",
      lab: "Trigger contoso_orders with breaking_schema three times in succession, immediately after a scheduled run finishes. Each run fails after its two retries (5 to 8 minutes) and renames the column again for the next run, so staging remains broken across a monitor check while raw continues to load.",
      wait: "5 to 25 minutes, while the three runs fail in succession",
    },
    diagram: {
      focus: ["raw", "staging", "marts"],
      marks: [
        { node: "raw", kind: "problem", label: "amount_cents renamed" },
        { node: "stagingTask", kind: "alert", label: "failed run" },
        { node: "staging", kind: "alert", label: "row gap vs raw" },
        { node: "marts", kind: "alert", label: "stale" },
      ],
    },
    detect: {
      intro: "Three monitors alert in sequence, each covering a different layer.",
      monitors: ["jobFailures", "rowGap", "martFreshness"],
    },
    investigate: [
      {
        tool: "djm",
        text: "Open the failed run of contoso_orders. The staging_orders task failed with column \"amount_cents\" does not exist, and the mart tasks did not run.",
      },
      {
        tool: "dataObservability",
        text: "Column-level lineage from raw.orders.amount_cents shows every column that depends on it: staging.orders.amount_usd, then revenue_usd in marts.daily_store_sales and marts.customer_value.",
      },
      {
        tool: "dbm",
        text: "Database Monitoring also records each table's schema every 10 minutes, so the columns of raw.orders can be verified there.",
      },
    ],
    bits: "Ask Bits Chat what depends on raw.orders.amount_cents, then ask Bits Code to propose the staging SQL change as a pull request.",
    resolve: "Rajesh reverts the rename, or Emily updates the staging SQL to read amount_minor.",
    tools: ["djm", "dataObservability", "dbm", "bits"],
    quiz: [
      {
        prompt: "Which view shows Rajesh what renaming raw.orders.amount_cents would break?",
        options: ["The DAG's task durations", "Column-level lineage from that column", "The order processor's traces"],
        answer: 1,
        why: "Column-level lineage traces amount_cents to amount_usd, and from there to revenue_usd in two marts.",
      },
      {
        prompt: "Why do the marts become stale even though the load succeeded?",
        options: [
          "The marts run on a different schedule",
          "staging_orders failed, so the mart tasks after it never ran",
          "The Agent stopped reading the marts",
        ],
        answer: 1,
        why: "The mart tasks depend on staging_orders. When that task fails after its retries, the mart tasks are never started.",
      },
    ],
  },
  {
    slug: "emily",
    name: "Emily",
    role: "Data engineer",
    owns: "The Airflow pipelines that move data from the landing zone through raw and staging",
    summary:
      "Emily operates the loads. A run that succeeds without moving any data is the most difficult failure to detect: no status changes until someone notices the figures.",
    avatarSeed: "emily-7",
    stack: [
      {
        kind: "Orchestration",
        name: "Apache Airflow 3.4",
        detail: "On Azure Container Apps: an api-server, a scheduler that runs the tasks (LocalExecutor), a triggerer and a dag-processor.",
      },
      {
        kind: "Language",
        name: "Python DAGs",
        detail: "contoso_orders runs every 15 minutes, one run at a time, with two retries 2 minutes apart.",
      },
      {
        kind: "Custom operator",
        name: "LoadLandedOrdersOperator",
        detail: "Lists new landing files with azure-storage-blob, loads them into raw.orders, and reports the datasets to OpenLineage directly.",
      },
      {
        kind: "Airflow operators",
        name: "SQLExecuteQueryOperator, SQLCheckOperator",
        detail: "The staging steps and check_staging are plain SQL, so OpenLineage derives their tables from the statements.",
      },
      {
        kind: "Lineage",
        name: "OpenLineage provider",
        detail: "Built into Airflow 3. Sends every DAG run and task run to Data Jobs Monitoring.",
      },
      {
        kind: "Tracing",
        name: "OpenTelemetry",
        detail: "Airflow's own traces, sent to APM through the Container Apps managed agent.",
      },
    ],
    handoff: {
      starts: "At the landing files that Dave's order processor writes. ingest_orders loads only files it has not previously loaded.",
      ends: "When staging.orders passes check_staging. Priya's mart builds proceed from that point.",
      contract:
        "Every landed order loads exactly once, within 15 minutes. raw._ingested_files records which files each run loaded.",
    },
    responsibilities: [
      "Load every landed batch into raw, every 15 minutes.",
      "Build the staging tables from raw, and check them.",
      "Rerun or backfill loads when a failure occurs.",
    ],
    problem: {
      title: "A load is skipped, then the volume drops",
      story:
        "One run loads no data yet still reports success. On the following run, only approximately 5% of the usual orders arrive. Every task succeeds, but staging and the marts stop updating.",
      inDemo:
        "The skip_load and volume_drop parameters each cause one faulty run, but the next scheduled run loads every pending batch 15 minutes later. To halt the load for long enough to observe the alerts, pause the DAG instead.",
      lab: "Pause contoso_orders in Airflow. Allow up to 15 minutes for the row count alert and approximately 45 minutes for both freshness alerts. Then unpause the DAG; the next run loads the backlog.",
      wait: "Approximately 45 minutes for freshness to cross its threshold",
    },
    diagram: {
      focus: ["ingest", "raw", "stagingTask", "staging"],
      marks: [
        { node: "ingest", kind: "problem", label: "Loads nothing, then 5%" },
        { node: "staging", kind: "alert", label: "freshness" },
        { node: "marts", kind: "alert", label: "freshness" },
      ],
    },
    detect: {
      intro: "No run fails, so the job monitor does not alert. The row count monitor detects first that no new data is loading; the freshness monitors then detect that the tables have stopped changing.",
      monitors: ["rowCount", "stagingFreshness", "martFreshness"],
    },
    investigate: [
      {
        tool: "djm",
        text: "The skipped run succeeded but loaded no data. The ingest_orders task's lineage shows zero rows written.",
      },
      { tool: "djm", text: "After volume_drop, the run's row counts are approximately 5% of normal." },
      {
        tool: "apm",
        text: "To rule out the platform, Airflow's own traces show whether the scheduler and the task process operated normally.",
      },
    ],
    bits: "Run Bits Investigation from the freshness alert and ask why the table stopped updating.",
    resolve: "Emily reruns the load. Because the batches remain in the landing zone, they load on the next normal run.",
    tools: ["djm", "dataObservability", "apm", "bits"],
    quiz: [
      {
        prompt: "Every task in the run succeeded. Which monitor detects the problem?",
        options: ["A freshness monitor on the table", "The DAG's failure count", "The scheduler's heartbeat"],
        answer: 0,
        why: "The run succeeded, so job status monitors do not alert. The freshness monitor detects that the table stopped changing.",
      },
      {
        prompt: "Where can Emily confirm that the run moved no data?",
        options: [
          "In the ingest_orders task's lineage, which shows zero rows written",
          "In the storefront's error rate",
          "In the Postgres connection count",
        ],
        answer: 0,
        why: "Data Jobs Monitoring records the datasets and row counts each task wrote.",
      },
    ],
  },
  {
    slug: "priya",
    name: "Priya",
    role: "Analytics engineer",
    owns: "The marts transformations: the SQL that turns clean orders into business metrics",
    summary:
      "Priya writes the business logic. A defect there produces figures that appear plausible but are incorrect, while the pipeline itself reports nothing unusual.",
    avatarSeed: "priya-5",
    stack: [
      {
        kind: "Language",
        name: "SQL (PostgreSQL)",
        detail: "Each mart is a TRUNCATE and an INSERT … SELECT from staging, rebuilt on every run.",
      },
      {
        kind: "Orchestration",
        name: "Airflow SQL tasks",
        detail: "mart_daily_store_sales, mart_customer_value and mart_hourly_order_volume, after check_staging passes.",
      },
      {
        kind: "Templating",
        name: "Jinja",
        detail: "Airflow renders the SQL before it runs, so run parameters can alter the logic. The demo's refund_bug scenario relies on this.",
      },
      {
        kind: "Datadog",
        name: "Data Observability monitors",
        detail: "Custom SQL rules on the marts, such as a rule against negative revenue, and anomaly detection on daily totals.",
      },
    ],
    handoff: {
      starts: "At staging.orders and staging.customers, once check_staging has passed.",
      ends: "At the marts tables, which John's Metabase dashboards read.",
      contract:
        "Each mart's figures match the business definition: revenue_usd in marts.daily_store_sales is the total value of a store's orders that day, and is never negative.",
    },
    responsibilities: [
      "Write and review the marts transformations.",
      "Define the business metrics, such as revenue per store per day.",
      "Ensure that the marts' figures are correct, not merely current.",
    ],
    problem: {
      title: "A refund change makes revenue negative",
      story:
        "A change to the refund logic counts every Denver order as a refund, and Denver's revenue in marts.daily_store_sales becomes negative. staging.orders is unaffected, so check_staging passes and every task succeeds.",
      trigger: "refund_bug",
      lab: "Trigger contoso_orders with refund_bug immediately after a scheduled run finishes (at approximately :31). The incorrect revenue persists only until the next scheduled run rebuilds the mart, so it must be present at the monitor's next check. Allow one run of approximately 2 minutes, then the next check.",
      wait: "One run of approximately 2 minutes, then the next check",
    },
    diagram: {
      focus: ["martTasks", "marts"],
      marks: [
        { node: "martTasks", kind: "problem", label: "Denver counted as refunds" },
        { node: "marts", kind: "alert", label: "negative revenue" },
        { node: "metabase", kind: "impact", label: "wrong revenue" },
      ],
    },
    detect: {
      intro: "The run is healthy, so the alert must come from a rule applied to the figures.",
      monitors: ["negativeRevenue", "revenueAnomaly"],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "Lineage shows that the problem originates at the mart: staging.orders is clean, and check_staging passed.",
      },
      {
        tool: "djm",
        text: "The mart_daily_store_sales task ran normally; the defect is in the logic, not the run.",
      },
    ],
    bits: "Ask Bits Code to find the change to the mart SQL and propose a revert.",
    resolve: "Priya reverts the refund change, and the next run rebuilds the mart with correct revenue.",
    tools: ["dataObservability", "djm", "bits"],
    quiz: [
      {
        prompt: "staging.orders is clean and the mart task succeeded. Where is the defect?",
        options: ["In the load", "In the mart's SQL logic", "In the storefront"],
        answer: 1,
        why: "With clean inputs and a successful run, the transformation itself is the remaining cause.",
      },
      {
        prompt: "Which monitor detects a plausible but incorrect figure on the first day?",
        options: ["A custom SQL monitor for negative revenue", "The revenue anomaly monitor", "A Job monitor on contoso_orders"],
        answer: 0,
        why: "The anomaly monitor requires a week of history, whereas a custom SQL rule is effective from the first run.",
      },
    ],
  },
  {
    slug: "john",
    name: "John",
    role: "BI lead",
    owns: "The reports built on the marts tables, and the people who read them",
    summary:
      "John's work does not cause failures, but John is the first to hear about them. Whatever fails upstream, John must answer the same two questions: which reports are incorrect, and which team owns the problem.",
    avatarSeed: "john-1",
    stack: [
      {
        kind: "BI tool",
        name: "Metabase (open source)",
        detail: "Version 0.65, running as a Container App. Its settings are stored in a metabase database on the same PostgreSQL server.",
      },
      {
        kind: "Data source",
        name: "contoso_dw marts",
        detail: "Read through the metabase role, which can see only the marts schema.",
      },
      {
        kind: "Dashboards",
        name: "Store Sales — Today, Customer Value, Pipeline Health",
        detail: "Approximately 19 questions in total, each built on one or more marts tables.",
      },
      {
        kind: "Datadog",
        name: "Data Observability for Metabase",
        detail: "Datadog reads Metabase's dashboards and questions, so lineage extends beyond the marts to the reports.",
      },
    ],
    handoff: {
      starts: "At the marts tables, after Priya's builds complete.",
      ends: "With the dashboard readers: finance and the store managers.",
      contract: "Readers can rely on a dashboard, or they are informed that it is incorrect before they act on it.",
    },
    responsibilities: [
      "Build and maintain the dashboards on the marts.",
      "Inform readers when figures are incorrect or stale, and when they are corrected.",
      "Route data problems to their owners.",
    ],
    problem: {
      title: "Identifying the affected reports",
      story:
        "Each of the other four scenarios ends at John's dashboards. Readers see stale or incorrect figures, and John must state which reports can be trusted and who is correcting the others.",
      wait: "Follows any of the other scenarios",
    },
    diagram: {
      focus: ["metabase"],
      marks: [
        { node: "marts", kind: "alert", label: "an upstream alert" },
        { node: "metabase", kind: "impact", label: "wrong or stale numbers" },
      ],
    },
    detect: {
      intro:
        "John does not own a monitor. The alert originates from whichever upstream table failed, and lineage connects it to the dashboards: nulls, freshness, the row gap or negative revenue.",
      monitors: [],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "Follow lineage downstream beyond the marts tables to the Metabase dashboards. A table marked as affected leads to the questions and dashboards that show incorrect or stale figures.",
      },
      {
        tool: "dataObservability",
        text: "For example, refund_bug reaches \"Revenue by Store\" on Store Sales — Today, and \"Stores with Negative Revenue\" on Pipeline Health.",
      },
      { tool: "dataObservability", text: "Tracing the lineage path upstream identifies the team that owns the issue." },
    ],
    bits: "Ask Bits Chat which dashboards read marts.daily_store_sales, and which monitor alerted first.",
    resolve: "John informs readers which dashboards to disregard until further notice, and assigns the issue to the owner identified by lineage.",
    tools: ["dataObservability", "bits"],
    quiz: [
      {
        prompt: "How does John identify the dashboards affected by a faulty table?",
        options: ["Lineage downstream from the table to Metabase", "Search Metabase for the table name", "Ask each analyst"],
        answer: 0,
        why: "Data Observability's lineage extends beyond the marts to the Metabase questions and dashboards built on them.",
      },
      {
        prompt: "How does John determine which team owns the problem?",
        options: ["The monitor's name", "The lineage path upstream to where the problem originates", "The most recent editor of the dashboard"],
        answer: 1,
        why: "Lineage traced upstream ends at the first failed step, and each step has a defined owner.",
      },
    ],
  },
];

export const personaBySlug = Object.fromEntries(personas.map((p) => [p.slug, p])) as Record<Owner, Persona>;
