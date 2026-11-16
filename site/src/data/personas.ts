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
      "Dave builds the services that take orders and hand them to the data team. When the data goes wrong, Dave needs to know fast whether a release caused it.",
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
        detail: "The order processor reports each landed batch, so lineage starts at the topic.",
      },
    ],
    handoff: {
      starts:
        "When a shopper checks out. The storefront publishes the order to the orders topic straight away, as one JSON message keyed by store.",
      ends:
        "When the order processor writes a batch file to the landing zone, every 5 minutes. From there, Emily's ingest_orders task loads it into the warehouse.",
      contract:
        "Dave owns the order message: which fields it has and what they mean. The processor rejects a message missing a required field, but customer_id isn't one of them, so an order without a customer goes straight through.",
      examples: [
        { label: "One message on the orders topic", code: ORDER_MESSAGE },
        { label: "Where the batches land", code: BATCH_PATH },
      ],
    },
    responsibilities: [
      "Release the storefront and the order processor.",
      "Keep the order message's fields filled in and their meaning stable, for every team downstream.",
      "Keep every order flowing from checkout onto the orders topic.",
      "Land complete batches in the landing zone, without losing or doubling any order.",
    ],
    problem: {
      title: "A storefront release drops customer IDs",
      story:
        "A new storefront release stops sending customer_id on about 30% of orders. Nothing errors: the orders still reach the topic, land in the lake and load into the warehouse. But staging.orders fills with nulls, and the customer value mart quietly loses those orders, because it joins on the customer.",
      inDemo:
        "The null_customers param blanks customer_id on 30% of the orders as they load, standing in for the bad release.",
      trigger: "null_customers",
      wait: "One run, about 2 minutes",
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
      intro: "Nothing fails, so the alert comes from the data itself, further down the pipeline in staging.orders.",
      monitors: ["customerNulls"],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "From the monitor, open the table's lineage and go upstream: staging.orders ← staging_orders ← raw.orders ← ingest_orders ← the landing zone ← the orders topic.",
      },
      {
        tool: "dataObservability",
        text: "Downstream, marts.customer_value shows as affected: it's missing the orders without a customer.",
      },
      { tool: "apm", text: "Open contoso-storefront, the service that sent the orders." },
      {
        tool: "dsm",
        text: "The pathway from the storefront to order-processor is still healthy, so the problem is the data, not the delivery.",
      },
    ],
    bits: "Run Bits Investigation from the alert, and ask it which upstream service produced the null values.",
    resolve: "Dave rolls back the storefront release. The next scheduled load is clean.",
    tools: ["dataObservability", "apm", "dsm", "bits"],
    quiz: [
      {
        prompt: "Data Streams Monitoring shows the storefront → order-processor pathway is healthy. What does that tell Dave?",
        options: [
          "The orders were delivered; the problem is in what they contain",
          "The order processor dropped messages",
          "Event Hubs is throttling the storefront",
        ],
        answer: 0,
        why: "A healthy pathway means messages got through on time. The nulls were already in the orders the storefront sent.",
      },
      {
        prompt: "Why didn't the order processor stop the orders without a customer?",
        options: [
          "It only validates orders on weekends",
          "customer_id isn't one of its required fields",
          "It sends rejected orders to staging anyway",
        ],
        answer: 1,
        why: "The processor drops a message only when a required field is missing, and customer_id isn't on that list.",
      },
    ],
  },
  {
    slug: "rajesh",
    name: "Rajesh",
    role: "Data architect",
    owns: "The warehouse design: the schemas and tables, and what each column means",
    summary:
      "Rajesh decides how the warehouse is shaped. A rename that looks harmless in raw can break every table built on it, so Rajesh needs to see what depends on a column before changing it.",
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
      starts: "Rajesh doesn't run a step of the pipeline. Every step reads and writes tables Rajesh designed.",
      ends: "When a change ships. Rajesh decides how a column changes, and who has to change with it.",
      contract:
        "The column contract: raw.orders.amount_cents is whole cents, staging.orders.amount_usd is dollars, and revenue_usd in marts.daily_store_sales and marts.customer_value is summed from that. Renaming one breaks everything that reads it.",
    },
    responsibilities: [
      "Design the raw, staging and marts schemas and their tables.",
      "Define what each column means and its type.",
      "Approve schema changes, and know what each change affects before it ships.",
    ],
    problem: {
      title: "A schema change breaks staging",
      story:
        "raw.orders.amount_cents is renamed to amount_minor. The load still works, but the staging_orders task reads amount_cents. It fails, retries twice and fails again, so none of the mart tasks run and every report goes stale.",
      trigger: "breaking_schema",
      wait: "5 to 8 minutes, while staging_orders uses up its two retries",
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
      intro: "Three monitors fire in turn, each watching a different layer.",
      monitors: ["jobFailures", "rowGap", "martFreshness"],
    },
    investigate: [
      {
        tool: "djm",
        text: "Open the failed run of contoso_orders. The staging_orders task failed with column \"amount_cents\" does not exist, and the mart tasks never ran.",
      },
      {
        tool: "dataObservability",
        text: "Column-level lineage from raw.orders.amount_cents shows every column that depends on it: staging.orders.amount_usd, then revenue_usd in marts.daily_store_sales and marts.customer_value.",
      },
      {
        tool: "dbm",
        text: "Database Monitoring also keeps each table's schema, collected every 10 minutes, so you can check raw.orders' columns there.",
      },
    ],
    bits: "Ask Bits Chat what depends on raw.orders.amount_cents. Ask Bits Code to propose the staging SQL change as a pull request.",
    resolve: "Rajesh reverts the rename, or Emily updates the staging SQL to read amount_minor.",
    tools: ["djm", "dataObservability", "dbm", "bits"],
    quiz: [
      {
        prompt: "Which view shows Rajesh what a rename of raw.orders.amount_cents would break?",
        options: ["The DAG's task durations", "Column-level lineage from that column", "The order processor's traces"],
        answer: 1,
        why: "Column lineage follows amount_cents into amount_usd and on to revenue_usd in two marts.",
      },
      {
        prompt: "Why do the marts go stale even though the load succeeded?",
        options: [
          "The marts run on a different schedule",
          "staging_orders failed, so the mart tasks after it never ran",
          "The Agent stopped reading the marts",
        ],
        answer: 1,
        why: "The mart tasks depend on staging_orders. When it fails after its retries, they're never started.",
      },
    ],
  },
  {
    slug: "emily",
    name: "Emily",
    role: "Data engineer",
    owns: "The Airflow pipelines that move data from the landing zone through raw and staging",
    summary:
      "Emily runs the loads. A run that succeeds but moves no data is the worst kind of failure: nothing turns red until someone notices the numbers.",
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
        detail: "Lists new landing files with azure-storage-blob, loads them into raw.orders, and reports the datasets to OpenLineage itself.",
      },
      {
        kind: "Airflow operators",
        name: "SQLExecuteQueryOperator, SQLCheckOperator",
        detail: "The staging steps and check_staging are plain SQL, so OpenLineage reads their tables from the statements.",
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
      starts: "At the landing files Dave's order processor writes. ingest_orders loads only the files it hasn't loaded before.",
      ends: "When staging.orders passes check_staging. Priya's mart builds take over from there.",
      contract:
        "Every landed order loads exactly once, within 15 minutes. raw._ingested_files records which files each run loaded.",
    },
    responsibilities: [
      "Load every landed batch into raw, every 15 minutes.",
      "Build the staging tables from raw, and check them.",
      "Rerun or backfill loads when something goes wrong.",
    ],
    problem: {
      title: "A load is skipped, then the volume drops",
      story:
        "One run loads nothing at all, and it still reports success. On the next run, only about 5% of the usual orders arrive. Every task is green, but staging and the marts stop moving.",
      trigger: "skip_load, then volume_drop on the next run",
      wait: "About 45 minutes for freshness to cross its threshold",
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
      intro: "The runs succeed, so the job monitor stays quiet. Freshness notices that the tables stopped changing.",
      monitors: ["stagingFreshness", "martFreshness"],
    },
    investigate: [
      {
        tool: "djm",
        text: "The skipped run succeeded but loaded nothing. The ingest_orders task's lineage shows zero rows written.",
      },
      { tool: "djm", text: "After volume_drop, the run's row counts are about 5% of normal." },
      {
        tool: "apm",
        text: "To rule out the platform, Airflow's own traces show whether the scheduler and the task process ran normally.",
      },
    ],
    bits: "Run Bits Investigation from the freshness alert, and ask why the table stopped updating.",
    resolve: "Emily reruns the load. With the order stream, the waiting batches load on the next normal run.",
    tools: ["djm", "dataObservability", "apm", "bits"],
    quiz: [
      {
        prompt: "Every task in the run is green. What catches the problem?",
        options: ["A freshness monitor on the table", "The DAG's failure count", "The scheduler's heartbeat"],
        answer: 0,
        why: "The run succeeded, so job-status monitors stay quiet. Freshness notices the table stopped changing.",
      },
      {
        prompt: "Where does Emily see that the run moved no data?",
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
      "Priya writes the business logic. A bug there produces numbers that look plausible and are wrong, and the pipeline itself reports nothing unusual.",
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
        detail: "Airflow renders the SQL before it runs, so run params can change the logic. That's how the demo's refund_bug works.",
      },
      {
        kind: "Datadog",
        name: "Data Observability monitors",
        detail: "Custom SQL rules on the marts, such as no negative revenue, and anomaly detection on daily totals.",
      },
    ],
    handoff: {
      starts: "At staging.orders and staging.customers, once check_staging has passed.",
      ends: "At the marts tables, which John's Metabase dashboards read.",
      contract:
        "Each mart's numbers mean what the business thinks they mean: revenue_usd in marts.daily_store_sales is the total value of a store's orders that day, and never negative.",
    },
    responsibilities: [
      "Write and review the marts transformations.",
      "Define the business metrics, such as revenue per store per day.",
      "Make sure the marts' numbers are right, not just fresh.",
    ],
    problem: {
      title: "A refund change makes revenue negative",
      story:
        "A change to the refund logic counts every Denver order as a refund. Denver's revenue in marts.daily_store_sales goes negative. staging.orders is untouched, so check_staging passes, and every task succeeds.",
      trigger: "refund_bug",
      wait: "One run, about 2 minutes",
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
      intro: "The run is healthy, so the alert has to come from a rule about the numbers.",
      monitors: ["negativeRevenue", "revenueAnomaly"],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "Lineage shows the problem starts at the mart: staging.orders is clean, and check_staging passed.",
      },
      {
        tool: "djm",
        text: "The mart_daily_store_sales task ran normally. The bug is in the logic, not the run.",
      },
    ],
    bits: "Ask Bits Code to find the change to the mart SQL and propose a revert.",
    resolve: "Priya reverts the refund change. The next run rebuilds the mart with correct revenue.",
    tools: ["dataObservability", "djm", "bits"],
    quiz: [
      {
        prompt: "staging.orders is clean and the mart task succeeded. Where is the bug?",
        options: ["In the load", "In the mart's SQL logic", "In the storefront"],
        answer: 1,
        why: "Clean inputs and a successful run leave the transformation itself.",
      },
      {
        prompt: "Which monitor catches a wrong-but-plausible number on day one?",
        options: ["A custom SQL monitor for negative revenue", "The revenue anomaly monitor", "A Job monitor on contoso_orders"],
        answer: 0,
        why: "The anomaly monitor needs a week of history. A custom SQL rule works from the first run.",
      },
    ],
  },
  {
    slug: "john",
    name: "John",
    role: "BI lead",
    owns: "The reports built on the marts tables, and the people who read them",
    summary:
      "John doesn't break anything; John hears about it. Whatever goes wrong upstream, the question is the same: which reports are wrong, and whose problem is it?",
    avatarSeed: "john-1",
    stack: [
      {
        kind: "BI tool",
        name: "Metabase (open source)",
        detail: "Version 0.65, running as a Container App. Its own settings live in a metabase database on the same Postgres server.",
      },
      {
        kind: "Data source",
        name: "contoso_dw marts",
        detail: "Read through the metabase role, which can see only the marts schema.",
      },
      {
        kind: "Dashboards",
        name: "Store Sales — Today, Customer Value, Pipeline Health",
        detail: "About 19 questions in all, each built on one or more marts tables.",
      },
      {
        kind: "Datadog",
        name: "Data Observability for Metabase",
        detail: "Datadog reads Metabase's dashboards and questions, so lineage runs past the marts to the reports.",
      },
    ],
    handoff: {
      starts: "At the marts tables, after Priya's builds.",
      ends: "With the people who read the dashboards: finance and the store managers.",
      contract: "Readers can trust a dashboard, or they're told it's wrong before they act on it.",
    },
    responsibilities: [
      "Build and maintain the dashboards on the marts.",
      "Tell readers when numbers are wrong or stale, and when they're fixed.",
      "Route data problems to the person who owns them.",
    ],
    problem: {
      title: "Which reports are wrong?",
      story:
        "Any of the other four scenarios ends at John's dashboards. Readers see stale or wrong numbers, and John has to say which reports to trust and who is fixing the rest.",
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
        "John doesn't own a monitor. The alert comes from whichever table broke upstream, and lineage carries it to the dashboards: nulls, freshness, the row gap or negative revenue.",
      monitors: [],
    },
    investigate: [
      {
        tool: "dataObservability",
        text: "Open lineage downstream past the marts tables to the Metabase dashboards. A table marked affected leads to the questions and dashboards showing wrong or stale numbers.",
      },
      {
        tool: "dataObservability",
        text: "For example, refund_bug reaches \"Revenue by Store\" on Store Sales — Today, and \"Stores with Negative Revenue\" on Pipeline Health.",
      },
      { tool: "dataObservability", text: "The lineage path back up shows whose issue it is." },
    ],
    bits: "Ask Bits Chat which dashboards read marts.daily_store_sales, and which monitor alerted first.",
    resolve: "John tells readers which dashboards to hold off on, and hands the issue to the owner the lineage points to.",
    tools: ["dataObservability", "bits"],
    quiz: [
      {
        prompt: "How does John find the dashboards a bad table affects?",
        options: ["Lineage downstream from the table to Metabase", "Searching Metabase for the table name", "Asking each analyst"],
        answer: 0,
        why: "Data Observability's lineage reaches past the marts to the Metabase questions and dashboards built on them.",
      },
      {
        prompt: "How does John know whose problem it is?",
        options: ["The monitor's name", "The lineage path back up to where the problem starts", "Whoever last edited the dashboard"],
        answer: 1,
        why: "Following lineage upstream ends at the first broken step, and each step has an owner.",
      },
    ],
  },
];

export const personaBySlug = Object.fromEntries(personas.map((p) => [p.slug, p])) as Record<Owner, Persona>;
