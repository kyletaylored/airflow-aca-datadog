// The five people who run Contoso's order pipeline, and the scenario each one
// owns. Written from docs/data-pipeline-demo.md ("The story" and "Demo
// scenarios"); keep the two in step.

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

export interface Persona {
  slug: string;
  name: string;
  role: string;
  owns: string;
  summary: string;
  tech: { name: string; detail: string }[];
  responsibilities: string[];
  problem: {
    title: string;
    story: string;
    // The demo param that causes it, and how long until the alert.
    trigger?: string;
    wait: string;
  };
  detect: Step[];
  investigate: Step[];
  bits: string;
  resolve: string;
  tools: ToolId[];
  quiz: Question[];
}

export const personas: Persona[] = [
  {
    slug: "dave",
    name: "Dave",
    role: "Application engineer",
    owns: "The storefront and the order processor, up to the files in the landing zone",
    summary:
      "Dave ships the services that take orders and land them for the data team. When the data goes wrong, he needs to know fast whether his release caused it.",
    tech: [
      { name: "contoso-storefront", detail: "Container App that takes orders and publishes them" },
      { name: "Azure Event Hubs", detail: "The orders topic, used through its Kafka endpoint" },
      { name: "order-processor", detail: "Container App that reads the topic and writes batches" },
      { name: "ADLS Gen2 landing zone", detail: "Order batches under orders/dt=…/hour=…" },
    ],
    responsibilities: [
      "Release the storefront and the order processor.",
      "Keep every order flowing from the storefront onto the orders topic.",
      "Land complete batches in the landing zone for Airflow to pick up.",
      "Keep each order's fields, such as customer_id, filled in for the teams downstream.",
    ],
    problem: {
      title: "A storefront release drops customer IDs",
      story:
        "A new storefront release stops sending customer_id on about 30% of orders. Nothing errors: the orders still reach the topic, land in the lake, and load into the warehouse. But staging.orders fills with nulls, and the customer value mart quietly loses those orders, because it joins on the customer.",
      trigger: "null_customers",
      wait: "One run, about 2 minutes",
    },
    detect: [
      {
        tool: "dataObservability",
        text: "The null-ratio monitor on staging.orders.customer_id alerts.",
      },
    ],
    investigate: [
      {
        tool: "dataObservability",
        text: "From the monitor, open the table's lineage and go upstream: staging.orders ← staging_orders ← raw.orders ← ingest_orders ← the landing zone ← the orders topic.",
      },
      {
        tool: "dataObservability",
        text: "Downstream, marts.customer_value shows as affected: it's missing the orders without a customer.",
      },
      {
        tool: "apm",
        text: "Open contoso-storefront, the service that sent the orders.",
      },
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
        prompt: "Why does marts.customer_value lose orders when customer_id is null?",
        options: [
          "The mart filters out orders older than a day",
          "The mart joins orders to customers on customer_id",
          "Airflow skips the mart task when staging has nulls",
        ],
        answer: 1,
        why: "An inner join on the customer drops every order that has no customer.",
      },
    ],
  },
  {
    slug: "rajesh",
    name: "Rajesh",
    role: "Data architect",
    owns: "The warehouse design: the schemas and tables, and what each column means",
    summary:
      "Rajesh decides how the warehouse is shaped. A rename that looks harmless in raw can break every table built on it, so he needs to see what depends on a column before he changes it.",
    tech: [
      { name: "Azure Database for PostgreSQL", detail: "Flexible Server, database contoso_dw" },
      { name: "raw", detail: "orders and customers, as loaded" },
      { name: "staging", detail: "Cleaned orders and customers" },
      { name: "marts", detail: "daily_store_sales, customer_value, hourly_order_volume" },
    ],
    responsibilities: [
      "Design the raw, staging and marts schemas and their tables.",
      "Define what each column means and its type.",
      "Approve schema changes, and know what each change affects before it ships.",
    ],
    problem: {
      title: "A schema change breaks staging",
      story:
        "raw.orders.amount_cents is renamed to amount_minor. The load still works, but the staging_orders task reads amount_cents. It fails, retries twice, and fails again, so none of the mart tasks run and every report goes stale.",
      trigger: "breaking_schema",
      wait: "5 to 8 minutes, while staging_orders uses up its two retries",
    },
    detect: [
      { tool: "djm", text: "The Job monitor on contoso_orders alerts on the failed run." },
      {
        tool: "dataObservability",
        text: "The Source to Target monitor alerts as staging.orders falls behind raw.orders, then freshness on marts.daily_store_sales.",
      },
    ],
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
        options: [
          "The DAG's task durations",
          "Column-level lineage from that column",
          "The order processor's traces",
        ],
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
    tech: [
      { name: "Apache Airflow 3", detail: "On Azure Container Apps" },
      { name: "contoso_orders", detail: "The DAG, every 15 minutes" },
      { name: "ingest_orders, staging_orders, check_staging", detail: "The load and staging tasks" },
      { name: "OpenLineage", detail: "Built into Airflow; reports every run and its datasets" },
    ],
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
    detect: [
      {
        tool: "dataObservability",
        text: "Freshness monitors on staging.orders and marts.daily_store_sales alert.",
      },
    ],
    investigate: [
      {
        tool: "djm",
        text: "The skipped run succeeded but loaded nothing. The ingest_orders task's lineage shows zero rows written.",
      },
      {
        tool: "djm",
        text: "After volume_drop, the run's row counts are about 5% of normal.",
      },
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
        options: [
          "A freshness monitor on the table",
          "The DAG's failure count",
          "The scheduler's heartbeat",
        ],
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
    tech: [
      { name: "marts SQL", detail: "daily_store_sales, customer_value, hourly_order_volume" },
      { name: "mart_daily_store_sales", detail: "The Airflow task that builds store sales" },
      { name: "Data quality monitors", detail: "Custom SQL and anomaly monitors on the marts" },
    ],
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
    detect: [
      {
        tool: "dataObservability",
        text: "The negative-revenue monitor on marts.daily_store_sales alerts. Once it has a week of history, the revenue anomaly monitor flags the dip too.",
      },
    ],
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
        options: [
          "A custom SQL monitor for negative revenue",
          "The revenue anomaly monitor",
          "A Job monitor on contoso_orders",
        ],
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
      "John doesn't break anything; he hears about it. Whatever goes wrong upstream, his question is the same: which reports are wrong, and whose problem is it?",
    tech: [
      { name: "Metabase", detail: "Dashboards on the marts tables" },
      { name: "Store Sales — Today", detail: "Revenue by store" },
      { name: "Customer Value", detail: "Customer lifetime value" },
      { name: "Pipeline Health", detail: "Each mart's age, and stores with negative revenue" },
    ],
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
    detect: [
      {
        tool: "dataObservability",
        text: "Start from whichever alert fired upstream: nulls, freshness, Source to Target, or negative revenue.",
      },
    ],
    investigate: [
      {
        tool: "dataObservability",
        text: "Open lineage downstream past the marts tables to the Metabase dashboards. A table marked affected leads to the questions and dashboards showing wrong or stale numbers.",
      },
      {
        tool: "dataObservability",
        text: "For example, refund_bug reaches \"Revenue by Store\" on Store Sales — Today, and \"Stores with Negative Revenue\" on Pipeline Health.",
      },
      {
        tool: "dataObservability",
        text: "The lineage path back up shows whose issue it is.",
      },
    ],
    bits: "Ask Bits Chat which dashboards read marts.daily_store_sales, and which monitor alerted first.",
    resolve: "John tells readers which dashboards to hold off on, and hands the issue to the owner the lineage points to.",
    tools: ["dataObservability", "bits"],
    quiz: [
      {
        prompt: "How does John find the dashboards a bad table affects?",
        options: [
          "Lineage downstream from the table to Metabase",
          "Searching Metabase for the table name",
          "Asking each analyst",
        ],
        answer: 0,
        why: "Data Observability's lineage reaches past the marts to the Metabase questions and dashboards built on them.",
      },
      {
        prompt: "How does John know whose problem it is?",
        options: [
          "The monitor's name",
          "The lineage path back up to where the problem starts",
          "Whoever last edited the dashboard",
        ],
        answer: 1,
        why: "Following lineage upstream ends at the first broken step, and each step has an owner.",
      },
    ],
  },
];

export const personaBySlug = Object.fromEntries(personas.map((p) => [p.slug, p])) as Record<string, Persona>;
