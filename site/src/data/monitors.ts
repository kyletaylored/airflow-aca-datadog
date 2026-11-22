// The monitors the scenarios alert on, as they're defined in the demo's
// Datadog org (names, types, queries, schedules and messages), and as each was
// tested against its scenario on 2026-11-16 (the column contract, dashboard
// freshness and customer_value coverage monitors on 2026-11-19). The queries
// are in docs/data-pipeline-demo.md, "Monitors to create".

import type { ToolId } from "./tools";

export interface Monitor {
  id: string;
  name: string;
  product: ToolId;
  // "Column monitor", "Custom SQL monitor", "Source to Target monitor", "Job monitor", "Metric monitor".
  type: string;
  measure: string;
  detection: "Threshold" | "Anomaly detection";
  // What it's attached to: a table, a column, a DAG.
  target: string;
  // The sentence the lesson builds: "We've made a <type> for <purpose>, by tracking <tracks>."
  purpose: string;
  tracks: string;
  query: { label: string; code: string; lang: "sql" | "formula" }[];
  alertWhen: string;
  window: string;
  // When the monitor runs. A monitor with no schedule runs once, at creation.
  schedule: string;
  message: string;
  note?: string;
}

export const monitors: Record<string, Monitor> = {
  customerNulls: {
    id: "customerNulls",
    name: "[contoso_dw] staging.orders customer_id nulls",
    product: "dataObservability",
    type: "Column monitor",
    measure: "Null ratio, in percent",
    detection: "Threshold",
    target: "staging.orders.customer_id",
    purpose: "detecting orders that arrive without a customer",
    tracks: "the share of the last hour's orders in staging.orders with no customer_id",
    query: [
      { label: "Measure", code: "nullness(staging.orders.customer_id)", lang: "formula" },
      { label: "WHERE", code: "ordered_at > now() - interval '1 hour'", lang: "sql" },
      { label: "Alert query", code: 'formula("query1").last("2h") > 1', lang: "formula" },
    ],
    alertWhen: "More than 1% of the last hour's orders have no customer_id (normally 0%)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "More than 1% of the orders placed in the last hour reached staging.orders without a customer_id (normally 0%). Likely cause: the null_customers chaos switch, standing in for a storefront release that stopped sending it. marts.customer_value loses those orders downstream.",
    note: "Measuring only the last hour is what makes a single faulty batch visible: across the whole table, which holds 14 days of orders, the ratio would barely change. In testing, it read 5.68% after one null_customers run and cleared an hour later.",
  },
  jobFailures: {
    id: "jobFailures",
    name: "[contoso_dw] Airflow DAG {{job_name.name}} has failed runs",
    product: "djm",
    type: "Job monitor",
    measure: "Failed runs",
    detection: "Threshold",
    target: "Every Airflow DAG, one alert per DAG",
    purpose: "detecting a failed pipeline run as soon as it occurs",
    tracks: "failed runs of every Airflow DAG in Data Jobs Monitoring, grouped by DAG and environment",
    query: [
      { label: "Jobs", code: "all Airflow DAGs  (job type: airflow.dag)", lang: "formula" },
      { label: "Alert query", code: 'formula("failed_runs(run_query)").by(job_name,env).last(1h) > 0', lang: "formula" },
    ],
    alertWhen: "The latest run of a DAG failed",
    window: "Last 1 hour",
    schedule: "Continuously, as runs finish",
    message:
      "The latest run of {{job_name.name}} failed in the last hour, so whatever it builds isn't being updated. In the demo, when the DAG is contoso_orders, the likely cause is the breaking_schema chaos switch: it renames raw.orders.amount_cents after the load, and staging_orders fails.",
    note: "It counts trailing failed runs, so it recovers as soon as the DAG completes a successful run. Because it covers every DAG, it also detected a genuine outage during testing, when Azure Resource Manager began rejecting the scheduler's requests and every ACA Job DAG failed.",
  },
  rowGap: {
    id: "rowGap",
    name: "[contoso_dw] raw.orders to staging.orders row gap",
    product: "dataObservability",
    type: "Source to Target monitor",
    measure: "Row count difference",
    detection: "Threshold",
    target: "raw.orders → staging.orders",
    purpose: "detecting orders that load but never reach staging",
    tracks: "the row count of staging.orders against the row count of raw.orders, excluding test orders",
    query: [
      { label: "Source", code: "Row Count of raw.orders WHERE channel <> 'test'", lang: "sql" },
      { label: "Target", code: "Row Count of staging.orders WHERE channel <> 'test'", lang: "sql" },
      { label: "Alert query", code: 'formula("query1").last("75m") < -50', lang: "formula" },
    ],
    alertWhen: "staging.orders falls more than 50 rows behind raw.orders (the difference is target minus source, so it goes negative)",
    window: "Last 75 minutes",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "staging.orders is more than 50 orders behind raw.orders (test orders excluded). Rows are piling up between the load and staging, so the marts are working from old data. Likely cause: staging_orders failing, as the breaking_schema chaos switch makes it.",
    note: "When staging is healthy, the two counts match and the difference is 0. While staging_orders was failing during testing, it read -149. The target has the same filter as the source, though staging already drops test orders: without a WHERE clause, the gap swung by up to 300 rows while staging was healthy, probably because Datadog then reads the target's count from Postgres table statistics, which are estimates. With the filter, both sides are counted the same way.",
  },
  rowCount: {
    id: "rowCount",
    name: "[contoso_dw] raw.orders row count",
    product: "dataObservability",
    type: "Table monitor",
    measure: "Row count",
    detection: "Threshold",
    target: "raw.orders",
    purpose: "detecting when the load stops or declines",
    tracks: "the rows loaded into raw.orders in the last 15 minutes, which corresponds to the most recent run",
    query: [
      { label: "Measure", code: "row_count(raw.orders)", lang: "formula" },
      { label: "WHERE", code: "_loaded_at > now() - interval '15 minutes'", lang: "sql" },
      { label: "Alert query", code: 'formula("query1").last("2h") < 100', lang: "formula" },
    ],
    alertWhen: "Fewer than 100 rows loaded in the last 15 minutes (about 200 to 500 is normal)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "Fewer than 100 orders were loaded into raw.orders in the last 15 minutes (about 300 is normal for one run). Likely causes: the volume_drop or skip_load chaos switch, or the order stream stalling.",
    note: "In testing, it read 0 fifteen minutes after the load stopped, and recovered on the next run.",
  },
  martFreshness: {
    id: "martFreshness",
    name: "[contoso_dw] marts.daily_store_sales freshness",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Freshness, in minutes",
    detection: "Threshold",
    target: "marts.daily_store_sales",
    purpose: "detecting when the store sales reports become stale",
    tracks: "the minutes since the mart was last rebuilt, from its updated_at column",
    query: [
      {
        label: "SQL",
        code: "SELECT EXTRACT(EPOCH FROM now() - max(updated_at)) / 60 AS dd_value\nFROM marts.daily_store_sales",
        lang: "sql",
      },
    ],
    alertWhen: "More than 45 minutes since the last rebuild (three missed runs)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "marts.daily_store_sales in contoso_dw hasn't been rebuilt in over 45 minutes, so the store sales reports are stale. Likely causes: a failed or paused contoso_orders run, or the skip_load chaos switch.",
  },
  stagingFreshness: {
    id: "stagingFreshness",
    name: "[contoso_dw] staging.orders freshness",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Freshness, in minutes",
    detection: "Threshold",
    target: "staging.orders",
    purpose: "detecting when new orders stop arriving",
    tracks: "the minutes since the newest order in staging.orders was placed",
    query: [
      {
        label: "SQL",
        code: "SELECT EXTRACT(EPOCH FROM now() - max(ordered_at)) / 60 AS dd_value\nFROM staging.orders",
        lang: "sql",
      },
    ],
    alertWhen: "No new order for more than 45 minutes",
    window: "Last 2 hours",
    schedule: "Every 15 minutes",
    message:
      "No new orders in staging.orders for over 45 minutes. Likely causes: the skip_load chaos switch, contoso_orders not running, or the order stream stopped.",
  },
  negativeRevenue: {
    id: "negativeRevenue",
    name: "[contoso_dw] marts.daily_store_sales negative revenue",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Business rule",
    detection: "Threshold",
    target: "marts.daily_store_sales",
    purpose: "detecting revenue that cannot be correct",
    tracks: "the number of store-days whose revenue_usd is below zero",
    query: [
      {
        label: "SQL",
        code: "SELECT count(*) AS dd_value\nFROM marts.daily_store_sales\nWHERE revenue_usd < 0",
        lang: "sql",
      },
    ],
    alertWhen: "Any row has negative revenue",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message: "marts.daily_store_sales has rows with negative revenue, which got past check_staging.",
  },
  revenueAnomaly: {
    id: "revenueAnomaly",
    name: "[contoso_dw] marts.daily_store_sales revenue, last 7 days (anomaly)",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Revenue over the last 7 complete days",
    detection: "Anomaly detection",
    target: "marts.daily_store_sales",
    purpose: "detecting revenue that is plausible but incorrect",
    tracks: "the total revenue across all stores for the last 7 complete days, compared with its usual level",
    query: [
      {
        label: "SQL",
        code: "SELECT coalesce(sum(revenue_usd), 0) AS dd_value\nFROM marts.daily_store_sales\nWHERE sales_date >= (now() AT TIME ZONE 'UTC')::date - 7\n  AND sales_date < (now() AT TIME ZONE 'UTC')::date",
        lang: "sql",
      },
    ],
    alertWhen: "The last 7 days' revenue moves outside its expected range",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "Total revenue in marts.daily_store_sales for the last 7 complete days is outside its expected range, so the store sales reports are wrong. The mart is rebuilt in full on every run, so a change to its logic rewrites every day at once. Likely cause: a bad change to the mart's revenue logic, as the refund_bug chaos switch makes (every Denver order counted as a refund).",
    note: "Completed days don't change, so the total stays almost flat between midnights, and a change to the mart's logic shows up at any hour. An earlier version measured today's revenue, which restarts from 0 at midnight UTC: early in the day its expected range began at 0, and refund_bug's result fell inside it. Anomaly detection requires approximately one week of history, including a weekend, before it can alert.",
  },
  columnContract: {
    id: "columnContract",
    name: "[contoso_dw] raw.orders column contract",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Schema differences",
    detection: "Threshold",
    target: "raw.orders",
    purpose: "detecting a change to the columns orders arrive with",
    tracks: "the columns raw.orders has beyond its 10 agreed ones, plus the agreed ones it no longer has, from PostgreSQL's information_schema",
    query: [
      {
        label: "SQL",
        code: "SELECT count(*) AS dd_value FROM (\n  (SELECT column_name::text FROM information_schema.columns\n   WHERE table_schema = 'raw' AND table_name = 'orders'\n   EXCEPT SELECT unnest(ARRAY['order_id', 'customer_id', 'store', 'channel', 'items',\n     'amount_cents', 'currency', 'ordered_at', '_batch_id', '_loaded_at']))\n  UNION ALL\n  (SELECT unnest(ARRAY['order_id', 'customer_id', 'store', 'channel', 'items',\n     'amount_cents', 'currency', 'ordered_at', '_batch_id', '_loaded_at'])\n   EXCEPT SELECT column_name::text FROM information_schema.columns\n   WHERE table_schema = 'raw' AND table_name = 'orders')\n) AS differences",
        lang: "sql",
      },
    ],
    alertWhen: "Any column is added, removed or renamed (normally 0 differences)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "raw.orders no longer matches its agreed columns: a column was added, removed or renamed (normally 0 differences). A field the storefront starts sending lands here first, and a rename or removal breaks staging_orders on its next run. Likely causes in the demo: the contoso_schema_flip DAG, which adds a promo_code column at 00:00, 06:00, 12:00 and 18:00 UTC and drops it 2 hours later; the schema_drift chaos switch (the same promo_code column); or the breaking_schema switch (amount_cents renamed). If the flip added the column, this recovers at its first check after the drop. Otherwise, agree the change with the order processor's owner, then update the agreed columns in this monitor's SQL.",
    note: "It reads PostgreSQL's own catalog of columns after every run, so it catches a change that lasts only minutes, which Datadog's schema change monitor can miss between collections of schema metadata. An added column, as schema_drift and the schema flip make, breaks nothing, so apart from Rajesh's raw schema change monitor, no other monitor notices it. A rename counts twice, once for the missing name and once for the new one: under breaking_schema, the job failure monitor reports that staging_orders failed, and this one reports which column changed.",
  },
  schemaChanges: {
    id: "schemaChanges",
    name: "[contoso_dw] raw schema changes",
    product: "dataObservability",
    type: "Schema change monitor",
    measure: "Schema changes detected",
    detection: "Threshold",
    target: "Every table in contoso_dw.raw",
    purpose: "detecting a change to the structure of any raw table",
    tracks: "the tables and columns Datadog finds added, removed, renamed or retyped in contoso_dw's raw schema, each time it collects the warehouse's schema metadata",
    query: [
      { label: "Data", code: "@account_entity_id:360dd5c607978958 @database:contoso_dw @schema:raw", lang: "formula" },
      {
        label: "Alert query",
        code: 'schema-changes("@account_entity_id:360dd5c607978958 @database:contoso_dw @schema:raw").rollup("count").last("10m") > 0',
        lang: "formula",
      },
    ],
    alertWhen: "Datadog detects any change in the last 10 minutes",
    window: "Last 10 minutes",
    schedule: "Each time Datadog collects the warehouse's schema metadata",
    message:
      "A table or column in contoso_dw's raw schema was added, removed, renamed or retyped. staging_orders and staging_customers read raw by column name, so check they still match. On Postgres a rename shows as one column removed and another added. In the demo, the contoso_schema_flip DAG causes this on a schedule: it adds raw.orders' promo_code column at 00:00, 06:00, 12:00 and 18:00 UTC and drops it 2 hours later. The schema_drift chaos switch adds the same column whenever you turn it on.",
    note: "Datadog's documentation lists schema change detection for Snowflake, BigQuery, Databricks and Redshift, but it detected this PostgreSQL server's changes too. It filters by the catalog's hierarchy, database then schema, so it covers every raw table, not only raw.orders. Datadog compares each collection of schema metadata with the one before, so a change that's undone before the next collection is never seen: for that reason, schema_drift keeps its column until you turn the switch off, and the schema flip keeps it for 2 hours, while breaking_schema's rename may come and go unseen. The column contract on raw.orders checks after every run, so it catches both.",
  },
  dashboardFreshness: {
    id: "dashboardFreshness",
    name: "[contoso_dw] Metabase dashboard marts freshness",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Freshness, in minutes",
    detection: "Threshold",
    target: "marts.daily_store_sales, marts.customer_value and marts.hourly_order_volume",
    purpose: "detecting when any dashboard is showing old figures",
    tracks: "the minutes since the least recently rebuilt of the three marts the Metabase dashboards read",
    query: [
      {
        label: "SQL",
        code: "SELECT EXTRACT(EPOCH FROM now() - least(\n  (SELECT max(updated_at) FROM marts.daily_store_sales),\n  (SELECT max(updated_at) FROM marts.customer_value),\n  (SELECT max(updated_at) FROM marts.hourly_order_volume)\n)) / 60 AS dd_value",
        lang: "sql",
      },
    ],
    alertWhen: "More than 45 minutes since the stalest mart was rebuilt (three missed runs; about 5 minutes is normal)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "At least one of the marts behind the Metabase dashboards (daily_store_sales, customer_value or hourly_order_volume) hasn't been rebuilt in over 45 minutes, so the dashboards show old figures without saying so. Likely causes: contoso_orders paused or failing, for example under the breaking_schema chaos switch. Check the contoso_orders runs in Data Jobs Monitoring to find the task that stopped.",
    note: "One monitor for all three dashboards' tables, because the question John has to answer is whether any report is out of date. The tables' own monitors and their lineage then say which one, and why.",
  },
  customerValueCoverage: {
    id: "customerValueCoverage",
    name: "[contoso_dw] orders missing from marts.customer_value",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Share of orders, in percent",
    detection: "Threshold",
    target: "marts.customer_value",
    purpose: "detecting orders the Customer Value dashboard leaves out",
    tracks: "the share of the last hour's orders in staging.orders with no matching customer in staging.customers, which the mart's join drops",
    query: [
      {
        label: "SQL",
        code: "SELECT coalesce(100.0 * count(*) FILTER (WHERE c.customer_id IS NULL)\n  / nullif(count(*), 0), 0) AS dd_value\nFROM staging.orders AS o\nLEFT JOIN staging.customers AS c ON c.customer_id = o.customer_id\nWHERE o.ordered_at > now() - interval '1 hour'",
        lang: "sql",
      },
    ],
    alertWhen: "More than 1% of the last hour's orders have no matching customer (normally 0%)",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "More than 1% of the last hour's orders have no matching customer (normally 0%), so marts.customer_value leaves them out and the Customer Value dashboard undercounts orders and revenue. The mart joins orders to customers, so an order without a customer_id, or with one that never loaded, drops out without an error. Likely cause: the null_customers chaos switch, standing in for a storefront release that stopped sending customer_id. If the staging.orders customer_id nulls monitor isn't alerting as well, look at the customers load instead.",
    note: "It measures what the dashboard loses, not why: an order with no customer_id and an order whose customer never loaded look the same here. The customer_id null monitor on staging.orders tells the two apart.",
  },
  schedulerMemory: {
    id: "schedulerMemory",
    name: "[contoso_dw] aflo-scheduler memory near its limit",
    product: "infra",
    type: "Metric monitor",
    measure: "Working set, in bytes",
    detection: "Threshold",
    target: "The aflo-scheduler container app",
    purpose: "detecting when the container every task runs in is close to its memory limit",
    tracks: "the scheduler container's working set, from Azure Monitor, against 80% of its 4 GiB limit",
    query: [
      {
        label: "Query",
        code: "max(last_10m):max:azure.app_containerapps.working_set_bytes{name:aflo-scheduler,resource_group:rg-ese-tola-airflow-aca-cus} > 3435973837",
        lang: "formula",
      },
    ],
    alertWhen: "The working set passes 3.2 GiB, 80% of the limit (a warning at 2.7 GiB; about 1.8 GB is normal)",
    window: "Last 10 minutes",
    schedule: "Continuously, as the metric arrives (about 3 minutes behind)",
    message:
      "aflo-scheduler is using more than 80% of its 4 GiB memory limit (normally 1.8 to 1.95 GB). With LocalExecutor every Airflow task runs in this container, so at the limit task processes are killed without reporting their state: Airflow logs \"task state changed externally\", retries the task minutes later, and loads arrive late. Likely cause in the demo: the memory_pressure chaos switch on contoso_orders. Otherwise, more or heavier tasks than the container's size allows. The threshold is 80% of the size set in infra/main.bicep; change both together.",
    note: "A metric monitor cannot read the limit, so the threshold is written as a number. Azure also reports memory as a percentage of the limit, but that metric arrives about 45 minutes late, too late to warn before tasks die.",
  },
};

// "a Column monitor" / "an Anomaly …": the lesson's sentence needs the article.
export function withArticle(words: string): string {
  return /^[aeiou]/i.test(words) ? `an ${words}` : `a ${words}`;
}
