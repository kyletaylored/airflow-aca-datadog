// The monitors the scenarios alert on, as they're defined in the demo's
// Datadog org (names, types, queries, schedules and messages), and as each was
// tested against its scenario on 2026-11-16. The queries are in
// docs/data-pipeline-demo.md, "Monitors to create".

import type { ToolId } from "./tools";

export interface Monitor {
  id: string;
  name: string;
  product: ToolId;
  // "Column monitor", "Custom SQL monitor", "Source to Target monitor", "Job monitor".
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
      { label: "Target", code: "Row Count of staging.orders", lang: "sql" },
      { label: "Alert query", code: 'formula("query1").last("75m") < -50', lang: "formula" },
    ],
    alertWhen: "staging.orders falls more than 50 rows behind raw.orders (the difference is target minus source, so it goes negative)",
    window: "Last 75 minutes",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "staging.orders is more than 50 orders behind raw.orders (test orders excluded). Rows are piling up between the load and staging, so the marts are working from old data. Likely cause: staging_orders failing, as the breaking_schema chaos switch makes it.",
    note: "When staging is healthy, the two counts match and the difference is 0. While staging_orders was failing during testing, it read -149.",
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
    name: "[contoso_dw] marts.daily_store_sales revenue today (anomaly)",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Today's revenue",
    detection: "Anomaly detection",
    target: "marts.daily_store_sales",
    purpose: "detecting revenue that is plausible but incorrect",
    tracks: "today's total revenue across all stores, compared with its usual daily pattern",
    query: [
      {
        label: "SQL",
        code: "SELECT coalesce(sum(revenue_usd), 0) AS dd_value\nFROM marts.daily_store_sales\nWHERE sales_date = (now() AT TIME ZONE 'UTC')::date",
        lang: "sql",
      },
    ],
    alertWhen: "Today's revenue moves outside its expected range",
    window: "Last 2 hours",
    schedule: "5, 20, 35 and 50 past the hour",
    message:
      "Today's total revenue in marts.daily_store_sales is outside its expected range, so the store sales reports are wrong. Likely cause: a bad change to the mart's revenue logic, as the refund_bug chaos switch makes (every Denver order counted as a refund).",
    note: "Anomaly detection requires approximately one week of history, including a weekend, before it can alert.",
  },
};

// "a Column monitor" / "an Anomaly …": the lesson's sentence needs the article.
export function withArticle(words: string): string {
  return /^[aeiou]/i.test(words) ? `an ${words}` : `a ${words}`;
}
