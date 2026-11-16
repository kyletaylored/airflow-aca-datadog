// The monitors the scenarios alert on, as they're defined in the demo's
// Datadog org (names, types, queries and messages). The queries are in
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
  message: string;
  note?: string;
}

export const monitors: Record<string, Monitor> = {
  customerNulls: {
    id: "customerNulls",
    name: "[contoso_dw] staging.orders customer_id nulls",
    product: "dataObservability",
    type: "Column monitor",
    measure: "Null ratio",
    detection: "Anomaly detection",
    target: "staging.orders.customer_id",
    purpose: "spotting orders that arrive without a customer",
    tracks: "the share of staging.orders rows with no customer_id, compared with its usual range",
    query: [
      { label: "Measure", code: "nullness(staging.orders.customer_id)", lang: "formula" },
      { label: "Alert query", code: 'formula("anomalies(query1)").last("2h") > 0', lang: "formula" },
    ],
    alertWhen: "The null ratio moves outside its expected range",
    window: "Last 2 hours",
    message:
      "The null ratio of customer_id in staging.orders moved outside its expected range. Likely cause: the null_customers chaos switch. marts.customer_value loses those orders downstream.",
  },
  jobFailures: {
    id: "jobFailures",
    name: "[contoso_dw] contoso_orders failed runs",
    product: "djm",
    type: "Job monitor",
    measure: "Failed runs",
    detection: "Threshold",
    target: "contoso_orders (Airflow DAG)",
    purpose: "knowing as soon as a pipeline run fails",
    tracks: "failed runs of the contoso_orders DAG in Data Jobs Monitoring, grouped by job",
    query: [
      { label: "Jobs", code: "job_name:contoso_orders  (job type: airflow.dag)", lang: "formula" },
      { label: "Alert query", code: 'formula("failed_runs(run_query)").by(job_name).last(1h) > 0', lang: "formula" },
    ],
    alertWhen: "Any run fails",
    window: "Last 1 hour",
    message:
      "A contoso_orders DAG run failed, so the marts aren't being rebuilt. The breaking_schema chaos switch does this: it renames raw.orders.amount_cents after the load, and staging_orders fails.",
    note: "It counts trailing failed runs, so it recovers as soon as a run succeeds.",
  },
  rowGap: {
    id: "rowGap",
    name: "[contoso_dw] raw.orders to staging.orders row gap",
    product: "dataObservability",
    type: "Source to Target monitor",
    measure: "Row count difference",
    detection: "Threshold",
    target: "raw.orders → staging.orders",
    purpose: "catching orders that load but never reach staging",
    tracks: "the row count of raw.orders (test orders excluded) against the row count of staging.orders",
    query: [
      { label: "Source", code: "SELECT count(*) AS dd_value FROM raw.orders WHERE channel <> 'test'", lang: "sql" },
      { label: "Target", code: "SELECT count(*) AS dd_value FROM staging.orders", lang: "sql" },
    ],
    alertWhen: "staging.orders falls more than 50 rows behind raw.orders",
    window: "Last 2 hours",
    message:
      "staging.orders is more than 50 orders behind raw.orders (test orders excluded). Rows are piling up between the load and staging, so the marts are working from old data. Likely cause: staging_orders failing, as the breaking_schema chaos switch makes it.",
  },
  martFreshness: {
    id: "martFreshness",
    name: "[contoso_dw] marts.daily_store_sales freshness",
    product: "dataObservability",
    type: "Custom SQL monitor",
    measure: "Freshness, in minutes",
    detection: "Threshold",
    target: "marts.daily_store_sales",
    purpose: "knowing when the store sales reports go stale",
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
    purpose: "noticing when new orders stop arriving",
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
    purpose: "catching revenue that can't be right",
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
    purpose: "catching revenue that's plausible but wrong",
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
    message:
      "Today's total revenue in marts.daily_store_sales is outside its expected range, so the store sales reports are wrong. Likely cause: a bad change to the mart's revenue logic, as the refund_bug chaos switch makes (every Denver order counted as a refund).",
    note: "Anomaly detection needs about a week of history, including a weekend, before it alerts.",
  },
};

// "a Column monitor" / "an Anomaly …": the lesson's sentence needs the article.
export function withArticle(words: string): string {
  return /^[aeiou]/i.test(words) ? `an ${words}` : `a ${words}`;
}
