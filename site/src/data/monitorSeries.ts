// What each monitor measured while its scenario ran in the demo, so a reader
// can see the metric without access to the demo's Datadog org. The values are
// copied from the monitors' recorded history (Data Observability, Data Jobs
// Monitoring and Azure metrics) on 2026-11-19; the one exception, the revenue
// anomaly monitor, is marked as an illustration, because it needs a week of
// history before Datadog draws its expected range.

export interface ChartEvent {
  // When the scenario started and, for a span, ended (UTC).
  from: string;
  to?: string;
  label: string;
}

interface Base {
  // What the y-axis measures, in a few words: "Null ratio".
  measure: string;
  // How a value reads: "%", " min", " rows", " GiB", "" (a count).
  unit: string;
  decimals?: number;
  events: ChartEvent[];
  // One sentence under the chart: what it shows and when it was recorded.
  caption: string;
  illustrative?: boolean;
}

export interface LineSeries extends Base {
  kind: "line";
  // The first evaluation (UTC) and the minutes between evaluations, or, when
  // the evaluations aren't evenly spaced, each one's time in `times`.
  start: string;
  stepMinutes: number;
  times?: string[];
  values: number[];
  threshold: number;
  // Which side of the threshold alerts.
  alertWhen: "above" | "below";
  // An anomaly monitor's expected range, one [lower, upper] per value.
  band?: [number, number][];
}

export interface RunSeries extends Base {
  kind: "runs";
  runs: { at: string; failed: boolean; note?: string }[];
  // What the markers are, when they aren't DAG runs: the table's column
  // heading, each marker's readout, and the legend's two keys.
  labels?: { item: string; ok: string; failed: string; okKey: string; failedKey: string };
  // When the monitor was in alert.
  alerts: { from: string; to: string }[];
  // The chart's time range.
  from: string;
  to: string;
}

export type MonitorSeries = LineSeries | RunSeries;

export const monitorSeries: Record<string, MonitorSeries> = {
  customerNulls: {
    kind: "line",
    measure: "Null ratio, last hour's orders",
    unit: "%",
    decimals: 1,
    start: "2026-11-18T23:50:00Z",
    stepMinutes: 15,
    values: [0, 0, 6.44, 6.41, 6.31, 1.5, 0, 0, 0, 0],
    threshold: 1,
    alertWhen: "above",
    events: [{ from: "2026-11-19T00:10:05Z", label: "null_customers run" }],
    caption:
      "One run with null_customers at 00:10 UTC. The ratio stays above 1% for as long as those orders are in the last hour, then clears on its own.",
  },
  rowCount: {
    kind: "line",
    measure: "Rows loaded, last 15 minutes",
    unit: " rows",
    start: "2026-11-19T00:05:00Z",
    stepMinutes: 15,
    values: [575, 282, 307, 311, 269, 370, 0, 0, 0, 0, 1457, 285, 309, 265, 317, 293],
    threshold: 100,
    alertWhen: "below",
    events: [{ from: "2026-11-19T01:22:00Z", to: "2026-11-19T02:21:42Z", label: "contoso_orders paused" }],
    caption:
      "contoso_orders paused for an hour. The count drops to 0 at the first check, and the first check after the pause counts 1,457, the orders that were held back.",
  },
  stagingFreshness: {
    kind: "line",
    measure: "Minutes since the newest order",
    unit: " min",
    start: "2026-11-19T00:45:00Z",
    stepMinutes: 15,
    values: [17.9, 17.9, 17.8, 12.7, 27.7, 42.7, 57.7, 12.5, 17.5, 17.4, 17.4],
    threshold: 45,
    alertWhen: "above",
    events: [{ from: "2026-11-19T01:22:00Z", to: "2026-11-19T02:21:42Z", label: "contoso_orders paused" }],
    caption:
      "The same pause. Freshness normally saws between about 12 and 18 minutes as each run lands, then climbs 15 minutes per check until it passes 45.",
  },
  martFreshness: {
    kind: "line",
    measure: "Minutes since the mart was rebuilt",
    unit: " min",
    start: "2026-11-19T00:50:00Z",
    stepMinutes: 15,
    values: [4.6, 4.7, 19.7, 13.2, 28.2, 43.2, 58.2, 4.8, 4.7, 4.8, 4.8],
    threshold: 45,
    alertWhen: "above",
    events: [
      { from: "2026-11-19T01:09:00Z", to: "2026-11-19T01:21:36Z", label: "breaking_schema runs" },
      { from: "2026-11-19T01:22:00Z", to: "2026-11-19T02:21:42Z", label: "contoso_orders paused" },
    ],
    caption:
      "The failed breaking_schema runs skip the mart, then the pause stops every rebuild. The first run after the pause brings it back to about 5 minutes.",
  },
  negativeRevenue: {
    kind: "line",
    measure: "Store-days with negative revenue",
    unit: "",
    start: "2026-11-18T23:35:00Z",
    stepMinutes: 15,
    values: [0, 0, 0, 6, 0, 0, 0, 0],
    threshold: 0,
    alertWhen: "above",
    events: [{ from: "2026-11-19T00:10:05Z", to: "2026-11-19T00:16:27Z", label: "refund_bug runs" }],
    caption:
      "Two runs with refund_bug, at 00:10 and 00:16 UTC, rebuild the mart with every Denver order counted as a refund, so six of Denver's days go negative. The 00:30 scheduled run rebuilds it correctly.",
  },
  rowGap: {
    kind: "line",
    measure: "staging.orders rows minus raw.orders rows",
    unit: " rows",
    start: "2026-11-19T00:20:00Z",
    stepMinutes: 15,
    values: [0, 0, 0, 0, -325, 0, 0, 0, 0, 0],
    threshold: -50,
    alertWhen: "below",
    events: [{ from: "2026-11-19T01:08:59Z", to: "2026-11-19T01:21:36Z", label: "breaking_schema runs" }],
    caption:
      "Three breaking_schema runs in a row keep staging_orders failing while raw.orders keeps loading, so staging falls 325 orders behind until the next scheduled run repairs it.",
  },
  jobFailures: {
    kind: "runs",
    measure: "contoso_orders runs",
    unit: "",
    from: "2026-11-19T00:25:00Z",
    to: "2026-11-19T01:30:00Z",
    runs: [
      { at: "2026-11-19T00:30:00Z", failed: false },
      { at: "2026-11-19T00:45:00Z", failed: false },
      { at: "2026-11-19T00:46:22Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T00:50:36Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T00:54:51Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T01:00:00Z", failed: false },
      { at: "2026-11-19T01:08:59Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T01:13:12Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T01:17:25Z", failed: true, note: "breaking_schema" },
      { at: "2026-11-19T01:21:37Z", failed: false },
    ],
    alerts: [
      { from: "2026-11-19T00:53:16Z", to: "2026-11-19T01:02:16Z" },
      { from: "2026-11-19T01:14:16Z", to: "2026-11-19T01:23:16Z" },
    ],
    events: [],
    caption:
      "Two rounds of three breaking_schema runs. The monitor alerts once a failed run finishes and recovers when the next run succeeds.",
  },
  revenueAnomaly: {
    kind: "line",
    measure: "Revenue, last 7 complete days (USD millions)",
    unit: "",
    decimals: 2,
    illustrative: true,
    start: "2026-11-23T00:05:00Z",
    stepMinutes: 360,
    values: [9.52, 9.52, 9.53, 9.53, 9.49, 9.49, 9.51, 9.51, 9.55, 9.55, 6.79, 9.55, 9.54, 9.54, 9.5, 9.5],
    band: [
      [9.18, 9.86], [9.18, 9.86], [9.19, 9.87], [9.19, 9.87], [9.16, 9.84], [9.16, 9.84], [9.17, 9.85], [9.17, 9.85],
      [9.2, 9.89], [9.2, 9.89], [9.21, 9.9], [9.21, 9.9], [9.2, 9.89], [9.2, 9.89], [9.17, 9.86], [9.17, 9.86],
    ],
    threshold: 0,
    alertWhen: "above",
    events: [{ from: "2026-11-25T12:10:00Z", label: "refund_bug run" }],
    caption:
      "An illustration at the level the monitor records, about $9.5M a week. With Denver's orders counted as refunds, the total falls by about two sevenths, far below the range Datadog expects.",
  },
  columnContract: {
    kind: "line",
    measure: "Columns added, removed or renamed",
    unit: "",
    start: "2026-11-19T22:53:00Z",
    stepMinutes: 15,
    times: [
      "2026-11-19T22:53:00Z", "2026-11-19T23:05:00Z", "2026-11-19T23:20:00Z", "2026-11-19T23:35:00Z",
      "2026-11-19T23:50:00Z", "2026-11-20T00:05:00Z", "2026-11-20T00:20:00Z", "2026-11-20T00:35:00Z",
    ],
    values: [0, 1, 0, 2, 0, 0, 0, 0],
    threshold: 0,
    alertWhen: "above",
    events: [
      { from: "2026-11-19T23:01:06Z", label: "schema_drift run" },
      { from: "2026-11-19T23:31:10Z", to: "2026-11-19T23:35:23Z", label: "breaking_schema run" },
    ],
    caption:
      "In this test, schema_drift added promo_code (1 difference) and the next scheduled run dropped it; the switch now keeps the column until a run turns it off. breaking_schema renames amount_cents to amount_minor, which counts twice: one agreed column missing, one new.",
  },
  schemaChanges: {
    kind: "runs",
    measure: "Schema changes to contoso_dw.raw",
    unit: "",
    from: "2026-11-19T22:50:00Z",
    to: "2026-11-20T00:35:00Z",
    labels: {
      item: "Schema change",
      ok: "Detected",
      failed: "Not detected",
      okKey: "Change Datadog detected",
      failedKey: "Change no collection saw",
    },
    runs: [
      { at: "2026-11-19T23:01:06Z", failed: false, note: "promo_code added by schema_drift; detected 23:03" },
      { at: "2026-11-19T23:15:00Z", failed: false, note: "promo_code dropped by the next run; detected 00:23" },
      { at: "2026-11-19T23:35:23Z", failed: true, note: "amount_cents renamed by breaking_schema" },
      { at: "2026-11-19T23:45:00Z", failed: true, note: "amount_cents renamed back by the next run" },
    ],
    alerts: [],
    events: [],
    caption:
      "The changes the chaos switches made to raw.orders during the column contract's test, and which ones Datadog's schema metadata collection detected (times in UTC). The rename lasted 10 minutes and no collection saw it. The monitor was created the next day, so no alert is shown; it alerts for 10 minutes after each detection.",
  },
  dashboardFreshness: {
    kind: "line",
    measure: "Minutes since the stalest dashboard mart was rebuilt",
    unit: " min",
    decimals: 1,
    start: "2026-11-19T22:53:00Z",
    stepMinutes: 15,
    times: [
      "2026-11-19T22:53:00Z", "2026-11-19T23:05:00Z", "2026-11-19T23:20:00Z", "2026-11-19T23:35:00Z",
      "2026-11-19T23:50:00Z", "2026-11-20T00:05:00Z", "2026-11-20T00:20:00Z", "2026-11-20T00:35:00Z",
      "2026-11-20T00:50:00Z",
    ],
    values: [7.8, 3.9, 4.9, 4.9, 4.9, 19.9, 34.9, 49.9, 4.9],
    threshold: 45,
    alertWhen: "above",
    events: [{ from: "2026-11-19T23:46:30Z", to: "2026-11-20T00:36:35Z", label: "contoso_orders paused" }],
    caption:
      "contoso_orders paused for 50 minutes after its 23:45 run. The marts go 15 minutes staler at each check and pass 45 at 00:35; the first run after the pause rebuilds all three.",
  },
  customerValueCoverage: {
    kind: "line",
    measure: "Last hour's orders missing from customer_value",
    unit: "%",
    decimals: 2,
    start: "2026-11-19T22:53:00Z",
    stepMinutes: 15,
    times: [
      "2026-11-19T22:53:00Z", "2026-11-19T23:05:00Z", "2026-11-19T23:20:00Z", "2026-11-19T23:35:00Z",
      "2026-11-19T23:50:00Z", "2026-11-20T00:05:00Z", "2026-11-20T00:20:00Z", "2026-11-20T00:35:00Z",
    ],
    values: [0, 2.24, 2.43, 2.53, 2.55, 0, 0, 0],
    threshold: 1,
    alertWhen: "above",
    events: [{ from: "2026-11-19T23:01:06Z", label: "null_customers run" }],
    caption:
      "One run with null_customers at 23:01 UTC. Its orders stay in the one-hour window until about 00:01, so the share holds above 1% for an hour, then drops to 0.",
  },
  schedulerMemory: {
    kind: "line",
    measure: "Working set",
    unit: " GiB",
    decimals: 2,
    start: "2026-11-19T18:40:00Z",
    stepMinutes: 5,
    values: [1.93, 2.0, 1.68, 1.68, 2.24, 3.51, 3.54, 3.47, 2.15, 1.7, 2.05, 1.95, 1.96],
    threshold: 3.2,
    alertWhen: "above",
    events: [{ from: "2026-11-19T19:04:31Z", to: "2026-11-19T19:14:34Z", label: "memory_pressure holds memory" }],
    caption:
      "memory_pressure holds memory for 10 minutes, taking the 4 GiB container to about 3.5 GiB. The metric arrives about 3 minutes behind, so the alert starts at 19:08 UTC.",
  },
};
