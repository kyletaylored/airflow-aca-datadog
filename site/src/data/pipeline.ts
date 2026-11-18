// Every step an order passes through, who owns it, and what Datadog watches
// there. Drawn by PipelineFlow; written from services/, airflow/dags/ and
// airflow/dags/contoso_dw/.

import type { ToolId } from "./tools";

export type NodeId =
  | "storefront"
  | "topic"
  | "processor"
  | "landing"
  | "ingest"
  | "raw"
  | "stagingTask"
  | "staging"
  | "martTasks"
  | "marts"
  | "metabase";

export type Owner = "dave" | "rajesh" | "emily" | "priya" | "john";

// The stages of the data engineering lifecycle, in the vendor-neutral names of Reis and Housley's
// Fundamentals of Data Engineering (O'Reilly, 2022). The terminology lesson explains each one.
export type Stage = "generation" | "ingestion" | "storage" | "transformation" | "serving";

export const stages: { id: Stage; name: string; meaning: string }[] = [
  { id: "generation", name: "Generation", meaning: "The applications and devices where data is first created, such as a checkout, a sensor or a CRM." },
  { id: "ingestion", name: "Ingestion", meaning: "Moving data from those sources into the data platform, in batches or as a continuous stream." },
  { id: "storage", name: "Storage", meaning: "Retaining data at each point in the pipeline: object storage for files, warehouse tables for structured data." },
  { id: "transformation", name: "Transformation", meaning: "Cleaning, combining and reshaping stored data into a form that answers business questions." },
  { id: "serving", name: "Serving", meaning: "Delivering the result to its consumers: dashboards, reports, machine learning models and other applications." },
];

export interface PipelineNode {
  id: NodeId;
  title: string;
  kind: string;
  owner: Owner;
  stage: Stage;
  detail: string;
  watchedBy: ToolId[];
  // Position in the diagram: three rows, snaking left to right, then back.
  x: number;
  y: number;
}

export interface PipelineEdge {
  from: NodeId;
  to: NodeId;
  label?: string;
  // Which side of each node the edge leaves and enters.
  fromSide: "left" | "right" | "top" | "bottom";
  toSide: "left" | "right" | "top" | "bottom";
}

const COL = 310;
const ROW = 190;

export const pipelineNodes: PipelineNode[] = [
  {
    id: "storefront",
    title: "contoso-storefront",
    kind: "Python service · Container App",
    owner: "dave",
    stage: "generation",
    detail:
      "Contoso's checkout. When a shopper places an order, it publishes one JSON message to the orders topic, keyed by store. In this demo it simulates real shoppers: approximately 20 orders a minute across seven stores and four channels (web, mobile, store and test).",
    watchedBy: ["apm", "dsm"],
    x: 0,
    y: 0,
  },
  {
    id: "topic",
    title: "orders topic",
    kind: "Azure Event Hubs · Kafka endpoint",
    owner: "dave",
    stage: "ingestion",
    detail:
      "Event Hubs Standard, used through its Kafka-compatible endpoint over SASL and TLS. It retains each order message until the order processor reads it.",
    watchedBy: ["dsm"],
    x: COL,
    y: 0,
  },
  {
    id: "processor",
    title: "order-processor",
    kind: "Python consumer · Container App",
    owner: "dave",
    stage: "ingestion",
    detail:
      "Reads the topic as consumer group order-processor and drops any message missing a required field. Every 5 minutes it writes the valid orders as one file to the landing zone, and only then commits its Kafka offsets, so a crash causes a batch to be replayed rather than lost. Each batch is reported to Datadog as an OpenLineage run.",
    watchedBy: ["apm", "dsm", "dataObservability"],
    x: COL * 2,
    y: 0,
  },
  {
    id: "landing",
    title: "Landing zone",
    kind: "ADLS Gen2 · container landing",
    owner: "dave",
    stage: "storage",
    detail:
      "One newline-delimited JSON file per batch, one order per line, at orders/dt=2026-11-16/hour=14/batch-20261116T143000-1a2b3c4d.json. The dt= and hour= folders sort the files by the UTC date and hour they landed, so the loader can locate new files without reading earlier ones.",
    watchedBy: ["dataObservability"],
    x: COL * 3,
    y: 0,
  },
  {
    id: "ingest",
    title: "ingest_orders",
    kind: "Airflow task · Python",
    owner: "emily",
    stage: "ingestion",
    detail:
      "Every 15 minutes, lists the landing files it has not yet loaded, inserts their orders into raw.orders, and records each file name in raw._ingested_files, so no file loads twice.",
    watchedBy: ["djm", "apm"],
    x: COL * 3,
    y: ROW,
  },
  {
    id: "raw",
    title: "raw.orders",
    kind: "Postgres table",
    owner: "rajesh",
    stage: "storage",
    detail:
      "Orders exactly as they landed, with the batch file each one came from. Kept for 14 days.",
    watchedBy: ["dataObservability", "dbm"],
    x: COL * 2,
    y: ROW,
  },
  {
    id: "stagingTask",
    title: "staging_orders",
    kind: "Airflow task · SQL",
    owner: "emily",
    stage: "transformation",
    detail:
      "Rebuilds staging.orders from raw.orders: drops test orders, converts amount_cents to amount_usd, and adds order_date. Then check_staging fails the run if staging is empty or has a negative amount.",
    watchedBy: ["djm"],
    x: COL,
    y: ROW,
  },
  {
    id: "staging",
    title: "staging.orders",
    kind: "Postgres table",
    owner: "rajesh",
    stage: "storage",
    detail: "Cleaned, typed orders, ready for the marts. Rebuilt on every run.",
    watchedBy: ["dataObservability", "dbm"],
    x: 0,
    y: ROW,
  },
  {
    id: "martTasks",
    title: "Mart builds",
    kind: "3 Airflow tasks · SQL",
    owner: "priya",
    stage: "transformation",
    detail:
      "Run after check_staging passes. Each rebuilds one mart from staging: sales per store per day, value per customer (joined to staging.customers), and orders per store per hour.",
    watchedBy: ["djm"],
    x: 0,
    y: ROW * 2,
  },
  {
    id: "marts",
    title: "marts tables",
    kind: "Postgres tables",
    owner: "rajesh",
    stage: "storage",
    detail: "daily_store_sales, customer_value and hourly_order_volume: the figures the business reads.",
    watchedBy: ["dataObservability", "dbm"],
    x: COL,
    y: ROW * 2,
  },
  {
    id: "metabase",
    title: "Metabase dashboards",
    kind: "Metabase (open source) · Container App",
    owner: "john",
    stage: "serving",
    detail:
      "Store Sales — Today, Customer Value and Pipeline Health, built on the marts. Metabase connects as a read-only role that can see only the marts.",
    watchedBy: ["dataObservability"],
    x: COL * 2,
    y: ROW * 2,
  },
];

export const pipelineEdges: PipelineEdge[] = [
  { from: "storefront", to: "topic", label: "per order", fromSide: "right", toSide: "left" },
  { from: "topic", to: "processor", label: "consumes", fromSide: "right", toSide: "left" },
  { from: "processor", to: "landing", label: "every 5 min", fromSide: "right", toSide: "left" },
  { from: "landing", to: "ingest", label: "every 15 min", fromSide: "bottom", toSide: "top" },
  { from: "ingest", to: "raw", fromSide: "left", toSide: "right" },
  { from: "raw", to: "stagingTask", fromSide: "left", toSide: "right" },
  { from: "stagingTask", to: "staging", fromSide: "left", toSide: "right" },
  { from: "staging", to: "martTasks", label: "after check_staging", fromSide: "bottom", toSide: "top" },
  { from: "martTasks", to: "marts", fromSide: "right", toSide: "left" },
  { from: "marts", to: "metabase", label: "read-only", fromSide: "right", toSide: "left" },
];

// One color per person, used for their nodes, avatar and badges.
export const ownerColors: Record<Owner, { accent: string; soft: string }> = {
  dave: { accent: "#3b7ddd", soft: "#dbeafe" },
  rajesh: { accent: "#c26a00", soft: "#fdecc8" },
  emily: { accent: "#1f9254", soft: "#d7f5e3" },
  priya: { accent: "#c2367a", soft: "#fbe0ee" },
  john: { accent: "#0b8aa3", soft: "#d3f2f8" },
};

// Highlights for one persona's diagram.
export interface DiagramMark {
  node: NodeId;
  kind: "problem" | "alert" | "impact";
  label: string;
}

export interface PersonaDiagram {
  focus: NodeId[];
  marks: DiagramMark[];
}
