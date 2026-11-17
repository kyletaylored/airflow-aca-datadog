// The concept lessons, in reading order, and a short glossary they share.

export const conceptLessons = [
  {
    slug: "why-pipelines",
    title: "The case for a distributed pipeline",
    summary:
      "A single shared database compared with a pipeline in which each team owns one step, and the benefits and costs of the additional components.",
  },
  {
    slug: "medallion",
    title: "Medallion architecture",
    summary:
      "Bronze, silver and gold: raw data retained as it arrived, cleaned data, and business-ready tables. The correspondence to this demo's raw, staging and marts schemas, and a comparison of ETL and ELT.",
  },
  {
    slug: "landscape",
    title: "The data tool landscape",
    summary:
      "Airflow, dbt, Fivetran, Snowflake, Kafka, Power BI and approximately twenty other products, grouped by function, with each cloud provider's equivalent shown side by side.",
  },
] as const;

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
];
