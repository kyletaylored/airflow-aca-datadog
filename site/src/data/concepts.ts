// The concept lessons, in reading order, and a short glossary they share.

export const conceptLessons = [
  {
    slug: "why-pipelines",
    title: "Why build a pipeline like this?",
    summary:
      "One database that does everything, compared with a pipeline where each team owns one step. What the extra moving parts buy, and what they cost.",
  },
  {
    slug: "medallion",
    title: "Medallion architecture",
    summary:
      "Bronze, silver and gold: raw data kept as it arrived, cleaned data, and business-ready tables. How this demo's raw, staging and marts map to it, and ETL compared with ELT.",
  },
  {
    slug: "landscape",
    title: "The data tool landscape",
    summary:
      "Airflow, dbt, Fivetran, Snowflake, Kafka, Power BI and twenty more, grouped by the job they do, with each cloud's version of the same job side by side.",
  },
] as const;

export const glossary: { term: string; meaning: string }[] = [
  { term: "OLTP", meaning: "Online transaction processing: the app's database, tuned for many small, fast reads and writes." },
  { term: "OLAP", meaning: "Online analytical processing: storage and compute tuned for big scans and aggregates over history." },
  { term: "ETL and ELT", meaning: "Extract, transform, load (clean the data before it lands) compared with extract, load, transform (land it raw, then transform inside the warehouse)." },
  { term: "Batch and streaming", meaning: "Processing data in scheduled chunks (every 15 minutes, nightly) compared with continuously, event by event." },
  { term: "CDC", meaning: "Change data capture: reading a database's change log to copy inserts, updates and deletes as they happen, without querying the tables." },
  { term: "Data lake", meaning: "Cheap object storage (S3, ADLS, Google Cloud Storage) holding files in any format, read by whatever engine needs them." },
  { term: "Lakehouse", meaning: "A data lake with open table formats (Delta Lake, Apache Iceberg) that add transactions, schemas and warehouse-style performance." },
  { term: "Data contract", meaning: "An agreement between the team producing data and the teams using it: which fields, which types, what they mean, and how changes are announced." },
  { term: "Schema on read", meaning: "Store data as it comes and apply a structure when it's read, as a landing zone does. Schema on write enforces the structure on the way in, as a warehouse table does." },
  { term: "Semantic layer", meaning: "Metric and join definitions kept in one place, so every dashboard computes revenue the same way." },
  { term: "Reverse ETL", meaning: "Syncing modeled data from the warehouse back into operational tools such as a CRM." },
  { term: "Lineage", meaning: "Which datasets each dataset is built from, and which jobs built it: the map for tracing a bad number to its cause." },
];
