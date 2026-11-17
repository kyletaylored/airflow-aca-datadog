// The data tools a team is likely to meet, grouped by the job they do. The
// same job is often sold by every cloud under a different name, which is most
// of what makes the landscape confusing. Facts are kept to what each product is
// for; check a vendor's own docs before quoting anything finer.

export type CategoryId =
  | "streaming"
  | "messaging"
  | "ingestion"
  | "orchestration"
  | "processing"
  | "transformation"
  | "operational"
  | "warehouse"
  | "bi"
  | "activation";

export interface Category {
  id: CategoryId;
  name: string;
  // The question a team is answering when it reaches for one of these.
  job: string;
  explain: string;
  inDemo: string;
}

export const categories: Category[] = [
  {
    id: "streaming",
    name: "Event streaming",
    job: "Move a continuous flow of events between systems, and keep it so it can be replayed.",
    explain:
      "A stream is an append-only log split into partitions. Producers write events; any number of consumer groups read the same events independently, each tracking its own position (offset). Events stay for a retention period, so a new consumer, or one recovering from a bug, can start again from the past.",
    inDemo: "The orders topic on Azure Event Hubs, used through its Kafka endpoint.",
  },
  {
    id: "messaging",
    name: "Message queues and pub/sub",
    job: "Hand a unit of work to exactly one worker, or fan a notification out to several subscribers.",
    explain:
      "A queue holds messages until a worker takes one and acknowledges it; then it's gone. Several workers share the load (competing consumers). Pub/sub copies each message to every subscription. Queues and brokers add features streams don't have: per-message retries, dead-letter queues, delays, sessions and routing rules.",
    inDemo: "Not used: the demo needs replay and several readers, which is a stream's job.",
  },
  {
    id: "ingestion",
    name: "Batch ingestion (ELT connectors)",
    job: "Copy data out of SaaS apps and databases into the warehouse on a schedule, without writing the code.",
    explain:
      "Connectors know each source's API or change log (Salesforce, Stripe, Postgres, and hundreds more), load the raw rows into the warehouse, and keep them up to date incrementally. This is the E and L of ELT: they extract and load, and the transformation happens later, in the warehouse.",
    inDemo: "Not used: the demo writes its own loader (ingest_orders) for the landing zone.",
  },
  {
    id: "orchestration",
    name: "Orchestration",
    job: "Run the steps of a pipeline in the right order, on a schedule, with retries, and show what happened.",
    explain:
      "An orchestrator holds the dependency graph (load, then clean, then build the marts), triggers each step, retries failures and records every run. It usually doesn't move or transform the data itself; it tells other systems to.",
    inDemo: "Apache Airflow 3 on Azure Container Apps runs contoso_orders every 15 minutes.",
  },
  {
    id: "processing",
    name: "Distributed processing",
    job: "Transform data too big, too messy or too non-SQL for one machine or one warehouse query.",
    explain:
      "A processing engine splits the work across a cluster: joins over billions of rows, parsing files, machine learning features, or streaming transformations. Spark is the common engine underneath most of the products here.",
    inDemo: "Not used: the demo's volume fits in Postgres SQL.",
  },
  {
    id: "transformation",
    name: "Transformation (in-warehouse modeling)",
    job: "Turn raw tables into clean, tested, documented models with SQL, managed like software.",
    explain:
      "The T of ELT. Models are SQL SELECT statements kept in Git; the tool works out their order from the references between them, builds them in the warehouse, runs tests (not null, unique, accepted values) and generates documentation and lineage.",
    inDemo: "The staging and mart SQL tasks in contoso_orders do this job by hand; in a larger team it would be dbt.",
  },
  {
    id: "operational",
    name: "Operational databases (OLTP)",
    job: "Store the application's live data: many small reads and writes, each one fast and consistent.",
    explain:
      "Online transaction processing: row-oriented storage, indexes and transactions tuned for an app's single-row work, such as placing an order or updating a customer. Fine for small reporting, but large analytical scans compete with the app for the same resources.",
    inDemo: "Azure Database for PostgreSQL holds Airflow's own metadata, Metabase's settings, and the contoso_dw warehouse.",
  },
  {
    id: "warehouse",
    name: "Analytical warehouses and lakehouses (OLAP)",
    job: "Answer big analytical queries over all the history, fast, for many users at once.",
    explain:
      "Online analytical processing: columnar storage and massively parallel compute, usually with storage and compute billed separately so each scales on its own. A warehouse stores data in its own format; a lakehouse keeps open table formats (Delta Lake, Apache Iceberg) as files in object storage and puts warehouse features on top.",
    inDemo: "Postgres plays the warehouse here, which is fine at this size; at scale it would be one of these.",
  },
  {
    id: "bi",
    name: "BI, semantic layers and notebooks",
    job: "Let people explore, chart and share the numbers, with metrics defined once.",
    explain:
      "BI tools query the warehouse and turn results into dashboards. A semantic layer defines metrics and joins once (revenue means this, joined like that), so every report agrees. Notebooks mix SQL, Python and charts for analysis that doesn't fit a dashboard.",
    inDemo: "Metabase (open source) reads the marts for John's dashboards.",
  },
  {
    id: "activation",
    name: "Reverse ETL (data activation)",
    job: "Send modeled data from the warehouse back into the tools teams work in.",
    explain:
      "The warehouse becomes a source: a customer's lifetime value or churn score is synced into Salesforce, HubSpot, an ad platform or a support tool, so sales and marketing act on the same numbers as the dashboards.",
    inDemo: "Not used. marts.customer_value is the kind of table it would sync.",
  },
];

export const categoryById = Object.fromEntries(categories.map((c) => [c.id, c])) as Record<CategoryId, Category>;

export type Maker = "Open source" | "AWS" | "Microsoft" | "Google Cloud" | "Independent";

export interface Tool {
  name: string;
  maker: Maker;
  // Where it belongs first, and the other jobs it also does.
  category: CategoryId;
  also?: CategoryId[];
  what: string;
  // The tool people most often mix it up with, and the difference.
  versus?: string;
  inDemo?: boolean;
}

export const tools: Tool[] = [
  // Streaming and messaging
  {
    name: "Apache Kafka",
    maker: "Open source",
    category: "streaming",
    what: "The reference event-streaming platform: partitioned, replicated logs with consumer groups and retention. Run it yourself, or as a managed service from Confluent or the clouds.",
    versus: "Azure Event Hubs, Amazon Kinesis and Google Pub/Sub are the clouds' managed equivalents; Event Hubs even speaks Kafka's protocol.",
    inDemo: true,
  },
  {
    name: "Amazon Kinesis",
    maker: "AWS",
    category: "streaming",
    what: "AWS's managed streaming: Kinesis Data Streams for partitioned streams with retention, and Amazon Data Firehose for delivering a stream straight into S3, Redshift and other stores.",
    versus: "Amazon MSK is AWS's managed Kafka; Kinesis is AWS's own API.",
  },
  {
    name: "Azure Event Hubs",
    maker: "Microsoft",
    category: "streaming",
    what: "Azure's big-data event streaming: partitions, consumer groups and retention, with a Kafka-compatible endpoint so Kafka clients work unchanged.",
    versus: "Azure Service Bus is the other Azure messaging service: a broker for business messages and commands, not a high-volume stream.",
    inDemo: true,
  },
  {
    name: "Google Cloud Pub/Sub",
    maker: "Google Cloud",
    category: "streaming",
    also: ["messaging"],
    what: "Google's global messaging service: topics with subscriptions that are pushed to or pulled from. It covers both streaming ingestion and fan-out messaging, and can retain and replay messages.",
    versus: "It does the job of both Kafka-style streams and SNS-style fan-out in one service.",
  },
  {
    name: "RabbitMQ",
    maker: "Open source",
    category: "messaging",
    what: "An open-source message broker (AMQP). Producers publish to exchanges, which route messages into queues by rules; workers acknowledge each message.",
    versus: "Kafka keeps a replayable log for many readers; RabbitMQ delivers each message to a worker and removes it, with richer routing.",
  },
  {
    name: "Amazon SQS",
    maker: "AWS",
    category: "messaging",
    what: "A fully managed queue: standard (best-effort ordering, at-least-once delivery) or FIFO (strict order, exactly-once processing), with dead-letter queues.",
    versus: "SNS pushes one message to many subscribers; SQS holds messages for workers to pull. They're often used together: SNS fans out into several SQS queues.",
  },
  {
    name: "Amazon SNS",
    maker: "AWS",
    category: "messaging",
    what: "Managed pub/sub notifications: publish once to a topic, and SNS pushes a copy to every subscriber (SQS queues, Lambda functions, HTTP endpoints, email, SMS).",
    versus: "SNS doesn't keep messages for later; to buffer or replay, it delivers into SQS or a stream.",
  },
  {
    name: "Azure Service Bus",
    maker: "Microsoft",
    category: "messaging",
    what: "Azure's enterprise message broker: queues, and topics with filtered subscriptions, plus sessions for ordering, transactions, scheduled delivery and dead-lettering.",
    versus: "Event Hubs is for high-volume event streams; Service Bus is for business messages that each must be handled reliably.",
  },
  // Ingestion
  {
    name: "Fivetran",
    maker: "Independent",
    category: "ingestion",
    what: "Managed ELT connectors: point it at a source (a SaaS app, a database's change log, files) and a destination warehouse, and it loads and keeps the raw tables in sync.",
    versus: "Airbyte does the same job with open-source connectors you can run yourself or use as a cloud service.",
  },
  {
    name: "Airbyte",
    maker: "Open source",
    category: "ingestion",
    what: "Open-source ELT connectors, self-hosted or as Airbyte Cloud, with a framework for building new connectors.",
    versus: "Fivetran is the managed-only alternative; Airflow can trigger an Airbyte sync but doesn't replace it.",
  },
  {
    name: "Azure Data Factory",
    maker: "Microsoft",
    category: "ingestion",
    also: ["orchestration"],
    what: "Azure's data integration service: a visual designer for copy activities across many connectors, plus pipelines that schedule and chain them. It can also run SSIS packages in the cloud.",
    versus: "It overlaps Fivetran (moving data) and Airflow (scheduling steps). Microsoft Fabric includes its successor, Data Factory in Fabric.",
  },
  {
    name: "AWS Glue",
    maker: "AWS",
    category: "processing",
    also: ["ingestion", "orchestration"],
    what: "AWS's serverless data integration: Spark-based ETL jobs, crawlers that discover schemas, and the Glue Data Catalog that Athena, Redshift Spectrum and EMR share as their table catalog.",
    versus: "Its jobs are Spark without managing a cluster; its catalog is a metastore, which is a different job from the ETL.",
  },
  // Orchestration
  {
    name: "Apache Airflow",
    maker: "Open source",
    category: "orchestration",
    what: "The most widely used open-source orchestrator: pipelines (DAGs) written in Python, with a scheduler, retries, a UI and hundreds of provider integrations. Offered as managed services such as Amazon MWAA and Google Cloud Composer.",
    versus: "Airflow decides when and in what order; the work itself runs in the systems it calls (a warehouse, Spark, dbt, a container).",
    inDemo: true,
  },
  // Processing
  {
    name: "Apache Spark",
    maker: "Open source",
    category: "processing",
    what: "The open-source distributed processing engine: batch and streaming (Structured Streaming) transformations in SQL, Python, Scala or R, across a cluster.",
    versus: "Spark is the engine; Databricks, AWS Glue, Synapse and Fabric are products that run it for you.",
  },
  {
    name: "Databricks",
    maker: "Independent",
    category: "warehouse",
    also: ["processing", "orchestration"],
    what: "A lakehouse platform from Spark's creators, on AWS, Azure and Google Cloud: managed Spark, Delta Lake tables, SQL warehouses, Workflows for scheduling, Unity Catalog for governance, and machine learning.",
    versus: "Often compared with Snowflake. Both now cover warehouse and data-science work; Databricks starts from Spark and open files, Snowflake from a managed SQL warehouse.",
  },
  // Transformation
  {
    name: "dbt (Core and Cloud)",
    maker: "Independent",
    category: "transformation",
    what: "SQL transformation as code: models are SELECT statements in Git; dbt builds them in dependency order inside the warehouse, tests them and documents them. dbt Core is the open-source command-line tool; dbt Cloud adds a hosted IDE, scheduling, CI and a semantic layer.",
    versus: "dbt doesn't move data in (that's Fivetran or Airbyte) or schedule other systems (that's Airflow); it runs SQL in the warehouse you already have.",
  },
  // Operational databases
  {
    name: "PostgreSQL",
    maker: "Open source",
    category: "operational",
    also: ["warehouse"],
    what: "The leading open-source relational database: transactions, rich SQL, extensions. Offered managed as Azure Database for PostgreSQL, Amazon RDS and Aurora, and Google Cloud SQL.",
    versus: "Amazon Redshift started from PostgreSQL's SQL dialect but is a columnar warehouse, not a Postgres server.",
    inDemo: true,
  },
  {
    name: "Microsoft SQL Server (with SSIS and SSAS)",
    maker: "Microsoft",
    category: "operational",
    also: ["transformation", "bi"],
    what: "Microsoft's relational database, sold with companion services: SQL Server Integration Services (SSIS) for ETL packages, Analysis Services (SSAS) for OLAP cubes and tabular semantic models, and Reporting Services (SSRS).",
    versus: "SSIS's cloud successor is Azure Data Factory; SSAS's tabular engine lives on in Power BI semantic models.",
  },
  // Warehouses
  {
    name: "Snowflake",
    maker: "Independent",
    category: "warehouse",
    what: "A cloud data warehouse on AWS, Azure and Google Cloud, with storage separate from compute (virtual warehouses that scale and pause), data sharing between accounts, and Python and app workloads on top.",
    versus: "BigQuery and Redshift are the Google and AWS equivalents; Snowflake runs on any of the three clouds.",
  },
  {
    name: "Google BigQuery",
    maker: "Google Cloud",
    category: "warehouse",
    what: "Google's serverless data warehouse: no clusters to size, billed by data scanned or by reserved capacity, with built-in machine learning and BI acceleration.",
    versus: "The Google Cloud counterpart of Snowflake and Redshift.",
  },
  {
    name: "Amazon Redshift",
    maker: "AWS",
    category: "warehouse",
    what: "AWS's cloud data warehouse: provisioned clusters or Redshift Serverless, with Redshift Spectrum to query files in S3 through the Glue Data Catalog.",
    versus: "The AWS counterpart of Snowflake and BigQuery.",
  },
  {
    name: "Azure Synapse Analytics",
    maker: "Microsoft",
    category: "warehouse",
    also: ["processing", "orchestration"],
    what: "Azure's analytics workspace combining dedicated SQL pools (formerly Azure SQL Data Warehouse), serverless SQL over the lake, Spark pools and Data Factory pipelines.",
    versus: "Microsoft Fabric is the newer, software-as-a-service successor that brings these pieces together with Power BI.",
  },
  {
    name: "Microsoft Fabric",
    maker: "Microsoft",
    category: "warehouse",
    also: ["processing", "orchestration", "ingestion", "bi"],
    what: "Microsoft's all-in-one analytics service: one lake (OneLake) under a warehouse, lakehouses with Spark, Data Factory pipelines, real-time analytics, and Power BI, billed as shared capacity.",
    versus: "It's Microsoft's answer to running the whole stack in one product; Synapse and Data Factory are the earlier, separate services.",
  },
  // BI
  {
    name: "Power BI",
    maker: "Microsoft",
    category: "bi",
    what: "Microsoft's BI suite: semantic models (built on the same engine as SSAS tabular), reports and dashboards, from Power BI Desktop to the service in Fabric.",
    versus: "Tableau is its closest independent competitor.",
  },
  {
    name: "Tableau",
    maker: "Independent",
    category: "bi",
    what: "Visual analytics: drag-and-drop exploration and dashboards over extracts or live warehouse connections. Owned by Salesforce.",
    versus: "Power BI and Looker cover the same ground with different modeling approaches.",
  },
  {
    name: "Looker",
    maker: "Google Cloud",
    category: "bi",
    what: "BI built around a semantic model, LookML: metrics, dimensions and joins defined once in code, and every query generated from them against the warehouse.",
    versus: "Looker Studio (formerly Data Studio) is a separate, free dashboard tool from Google.",
  },
  {
    name: "Sigma",
    maker: "Independent",
    category: "bi",
    what: "Cloud BI with a spreadsheet interface: formulas and pivots that run as SQL directly on the warehouse, so there's no extract to keep fresh.",
    versus: "Feels like a spreadsheet, works like Tableau or Looker on live warehouse data.",
  },
  {
    name: "Hex",
    maker: "Independent",
    category: "bi",
    what: "Collaborative notebooks that mix SQL, Python and charts, and can be published as interactive data apps.",
    versus: "Hex is for analysis and data apps; Tableau and Power BI are for governed dashboards.",
  },
  {
    name: "Metabase",
    maker: "Open source",
    category: "bi",
    what: "Open-source BI: questions and dashboards over SQL databases and warehouses, self-hosted or as Metabase Cloud.",
    inDemo: true,
  },
  // Activation
  {
    name: "Hightouch",
    maker: "Independent",
    category: "activation",
    what: "Reverse ETL: syncs tables and audiences from the warehouse into SaaS tools (Salesforce, HubSpot, ad platforms, support tools) on a schedule.",
    versus: "It's Fivetran in reverse: from the warehouse out, not into it.",
  },
];

// Same job, different vendor. Cells list products from the tools above, plus a
// few well-known managed versions in parentheses.
export const makers: Maker[] = ["AWS", "Microsoft", "Google Cloud", "Open source", "Independent"];

export const matrix: { category: CategoryId; cells: Partial<Record<Maker, string>> }[] = [
  { category: "streaming", cells: { AWS: "Kinesis (and Amazon MSK)", Microsoft: "Event Hubs", "Google Cloud": "Pub/Sub", "Open source": "Kafka", Independent: "(Confluent)" } },
  { category: "messaging", cells: { AWS: "SQS, SNS", Microsoft: "Service Bus", "Google Cloud": "Pub/Sub", "Open source": "RabbitMQ" } },
  { category: "ingestion", cells: { AWS: "Glue", Microsoft: "Data Factory", "Open source": "Airbyte", Independent: "Fivetran" } },
  { category: "orchestration", cells: { AWS: "(Amazon MWAA), Glue workflows", Microsoft: "Data Factory pipelines", "Google Cloud": "(Cloud Composer)", "Open source": "Airflow", Independent: "dbt Cloud jobs, Databricks Workflows" } },
  { category: "processing", cells: { AWS: "Glue", Microsoft: "Synapse and Fabric Spark, Azure Databricks", "Open source": "Spark", Independent: "Databricks" } },
  { category: "transformation", cells: { Microsoft: "SSIS (classic ETL)", "Open source": "dbt Core", Independent: "dbt Cloud" } },
  { category: "operational", cells: { AWS: "(RDS, Aurora)", Microsoft: "SQL Server, Azure Database for PostgreSQL", "Google Cloud": "(Cloud SQL)", "Open source": "PostgreSQL" } },
  { category: "warehouse", cells: { AWS: "Redshift", Microsoft: "Synapse, Fabric", "Google Cloud": "BigQuery", Independent: "Snowflake, Databricks" } },
  { category: "bi", cells: { Microsoft: "Power BI, SSAS", "Google Cloud": "Looker", "Open source": "Metabase", Independent: "Tableau, Sigma, Hex" } },
  { category: "activation", cells: { Independent: "Hightouch" } },
];
