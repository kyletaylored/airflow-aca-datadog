"""SQL for the contoso_dw medallion: raw -> staging -> marts.

Every statement list runs in one transaction (SQLExecuteQueryOperator commits once
at the end), so the full-rebuild tasks never expose a half-empty table. The
ingest SQL is a Jinja template rendered by Airflow; the chaos switches are DAG
params.
"""
from __future__ import annotations

STORES = [
    "Seattle",
    "Portland",
    "San Francisco",
    "Denver",
    "Austin",
    "Chicago",
    "New York",
]
REGIONS = ["West", "Mountain", "Central", "South", "East"]
CUSTOMER_POOL = 2000
SCHEMAS = ["raw", "staging", "marts"]


def _sql_array(values: list[str]) -> str:
    return "ARRAY[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


# --- Bootstrap (run once by contoso_dw_bootstrap, as the admin) ------------------

CREATE_TABLES = [
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS staging",
    "CREATE SCHEMA IF NOT EXISTS marts",
    """
    CREATE TABLE IF NOT EXISTS raw.customers (
        customer_id  text PRIMARY KEY,
        email        text,
        region       text,
        signed_up_at timestamptz,
        _loaded_at   timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.orders (
        order_id     text PRIMARY KEY,
        customer_id  text,
        store        text NOT NULL,
        channel      text NOT NULL,
        items        integer NOT NULL,
        amount_cents integer NOT NULL,
        currency     text NOT NULL,
        ordered_at   timestamptz NOT NULL,
        _batch_id    text NOT NULL,
        _loaded_at   timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS orders_ordered_at ON raw.orders (ordered_at)",
    """
    CREATE TABLE IF NOT EXISTS staging.customers (
        customer_id  text PRIMARY KEY,
        email        text,
        region       text,
        signed_up_at timestamptz
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS staging.orders (
        order_id    text PRIMARY KEY,
        customer_id text,
        store       text NOT NULL,
        channel     text NOT NULL,
        items       integer NOT NULL,
        amount_usd  numeric(12, 2) NOT NULL,
        ordered_at  timestamptz NOT NULL,
        order_date  date NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS marts.daily_store_sales (
        sales_date    date NOT NULL,
        store         text NOT NULL,
        orders        integer NOT NULL,
        revenue_usd   numeric(14, 2) NOT NULL,
        last_order_at timestamptz NOT NULL,
        updated_at    timestamptz NOT NULL,
        PRIMARY KEY (sales_date, store)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS marts.customer_value (
        customer_id    text PRIMARY KEY,
        region         text,
        orders         integer NOT NULL,
        revenue_usd    numeric(14, 2) NOT NULL,
        first_order_at timestamptz NOT NULL,
        last_order_at  timestamptz NOT NULL,
        updated_at     timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS marts.hourly_order_volume (
        order_hour timestamptz NOT NULL,
        store      text NOT NULL,
        orders     integer NOT NULL,
        updated_at timestamptz NOT NULL,
        PRIMARY KEY (order_hour, store)
    )
    """,
]


DATADOG_ROLE = "datadog"


def datadog_grants(database: str) -> list[str]:
    """What the Datadog Agent's role needs for Data Observability on ``database``.

    Follows Datadog's PostgreSQL setup: pg_monitor, pg_stat_database, and SELECT
    on every table in the medallion schemas, including tables created later.
    pg_monitor also covers Database Monitoring's pg_read_all_settings and
    pg_read_all_stats. The SELECT grants are what the data quality monitors'
    queries need; the catalog itself lists every database on the server.
    """
    grants = [
        f"GRANT pg_monitor TO {DATADOG_ROLE}",
        f"GRANT SELECT ON pg_stat_database TO {DATADOG_ROLE}",
        f"GRANT CONNECT ON DATABASE {database} TO {DATADOG_ROLE}",
    ]
    for schema in SCHEMAS:
        grants += [
            f"GRANT USAGE ON SCHEMA {schema} TO {DATADOG_ROLE}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {DATADOG_ROLE}",
            # Tables the pipeline creates later (it runs as this same admin role).
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {DATADOG_ROLE}",
        ]
    return grants


# Database Monitoring, which the Agent runs on every database on the server: the
# databases it connects to, and which are left out.
LIST_DATABASES = """
    SELECT datname FROM pg_database
    WHERE datallowconn AND NOT datistemplate
      AND datname NOT IN ('azure_maintenance', 'azure_sys')
    ORDER BY datname
"""

# Run in each of those databases, following Datadog's DBM setup for Azure. The
# Agent explains query samples through datadog.explain_statement, which runs as
# its owner (the admin), so the datadog role can explain queries on tables it
# can't read. pg_stat_statements is on the server's azure.extensions list, which
# Bicep sets. The role's cluster-wide grants (pg_monitor) are in datadog_grants.
DBM_DATABASE_SETUP = [
    "CREATE SCHEMA IF NOT EXISTS datadog",
    f"GRANT USAGE ON SCHEMA datadog TO {DATADOG_ROLE}",
    f"GRANT USAGE ON SCHEMA public TO {DATADOG_ROLE}",
    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
    """
    CREATE OR REPLACE FUNCTION datadog.explain_statement(
        l_query TEXT,
        OUT explain JSON
    )
    RETURNS SETOF JSON AS
    $$
    DECLARE
    curs REFCURSOR;
    plan JSON;

    BEGIN
        SET TRANSACTION READ ONLY;

        OPEN curs FOR EXECUTE pg_catalog.concat('EXPLAIN (FORMAT JSON) ', l_query);
        FETCH curs INTO plan;
        CLOSE curs;
        RETURN QUERY SELECT plan;
    END;
    $$
    LANGUAGE 'plpgsql'
    RETURNS NULL ON NULL INPUT
    SECURITY DEFINER
    """,
]


METABASE_ROLE = "metabase"
METABASE_DATABASE = "metabase"


def metabase_grants(database: str) -> list[str]:
    """What Metabase's role needs: its own database, and read access to the marts only.

    Azure's admin isn't a superuser, so it takes the role itself before it can hand
    the role a database.
    """
    return [
        f"GRANT {METABASE_ROLE} TO CURRENT_USER",
        f"ALTER DATABASE {METABASE_DATABASE} OWNER TO {METABASE_ROLE}",
        f"GRANT CONNECT ON DATABASE {database} TO {METABASE_ROLE}",
        f"GRANT USAGE ON SCHEMA marts TO {METABASE_ROLE}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA marts TO {METABASE_ROLE}",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO {METABASE_ROLE}",
    ]


# Run in the metabase database. Metabase's migrations need citext (on the server's
# azure.extensions list, which Bicep sets) and a public schema they can write to;
# in a new Azure database that schema belongs to azure_pg_admin.
METABASE_DATABASE_SETUP = [
    "CREATE EXTENSION IF NOT EXISTS citext",
    f"ALTER SCHEMA public OWNER TO {METABASE_ROLE}",
]


def upsert_customers(fraction: str) -> str:
    """Upsert the fixed customer pool; ``fraction`` of it is touched each time.

    ``fraction`` is SQL text: ``"1"`` seeds every customer, ``"0.02"`` refreshes
    about 2% per run so raw.customers keeps receiving loads.
    """
    return f"""
    INSERT INTO raw.customers (customer_id, email, region, signed_up_at, _loaded_at)
    SELECT 'C' || lpad(n::text, 5, '0'),
           'Customer' || n || '@Example.com ',
           ({_sql_array(REGIONS)})[1 + n % {len(REGIONS)}],
           timestamptz '2025-01-01' + n * interval '4 hours',
           now()
    -- generate_series inside a subquery: as a bare FROM item, OpenLineage's SQL
    -- parser would report it as an input table.
    FROM (SELECT generate_series(1, {CUSTOMER_POOL}) AS n) AS pool
    WHERE random() < {fraction}
    ON CONFLICT (customer_id) DO UPDATE
        SET email = excluded.email,
            region = excluded.region,
            _loaded_at = excluded._loaded_at
    """


# --- contoso_orders: ingest ----------------------------------------------------

INGEST_CUSTOMERS = [upsert_customers("0.02")]

# Rendered by Airflow. The chaos params shape what lands in raw.orders:
#   null_customers -> 30% of orders have no customer_id
#   volume_drop    -> 5% of the normal volume
#   skip_load      -> no orders at all
#   schema_drift   -> raw.orders gains a promo_code column. It stays until a run sets
#                     schema_drift to false, or contoso_schema_flip drops it; a run
#                     that leaves it unset doesn't touch it
#   breaking_schema -> after the load, raw.orders.amount_cents is renamed to
#                     amount_minor, so staging_orders fails. The next run renames it
#                     back before it loads.
ADD_PROMO_CODE = "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS promo_code text"
DROP_PROMO_CODE = "ALTER TABLE raw.orders DROP COLUMN IF EXISTS promo_code"
# What a run with schema_drift unset runs in their place, so the list keeps its shape.
KEEP_PROMO_CODE = "SELECT 1"
PURGE_OLD_ORDERS = "DELETE FROM raw.orders WHERE ordered_at < now() - interval '14 days'"
BREAK_AMOUNT_COLUMN = "ALTER TABLE raw.orders RENAME COLUMN amount_cents TO amount_minor"
# The columns raw.orders has right now. The stream load runs it around each ALTER TABLE
# above, to check that the statement did what it was meant to.
RAW_ORDERS_COLUMNS = (
    "SELECT column_name FROM information_schema.columns"
    " WHERE table_schema = 'raw' AND table_name = 'orders'"
)
RESTORE_AMOUNT_COLUMN = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'orders' AND column_name = 'amount_minor'
    ) THEN
        ALTER TABLE raw.orders RENAME COLUMN amount_minor TO amount_cents;
    END IF;
END
$$
"""

INGEST_ORDERS = [
    "{% if params.schema_drift %}" + ADD_PROMO_CODE
    + "{% elif params.schema_drift is sameas false %}" + DROP_PROMO_CODE
    + "{% else %}" + KEEP_PROMO_CODE + "{% endif %}",
    RESTORE_AMOUNT_COLUMN,
    f"""
    INSERT INTO raw.orders
        (order_id, customer_id, store, channel, items, amount_cents, currency, ordered_at, _batch_id)
    SELECT md5('{{{{ run_id }}}}' || '-' || n),
           CASE WHEN {{{{ 'true' if params.null_customers else 'false' }}}} AND random() < 0.3
                THEN NULL
                ELSE 'C' || lpad((1 + floor(random() * {CUSTOMER_POOL}))::int::text, 5, '0')
           END,
           ({_sql_array(STORES)})[1 + floor(random() * {len(STORES)})::int],
           (ARRAY['web', 'web', 'web', 'mobile', 'mobile', 'store', 'test'])[1 + floor(random() * 7)::int],
           1 + floor(random() * 5)::int,
           500 + floor(random() * 15000)::int,
           'USD',
           now() - random() * interval '{{{{ params.interval_minutes }}}} minutes',
           '{{{{ run_id }}}}'
    FROM (
        SELECT generate_series(
            1,
            {{%- if params.skip_load %}} 0
            {{%- elif params.volume_drop %}} greatest(1, ((200 + floor(random() * 200)) * 0.05)::int)
            {{%- else %}} (200 + floor(random() * 200))::int
            {{%- endif %}}
        ) AS n
    ) AS batch
    ON CONFLICT (order_id) DO NOTHING
    """,
    PURGE_OLD_ORDERS + "{% if params.breaking_schema %};\n" + BREAK_AMOUNT_COLUMN + "{% endif %}",
]

# --- contoso_orders: ingest from the landing zone (stream mode) ----------------

# Which landing blobs are already in raw.orders. Created by the load task itself,
# so deployments bootstrapped before stream mode existed pick it up too.
CREATE_INGESTED_FILES = """
CREATE TABLE IF NOT EXISTS raw._ingested_files (
    name        text PRIMARY KEY,
    orders      integer NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
)
"""

INSERT_ORDER = """
INSERT INTO raw.orders
    (order_id, customer_id, store, channel, items, amount_cents, currency, ordered_at, _batch_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO NOTHING
"""

# --- contoso_orders: staging ---------------------------------------------------

# In the staging and marts SQL, every SELECT column has an alias matching its
# INSERT target. OpenLineage's SQL parser names column lineage after the SELECT
# output, not the INSERT list, so without the aliases a computed column comes out
# as _0 and Datadog's column lineage stops there.
STAGING_ORDERS = [
    "TRUNCATE staging.orders",
    """
    INSERT INTO staging.orders
        (order_id, customer_id, store, channel, items, amount_usd, ordered_at, order_date)
    SELECT order_id,
           customer_id,
           store,
           channel,
           items,
           round(amount_cents / 100.0, 2) AS amount_usd,
           ordered_at,
           (ordered_at AT TIME ZONE 'UTC')::date AS order_date
    FROM raw.orders
    WHERE channel <> 'test'
    """,
]

STAGING_CUSTOMERS = [
    "TRUNCATE staging.customers",
    """
    INSERT INTO staging.customers (customer_id, email, region, signed_up_at)
    SELECT customer_id, lower(trim(email)) AS email, region, signed_up_at
    FROM raw.customers
    """,
]

# SQLCheckOperator fails the task if any column of the first row is false or NULL.
CHECK_STAGING = """
SELECT count(*) > 0               AS has_orders,
       bool_and(amount_usd >= 0)  AS no_negative_amounts
FROM staging.orders
"""

# --- contoso_orders: marts -----------------------------------------------------

# Rendered by Airflow. refund_bug stands for a bad change to this mart's refund
# handling: every Denver order counts as a refund, so Denver's revenue goes negative.
# staging.orders is untouched, so check_staging still passes.
MART_DAILY_STORE_SALES = [
    "TRUNCATE marts.daily_store_sales",
    """
    INSERT INTO marts.daily_store_sales
        (sales_date, store, orders, revenue_usd, last_order_at, updated_at)
    SELECT order_date AS sales_date, store, count(*) AS orders,
           {% if params.refund_bug -%}
           sum(CASE WHEN store = 'Denver' THEN -amount_usd ELSE amount_usd END) AS revenue_usd,
           {%- else -%}
           sum(amount_usd) AS revenue_usd,
           {%- endif %}
           max(ordered_at) AS last_order_at, now() AS updated_at
    FROM staging.orders
    GROUP BY order_date, store
    """,
]

# Inner join: orders without a known customer drop out of this mart, which is
# what null_customers chaos makes visible downstream.
MART_CUSTOMER_VALUE = [
    "TRUNCATE marts.customer_value",
    """
    INSERT INTO marts.customer_value
        (customer_id, region, orders, revenue_usd, first_order_at, last_order_at, updated_at)
    SELECT c.customer_id, c.region, count(*) AS orders, sum(o.amount_usd) AS revenue_usd,
           min(o.ordered_at) AS first_order_at, max(o.ordered_at) AS last_order_at,
           now() AS updated_at
    FROM staging.orders AS o
    JOIN staging.customers AS c ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.region
    """,
]

MART_HOURLY_ORDER_VOLUME = [
    "TRUNCATE marts.hourly_order_volume",
    """
    INSERT INTO marts.hourly_order_volume (order_hour, store, orders, updated_at)
    SELECT date_trunc('hour', ordered_at) AS order_hour, store, count(*) AS orders,
           now() AS updated_at
    FROM staging.orders
    WHERE ordered_at >= now() - interval '7 days'
    GROUP BY date_trunc('hour', ordered_at), store
    """,
]
