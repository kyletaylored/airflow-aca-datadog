"""Load the order processor's landed batches into raw.orders (stream mode).

The processor writes newline-delimited JSON under ``orders/`` in the landing
container. Each run loads the blobs it hasn't loaded before, recorded in
``raw._ingested_files``. The chaos switches act on the load, as if the load code
had a bug:

    null_customers  30% of the loaded orders lose their customer_id
    volume_drop     only 5% of each batch is loaded (the rest is lost)
    skip_load       nothing is loaded; the blobs wait for the next normal run
    schema_drift    raw.orders gains a promo_code column, until a run sets it to
                    false; when it's unset, the load doesn't touch the column
    breaking_schema after the load, amount_cents is renamed to amount_minor, so
                    staging_orders fails; the next run renames it back first

This is Python rather than SQL, so OpenLineage can't read the tables from a
statement. LoadLandedOrdersOperator reports them itself instead. The names here
match the processor's OpenLineage events and the Postgres tables, so all three join
in Data Observability.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass

from contoso_dw import sql as dw

MAX_BLOBS_PER_RUN = 200
ORDER_FIELDS = (
    "order_id",
    "customer_id",
    "store",
    "channel",
    "items",
    "amount_cents",
    "currency",
    "ordered_at",
)


@dataclass(frozen=True)
class Chaos:
    null_customers: bool = False
    volume_drop: bool = False
    skip_load: bool = False
    schema_drift: bool | None = None
    breaking_schema: bool = False

    @classmethod
    def from_params(cls, params: dict) -> Chaos:
        flags = {f: bool(params.get(f)) for f in cls.__dataclass_fields__}
        # Unset and off mean different things for schema_drift: leave the column, or drop it.
        drift = params.get("schema_drift")
        return cls(**{**flags, "schema_drift": None if drift is None else bool(drift)})


def landing_dataset(account: str, container: str) -> tuple[str, str]:
    """(namespace, name) of the landing zone, as the processor reports it."""
    return f"abfss://{container}@{account}.dfs.core.windows.net", "orders"


def postgres_dataset(host: str, port: int | None, database: str, schema: str, table: str) -> tuple[str, str]:
    """(namespace, name) of a contoso_dw table, as Airflow's Postgres provider reports it."""
    return f"postgres://{host}:{port or 5432}", f"{database}.{schema}.{table}"


def apply_chaos(orders: list[dict], chaos: Chaos, rng: random.Random) -> list[dict]:
    if chaos.volume_drop:
        orders = [o for o in orders if rng.random() < 0.05]
    if chaos.null_customers:
        orders = [{**o, "customer_id": None} if rng.random() < 0.3 else o for o in orders]
    return orders


def parse_batch(data: bytes) -> list[dict]:
    orders = []
    for line in data.decode().splitlines():
        if line.strip():
            orders.append(json.loads(line))
    return orders


def schema_changes(chaos: Chaos) -> list[str]:
    """What load_landed_orders does to raw.orders' columns, one line each for the task log."""
    changes = []
    if chaos.schema_drift:
        changes.append("schema_drift on: added the promo_code column to raw.orders (ADD COLUMN IF NOT EXISTS)")
    elif chaos.schema_drift is False:
        changes.append("schema_drift off: dropped the promo_code column from raw.orders (DROP COLUMN IF EXISTS)")
    if chaos.breaking_schema:
        changes.append("breaking_schema on: renamed raw.orders.amount_cents to amount_minor after the load")
    return changes


def load_landed_orders(conn, container, chaos: Chaos, rng: random.Random | None = None) -> dict:
    """Load new landing blobs into raw.orders in one transaction. Returns counts."""
    rng = rng or random.Random()
    stats = {"blobs": 0, "orders": 0, "waiting": 0}
    with conn.cursor() as cur:
        cur.execute(dw.CREATE_INGESTED_FILES)
        if chaos.schema_drift:
            cur.execute(dw.ADD_PROMO_CODE)
        elif chaos.schema_drift is False:
            cur.execute(dw.DROP_PROMO_CODE)
        cur.execute(dw.RESTORE_AMOUNT_COLUMN)
        cur.execute("SELECT name FROM raw._ingested_files")
        done = {row[0] for row in cur.fetchall()}
        # On an ADLS Gen2 account the listing also returns the directories
        # (orders/dt=..., size 0), so only take the batch files.
        pending = sorted(
            b.name
            for b in container.list_blobs(name_starts_with="orders/")
            if b.name.endswith(".json") and b.name not in done
        )
        stats["waiting"] = len(pending)
        if not chaos.skip_load:
            for name in pending[:MAX_BLOBS_PER_RUN]:
                orders = apply_chaos(parse_batch(container.download_blob(name).readall()), chaos, rng)
                cur.executemany(
                    dw.INSERT_ORDER,
                    [tuple(o.get(f) for f in ORDER_FIELDS) + (name,) for o in orders],
                )
                cur.execute(
                    "INSERT INTO raw._ingested_files (name, orders) VALUES (%s, %s)",
                    (name, len(orders)),
                )
                stats["blobs"] += 1
                stats["orders"] += len(orders)
            stats["waiting"] -= stats["blobs"]
        cur.execute(dw.PURGE_OLD_ORDERS)
        if chaos.breaking_schema:
            cur.execute(dw.BREAK_AMOUNT_COLUMN)
    conn.commit()
    return stats
