"""Load the order processor's landed batches into raw.orders (stream mode).

The processor writes newline-delimited JSON under ``orders/`` in the landing
container. Each run loads the blobs it hasn't loaded before, recorded in
``raw._ingested_files``. The chaos switches act on the load, as if the load code
had a bug:

    null_customers  30% of the loaded orders lose their customer_id
    volume_drop     only 5% of each batch is loaded (the rest is lost)
    skip_load       nothing is loaded; the blobs wait for the next normal run
    schema_drift    raw.orders gains a promo_code column

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
    schema_drift: bool = False

    @classmethod
    def from_params(cls, params: dict) -> Chaos:
        return cls(**{f: bool(params.get(f)) for f in cls.__dataclass_fields__})


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


def load_landed_orders(conn, container, chaos: Chaos, rng: random.Random | None = None) -> dict:
    """Load new landing blobs into raw.orders in one transaction. Returns counts."""
    rng = rng or random.Random()
    stats = {"blobs": 0, "orders": 0, "waiting": 0}
    with conn.cursor() as cur:
        cur.execute(dw.CREATE_INGESTED_FILES)
        cur.execute(dw.ADD_PROMO_CODE if chaos.schema_drift else dw.DROP_PROMO_CODE)
        cur.execute("SELECT name FROM raw._ingested_files")
        done = {row[0] for row in cur.fetchall()}
        pending = sorted(b.name for b in container.list_blobs(name_starts_with="orders/") if b.name not in done)
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
    conn.commit()
    return stats
