"""Contoso order processor: consumes the "orders" topic and lands batches in ADLS.

Runs as the ``order-processor`` Container App under ``ddtrace-run`` with
``DD_DATA_STREAMS_ENABLED=true``, so Data Streams Monitoring shows it as the
consumer on the orders pathway. Every FLUSH_SECONDS it writes the valid orders as
one newline-delimited JSON blob:

    landing/orders/dt=YYYY-MM-DD/hour=HH/batch-<timestamp>-<id>.json

and reports the batch as an OpenLineage run (job ``order_processor.land_orders``,
Kafka topic in, landing zone out). The contoso_orders DAG loads the blobs into
Postgres. Offsets are committed only after the blob is written, so a crash
replays the batch rather than losing it.

Settings (env): KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONNECTION_STRING, KAFKA_TOPIC,
KAFKA_GROUP_ID, LANDING_CONNECTION_STRING, LANDING_ACCOUNT, LANDING_CONTAINER,
FLUSH_SECONDS (default 300), and DD_API_KEY / DD_SITE / OPENLINEAGE_NAMESPACE
for lineage.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

from contoso_stream import (
    kafka_config,
    kafka_dataset,
    landing_dataset,
    log,
    run_event,
    send_lineage,
)

REQUIRED_FIELDS = ("order_id", "store", "channel", "items", "amount_cents", "currency", "ordered_at")
JOB_NAME = "order_processor.land_orders"


def parse_order(raw: bytes | None) -> dict | None:
    """The order, or None if the message isn't a usable order."""
    if not raw:
        return None
    try:
        order = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(order, dict) or any(order.get(f) in (None, "") for f in REQUIRED_FIELDS):
        return None
    return order


def blob_name(now: datetime, run_id: str) -> str:
    return (
        f"orders/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"batch-{now:%Y%m%dT%H%M%S}-{run_id[:8]}.json"
    )


def to_ndjson(orders: list[dict]) -> bytes:
    return "".join(json.dumps(o, separators=(",", ":")) + "\n" for o in orders).encode()


def main() -> int:
    from azure.storage.blob import BlobServiceClient
    from confluent_kafka import Consumer

    env = os.environ
    topic = env.get("KAFKA_TOPIC", "orders")
    flush_seconds = int(env.get("FLUSH_SECONDS", "300"))
    container = BlobServiceClient.from_connection_string(
        env["LANDING_CONNECTION_STRING"]
    ).get_container_client(env.get("LANDING_CONTAINER", "landing"))
    inputs = [kafka_dataset(env["KAFKA_BOOTSTRAP_SERVERS"], topic)]
    outputs = [landing_dataset(env["LANDING_ACCOUNT"], env.get("LANDING_CONTAINER", "landing"))]
    namespace = env.get("OPENLINEAGE_NAMESPACE", "default")

    consumer = Consumer(
        {
            **kafka_config(),
            "group.id": env.get("KAFKA_GROUP_ID", "order-processor"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    log(f"consuming {topic}, landing a batch every {flush_seconds}s")

    batch: list[dict] = []
    rejected = 0
    started = datetime.now(timezone.utc)
    deadline = time.monotonic() + flush_seconds
    while True:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                log(f"consumer error: {msg.error()}")
            elif (order := parse_order(msg.value())) is not None:
                batch.append(order)
            else:
                rejected += 1
        if time.monotonic() < deadline:
            continue

        if batch:
            run_id = str(uuid.uuid4())
            lineage = {
                "job_namespace": namespace,
                "job_name": JOB_NAME,
                "run_id": run_id,
                "inputs": inputs,
                "outputs": outputs,
            }
            send_lineage(run_event("START", event_time=started, **lineage))
            name = blob_name(datetime.now(timezone.utc), run_id)
            container.upload_blob(name, to_ndjson(batch), overwrite=True)
            consumer.commit(asynchronous=False)
            send_lineage(
                run_event(
                    "COMPLETE",
                    event_time=datetime.now(timezone.utc),
                    output_rows=len(batch),
                    **lineage,
                )
            )
            log(f"landed {len(batch)} orders in {name} ({rejected} rejected)")
        batch, rejected = [], 0
        started = datetime.now(timezone.utc)
        deadline = time.monotonic() + flush_seconds


if __name__ == "__main__":
    raise SystemExit(main())
