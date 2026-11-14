"""Contoso storefront: sells things and publishes each order to the "orders" topic.

Runs as the ``contoso-storefront`` Container App under ``ddtrace-run`` with
``DD_DATA_STREAMS_ENABLED=true``, so Datadog Data Streams Monitoring shows it as
the producer on the orders pathway.

Settings (env): KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONNECTION_STRING, KAFKA_TOPIC
(default "orders"), ORDERS_PER_MINUTE (default 20).
"""
from __future__ import annotations

import json
import os
import random
import time

from contoso_stream import kafka_config, log, make_order


def main() -> int:
    from confluent_kafka import Producer

    topic = os.environ.get("KAFKA_TOPIC", "orders")
    per_minute = max(float(os.environ.get("ORDERS_PER_MINUTE", "20")), 0.1)
    producer = Producer({**kafka_config(), "client.id": "contoso-storefront", "linger.ms": 100})
    rng = random.Random()
    sent = 0

    def delivered(err, _msg) -> None:
        if err is not None:
            log(f"delivery failed: {err}")

    log(f"producing about {per_minute:g} orders a minute to {topic}")
    while True:
        order = make_order(rng)
        producer.produce(topic, key=order["store"], value=json.dumps(order), on_delivery=delivered)
        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            log(f"{sent} orders sent")
        # Uneven gaps, like real traffic.
        time.sleep(rng.expovariate(per_minute / 60.0))


if __name__ == "__main__":
    raise SystemExit(main())
