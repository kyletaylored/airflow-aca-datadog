"""Shared pieces for the Contoso order stream (storefront -> Event Hubs -> processor).

Both services run on the stock ``python:3.12-slim`` image with this folder mounted
from the Azure Files share, under ``ddtrace-run`` so Data Streams Monitoring sees
the Kafka produce and consume. Kafka is Event Hubs' Kafka endpoint.

Dataset names follow OpenLineage's conventions, and the Airflow load task uses the
same ones, so Data Observability joins the stream, the landing zone and the
Postgres tables into one lineage graph:

    kafka://<namespace>.servicebus.windows.net:9093   orders
    abfss://landing@<account>.dfs.core.windows.net    orders
"""
from __future__ import annotations

import json
import os
import random
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

STORES = [
    "Seattle",
    "Portland",
    "San Francisco",
    "Denver",
    "Austin",
    "Chicago",
    "New York",
]
CHANNELS = ["web", "web", "web", "mobile", "mobile", "store", "test"]
CUSTOMER_POOL = 2000
PRODUCER = "https://github.com/kyletaylored/airflow-aca-datadog"


def log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {message}", flush=True)


def kafka_config(env: dict[str, str] | None = None) -> dict[str, str]:
    """librdkafka settings for Event Hubs' Kafka endpoint (SASL PLAIN over TLS)."""
    env = os.environ if env is None else env
    return {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        # Event Hubs takes the literal user "$ConnectionString" and the
        # connection string (scoped to one event hub) as the password.
        "sasl.username": "$ConnectionString",
        "sasl.password": env["KAFKA_CONNECTION_STRING"],
    }


def kafka_dataset(bootstrap_servers: str, topic: str) -> dict:
    return {"namespace": f"kafka://{bootstrap_servers.split(',')[0]}", "name": topic}


def landing_dataset(account: str, container: str) -> dict:
    return {"namespace": f"abfss://{container}@{account}.dfs.core.windows.net", "name": "orders"}


def make_order(rng: random.Random, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "order_id": uuid.UUID(int=rng.getrandbits(128), version=4).hex,
        "customer_id": f"C{rng.randint(1, CUSTOMER_POOL):05d}",
        "store": rng.choice(STORES),
        "channel": rng.choice(CHANNELS),
        "items": rng.randint(1, 5),
        "amount_cents": rng.randint(500, 15499),
        "currency": "USD",
        "ordered_at": now.isoformat(),
    }


# --- OpenLineage, straight to Datadog ------------------------------------------


def lineage_endpoint(env: dict[str, str] | None = None) -> tuple[str, dict[str, str]] | None:
    """Datadog's OpenLineage intake when DD_API_KEY and DD_SITE are set, else None."""
    env = os.environ if env is None else env
    if env.get("DD_API_KEY") and env.get("DD_SITE"):
        return (
            f"https://data-obs-intake.{env['DD_SITE']}/api/v1/lineage",
            {"DD-API-KEY": env["DD_API_KEY"]},
        )
    return None


def run_event(
    event_type: str,
    *,
    job_namespace: str,
    job_name: str,
    run_id: str,
    event_time: datetime,
    inputs: list[dict],
    outputs: list[dict],
    output_rows: int | None = None,
) -> dict:
    out = []
    for dataset in outputs:
        dataset = dict(dataset)
        if output_rows is not None and event_type == "COMPLETE":
            dataset["outputFacets"] = {
                "outputStatistics": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
                    "rowCount": output_rows,
                }
            }
        out.append(dataset)
    return {
        "eventType": event_type,
        "eventTime": event_time.isoformat(),
        "producer": PRODUCER,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": run_id},
        "job": {
            "namespace": job_namespace,
            "name": job_name,
            "facets": {
                "jobType": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
                    "processingType": "STREAMING",
                    "integration": "AZURE_CONTAINER_APPS",
                    "jobType": "JOB",
                }
            },
        },
        "inputs": inputs,
        "outputs": out,
    }


def send_lineage(event: dict, env: dict[str, str] | None = None) -> bool:
    """POST one event. Lineage must never take the service down, so errors only log."""
    endpoint = lineage_endpoint(env)
    if endpoint is None:
        return False
    url, headers = endpoint
    request = urllib.request.Request(
        url,
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError) as exc:
        print(f"OpenLineage event not sent: {exc}", file=sys.stderr, flush=True)
        return False
