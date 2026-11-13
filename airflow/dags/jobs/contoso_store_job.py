"""Contoso store job: the code each ACA Job execution runs.

The contoso_sales DAG sends this file's source as ``python -c <source> <try>``
to the stock ``python:3.12-slim`` image, so it is stdlib only.

    MODE=store     one store tallies its registers and reports its sales
    MODE=finalize  adds every store's sales into the company total

It also reports its own run to OpenLineage as a child of the task that started
it: the operator passes OPENLINEAGE_PARENT_ID / OPENLINEAGE_ROOT_PARENT_ID
(``{namespace}/{job_name}/{run_id}``). Datadog then nests this execution under
that task, like a Databricks run, with the datasets it wrote or read as lineage.
Events go to Datadog when DD_API_KEY and DD_SITE are set, else to OPENLINEAGE_URL
(any OpenLineage backend); with neither, nothing is sent. Lineage problems are
logged and never fail the job.

The operator renders the command with Jinja, so this file must not contain a
Jinja opener (an open brace followed by another brace, a percent sign or a hash);
tests/test_contoso_job.py checks that.
"""
import datetime
import hashlib
import json
import os
import sys
import time
import urllib.request
import uuid

PRODUCER = "https://github.com/kyletaylored/airflow-aca-datadog"
SPEC = "https://openlineage.io/spec/"
# Where the job "writes" (illustrative; the sales figures are derived, not stored).
DATA_NAMESPACE = "abfss://sales@contosodata.dfs.core.windows.net"
# Job names are "<prefix>.<step>"; Datadog shows the prefix as the service.
JOB_PREFIX = "contoso_store_job"


def sales(store, date):
    h = hashlib.sha256((store + "|" + date).encode()).hexdigest()
    return 1000 + int(h[:6], 16) % 9000


def registers(store):
    return 3 + int(hashlib.sha256(store.encode()).hexdigest()[:2], 16) % 5


def slug(name):
    return name.strip().lower().replace(" ", "_")


# --- OpenLineage ----------------------------------------------------------------

def _facet(schema, **fields):
    return dict(_producer=PRODUCER, _schemaURL=SPEC + schema, **fields)


def _ref(value):
    """'{namespace}/{job_name}/{run_id}' -> the run/job part of a parent facet."""
    namespace, rest = value.split("/", 1)
    name, run_id = rest.rsplit("/", 1)
    return dict(run=dict(runId=run_id), job=dict(namespace=namespace, name=name))


def _parent_facet():
    parent = os.environ.get("OPENLINEAGE_PARENT_ID")
    if not parent:
        return None
    facet = _facet("facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet", **_ref(parent))
    root = os.environ.get("OPENLINEAGE_ROOT_PARENT_ID")
    if root:
        facet["root"] = _ref(root)
    return facet


def _endpoint():
    key, site = os.environ.get("DD_API_KEY"), os.environ.get("DD_SITE")
    if key and site:
        return "https://data-obs-intake." + site + "/api/v1/lineage", dict([("DD-API-KEY", key)])
    url = os.environ.get("OPENLINEAGE_URL")
    if url:
        headers = dict()
        if os.environ.get("OPENLINEAGE_API_KEY"):
            headers["Authorization"] = "Bearer " + os.environ["OPENLINEAGE_API_KEY"]
        return url.rstrip("/") + "/api/v1/lineage", headers
    return None, None


def dataset(name, fields=None, rows=None):
    ds = dict(namespace=DATA_NAMESPACE, name=name, facets=dict())
    if fields:
        ds["facets"]["schema"] = _facet(
            "facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
            fields=[dict(name=n, type=t) for n, t in fields],
        )
    if rows is not None:
        ds["outputFacets"] = dict(outputStatistics=_facet(
            "facets/1-0-2/OutputStatisticsOutputDatasetFacet.json"
            "#/$defs/OutputStatisticsOutputDatasetFacet",
            rowCount=rows,
        ))
    return ds


class Lineage:
    """One OpenLineage run for this execution: START, then COMPLETE or FAIL."""

    def __init__(self, step, inputs=(), outputs=()):
        self.run_id = str(uuid.uuid4())
        self.step = step
        self.inputs, self.outputs = list(inputs), list(outputs)
        self.url, self.headers = _endpoint()
        parent = os.environ.get("OPENLINEAGE_PARENT_ID", "")
        self.namespace = (
            os.environ.get("OPENLINEAGE_NAMESPACE")
            or (parent.split("/", 1)[0] if parent else "default")
        )

    def emit(self, event_type, error=None):
        if not self.url:
            return
        run_facets = dict(azureContainerAppsJob=_facet(
            "facets/1-0-0/AzureContainerAppsJobRunFacet.json",
            jobName=os.environ.get("ACA_JOB_NAME", ""),
            executionName=os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME", ""),
            replicaName=os.environ.get("CONTAINER_APP_REPLICA_NAME", ""),
            attempt=sys.argv[1] if len(sys.argv) > 1 else "1",
        ))
        parent = _parent_facet()
        if parent:
            run_facets["parent"] = parent
        if error:
            run_facets["errorMessage"] = _facet(
                "facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
                message=error, programmingLanguage="python",
            )
        now = datetime.datetime.now(datetime.timezone.utc)
        event = dict(
            eventType=event_type,
            eventTime=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            producer=PRODUCER,
            schemaURL=SPEC + "2-0-2/OpenLineage.json#/$defs/RunEvent",
            run=dict(runId=self.run_id, facets=run_facets),
            job=dict(
                namespace=self.namespace,
                name=JOB_PREFIX + "." + self.step,
                facets=dict(jobType=_facet(
                    "facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    processingType="BATCH", integration="AZURE_CONTAINER_APPS", jobType="JOB",
                )),
            ),
            inputs=self.inputs,
            outputs=self.outputs if event_type != "START" else [],
        )
        headers = dict(self.headers)
        headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.url, data=json.dumps(event).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=10):
                pass
        except Exception as exc:  # lineage must never fail the job
            print("OpenLineage " + event_type + " not sent: " + str(exc), file=sys.stderr)


# --- The job --------------------------------------------------------------------

STORE_FIELDS = [("store", "string"), ("business_date", "date"), ("sales_usd", "decimal")]


def store_dataset(store):
    return "stores/" + slug(store) + "/daily_sales"


def run_store(date, attempt):
    store = os.environ.get("STORE_NAME", "Unknown Store")
    regs = registers(store)
    lineage = Lineage(
        "report_" + slug(store),
        outputs=[dataset(store_dataset(store), STORE_FIELDS, rows=regs)],
    )
    lineage.emit("START")
    print(store + " is tallying " + str(regs) + " registers for " + date + " ...")
    time.sleep(int(os.environ.get("WORK_SECONDS", "4")))
    if os.environ.get("CHAOS") == "1" and store == os.environ.get("CHAOS_STORE", ""):
        if attempt == "1":
            message = store + " register crashed before reporting (attempt 1)."
            sys.stderr.write("!! " + message + "\n")
            lineage.emit("FAIL", message)
            return 1
        print(store + " register recovered on retry (attempt " + attempt + ").")
    print(f"{store} sold ${sales(store, date):,} today.")
    lineage.emit("COMPLETE")
    return 0


def run_finalize(date):
    stores = [s for s in os.environ.get("STORE_LIST", "").split("|") if s]
    lineage = Lineage(
        "company_total",
        inputs=[dataset(store_dataset(s)) for s in stores],
        outputs=[dataset(
            "finance/company_daily_total",
            [("business_date", "date"), ("stores", "integer"), ("total_sales_usd", "decimal")],
            rows=1,
        )],
    )
    lineage.emit("START")
    total = 0
    print("=" * 54)
    print("CONTOSO  daily sales rollup   " + date)
    print("-" * 54)
    for s in stores:
        v = sales(s, date)
        total += v
        print(f"  {s:<24} ${v:>9,}")
    print("-" * 54)
    print(f"  TOTAL across {len(stores)} stores:   ${total:,}")
    print("=" * 54)
    lineage.emit("COMPLETE")
    return 0


def main():
    date = os.environ.get("BUSINESS_DATE", "today")
    attempt = sys.argv[1] if len(sys.argv) > 1 else "1"
    if os.environ.get("MODE", "store") == "finalize":
        return run_finalize(date)
    return run_store(date, attempt)


if __name__ == "__main__":
    sys.exit(main())
