"""The Contoso job program, run the way ACA runs it (``python -c <source>``).

A local HTTP server stands in for the OpenLineage intake. No Airflow needed.
"""
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

SOURCE = (
    Path(__file__).parent.parent / "airflow" / "dags" / "jobs" / "contoso_store_job.py"
).read_text()


def test_source_has_no_jinja_openers():
    # The operator renders the command with Jinja.
    for opener in ("{" + "{", "{" + "%", "{" + "#"):
        assert opener not in SOURCE


@pytest.fixture
def intake():
    events = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            events.append((self.path, dict(self.headers), json.loads(body)))
            self.send_response(201)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", events
    server.shutdown()


def run(env, attempt="1"):
    base = {k: v for k, v in os.environ.items() if not k.startswith(("DD_", "OPENLINEAGE"))}
    return subprocess.run(
        [sys.executable, "-I", "-c", SOURCE, attempt],
        env={**base, "WORK_SECONDS": "0", "BUSINESS_DATE": "2026-11-13", **env},
        capture_output=True, text=True, timeout=30,
    )


PARENT = {
    "OPENLINEAGE_PARENT_ID": "ns/contoso_sales.report_store_sales/task-run",
    "OPENLINEAGE_ROOT_PARENT_ID": "ns/contoso_sales/dag-run",
}


def test_store_reports_child_run_with_output(intake):
    url, events = intake
    out = run({"MODE": "store", "STORE_NAME": "San Francisco", "OPENLINEAGE_URL": url, **PARENT})
    assert out.returncode == 0, out.stderr
    assert "San Francisco sold $" in out.stdout
    assert [e["eventType"] for _, _, e in events] == ["START", "COMPLETE"]
    path, _, done = events[-1]
    assert path == "/api/v1/lineage"
    assert done["job"]["name"] == "contoso_store_job.report_san_francisco"
    assert done["job"]["namespace"] == "ns"  # from the parent when unset
    parent = done["run"]["facets"]["parent"]
    assert parent["run"]["runId"] == "task-run"
    assert parent["job"] == {"namespace": "ns", "name": "contoso_sales.report_store_sales"}
    assert parent["root"]["run"]["runId"] == "dag-run"
    assert done["outputs"][0]["name"] == "stores/san_francisco/daily_sales"
    assert events[0][2]["outputs"] == []  # outputs only once written


def test_chaos_store_reports_failure_then_recovers(intake):
    url, events = intake
    env = {"MODE": "store", "STORE_NAME": "Denver", "CHAOS": "1", "CHAOS_STORE": "Denver",
           "OPENLINEAGE_URL": url}
    assert run(env, attempt="1").returncode == 1
    fail = events[-1][2]
    assert fail["eventType"] == "FAIL"
    assert "crashed" in fail["run"]["facets"]["errorMessage"]["message"]
    assert run(env, attempt="2").returncode == 0
    assert events[-1][2]["eventType"] == "COMPLETE"


def test_finalize_reads_every_store(intake):
    url, events = intake
    out = run({"MODE": "finalize", "STORE_LIST": "Austin|New York",
               "OPENLINEAGE_URL": url, "OPENLINEAGE_NAMESPACE": "env1"})
    assert out.returncode == 0, out.stderr
    assert "TOTAL across 2 stores" in out.stdout
    done = events[-1][2]
    assert done["job"] == {**done["job"], "namespace": "env1", "name": "contoso_store_job.company_total"}
    assert [d["name"] for d in done["inputs"]] == ["stores/austin/daily_sales", "stores/new_york/daily_sales"]
    assert "parent" not in done["run"]["facets"]


def test_no_backend_sends_nothing_and_unreachable_backend_is_harmless():
    assert run({"MODE": "store", "STORE_NAME": "Austin"}).returncode == 0
    out = run({"MODE": "store", "STORE_NAME": "Austin", "OPENLINEAGE_URL": "http://127.0.0.1:9"})
    assert out.returncode == 0
    assert "not sent" in out.stderr
