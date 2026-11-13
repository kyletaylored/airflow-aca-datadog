"""Offline tests for the shared ARM helpers (no Azure, no network)."""
import json

import pytest
import requests
from airflow.exceptions import AirflowException
from operators import aca_jobs_common as aca


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None, text=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(body or {})

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


# --- ACAJobRef ------------------------------------------------------------

def test_jobref_from_resource_id_roundtrip():
    rid = (
        "/subscriptions/sub1/resourceGroups/rg1/providers/"
        "Microsoft.App/jobs/job1"
    )
    ref = aca.ACAJobRef.from_resource_id(rid)
    assert ref.subscription_id == "sub1"
    assert ref.resource_group == "rg1"
    assert ref.job_name == "job1"
    assert ref.base_url.endswith("Microsoft.App/jobs/job1")
    assert aca.ACAJobRef.from_dict(ref.to_dict()) == ref


def test_jobref_from_resource_id_invalid():
    with pytest.raises(AirflowException):
        aca.ACAJobRef.from_resource_id("/not/a/valid/id")


# --- retry / backoff ------------------------------------------------------

def test_retry_delay_honors_retry_after():
    resp = FakeResponse(status_code=429, headers={"Retry-After": "7"})
    assert aca._retry_delay(resp, attempt=0, backoff_base=1.0, backoff_cap=60.0) == 7.0


def test_retry_delay_falls_back_to_backoff():
    resp = FakeResponse(status_code=503, headers={})
    # attempt=2 -> 1 * 2**2 = 4
    assert aca._retry_delay(resp, attempt=2, backoff_base=1.0, backoff_cap=60.0) == 4.0


def test_request_with_retry_success(monkeypatch):
    monkeypatch.setattr(
        aca.requests, "request", lambda *a, **k: FakeResponse(200, {"ok": True})
    )
    resp = aca.request_with_retry("GET", "http://x", "tok", sleep=lambda _s: None)
    assert resp.status_code == 200


def test_request_with_retry_429_then_success(monkeypatch):
    calls = {"n": 0}
    slept = []

    def fake_request(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(429, headers={"Retry-After": "2"})
        return FakeResponse(200, {"ok": True})

    monkeypatch.setattr(aca.requests, "request", fake_request)
    resp = aca.request_with_retry(
        "GET", "http://x", "tok", sleep=slept.append
    )
    assert resp.status_code == 200
    assert slept == [2.0]  # honored Retry-After


def test_request_with_retry_401_raises_token_expired(monkeypatch):
    monkeypatch.setattr(
        aca.requests, "request", lambda *a, **k: FakeResponse(401)
    )
    with pytest.raises(aca.ACATokenExpired):
        aca.request_with_retry("GET", "http://x", "tok", sleep=lambda _s: None)


def test_request_with_retry_transient_5xx_retried(monkeypatch):
    seq = [FakeResponse(503), FakeResponse(502), FakeResponse(200, {"ok": True})]
    monkeypatch.setattr(aca.requests, "request", lambda *a, **k: seq.pop(0))
    resp = aca.request_with_retry(
        "GET", "http://x", "tok", sleep=lambda _s: None, max_retries=5
    )
    assert resp.status_code == 200


def test_request_with_retry_network_error_retried_then_raises(monkeypatch):
    def boom(*a, **k):
        raise requests.RequestException("connection reset")

    monkeypatch.setattr(aca.requests, "request", boom)
    with pytest.raises(AirflowException):
        aca.request_with_retry(
            "GET", "http://x", "tok", sleep=lambda _s: None, max_retries=2
        )


# --- start / state parsing -----------------------------------------------

def test_start_execution_name_from_body(monkeypatch):
    monkeypatch.setattr(
        aca, "request_with_retry",
        lambda *a, **k: FakeResponse(200, {"name": "exec-123"}),
    )
    ref = aca.ACAJobRef("s", "r", "j")
    assert aca.start_execution(ref, "tok", {}, "2024-03-01") == "exec-123"


def test_start_execution_name_from_location_header(monkeypatch):
    resp = FakeResponse(
        202, body={}, headers={"Location": "https://arm/executions/exec-xyz?api-version=x"}
    )
    monkeypatch.setattr(aca, "request_with_retry", lambda *a, **k: resp)
    ref = aca.ACAJobRef("s", "r", "j")
    assert aca.start_execution(ref, "tok", {}, "2024-03-01") == "exec-xyz"


def test_start_execution_error_status(monkeypatch):
    monkeypatch.setattr(
        aca, "request_with_retry",
        lambda *a, **k: FakeResponse(400, {}, text="bad request"),
    )
    ref = aca.ACAJobRef("s", "r", "j")
    with pytest.raises(AirflowException):
        aca.start_execution(ref, "tok", {}, "2024-03-01")


def test_get_execution_state(monkeypatch):
    monkeypatch.setattr(
        aca, "request_with_retry",
        lambda *a, **k: FakeResponse(200, {"properties": {"status": "Running"}}),
    )
    ref = aca.ACAJobRef("s", "r", "j")
    assert aca.get_execution_state(ref, "exec-1", "tok", "2024-03-01") == "Running"


def test_get_execution_state_missing_status(monkeypatch):
    monkeypatch.setattr(
        aca, "request_with_retry",
        lambda *a, **k: FakeResponse(200, {"properties": {}}),
    )
    ref = aca.ACAJobRef("s", "r", "j")
    with pytest.raises(AirflowException):
        aca.get_execution_state(ref, "exec-1", "tok", "2024-03-01")



# --- OTel span naming -----------------------------------------------------

_JOB = (
    "https://management.azure.com/subscriptions/sub1/resourceGroups/rg1/"
    "providers/Microsoft.App/jobs/job1"
)


@pytest.mark.parametrize(
    "method,url,expected",
    [
        ("post", f"{_JOB}/start?api-version=2024-03-01", "POST Microsoft.App/jobs/{job}/start"),
        (
            "GET",
            f"{_JOB}/executions/job1-abc12?api-version=2024-03-01",
            "GET Microsoft.App/jobs/{job}/executions/{execution}",
        ),
        ("GET", "http://localhost:42356/msi/token?resource=x", None),
    ],
)
def test_arm_resource_name(method, url, expected):
    assert aca.arm_resource_name(method, url) == expected


def test_name_arm_span_sets_resource_only_for_arm_calls():
    class Span:
        def __init__(self, recording=True):
            self.attrs = {}
            self.recording = recording

        def is_recording(self):
            return self.recording

        def set_attribute(self, key, value):
            self.attrs[key] = value

    arm = requests.Request("POST", f"{_JOB}/start?api-version=1").prepare()
    token = requests.Request("GET", "http://localhost:12356/msi/token?resource=x").prepare()
    other = requests.Request("GET", "https://example.com/x").prepare()

    span = Span()
    aca._name_arm_span(span, arm)
    assert span.attrs == {"resource.name": "POST Microsoft.App/jobs/{job}/start"}

    span = Span()
    aca._name_arm_span(span, token)
    assert span.attrs == {"resource.name": "GET managed identity token"}

    span = Span()
    aca._name_arm_span(span, other)
    assert span.attrs == {}

    span = Span(recording=False)
    aca._name_arm_span(span, arm)
    assert span.attrs == {}
