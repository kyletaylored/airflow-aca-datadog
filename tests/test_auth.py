"""Offline tests for auth resolution (mocked connections + credentials)."""
import sys
import types

from operators import aca_jobs_common as aca


class FakeConn:
    def __init__(self, login=None, password=None, extra=None):
        self.login = login
        self.password = password
        self.extra_dejson = extra or {}


def _mock_basehook(monkeypatch, conn):
    """Make the Airflow Connection lookup return our fake connection."""

    def get_connection(conn_id):
        if conn is None:
            raise KeyError(conn_id)
        return conn

    monkeypatch.setattr(aca, "_get_connection", get_connection)


def _mock_identity(monkeypatch):
    """Stub azure.identity credentials so no real Azure call is made."""
    identity = types.ModuleType("azure.identity")

    class _Tok:
        def __init__(self, token):
            self.token = token

    class ClientSecretCredential:
        def __init__(self, tenant_id, client_id, client_secret):
            self.args = (tenant_id, client_id, client_secret)

        def get_token(self, scope):
            return _Tok("sp-token")

    class DefaultAzureCredential:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def get_token(self, scope):
            mi = self.kwargs.get("managed_identity_client_id")
            return _Tok(f"default-token:{mi}" if mi else "default-token")

    identity.ClientSecretCredential = ClientSecretCredential
    identity.DefaultAzureCredential = DefaultAzureCredential
    azure_pkg = sys.modules.get("azure") or types.ModuleType("azure")
    monkeypatch.setitem(sys.modules, "azure", azure_pkg)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)


def test_env_token_path(monkeypatch):
    monkeypatch.setenv("AZURE_ACCESS_TOKEN", "env-token-abc")
    assert aca.get_token() == "env-token-abc"


def test_default_credential_path(monkeypatch):
    monkeypatch.delenv("AZURE_ACCESS_TOKEN", raising=False)
    _mock_identity(monkeypatch)
    assert aca.get_token() == "default-token"


def test_default_credential_with_managed_identity(monkeypatch):
    monkeypatch.delenv("AZURE_ACCESS_TOKEN", raising=False)
    _mock_identity(monkeypatch)
    token = aca.get_token({"managed_identity_client_id": "mi-123"})
    assert token == "default-token:mi-123"


def test_connection_service_principal(monkeypatch):
    conn = FakeConn(
        login="client-1",
        password="secret-1",
        extra={"tenant_id": "tenant-1"},
    )
    _mock_basehook(monkeypatch, conn)
    _mock_identity(monkeypatch)
    assert aca.get_token({"conn_id": "azure_default"}) == "sp-token"


def test_connection_access_token_extra(monkeypatch):
    conn = FakeConn(extra={"access_token": "pre-fetched-tok"})
    _mock_basehook(monkeypatch, conn)
    assert aca.get_token({"conn_id": "azure_default"}) == "pre-fetched-tok"


def test_connection_falls_back_to_managed_identity(monkeypatch):
    # No secret/tenant -> falls back to DefaultAzureCredential with the MI id.
    conn = FakeConn(extra={"managed_identity_client_id": "mi-xyz"})
    _mock_basehook(monkeypatch, conn)
    _mock_identity(monkeypatch)
    assert aca.get_token({"conn_id": "azure_default"}) == "default-token:mi-xyz"


def test_connection_defaults_extracts_sub_and_rg(monkeypatch):
    conn = FakeConn(extra={"subscription_id": "sub-9", "resource_group": "rg-9"})
    _mock_basehook(monkeypatch, conn)
    defaults = aca.connection_defaults("azure_default")
    assert defaults == {"subscription_id": "sub-9", "resource_group": "rg-9"}


def test_connection_defaults_missing_conn_returns_empty(monkeypatch):
    _mock_basehook(monkeypatch, None)
    assert aca.connection_defaults("nope") == {}


def test_connection_defaults_none_conn_id():
    assert aca.connection_defaults(None) == {}
