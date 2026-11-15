#!/usr/bin/env python3
"""Create the Contoso demo's Metabase dashboards, or export them back to the spec.

    python3 scripts/metabase_seed.py seed     # set up Metabase and create the dashboards
    python3 scripts/metabase_seed.py export   # write the dashboards to metabase/dashboards.json

``seed`` works on a new Metabase: it creates the admin user, connects the
contoso_dw database (as the read-only ``metabase`` role, marts only), and creates
the "Contoso Demo" collection with its questions and dashboards. It's safe to run
again: anything that already exists, by name, is left alone.

``export`` reads that collection back, so changes made in the Metabase UI can be
committed. Questions built in the query builder refer to tables and columns by
Metabase's internal IDs, which differ between instances; the spec stores names
instead (``@table:marts.daily_store_sales``, ``@field:marts.daily_store_sales.store``)
and ``seed`` looks the IDs up again.

Settings, from the environment:
    METABASE_URL              e.g. the deployment's metabaseUrl output
    METABASE_ADMIN_EMAIL      the admin user (seed creates it on a new instance)
    METABASE_ADMIN_PASSWORD
    METABASE_DB_PASSWORD      seed only: the metabase role's password
    CONTOSO_DW_HOST           seed only: the Postgres server's FQDN

Standard library only, so it runs anywhere Python 3.9+ does.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

SPEC = Path(__file__).resolve().parent.parent / "metabase" / "dashboards.json"
COLLECTION = "Contoso Demo"
DATABASE = "contoso_dw"
SCHEMA = "marts"

# Keys whose integer values are Metabase IDs in a query or a parameter target.
_TABLE_KEYS = {"source-table"}
_FIELD_KEYS = {"source-field"}


# --- Portable references ----------------------------------------------------------


def to_portable(value, tables: dict[int, str], fields: dict[int, str], database_id: int):
    """Replace Metabase IDs in a query (legacy or pMBQL) with names."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key == "database" and item == database_id:
                out[key] = "@database"
            elif key in _TABLE_KEYS and isinstance(item, int):
                out[key] = "@table:" + tables[item]
            elif key in _FIELD_KEYS and isinstance(item, int):
                out[key] = "@field:" + fields[item]
            else:
                out[key] = to_portable(item, tables, fields, database_id)
        return out
    if isinstance(value, list):
        # A field reference: ["field", <id>, {opts}] (legacy) or ["field", {opts}, <id>].
        if len(value) == 3 and value[0] == "field":
            return [
                "@field:" + fields[v] if isinstance(v, int) and not isinstance(v, bool) else
                to_portable(v, tables, fields, database_id)
                for v in value
            ]
        return [to_portable(v, tables, fields, database_id) for v in value]
    return value


def from_portable(value, tables: dict[str, int], fields: dict[str, int], database_id: int):
    """The reverse of ``to_portable``, with this instance's IDs."""
    if isinstance(value, dict):
        return {k: from_portable(v, tables, fields, database_id) for k, v in value.items()}
    if isinstance(value, list):
        return [from_portable(v, tables, fields, database_id) for v in value]
    if isinstance(value, str):
        if value == "@database":
            return database_id
        if value.startswith("@table:"):
            return tables[value[len("@table:"):]]
        if value.startswith("@field:"):
            return fields[value[len("@field:"):]]
    return value


# --- Metabase API -------------------------------------------------------------------


class Metabase:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session: str | None = None

    def call(self, method: str, path: str, body=None, retries: int = 0):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.url + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    raw = response.read()
                    return json.loads(raw) if raw else None
            except urllib.error.HTTPError as err:
                detail = err.read().decode(errors="replace")[:500]
                raise SystemExit(f"{method} {path} failed: {err.code} {detail}") from None
            except (urllib.error.URLError, TimeoutError):
                # The app scales to zero; the first request waits for it to start.
                if attempt == retries:
                    raise
                time.sleep(10)

    def wait_until_up(self) -> dict:
        print(f"Waiting for Metabase at {self.url} (it can take a minute to start)...")
        deadline = time.time() + 600
        while True:
            try:
                return self.call("GET", "/api/session/properties")
            # SystemExit too: while it starts, the ingress answers 502 or 503.
            except (urllib.error.URLError, TimeoutError, SystemExit):
                if time.time() > deadline:
                    raise SystemExit("Metabase didn't respond within 10 minutes.") from None
                time.sleep(10)

    def log_in(self, email: str, password: str) -> None:
        self.session = self.call("POST", "/api/session", {"username": email, "password": password})["id"]

    def collection_id(self, create: bool) -> int | None:
        for collection in self.call("GET", "/api/collection"):
            if collection.get("name") == COLLECTION and not collection.get("archived"):
                return collection["id"]
        if not create:
            return None
        return self.call("POST", "/api/collection", {"name": COLLECTION, "color": "#509EE3"})["id"]

    def items(self, collection_id: int, model: str) -> list[dict]:
        result = self.call("GET", f"/api/collection/{collection_id}/items?models={model}")
        return result["data"] if isinstance(result, dict) else result

    def database_id(self) -> int | None:
        result = self.call("GET", "/api/database")
        for db in result["data"] if isinstance(result, dict) else result:
            if db["name"] == DATABASE:
                return db["id"]
        return None

    def ids(self, database_id: int) -> tuple[dict[int, str], dict[int, str]]:
        """Table and field IDs in the marts schema, by ``schema.table[.column]``."""
        metadata = self.call("GET", f"/api/database/{database_id}/metadata")
        tables, fields = {}, {}
        for table in metadata["tables"]:
            if table["schema"] != SCHEMA:
                continue
            name = f"{table['schema']}.{table['name']}"
            tables[table["id"]] = name
            for field in table["fields"]:
                fields[field["id"]] = f"{name}.{field['name']}"
        return tables, fields


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Set {name}.")
    return value


# --- seed ---------------------------------------------------------------------------


def seed() -> None:
    spec = json.loads(SPEC.read_text())
    mb = Metabase(_env("METABASE_URL"))
    email, password = _env("METABASE_ADMIN_EMAIL"), _env("METABASE_ADMIN_PASSWORD")

    properties = mb.wait_until_up()
    if not properties.get("has-user-setup"):
        print(f"New instance: creating the admin user {email}.")
        mb.session = mb.call("POST", "/api/setup", {
            "token": properties["setup-token"],
            "user": {"email": email, "password": password, "first_name": "Contoso", "last_name": "Admin"},
            "prefs": {"site_name": "Contoso", "allow_tracking": False},
        })["id"]
    else:
        mb.log_in(email, password)

    database_id = mb.database_id()
    if database_id is None:
        print(f"Connecting {DATABASE} (marts only, as the metabase role).")
        database_id = mb.call("POST", "/api/database", {
            "engine": "postgres",
            "name": DATABASE,
            "details": {
                "host": _env("CONTOSO_DW_HOST"),
                "port": 5432,
                "dbname": DATABASE,
                "user": "metabase",
                "password": _env("METABASE_DB_PASSWORD"),
                "ssl": True,
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": SCHEMA,
            },
        })["id"]

    needed = {f"{SCHEMA}.{t}" for t in spec["tables"]}
    print("Waiting for Metabase to sync the marts...")
    mb.call("POST", f"/api/database/{database_id}/sync_schema")
    deadline = time.time() + 300
    while True:
        tables, fields = mb.ids(database_id)
        if needed <= set(tables.values()):
            break
        if time.time() > deadline:
            raise SystemExit(f"Metabase hasn't synced {sorted(needed - set(tables.values()))}.")
        time.sleep(10)
    table_ids = {v: k for k, v in tables.items()}
    field_ids = {v: k for k, v in fields.items()}

    collection_id = mb.collection_id(create=True)
    existing = {c["name"]: c["id"] for c in mb.items(collection_id, "card")}
    card_ids = {}
    for card in spec["cards"]:
        if card["name"] in existing:
            card_ids[card["name"]] = existing[card["name"]]
            continue
        created = mb.call("POST", "/api/card", {
            "name": card["name"],
            "description": card.get("description"),
            "display": card["display"],
            "type": card.get("type", "question"),
            "dataset_query": from_portable(card["dataset_query"], table_ids, field_ids, database_id),
            "visualization_settings": card.get("visualization_settings", {}),
            "collection_id": collection_id,
        })
        card_ids[card["name"]] = created["id"]
        print(f"  question: {card['name']}")

    dashboards = {d["name"] for d in mb.items(collection_id, "dashboard")}
    for dash in spec["dashboards"]:
        if dash["name"] in dashboards:
            print(f"  dashboard exists, left as it is: {dash['name']}")
            continue
        created = mb.call("POST", "/api/dashboard", {
            "name": dash["name"],
            "description": dash.get("description"),
            "collection_id": collection_id,
        })
        dashcards = []
        for n, dc in enumerate(dash["dashcards"], start=1):
            card_id = card_ids[dc["card"]] if dc.get("card") else None
            mappings = [
                {**m, "card_id": card_id,
                 "target": from_portable(m["target"], table_ids, field_ids, database_id)}
                for m in dc.get("parameter_mappings", [])
            ]
            dashcards.append({
                "id": -n,
                "card_id": card_id,
                "row": dc["row"], "col": dc["col"],
                "size_x": dc["size_x"], "size_y": dc["size_y"],
                "visualization_settings": dc.get("visualization_settings", {}),
                "parameter_mappings": mappings,
            })
        mb.call("PUT", f"/api/dashboard/{created['id']}", {
            "dashcards": dashcards,
            "parameters": dash.get("parameters", []),
            "width": dash.get("width", "full"),
        })
        print(f"  dashboard: {dash['name']}")
    print(f"Done: {mb.url}/collection/{collection_id}")


# --- export -------------------------------------------------------------------------


def export() -> None:
    mb = Metabase(_env("METABASE_URL"))
    mb.wait_until_up()
    mb.log_in(_env("METABASE_ADMIN_EMAIL"), _env("METABASE_ADMIN_PASSWORD"))
    database_id = mb.database_id()
    collection_id = mb.collection_id(create=False)
    if database_id is None or collection_id is None:
        raise SystemExit(f"No {DATABASE} database or {COLLECTION!r} collection to export.")
    tables, fields = mb.ids(database_id)

    cards, names = [], {}
    for item in sorted(mb.items(collection_id, "card"), key=lambda c: c["id"]):
        card = mb.call("GET", f"/api/card/{item['id']}")
        names[card["id"]] = card["name"]
        cards.append({
            "name": card["name"],
            "description": card.get("description"),
            "display": card["display"],
            "type": card.get("type", "question"),
            "dataset_query": to_portable(card["dataset_query"], tables, fields, database_id),
            "visualization_settings": card.get("visualization_settings") or {},
        })

    dashboards = []
    for item in sorted(mb.items(collection_id, "dashboard"), key=lambda d: d["id"]):
        dash = mb.call("GET", f"/api/dashboard/{item['id']}")
        dashboards.append(portable_dashboard(dash, names, tables, fields, database_id))

    used = sorted({t.split(".", 1)[1] for t in tables.values()})
    SPEC.write_text(json.dumps({"tables": used, "cards": cards, "dashboards": dashboards}, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {len(cards)} questions and {len(dashboards)} dashboards to {SPEC}")


def portable_dashboard(dash: dict, card_names: dict[int, str], tables, fields, database_id) -> dict:
    dashcards = []
    for dc in sorted(dash["dashcards"], key=lambda d: (d["row"], d["col"])):
        dashcards.append({
            "card": card_names.get(dc.get("card_id")) if dc.get("card_id") else None,
            "row": dc["row"], "col": dc["col"],
            "size_x": dc["size_x"], "size_y": dc["size_y"],
            "visualization_settings": dc.get("visualization_settings") or {},
            "parameter_mappings": [
                {k: v for k, v in m.items() if k != "card_id"} | {"target": to_portable(m["target"], tables, fields, database_id)}
                for m in dc.get("parameter_mappings") or []
            ],
        })
    return {
        "name": dash["name"],
        "description": dash.get("description"),
        "width": dash.get("width", "full"),
        "parameters": dash.get("parameters") or [],
        "dashcards": dashcards,
    }


if __name__ == "__main__":
    commands = {"seed": seed, "export": export}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        raise SystemExit(__doc__)
    commands[sys.argv[1]]()
