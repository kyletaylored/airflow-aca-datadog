"""The Metabase dashboard spec and the seed script's ID handling. No Metabase needed."""
from __future__ import annotations

import json
import os
import re
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(__file__))
for path in (os.path.join(_ROOT, "scripts"), os.path.join(_ROOT, "airflow", "dags")):
    if path not in sys.path:
        sys.path.insert(0, path)

import metabase_seed as ms  # noqa: E402
from contoso_dw import sql as dw  # noqa: E402

SPEC = json.loads(ms.SPEC.read_text())


def _mart_columns() -> set[str]:
    """``marts.table.column`` for every column CREATE_TABLES defines in marts."""
    columns = set()
    for statement in dw.CREATE_TABLES:
        match = re.search(r"CREATE TABLE IF NOT EXISTS (marts\.\w+) \((.*)\)", statement, re.S)
        if match:
            for line in match.group(2).split(","):
                if line.strip():
                    columns.add(f"{match.group(1)}.{line.split()[0]}")
    return columns


def test_spec_only_refers_to_mart_tables_and_columns_that_exist():
    text = json.dumps(SPEC)
    columns = _mart_columns()
    assert set(re.findall(r"@field:([\w.]+)", text)) <= columns
    tables = {c.rsplit(".", 1)[0] for c in columns}
    assert set(re.findall(r"@table:([\w.]+)", text)) <= tables
    assert {f"marts.{t}" for t in SPEC["tables"]} <= tables


def test_spec_has_no_instance_ids_left():
    text = json.dumps(SPEC)
    assert not re.search(r'"(source-table|source-field|database)": \d', text)


def test_every_dashboard_tile_names_a_question_in_the_spec():
    names = {c["name"] for c in SPEC["cards"]}
    for dash in SPEC["dashboards"]:
        for tile in dash["dashcards"]:
            assert tile["card"] is None or tile["card"] in names
        for mapping in (m for t in dash["dashcards"] for m in t["parameter_mappings"]):
            assert mapping["parameter_id"] in {p["id"] for p in dash["parameters"]}


def test_spec_has_the_three_demo_dashboards():
    assert [d["name"] for d in SPEC["dashboards"]] == ["Store Sales — Today", "Customer Value", "Pipeline Health"]


@pytest.mark.parametrize(
    "query",
    [
        # pMBQL, as Metabase 0.65 stores and returns it.
        {"lib/type": "mbql/query", "database": 2, "stages": [{
            "source-table": 10,
            "breakout": [["field", {"lib/uuid": "u1", "source-field": 81}, 80]],
        }]},
        # Legacy MBQL, and a dashboard filter target.
        {"database": 2, "type": "query", "query": {"source-table": 10, "breakout": [["field", 80, None]]}},
        ["dimension", ["field", 80, None], {"stage-number": 0}],
    ],
)
def test_ids_round_trip_through_names_onto_another_instance(query):
    tables = {10: "marts.daily_store_sales"}
    fields = {80: "marts.daily_store_sales.store", 81: "marts.daily_store_sales.orders"}
    portable = ms.to_portable(query, tables, fields, database_id=2)
    assert not re.search(r"\b(2|10|80|81)\b", json.dumps(portable))

    # A new instance numbers everything differently.
    other_tables = {"marts.daily_store_sales": 3}
    other_fields = {"marts.daily_store_sales.store": 30, "marts.daily_store_sales.orders": 31}
    restored = ms.from_portable(portable, other_tables, other_fields, database_id=7)
    renumbered = json.loads(
        json.dumps(query).replace(": 2,", ": 7,").replace("10", "3").replace("80", "30").replace("81", "31")
    )
    assert restored == renumbered


def test_numbers_that_arent_ids_are_left_alone():
    query = {"stages": [{"limit": 25, "filters": [["<", ["field", {}, 80], 0]]}]}
    portable = ms.to_portable(query, {}, {80: "marts.daily_store_sales.revenue_usd"}, database_id=2)
    assert portable == {"stages": [{"limit": 25, "filters": [["<", ["field", {}, "@field:marts.daily_store_sales.revenue_usd"], 0]]}]}
