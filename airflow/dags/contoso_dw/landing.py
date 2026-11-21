"""Load the order processor's landed batches into raw.orders (stream mode).

The processor writes newline-delimited JSON under ``orders/`` in the landing
container. Each run loads the blobs it hasn't loaded before, recorded in
``raw._ingested_files``. The chaos switches act on the load, as if the load code
had a bug:

    null_customers  30% of the loaded orders lose their customer_id
    volume_drop     only 5% of each batch is loaded (the rest is lost)
    skip_load       nothing is loaded; the blobs wait for the next normal run
    schema_drift    raw.orders gains a promo_code column, until a run sets it to
                    false; when it's unset, the load doesn't touch the column
    breaking_schema after the load, amount_cents is renamed to amount_minor, so
                    staging_orders fails; the next run renames it back first

Each ALTER TABLE runs in its own OpenTelemetry span, under the task's span in the
run's trace. The load reads raw.orders' columns from information_schema before and
after each one, and fails the task (rolling the load back) if the column isn't in
the state the switch asked for.

This is Python rather than SQL, so OpenLineage can't read the tables from a
statement. LoadLandedOrdersOperator reports them itself instead. The names here
match the processor's OpenLineage events and the Postgres tables, so all three join
in Data Observability.
"""
from __future__ import annotations

import json
import random
from contextlib import contextmanager
from dataclasses import dataclass

from contoso_dw import sql as dw

try:
    from opentelemetry import trace
except ImportError:  # pragma: no cover - Airflow installs it; the tests may not have it
    trace = None

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
    schema_drift: bool | None = None
    breaking_schema: bool = False

    @classmethod
    def from_params(cls, params: dict) -> Chaos:
        flags = {f: bool(params.get(f)) for f in cls.__dataclass_fields__}
        # Unset and off mean different things for schema_drift: leave the column, or drop it.
        drift = params.get("schema_drift")
        return cls(**{**flags, "schema_drift": None if drift is None else bool(drift)})


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


@dataclass(frozen=True)
class SchemaStep:
    """One ALTER TABLE the load runs on raw.orders, and the column it should leave there or remove."""

    switch: str  # what asked for it, as the log line names it
    statement: str
    column: str
    present: bool  # whether raw.orders should have the column afterwards
    renamed_from: str | None = None
    # Runs on every load, so it's only worth a log line when it changes something.
    quiet: bool = False


RESTORE_AMOUNT = SchemaStep(
    "breaking_schema cleanup", dw.RESTORE_AMOUNT_COLUMN, "amount_cents", True, "amount_minor", quiet=True
)


def steps_before_load(chaos: Chaos) -> list[SchemaStep]:
    steps = []
    if chaos.schema_drift:
        steps.append(SchemaStep("schema_drift on", dw.ADD_PROMO_CODE, "promo_code", True))
    elif chaos.schema_drift is False:
        steps.append(SchemaStep("schema_drift off", dw.DROP_PROMO_CODE, "promo_code", False))
    steps.append(RESTORE_AMOUNT)
    return steps


def steps_after_load(chaos: Chaos) -> list[SchemaStep]:
    if chaos.breaking_schema:
        return [SchemaStep("breaking_schema on", dw.BREAK_AMOUNT_COLUMN, "amount_minor", True, "amount_cents")]
    return []


@dataclass(frozen=True)
class SchemaChange:
    """What a SchemaStep did, from raw.orders' columns before and after it."""

    step: SchemaStep
    before: bool  # whether raw.orders had the column before the statement
    after: bool
    trace_id: str | None = None  # the step's span, as hex, for the log line
    span_id: str | None = None

    @property
    def ok(self) -> bool:
        return self.after == self.step.present

    @property
    def outcome(self) -> str:
        if not self.ok:
            return "failed"
        if self.before == self.after:
            return "unchanged"
        if self.step.renamed_from:
            return "renamed"
        return "added" if self.after else "dropped"

    @property
    def reportable(self) -> bool:
        return not self.step.quiet or self.outcome != "unchanged"

    def describe(self) -> str:
        s = self.step
        outcome = self.outcome
        if outcome == "failed":
            want, got = ("have", "doesn't") if s.present else ("not have", "still does")
            return f"{s.switch}: raw.orders should {want} a {s.column} column after the ALTER TABLE, but it {got}"
        if outcome == "renamed":
            return f"{s.switch}: renamed raw.orders.{s.renamed_from} to {s.column}"
        if outcome == "added":
            return f"{s.switch}: added the {s.column} column to raw.orders"
        if outcome == "dropped":
            return f"{s.switch}: dropped the {s.column} column from raw.orders"
        if s.present:
            return f"{s.switch}: raw.orders already had the {s.column} column, so nothing changed"
        return f"{s.switch}: raw.orders had no {s.column} column to drop, so nothing changed"


class SchemaChangeFailed(Exception):
    """An ALTER TABLE on raw.orders errored, or left the column in the wrong state."""

    def __init__(self, message: str, trace_id: str | None = None, span_id: str | None = None):
        super().__init__(message)
        self.trace_id = trace_id
        self.span_id = span_id


def _tracer():
    return trace.get_tracer(__name__)


@contextmanager
def _span(step: SchemaStep):
    """A client span for the statement, current while it runs; None without OpenTelemetry."""
    if trace is None:
        yield None
        return
    attributes = {
        "db.system": "postgresql",
        "db.operation": "ALTER TABLE",
        "db.sql.table": "raw.orders",
        "db.statement": step.statement.strip(),
        # Datadog takes the span's resource from this.
        "resource.name": f"ALTER TABLE raw.orders ({step.switch})",
        "contoso.chaos.switch": step.switch,
        # Under contoso.column, not on it: Datadog nests dotted names, so a value at
        # contoso.column would collide with contoso.column.before / .after.
        "contoso.column.name": step.column,
        "contoso.column.wanted": "present" if step.present else "absent",
    }
    with _tracer().start_as_current_span(
        "ALTER TABLE raw.orders", kind=trace.SpanKind.CLIENT, attributes=attributes
    ) as span:
        yield span


def _span_ids(span) -> tuple[str | None, str | None]:
    context = span.get_span_context() if span is not None else None
    if context is None or not context.is_valid:
        return None, None
    return trace.format_trace_id(context.trace_id), trace.format_span_id(context.span_id)


def _has_column(cur, column: str) -> bool:
    cur.execute(dw.RAW_ORDERS_COLUMNS)
    return column in {row[0] for row in cur.fetchall()}


def run_schema_step(cur, step: SchemaStep) -> SchemaChange:
    """Run one ALTER TABLE, and check raw.orders' columns before and after it.

    Raises SchemaChangeFailed if the statement errors or leaves the column in the wrong
    state; the span records it as an error either way.
    """
    with _span(step) as span:
        trace_id, span_id = _span_ids(span)
        before = _has_column(cur, step.column)
        try:
            cur.execute(step.statement)
        except Exception as exc:
            raise SchemaChangeFailed(f"{step.switch}: the ALTER TABLE failed: {exc}", trace_id, span_id) from exc
        change = SchemaChange(step, before, _has_column(cur, step.column), trace_id, span_id)
        if span is not None:
            span.set_attribute("contoso.column.before", "present" if change.before else "absent")
            span.set_attribute("contoso.column.after", "present" if change.after else "absent")
            span.set_attribute("contoso.schema.outcome", change.outcome)
        if not change.ok:
            raise SchemaChangeFailed(change.describe(), trace_id, span_id)
    return change


def load_landed_orders(
    conn, container, chaos: Chaos, rng: random.Random | None = None
) -> tuple[dict, list[SchemaChange]]:
    """Load new landing blobs into raw.orders in one transaction.

    Returns the counts, and the column changes worth logging. A SchemaChangeFailed
    leaves the transaction uncommitted.
    """
    rng = rng or random.Random()
    stats = {"blobs": 0, "orders": 0, "waiting": 0}
    changes = []
    with conn.cursor() as cur:
        cur.execute(dw.CREATE_INGESTED_FILES)
        for step in steps_before_load(chaos):
            changes.append(run_schema_step(cur, step))
        cur.execute("SELECT name FROM raw._ingested_files")
        done = {row[0] for row in cur.fetchall()}
        # On an ADLS Gen2 account the listing also returns the directories
        # (orders/dt=..., size 0), so only take the batch files.
        pending = sorted(
            b.name
            for b in container.list_blobs(name_starts_with="orders/")
            if b.name.endswith(".json") and b.name not in done
        )
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
        for step in steps_after_load(chaos):
            changes.append(run_schema_step(cur, step))
    conn.commit()
    return stats, [c for c in changes if c.reportable]
