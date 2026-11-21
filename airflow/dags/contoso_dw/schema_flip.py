"""When contoso_schema_flip adds raw.orders' promo_code column, and when it drops it.

The column is on raw.orders for the first 2 hours of every 6 (from 00:00, 06:00,
12:00 and 18:00 UTC), and off for the other 4. That's long enough for Datadog's
schema metadata collection to see each change, so the schema change and column
contract monitors alert and recover four times a day without anyone triggering a
run.

Each run puts the column in the state it should be in at the run's time, so a retry
or a manual run never leaves it out of step with the schedule. The change goes
through landing.run_schema_step, so it gets the same before and after check, span
and log line as the schema_drift switch on contoso_orders.
"""
from __future__ import annotations

from datetime import datetime, timezone

from contoso_dw import landing
from contoso_dw import sql as dw

EVERY_HOURS = 6
PRESENT_HOURS = 2
# A run at each change: on at the start of each 6 hours, off 2 hours later.
SCHEDULE = "0 {} * * *".format(
    ",".join(str(h) for start in range(0, 24, EVERY_HOURS) for h in (start, start + PRESENT_HOURS))
)
# The ALTER TABLE waits for a contoso_orders load to commit; give up, and retry, rather
# than queue every other query on raw.orders behind it for long.
LOCK_TIMEOUT = "SET LOCAL lock_timeout = '1min'"

ON = landing.SchemaStep("schema flip on", dw.ADD_PROMO_CODE, "promo_code", True)
OFF = landing.SchemaStep("schema flip off", dw.DROP_PROMO_CODE, "promo_code", False)


def step_at(when: datetime) -> landing.SchemaStep:
    """The step that leaves raw.orders as it should be at ``when``."""
    return ON if when.astimezone(timezone.utc).hour % EVERY_HOURS < PRESENT_HOURS else OFF


def flip(conn, when: datetime) -> landing.SchemaChange:
    """Add or drop promo_code for ``when``, and commit. Raises SchemaChangeFailed without committing."""
    with conn.cursor() as cur:
        cur.execute(LOCK_TIMEOUT)
        change = landing.run_schema_step(cur, step_at(when))
    conn.commit()
    return change
