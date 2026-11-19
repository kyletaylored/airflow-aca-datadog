"""The memory_pressure chaos switch: hold memory in the container the task runs in.

With LocalExecutor every task runs in the scheduler's container, so memory one task
holds is memory every other task loses. Near the container's limit, the kernel kills
task processes, which Airflow reports as "task state changed externally" and retries.

This stays short of that. It allocates memory until the container's working set
reaches TARGET of its cgroup limit, holds it for the given time, then releases it. If
other tasks push the container past CEILING meanwhile, it gives memory back. It never
reaches the limit itself, because on Container Apps an out-of-memory kill can restart the
whole container, scheduler included.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)

CGROUP = Path("/sys/fs/cgroup")
TARGET = 0.85
CEILING = 0.90
CHUNK = 64 * 1024 * 1024
HOLD_SECONDS = 600
POLL_SECONDS = 5


def read_memory(root: Path = CGROUP) -> tuple[int, int] | None:
    """(working set, limit) in bytes from the container's cgroup, or None when there's no limit.

    The working set is the memory in use less the inactive page cache, which is what
    Azure's working_set_bytes metric reports. Container Apps uses cgroup v1, under
    memory/; cgroup v2 keeps the same figures in the root, under other names.
    """
    for directory, limit_file, usage_file, inactive in (
        (root, "memory.max", "memory.current", "inactive_file"),
        (root / "memory", "memory.limit_in_bytes", "memory.usage_in_bytes", "total_inactive_file"),
    ):
        try:
            limit = (directory / limit_file).read_text().strip()
            usage = int((directory / usage_file).read_text())
            stat = dict(line.split() for line in (directory / "memory.stat").read_text().splitlines() if line.strip())
        except (OSError, ValueError):
            continue
        # No limit: "max" in v2, a number near 2**63 in v1.
        if limit == "max" or int(limit) >= 1 << 60:
            return None
        return usage - int(stat.get(inactive, 0)), int(limit)
    return None


def hold_memory(
    seconds: float = HOLD_SECONDS,
    *,
    read: Callable[[], tuple[int, int] | None] = read_memory,
    chunk: int = CHUNK,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    chunks: list[bytearray] | None = None,
) -> int:
    """Hold memory up to TARGET of the limit for ``seconds``. Returns the most held, in bytes.

    ``chunks`` is the list the memory is held in; tests pass their own to watch it.
    """
    if read() is None:
        log.warning("No cgroup memory limit found, so memory_pressure holds nothing.")
        return 0
    chunks = [] if chunks is None else chunks
    peak = 0
    reported = False
    end = clock() + seconds
    try:
        while clock() < end:
            reading = read()
            if reading is None:
                break
            used, limit = reading
            if used > CEILING * limit and chunks:
                chunks.pop()
                continue
            if used + chunk < TARGET * limit:
                # Filled rather than zeroed, so the pages are really committed.
                chunks.append(bytearray(b"\x01") * chunk)
                peak = max(peak, len(chunks) * chunk)
                continue
            if not reported:
                log.info(
                    "Holding %d MiB: the container is at %.0f%% of its %d MiB limit. Releasing in %.0f s.",
                    len(chunks) * chunk >> 20, 100 * used / limit, limit >> 20, end - clock(),
                )
                reported = True
            sleep(POLL_SECONDS)
    finally:
        chunks.clear()
    log.info("Released %d MiB.", peak >> 20)
    return peak
