"""The memory_pressure chaos switch: it fills up to its target and never stays past the ceiling."""
from __future__ import annotations

import os
import sys

_DAGS = os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow", "dags")
if _DAGS not in sys.path:
    sys.path.insert(0, _DAGS)

from contoso_dw import memory_pressure as mp  # noqa: E402

CHUNK = 1024
LIMIT = 100 * CHUNK


class FakeContainer:
    """A container whose working set is a baseline plus the chunks hold_memory holds."""

    def __init__(self, baseline: int):
        self.baseline = baseline
        self.held: list[bytearray] = []
        self.now = 0.0
        self.readings: list[int] = []
        self.on_sleep = None

    def read(self):
        used = self.baseline + len(self.held) * CHUNK
        self.readings.append(used)
        return used, LIMIT

    def sleep(self, seconds):
        if self.on_sleep:
            self.on_sleep()
        self.now += seconds

    def hold(self, seconds: float = 60) -> int:
        return mp.hold_memory(
            seconds, read=self.read, chunk=CHUNK, sleep=self.sleep, clock=lambda: self.now, chunks=self.held
        )


def test_fills_to_the_target_and_releases():
    container = FakeContainer(baseline=40 * CHUNK)
    assert container.hold() == 44 * CHUNK
    assert mp.TARGET * LIMIT - CHUNK <= max(container.readings) < mp.TARGET * LIMIT
    assert container.held == []


def test_gives_memory_back_when_other_tasks_grow():
    container = FakeContainer(baseline=40 * CHUNK)

    def other_tasks_grow():
        container.baseline = 60 * CHUNK

    container.on_sleep = other_tasks_grow
    container.hold()
    assert container.readings[-1] <= mp.CEILING * LIMIT


def test_holds_nothing_without_a_limit():
    assert mp.hold_memory(60, read=lambda: None) == 0


def test_reads_cgroup_v2(tmp_path):
    (tmp_path / "memory.max").write_text("4294967296\n")
    (tmp_path / "memory.current").write_text("2000000000\n")
    (tmp_path / "memory.stat").write_text("anon 1500000000\ninactive_file 300000000\n")
    assert mp.read_memory(tmp_path) == (1_700_000_000, 4_294_967_296)
    (tmp_path / "memory.max").write_text("max\n")
    assert mp.read_memory(tmp_path) is None
    assert mp.read_memory(tmp_path / "missing") is None


def test_reads_cgroup_v1_as_container_apps_has_it(tmp_path):
    v1 = tmp_path / "memory"
    v1.mkdir()
    (v1 / "memory.limit_in_bytes").write_text("4294967296\n")
    (v1 / "memory.usage_in_bytes").write_text("1792249856\n")
    (v1 / "memory.stat").write_text("cache 400000000\ntotal_inactive_file 92249856\n")
    assert mp.read_memory(tmp_path) == (1_700_000_000, 4_294_967_296)
    (v1 / "memory.limit_in_bytes").write_text("9223372036854771712\n")
    assert mp.read_memory(tmp_path) is None
