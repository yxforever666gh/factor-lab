from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path


class WorkflowTiming:
    def __init__(self) -> None:
        self._starts: dict[str, float] = {}
        self.metrics_ms: dict[str, float] = {}
        self.counters: dict[str, int | str] = {}
        self._total_started_at = time.perf_counter()

    @contextmanager
    def stage(self, name: str):
        started_at = time.perf_counter()
        self._starts[name] = started_at
        try:
            yield
        finally:
            self.metrics_ms[f"{name}_ms"] = round((time.perf_counter() - started_at) * 1000, 3)

    def set_counter(self, name: str, value: int | str) -> None:
        self.counters[name] = value

    def add_counter(self, name: str, value: int = 1) -> None:
        self.counters[name] = int(self.counters.get(name, 0)) + value

    def snapshot(self) -> dict:
        payload = dict(self.metrics_ms)
        payload.update(self.counters)
        payload["total_ms"] = round((time.perf_counter() - self._total_started_at) * 1000, 3)
        return payload

    def write_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.snapshot(), ensure_ascii=False, indent=2), encoding="utf-8")
