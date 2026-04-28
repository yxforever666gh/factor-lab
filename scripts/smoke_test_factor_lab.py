#!/usr/bin/env python3
"""One-shot smoke test for Factor Lab local runtime."""

from __future__ import annotations

import importlib
import json
import sqlite3
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = PROJECT_ROOT / "artifacts"
DB_PATH = ARTIFACTS / "factor_lab.db"
ROUTES = ["/", "/health", "/runs", "/agents", "/settings", "/llm", "/control"]


@dataclass
class Check:
    name: str
    ok: bool
    detail: str

    def line(self) -> str:
        return f"{'PASS' if self.ok else 'FAIL'} {self.name}: {self.detail}"


def check_import() -> Check:
    try:
        module = importlib.import_module("factor_lab")
        return Check("import_factor_lab", True, getattr(module, "__doc__", "imported") or "imported")
    except Exception as exc:
        return Check("import_factor_lab", False, str(exc))


def check_db() -> Check:
    if not DB_PATH.exists():
        return Check("db_open", False, f"missing {DB_PATH}")
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=1.0)
        try:
            tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 10")]
        finally:
            conn.close()
        return Check("db_open", True, f"tables_sample={tables[:5]}")
    except Exception as exc:
        return Check("db_open", False, str(exc))


def check_routes(base_url: str = "http://127.0.0.1:8765") -> list[Check]:
    checks: list[Check] = []
    for route in ROUTES:
        start = time.time()
        try:
            response = urllib.request.urlopen(base_url + route, timeout=8)
            response.read(200)
            elapsed = time.time() - start
            checks.append(Check(f"route:{route}", response.status == 200, f"status={response.status} elapsed={elapsed:.3f}s"))
        except Exception as exc:
            checks.append(Check(f"route:{route}", False, str(exc)))
    return checks


def check_heartbeat() -> Check:
    path = ARTIFACTS / "research_daemon_heartbeat.json"
    if not path.exists():
        return Check("daemon_heartbeat", False, f"missing {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        required = {"timestamp", "pid", "project_root", "provider", "queue"}
        missing = sorted(required - set(payload))
        if missing:
            return Check("daemon_heartbeat", False, f"missing={missing}")
        return Check("daemon_heartbeat", True, f"provider={payload.get('provider')} state={payload.get('state')}")
    except Exception as exc:
        return Check("daemon_heartbeat", False, str(exc))


def check_de_openclaw() -> Check:
    proc = subprocess.run([sys.executable, "scripts/verify_de_openclaw_runtime.py"], cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=15, check=False)
    if proc.returncode == 0:
        return Check("de_openclaw_runtime", True, "verifier passed")
    return Check("de_openclaw_runtime", False, (proc.stdout + proc.stderr).strip()[-1000:])


def check_llm_config_redacted() -> Check:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return Check("llm_config", False, "missing .env")
    text = env_path.read_text(encoding="utf-8", errors="ignore")
    fields = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.startswith("FACTOR_LAB_LLM_") or key.startswith("FACTOR_LAB_DECISION_PROVIDER"):
            sensitive = "KEY" in key or "TOKEN" in key or "SECRET" in key or key.endswith("PROFILES_JSON")
            fields[key] = "***" if sensitive else value[:80]
    if not any(k.endswith("MODEL") for k in fields):
        return Check("llm_config", False, f"missing model fields keys={sorted(fields)}")
    return Check("llm_config", True, json.dumps(fields, ensure_ascii=False, sort_keys=True))


def check_tushare_cache_status() -> Check:
    candidates = [
        ARTIFACTS / "data_prepare_status.json",
        ARTIFACTS / "feature_store",
        ARTIFACTS / "tushare_cache",
    ]
    existing = [str(path.relative_to(PROJECT_ROOT)) for path in candidates if path.exists()]
    return Check("tushare_cache_status", bool(existing), f"existing={existing}")


def run_checks() -> list[Check]:
    checks = [check_import(), check_db(), check_heartbeat(), check_de_openclaw(), check_llm_config_redacted(), check_tushare_cache_status()]
    checks.extend(check_routes())
    return checks


def main() -> int:
    checks = run_checks()
    for check in checks:
        print(check.line())
    return 1 if any(not check.ok for check in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
