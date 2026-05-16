#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SERVICE_NAME = "factor-lab-research-daemon.service"
SENSITIVE_MARKERS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "CREDENTIAL", "AUTH")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SENSITIVE_MARKERS)


def redact_sensitive(value: Any, *, key: str = "") -> Any:
    if _is_sensitive_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): redact_sensitive(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_sensitive(v, key=key) for v in value]
    return value


def _run(command: list[str], *, cwd: Path | None = None, timeout: int = 10) -> dict[str, Any]:
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False, timeout=timeout)
        return {
            "command": command,
            "returncode": result.returncode,
            "stdout": (result.stdout or "")[-4000:],
            "stderr": (result.stderr or "")[-4000:],
        }
    except Exception as exc:
        return {"command": command, "returncode": None, "error": str(exc)}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"exists": True, "path": str(path), "error": str(exc)}
    return {"exists": True, "path": str(path), "payload": redact_sensitive(payload)}


def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "daemon_probe_once.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Research Daemon One-Shot Probe",
        "",
        f"OK: {payload.get('ok')}",
        f"Started at: {payload.get('started_at_utc')}",
        f"Finished at: {payload.get('finished_at_utc')}",
        f"Error: {payload.get('error') or ''}",
        "",
        "## Commands",
    ]
    for item in payload.get("commands") or []:
        lines.append(f"- `{ ' '.join(item.get('command') or []) }` rc={item.get('returncode')} error={item.get('error') or ''}")
    (output_dir / "daemon_probe_once.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def probe_daemon_once(*, seconds: int = 20, output_dir: str | Path = "artifacts") -> dict[str, Any]:
    root = _project_root()
    out = Path(output_dir)
    commands: list[dict[str, Any]] = []
    payload: dict[str, Any] = {
        "ok": False,
        "service": SERVICE_NAME,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "seconds": seconds,
        "commands": commands,
        "environment_sample": redact_sensitive({k: os.environ.get(k) for k in sorted(os.environ) if k.startswith(("FACTOR_LAB", "RESEARCH_", "PYTHON", "XDG"))}),
    }
    try:
        commands.append(_run(["systemctl", "--user", "start", SERVICE_NAME], cwd=root, timeout=15))
        if commands[-1].get("returncode") != 0:
            payload["error"] = commands[-1].get("error") or "systemctl_start_failed"
            return payload
        time.sleep(max(0, seconds))
        commands.append(_run(["systemctl", "--user", "is-active", SERVICE_NAME], cwd=root, timeout=5))
        commands.append(_run(["systemctl", "--user", "status", SERVICE_NAME, "--no-pager"], cwd=root, timeout=10))
        commands.append(_run(["journalctl", "--user", "-u", SERVICE_NAME, "-n", "80", "--no-pager"], cwd=root, timeout=10))
        commands.append(_run(["ps", "-eo", "pid,ppid,pcpu,pmem,etime,cmd"], cwd=root, timeout=10))
        payload["status_doc"] = _read_json(root / "artifacts" / "research_daemon_status.json")
        payload["heartbeat_doc"] = _read_json(root / "artifacts" / "research_daemon_heartbeat.json")
        payload["ok"] = True
        return payload
    except Exception as exc:
        payload["error"] = str(exc)
        return payload
    finally:
        commands.append(_run(["systemctl", "--user", "stop", SERVICE_NAME], cwd=root, timeout=15))
        commands.append(_run(["systemctl", "--user", "reset-failed", SERVICE_NAME], cwd=root, timeout=10))
        payload["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        _write_outputs(out, payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--output-dir", default="artifacts")
    args = parser.parse_args()
    result = probe_daemon_once(seconds=args.seconds, output_dir=args.output_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
