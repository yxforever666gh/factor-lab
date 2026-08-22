#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SERVICE_NAME = "factor-lab-research-daemon.service"
ROOT = Path(__file__).resolve().parents[1]
SENSITIVE = ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "CREDENTIAL", "AUTH")


def _is_sensitive_name(name: str) -> bool:
    upper = name.upper()
    return any(marker in upper for marker in SENSITIVE)


def redact_sensitive_text(text: str) -> str:
    def redact_token(token: str) -> str:
        if "=" not in token:
            return token
        prefix = ""
        name, value = token.split("=", 1)
        if name == "Environment" and "=" in value:
            prefix = "Environment="
            name, value = value.split("=", 1)
        if _is_sensitive_name(name):
            return f"{prefix}{name}=[REDACTED]"
        return token

    return " ".join(redact_token(token) for token in text.split(" "))


def _run(command: list[str]) -> dict[str, Any]:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)
        return {
            "command": command,
            "returncode": result.returncode,
            "stdout": redact_sensitive_text(result.stdout or ""),
            "stderr": redact_sensitive_text(result.stderr or ""),
        }
    except Exception as exc:
        return {"command": command, "returncode": None, "error": str(exc)}


def _env_value(combined: str, name: str) -> str | None:
    marker = f"{name}="
    for token in combined.replace("\n", " ").split():
        if token.startswith(marker):
            return token.split("=", 1)[1].strip('"')
        if token.startswith("Environment=") and marker in token:
            tail = token.split("Environment=", 1)[1]
            if tail.startswith(marker):
                return tail.split("=", 1)[1].strip('"')
    return None


def _property_value(combined: str, name: str) -> str | None:
    marker = f"{name}="
    for line in combined.splitlines():
        line = line.strip()
        if line.startswith(marker):
            return line.split("=", 1)[1].strip().strip('"')
    return None


def _normalized_path(value: str | Path) -> str:
    return str(value).replace("\\", "/").rstrip("/")


def _coherent_deployment_root(combined: str, expected_root: Path) -> tuple[bool, str | None]:
    """Accept the checkout itself or a coherent Linux systemd deployment.

    Audit fixtures and remote systemd output can legitimately describe a Linux
    checkout while this audit is being evaluated on Windows.  We therefore do
    not compare a foreign service path with ``Path(__file__)`` byte-for-byte;
    the working directory and daemon path must instead agree on the same
    ``factor-lab`` root.
    """
    actual = _property_value(combined, "WorkingDirectory")
    if not actual:
        return False, None
    actual_norm = _normalized_path(actual)
    expected_norm = _normalized_path(expected_root)
    combined_norm = combined.replace("\\", "/")
    if actual_norm == expected_norm:
        return True, actual
    coherent_foreign_checkout = (
        actual_norm.rsplit("/", 1)[-1] == "factor-lab"
        and f"{actual_norm}/scripts/run_research_daemon.py" in combined_norm
    )
    return coherent_foreign_checkout, actual


def _controlled_normal_requirements(combined: str) -> dict[str, Any]:
    missing: list[str] = []
    unsafe: list[str] = []
    controlled_only = _env_value(combined, "RESEARCH_DAEMON_CONTROLLED_ONLY")
    if controlled_only is None:
        missing.append("RESEARCH_DAEMON_CONTROLLED_ONLY")
    elif controlled_only != "1":
        unsafe.append("RESEARCH_DAEMON_CONTROLLED_ONLY")

    max_tasks_value = _env_value(combined, "RESEARCH_DAEMON_MAX_TASKS") or _env_value(combined, "RESEARCH_DAEMON_MAX_TASKS_PER_LOOP")
    if max_tasks_value is not None:
        try:
            if int(max_tasks_value) > 3:
                unsafe.append("RESEARCH_DAEMON_MAX_TASKS")
        except ValueError:
            unsafe.append("RESEARCH_DAEMON_MAX_TASKS")

    return {
        "controlled_normal_ready": not missing and not unsafe,
        "missing_required_env": missing,
        "unsafe_env": unsafe,
    }


def audit_systemd_env(*, output_dir: str | Path = "artifacts") -> dict[str, Any]:
    show = _run([
        "systemctl",
        "--user",
        "show",
        SERVICE_NAME,
        "-p",
        "ExecStart",
        "-p",
        "WorkingDirectory",
        "-p",
        "Environment",
        "-p",
        "FragmentPath",
    ])
    cat = _run(["systemctl", "--user", "cat", SERVICE_NAME])
    combined = "\n".join([show.get("stdout") or "", cat.get("stdout") or ""])
    expected_daemon = str(ROOT / "scripts" / "run_research_daemon.py")
    expected_workdir = str(ROOT)
    expected_src = str(ROOT / "src")
    root_matches, deployed_workdir = _coherent_deployment_root(combined, ROOT)
    pythonpath = _env_value(combined, "PYTHONPATH")
    deployed_src = f"{_normalized_path(deployed_workdir)}/src" if deployed_workdir else None
    checks = {
        "execstart_mentions_daemon": expected_daemon in combined or "scripts/run_research_daemon.py" in combined,
        "working_directory_matches": root_matches,
        "pythonpath_mentions_src": (
            pythonpath is None
            or _normalized_path(expected_src) in _normalized_path(pythonpath)
            or bool(root_matches and deployed_src and deployed_src in _normalized_path(pythonpath))
        ),
        "fragment_available": bool(show.get("stdout") or cat.get("stdout")),
    }
    controlled_normal = _controlled_normal_requirements(combined)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "expected": {
            "project_root": str(ROOT),
            "daemon_script": expected_daemon,
            "src_path": expected_src,
            "artifacts_dir": str(ROOT / "artifacts"),
        },
        "checks": checks,
        "controlled_normal_ready": controlled_normal["controlled_normal_ready"],
        "missing_required_env": controlled_normal["missing_required_env"],
        "unsafe_env": controlled_normal["unsafe_env"],
        "commands": [show, cat],
    }
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "research_daemon_systemd_env_audit.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Research Daemon Systemd Environment Audit", ""]
    for key, value in checks.items():
        lines.append(f"- {key}: {value}")
    lines.extend([
        f"- controlled_normal_ready: {payload['controlled_normal_ready']}",
        f"- missing_required_env: {payload['missing_required_env']}",
        f"- unsafe_env: {payload['unsafe_env']}",
    ])
    (out / "research_daemon_systemd_env_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="artifacts")
    args = parser.parse_args()
    result = audit_systemd_env(output_dir=args.output_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
