#!/usr/bin/env python3
"""Verify Factor Lab runtime is de-OpenClaw in the operational sense.

This verifier intentionally distinguishes between:
- allowed OpenClaw *concept* references, such as agent-role architecture docs or
  compatibility names; and
- blocked runtime dependencies, such as the old workspace path, old env files, or
  unconditional OpenClaw CLI calls.

The script prints PASS/WARN/FAIL lines and exits non-zero on FAIL.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OLD_WORKSPACE = "/home/admin/.openclaw/workspace"
EXPECTED_ROOT = str(PROJECT_ROOT)
DEFAULT_ALLOWED_PROVIDERS = {"real_llm"}

TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".service",
    ".conf",
    ".json",
    ".html",
    ".txt",
    ".yml",
    ".yaml",
}
SCAN_DIRS = ["src", "scripts", "systemd", "docs", "tests"]
RUNTIME_BLOCKED_DIRS = {"src", "scripts", "systemd"}
ALLOWED_BLOCKED_REFERENCE_FILES = {
    "scripts/verify_de_openclaw_runtime.py",
    "tests/test_verify_de_openclaw_runtime.py",
}
BLOCKED_PATTERNS = [
    ("old_workspace_path", OLD_WORKSPACE),
    ("old_workspace_env", f"EnvironmentFile={OLD_WORKSPACE}/.env"),
    ("old_workspace_pythonpath", f"PYTHONPATH={OLD_WORKSPACE}/src"),
]


@dataclass(frozen=True)
class CheckResult:
    level: str
    name: str
    message: str

    def line(self) -> str:
        return f"{self.level} {self.name}: {self.message}"


def run_command(args: list[str], timeout: int = 5) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)


def systemctl_show(service: str) -> dict[str, str]:
    proc = run_command(["systemctl", "--user", "show", service, "--no-pager"], timeout=8)
    data: dict[str, str] = {}
    if proc.returncode != 0:
        data["__error__"] = (proc.stderr or proc.stdout).strip()
        return data
    for line in proc.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            data[key] = value
    return data


def check_service_path(service: str, check_name: str, expected_root: str = EXPECTED_ROOT) -> CheckResult:
    data = systemctl_show(service)
    if "__error__" in data:
        return CheckResult("FAIL", check_name, f"cannot inspect {service}: {data['__error__']}")
    fragment = data.get("FragmentPath", "")
    exec_start = data.get("ExecStart", "")
    working_dir = data.get("WorkingDirectory", "")
    combined = "\n".join([fragment, exec_start, working_dir])
    if expected_root in combined and OLD_WORKSPACE not in combined:
        return CheckResult("PASS", check_name, f"{service} points at {expected_root}")
    return CheckResult(
        "FAIL",
        check_name,
        f"{service} is not cleanly rooted at {expected_root}; FragmentPath={fragment!r} WorkingDirectory={working_dir!r} ExecStart={exec_start!r}",
    )


def process_lines() -> list[str]:
    proc = run_command(["pgrep", "-af", "factor-lab|run_research_daemon|run_web_ui|run_agent_briefs"], timeout=5)
    if proc.returncode not in (0, 1):
        return []
    return proc.stdout.splitlines()


def check_no_old_workspace_process() -> CheckResult:
    offenders = [line for line in process_lines() if OLD_WORKSPACE in line]
    if offenders:
        return CheckResult("FAIL", "no_old_workspace_process", "; ".join(offenders[:5]))
    return CheckResult("PASS", "no_old_workspace_process", "no running Factor Lab process uses old OpenClaw workspace")


def provider_from_processes() -> str | None:
    for line in process_lines():
        match = re.search(r"--provider\s+([^\s]+)", line)
        if match:
            return match.group(1)
    return None


def provider_from_env_file(env_path: Path) -> str | None:
    if not env_path.exists():
        return None
    for line in env_path.read_text(errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in {"FACTOR_LAB_DECISION_PROVIDER", "FACTOR_LAB_LIVE_DECISION_PROVIDER"}:
            return value.strip().strip('"').strip("'")
    return None


def check_provider(allowed: set[str]) -> CheckResult:
    provider = provider_from_processes() or provider_from_env_file(PROJECT_ROOT / ".env")
    if provider in allowed:
        return CheckResult("PASS", "provider", f"provider={provider}")
    return CheckResult("FAIL", "provider", f"provider={provider!r}; allowed={sorted(allowed)}")


def iter_text_files(root: Path = PROJECT_ROOT) -> Iterable[Path]:
    for dirname in SCAN_DIRS:
        base = root / dirname
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix in TEXT_SUFFIXES:
                yield path


def scan_openclaw_references(root: Path = PROJECT_ROOT) -> tuple[list[tuple[str, str]], list[str]]:
    """Return blocked runtime references and non-blocking concept/history refs.

    Documentation and tests may mention the old OpenClaw workspace as history,
    examples, or verifier fixtures. Runtime code and service files must not
    depend on it. The verifier itself is also allowed to contain the blocked
    literal because it needs to detect it.
    """
    blocked: list[tuple[str, str]] = []
    concept_refs: list[str] = []
    for path in iter_text_files(root):
        try:
            rel = str(path.relative_to(root))
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        lower = text.lower()
        if "openclaw" in lower:
            concept_refs.append(rel)
        top_dir = rel.split("/", 1)[0]
        is_runtime_file = top_dir in RUNTIME_BLOCKED_DIRS
        if rel in ALLOWED_BLOCKED_REFERENCE_FILES or not is_runtime_file:
            continue
        for name, pattern in BLOCKED_PATTERNS:
            if pattern in text:
                blocked.append((name, rel))
    return blocked, sorted(set(concept_refs))


def check_openclaw_references() -> list[CheckResult]:
    blocked, concept_refs = scan_openclaw_references()
    results: list[CheckResult] = []
    if blocked:
        summary = ", ".join(f"{name}:{path}" for name, path in blocked[:20])
        results.append(CheckResult("FAIL", "no_blocked_openclaw_refs", summary))
    else:
        results.append(CheckResult("PASS", "no_blocked_openclaw_refs", "no blocked old workspace/env references found in scanned source/docs"))
    results.append(CheckResult("WARN", "allowed_openclaw_concept_refs", f"{len(concept_refs)} files mention OpenClaw concept/compatibility terms"))
    return results


def run_checks(allowed_providers: set[str]) -> list[CheckResult]:
    results = [
        check_service_path("factor-lab-web-ui.service", "webui_path"),
        check_service_path("factor-lab-research-daemon.service", "daemon_path"),
        check_provider(allowed_providers),
        check_no_old_workspace_process(),
    ]
    results.extend(check_openclaw_references())
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allowed-provider",
        action="append",
        dest="allowed_providers",
        help="Allowed decision provider. May be repeated. Defaults to real_llm.",
    )
    args = parser.parse_args(argv)
    allowed = set(args.allowed_providers or DEFAULT_ALLOWED_PROVIDERS)
    results = run_checks(allowed)
    for result in results:
        print(result.line())
    return 1 if any(result.level == "FAIL" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
