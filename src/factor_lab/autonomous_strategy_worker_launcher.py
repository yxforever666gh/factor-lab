from __future__ import annotations

from pathlib import Path
from typing import Any

FORBIDDEN_CLI_FLAGS = {"--model", "--provider", "--resume", "--continue", "--yolo"}


def build_hermes_worker_command(
    request: dict[str, Any],
    *,
    prompt_path: str | Path,
    config: dict[str, Any],
) -> list[str]:
    invocation = config.get("preferred_invocation") or {}
    if invocation.get("command") != "hermes chat":
        raise ValueError("preferred_invocation.command must be 'hermes chat'")
    if invocation.get("resume") is not False or invocation.get("continue") is not False:
        raise ValueError("Hermes worker invocation must not resume or continue sessions")

    command = [
        "hermes",
        "chat",
        "-Q",
        "--source",
        str(invocation.get("source") or "factor-lab-worker"),
        "--skills",
        ",".join(request.get("skills") or ["factor-lab"]),
        "--toolsets",
        ",".join(request.get("toolsets") or []),
        "--query",
        f"$(cat {Path(prompt_path)})",
    ]
    forbidden_present = [flag for flag in FORBIDDEN_CLI_FLAGS if flag in command]
    if forbidden_present:
        raise ValueError(f"forbidden Hermes worker flags present: {forbidden_present}")
    return command


def shell_quote_command(command: list[str]) -> str:
    import shlex

    return " ".join(shlex.quote(part) for part in command)
