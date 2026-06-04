#!/usr/bin/env python3
"""Prepare Hermes CLI one-shot worker requests for Autonomous Strategy Lab.

This is a dry-run/preview step: it writes request and prompt artifacts plus a
command preview. It does not execute Hermes, write queues, start daemons, or run
backtests.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.autonomous_strategy_worker_launcher import build_hermes_worker_command
from factor_lab.autonomous_strategy_worker_requests import build_worker_requests, load_worker_config, write_worker_requests

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "autonomous_strategy_workers.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab" / "workers"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    run_id = args.run_id.strip() or "worker_preview_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    config = load_worker_config(args.config)
    requests = build_worker_requests(config, run_id=run_id, output_dir=args.output_dir)
    written = write_worker_requests(requests)

    previews = []
    for item, request in zip(written, requests):
        command = build_hermes_worker_command(request, prompt_path=item["prompt_path"], config=config)
        previews.append({
            "worker_key": request["worker_key"],
            "request_path": str(item["request_path"]),
            "prompt_path": str(item["prompt_path"]),
            "output_artifact_path": str(item["output_artifact_path"]),
            "command": command,
            "command_preview": _shell_preview_command(request, item["prompt_path"], config),
        })

    run_dir = Path(args.output_dir) / run_id
    preview_path = run_dir / "hermes_worker_commands_preview.json"
    preview_md_path = run_dir / "hermes_worker_commands_preview.md"
    preview_payload = {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "preview_only",
        "executed": False,
        "forbidden_flags": config["preferred_invocation"]["forbidden_flags"],
        "workers": previews,
    }
    preview_path.write_text(json.dumps(preview_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Hermes Worker Command Preview", "", f"run_id: {run_id}", "mode: preview_only", "executed: False", ""]
    for row in previews:
        lines.extend([f"## {row['worker_key']}", "", "```bash", row["command_preview"], "```", ""])
    preview_md_path.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "run_id": run_id,
        "worker_count": len(previews),
        "preview_path": str(preview_path.relative_to(ROOT)) if preview_path.is_relative_to(ROOT) else str(preview_path),
        "preview_md_path": str(preview_md_path.relative_to(ROOT)) if preview_md_path.is_relative_to(ROOT) else str(preview_md_path),
        "executed": False,
    }, ensure_ascii=False, indent=2))
    return 0


def _shell_preview_command(request: dict, prompt_path: Path, config: dict) -> str:
    source = config.get("preferred_invocation", {}).get("source") or "factor-lab-worker"
    skills = ",".join(request.get("skills") or ["factor-lab"])
    toolsets = ",".join(request.get("toolsets") or [])
    return (
        "hermes chat -Q "
        f"--source {source} "
        f"--skills {skills} "
        f"--toolsets {toolsets} "
        f"--query \"$(cat {prompt_path})\""
    )


if __name__ == "__main__":
    raise SystemExit(main())
