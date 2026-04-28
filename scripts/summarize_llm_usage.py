#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _int_value(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _load_rows(path: Path, days: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    cutoff = None
    if days is not None and days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if cutoff is not None:
            created_at = _parse_time(row.get("created_at_utc"))
            if created_at is None or created_at < cutoff:
                continue
        rows.append(row)
    return rows


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "rows": len(rows),
        "success": sum(1 for row in rows if row.get("success") is True),
        "failed": sum(1 for row in rows if row.get("success") is False),
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "estimated_user_prompt_tokens_4c": 0,
        "by_decision_type": defaultdict(lambda: {"rows": 0, "total_tokens": 0}),
        "by_model": defaultdict(lambda: {"rows": 0, "total_tokens": 0}),
    }
    for row in rows:
        usage = row.get("usage") or {}
        prompt_tokens = _int_value(usage.get("prompt_tokens"))
        completion_tokens = _int_value(usage.get("completion_tokens"))
        total_tokens = _int_value(usage.get("total_tokens"))
        estimated = _int_value(row.get("estimated_user_prompt_tokens_4c"))
        summary["prompt_tokens"] += prompt_tokens
        summary["completion_tokens"] += completion_tokens
        summary["total_tokens"] += total_tokens
        summary["estimated_user_prompt_tokens_4c"] += estimated
        decision_type = str(row.get("decision_type") or "unknown")
        model = str(row.get("model") or "unknown")
        summary["by_decision_type"][decision_type]["rows"] += 1
        summary["by_decision_type"][decision_type]["total_tokens"] += total_tokens
        summary["by_model"][model]["rows"] += 1
        summary["by_model"][model]["total_tokens"] += total_tokens
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize Factor Lab LLM usage ledger JSONL.")
    parser.add_argument("--ledger", default="artifacts/llm_usage_ledger.jsonl", help="Path to llm_usage_ledger.jsonl")
    parser.add_argument("--days", type=int, default=None, help="Only include rows from the last N days")
    args = parser.parse_args()

    rows = _load_rows(Path(args.ledger), days=args.days)
    summary = _summarize(rows)
    print(f"rows={summary['rows']} success={summary['success']} failed={summary['failed']}")
    print(
        f"prompt_tokens={summary['prompt_tokens']} "
        f"completion_tokens={summary['completion_tokens']} "
        f"total_tokens={summary['total_tokens']}"
    )
    print(f"estimated_user_prompt_tokens_4c={summary['estimated_user_prompt_tokens_4c']}")
    print("by_decision_type:")
    for key, value in sorted(summary["by_decision_type"].items()):
        print(f"  {key} total_tokens={value['total_tokens']} rows={value['rows']}")
    print("by_model:")
    for key, value in sorted(summary["by_model"].items()):
        print(f"  {key} total_tokens={value['total_tokens']} rows={value['rows']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
