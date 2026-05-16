#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from factor_lab.llm_pricing import estimate_llm_cost_usd


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
        "cached_tokens": 0,
        "cached_tokens_missing_rows": 0,
        "cache_creation_tokens": 0,
        "cache_creation_tokens_missing_rows": 0,
        "uncached_prompt_tokens": 0,
        "uncached_prompt_tokens_missing_rows": 0,
        "estimated_cost_usd": 0.0,
        "estimated_user_prompt_tokens_4c": 0,
        "by_decision_type": defaultdict(lambda: {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0}),
        "by_model": defaultdict(lambda: {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0}),
        "by_provider": defaultdict(lambda: {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0}),
    }
    for row in rows:
        usage = row.get("usage") or {}
        prompt_tokens = _int_value(usage.get("prompt_tokens"))
        completion_tokens = _int_value(usage.get("completion_tokens"))
        total_tokens = _int_value(usage.get("total_tokens"))
        cached_tokens = _int_value(usage.get("cached_tokens"))
        cache_creation_tokens = _int_value(usage.get("cache_creation_tokens"))
        uncached_prompt_tokens = _int_value(usage.get("uncached_prompt_tokens"))
        cost = estimate_llm_cost_usd(row.get("model"), usage)
        estimated_cost_usd = float(cost.get("estimated_cost_usd") or row.get("estimated_cost_usd") or 0.0)
        estimated = _int_value(row.get("estimated_user_prompt_tokens_4c"))
        summary["prompt_tokens"] += prompt_tokens
        summary["completion_tokens"] += completion_tokens
        summary["total_tokens"] += total_tokens
        if usage.get("cached_tokens") is None:
            summary["cached_tokens_missing_rows"] += 1
        if usage.get("cache_creation_tokens") is None:
            summary["cache_creation_tokens_missing_rows"] += 1
        if usage.get("uncached_prompt_tokens") is None:
            summary["uncached_prompt_tokens_missing_rows"] += 1
        summary["cached_tokens"] += cached_tokens
        summary["cache_creation_tokens"] += cache_creation_tokens
        summary["uncached_prompt_tokens"] += uncached_prompt_tokens
        summary["estimated_cost_usd"] += estimated_cost_usd
        summary["estimated_user_prompt_tokens_4c"] += estimated
        decision_type = str(row.get("decision_type") or "unknown")
        model = str(row.get("model") or "unknown")
        provider = str(row.get("profile_name") or row.get("provider") or "unknown")
        summary["by_decision_type"][decision_type]["rows"] += 1
        summary["by_decision_type"][decision_type]["total_tokens"] += total_tokens
        summary["by_decision_type"][decision_type]["cached_tokens"] += cached_tokens
        summary["by_decision_type"][decision_type]["estimated_cost_usd"] += estimated_cost_usd
        summary["by_model"][model]["rows"] += 1
        summary["by_model"][model]["total_tokens"] += total_tokens
        summary["by_model"][model]["cached_tokens"] += cached_tokens
        summary["by_model"][model]["estimated_cost_usd"] += estimated_cost_usd
        summary["by_provider"][provider]["rows"] += 1
        summary["by_provider"][provider]["total_tokens"] += total_tokens
        summary["by_provider"][provider]["cached_tokens"] += cached_tokens
        summary["by_provider"][provider]["estimated_cost_usd"] += estimated_cost_usd
    summary["estimated_cost_usd"] = round(summary["estimated_cost_usd"], 6)
    for bucket in list(summary["by_decision_type"].values()) + list(summary["by_model"].values()) + list(summary["by_provider"].values()):
        bucket["estimated_cost_usd"] = round(bucket["estimated_cost_usd"], 6)
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
    print(
        f"cached_tokens={summary['cached_tokens']} "
        f"cache_creation_tokens={summary['cache_creation_tokens']} "
        f"uncached_prompt_tokens={summary['uncached_prompt_tokens']} "
        f"cached_tokens_missing_rows={summary['cached_tokens_missing_rows']} "
        f"cache_creation_tokens_missing_rows={summary['cache_creation_tokens_missing_rows']} "
        f"uncached_prompt_tokens_missing_rows={summary['uncached_prompt_tokens_missing_rows']}"
    )
    print(f"estimated_cost_usd={summary['estimated_cost_usd']:.6f}")
    print(f"estimated_user_prompt_tokens_4c={summary['estimated_user_prompt_tokens_4c']}")
    print("by_decision_type:")
    for key, value in sorted(summary["by_decision_type"].items()):
        print(f"  {key} total_tokens={value['total_tokens']} cached_tokens={value['cached_tokens']} cost_usd={value['estimated_cost_usd']:.6f} rows={value['rows']}")
    print("by_provider:")
    for key, value in sorted(summary["by_provider"].items()):
        print(f"  {key} total_tokens={value['total_tokens']} cached_tokens={value['cached_tokens']} cost_usd={value['estimated_cost_usd']:.6f} rows={value['rows']}")
    print("by_model:")
    for key, value in sorted(summary["by_model"].items()):
        print(f"  {key} total_tokens={value['total_tokens']} cached_tokens={value['cached_tokens']} cost_usd={value['estimated_cost_usd']:.6f} rows={value['rows']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
