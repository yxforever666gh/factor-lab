#!/usr/bin/env python3
"""Normalize legacy research artifacts into the repository knowledge files.

This is a repository-local compatibility utility.  It has no dependency on an
external agent installation and never writes outside the Factor Lab checkout
unless its path constants are explicitly overridden by a caller.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
KNOWLEDGE_DIR = PROJECT_ROOT / "knowledge"
EXPERIMENTS_FILE = KNOWLEDGE_DIR / "factor_experiments.jsonl"
LESSONS_FILE = KNOWLEDGE_DIR / "factor_lessons.md"
WATCHLIST_FILE = KNOWLEDGE_DIR / "factor_watchlist.json"
BLACKLIST_FILE = KNOWLEDGE_DIR / "factor_blacklist.json"
STATE_FILE = KNOWLEDGE_DIR / "memory_sync_state.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_record(raw: dict[str, Any]) -> str:
    rank_ic = raw.get("rank_ic_mean")
    spread = raw.get("top_bottom_spread_mean")
    sharpe = raw.get("sharpe_net")
    if raw.get("pass_gate") is True:
        return "pass_candidate"
    if isinstance(spread, (int, float)) and spread < 0:
        return "weak_signal_negative_spread"
    if isinstance(rank_ic, (int, float)) and abs(rank_ic) < 0.005 and isinstance(sharpe, (int, float)) and sharpe < 0:
        return "weak_signal"
    if isinstance(sharpe, (int, float)) and sharpe < -3:
        return "reject_candidate"
    return "needs_review"


def build_lesson(raw: dict[str, Any], classification: str) -> str:
    name = raw.get("factor_name") or raw.get("name") or "unknown_factor"
    if classification == "pass_candidate":
        return f"{name} passed the current gates; validate it on independent windows before promotion."
    if classification == "weak_signal_negative_spread":
        return f"{name} had a negative spread; test a preregistered reversed direction before discarding it."
    if classification == "reject_candidate":
        return f"{name} performed poorly; avoid unchanged retests unless the data window or universe changes."
    if classification == "weak_signal":
        return f"{name} had near-zero IC and negative net Sharpe; deprioritize it unless a mechanism justifies combination."
    return f"{name} needs manual review before reuse."


def _extract_evaluations(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("factor_evaluations", "results", "factors", "evaluations"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        if "factor_name" in data or "name" in data:
            return [data]
    return []


def _first_present(raw: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def parse_result_file(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = []
    for raw in _extract_evaluations(data):
        classification = classify_record(raw)
        normalized = {
            "factor_name": _first_present(raw, "factor_name", "name"),
            "expression": raw.get("expression"),
            "rank_ic_mean": raw.get("rank_ic_mean"),
            "rank_ic_ir": _first_present(raw, "rank_ic_ir", "information_ratio"),
            "top_bottom_spread_mean": raw.get("top_bottom_spread_mean"),
            "sharpe_net": raw.get("sharpe_net"),
            "net_return_annual": raw.get("net_return_annual"),
            "pass_gate": raw.get("pass_gate"),
            "fail_reason": raw.get("fail_reason"),
            "classification": classification,
            "lesson": build_lesson(raw, classification),
        }
        if normalized["factor_name"]:
            records.append(normalized)
    return records


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"version": 1, "synced_files": {}}
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "synced_files": {}}
    if not isinstance(state, dict):
        return {"version": 1, "synced_files": {}}
    state.setdefault("version", 1)
    state.setdefault("synced_files", {})
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _state_entry(synced: dict[str, Any], path: Path) -> dict[str, Any] | None:
    for key in (str(path), path.as_posix()):
        value = synced.get(key)
        if isinstance(value, dict):
            return value
    return None


def discover_result_files(artifacts_dir: Path, state: dict[str, Any]) -> list[Path]:
    names = ("results.json", "factor_evaluations.json")
    candidates = sorted({path for name in names for path in artifacts_dir.glob(f"**/{name}")})
    synced = state.get("synced_files") if isinstance(state.get("synced_files"), dict) else {}
    unsynced: list[Path] = []
    for path in candidates:
        if not path.is_file():
            continue
        existing = _state_entry(synced, path)
        if existing and existing.get("sha256") == sha256_file(path):
            continue
        unsynced.append(path)
    return unsynced


def _record_identity(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("artifact_sha256") or ""),
        str(record.get("factor_name") or ""),
        str(record.get("expression") or ""),
    )


def load_all_experiment_records() -> list[dict[str, Any]]:
    if not EXPERIMENTS_FILE.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in EXPERIMENTS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def append_experiment_records(records: list[dict[str, Any]]) -> int:
    """Append new facts and return the number written, deduplicating retries."""
    EXPERIMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = {_record_identity(record) for record in load_all_experiment_records()}
    fresh: list[dict[str, Any]] = []
    for record in records:
        identity = _record_identity(record)
        if identity in existing:
            continue
        existing.add(identity)
        fresh.append(record)
    if not fresh:
        return 0
    with EXPERIMENTS_FILE.open("a", encoding="utf-8", newline="\n") as handle:
        for record in fresh:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return len(fresh)


def _metric(record: dict[str, Any], key: str, fallback: float = -999.0) -> float:
    value = record.get(key)
    return float(value) if isinstance(value, (int, float)) else fallback


def build_lessons_markdown(records: list[dict[str, Any]]) -> str:
    lines = [
        "# Factor Lab Knowledge Lessons",
        "",
        "Generated from repository research artifacts.",
        "",
        "## Current High-Level Lessons",
        "",
    ]
    if not records:
        lines.append("_No synced experiments yet._")
        return "\n".join(lines) + "\n"
    ordered = sorted(records, key=lambda item: (item.get("pass_gate") is True, _metric(item, "sharpe_net")), reverse=True)
    for record in ordered[:30]:
        lines.append(
            f"- `{record.get('factor_name')}`: classification=`{record.get('classification')}`, "
            f"rank_ic={record.get('rank_ic_mean')}, sharpe_net={record.get('sharpe_net')}. "
            f"Lesson: {record.get('lesson')}"
        )
    return "\n".join(lines) + "\n"


def write_lessons(records: list[dict[str, Any]]) -> None:
    LESSONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    LESSONS_FILE.write_text(build_lessons_markdown(records), encoding="utf-8")


def build_watchlist(records: list[dict[str, Any]], now: str | None = None) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for record in records:
        if record.get("pass_gate") is True or record.get("classification") == "weak_signal_negative_spread":
            candidates.append(
                {
                    "factor_name": record.get("factor_name"),
                    "reason": record.get("lesson"),
                    "sharpe_net": record.get("sharpe_net"),
                    "rank_ic_mean": record.get("rank_ic_mean"),
                    "suggested_next_tests": ["reverse", "independent historical window", "larger liquid universe"],
                }
            )
    candidates.sort(key=lambda item: _metric(item, "sharpe_net"), reverse=True)
    return {"updated_at": now or utc_now(), "factors": candidates[:20]}


def build_blacklist(records: list[dict[str, Any]], now: str | None = None) -> dict[str, Any]:
    rejected = [
        {
            "factor_name": record.get("factor_name"),
            "reason": record.get("lesson"),
            "sharpe_net": record.get("sharpe_net"),
            "rank_ic_mean": record.get("rank_ic_mean"),
        }
        for record in records
        if record.get("classification") == "reject_candidate"
    ]
    return {"updated_at": now or utc_now(), "factors": rejected[:50]}


def write_watchlist_and_blacklist(records: list[dict[str, Any]]) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    WATCHLIST_FILE.write_text(json.dumps(build_watchlist(records), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    BLACKLIST_FILE.write_text(json.dumps(build_blacklist(records), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def enrich_records(path: Path, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    digest = sha256_file(path)
    now = utc_now()
    enriched: list[dict[str, Any]] = []
    for record in records:
        item = dict(record)
        item.update(
            {
                "artifact_path": path.as_posix(),
                "artifact_sha256": digest,
                "synced_at": now,
                "run_id": path.parent.name,
            }
        )
        enriched.append(item)
    return enriched


def sync(write: bool = False, latest: bool = False) -> dict[str, Any]:
    state = load_state()
    files = discover_result_files(ARTIFACTS_DIR, state)
    if latest and files:
        files = [max(files, key=lambda path: path.stat().st_mtime)]
    new_records: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for path in files:
        records = enrich_records(path, parse_result_file(path))
        new_records.extend(records)
        summaries.append({"path": path.as_posix(), "records": len(records)})
        if write:
            state.setdefault("synced_files", {})[path.as_posix()] = {
                "sha256": sha256_file(path),
                "synced_at": utc_now(),
                "record_count": len(records),
            }
    written = 0
    if write:
        written = append_experiment_records(new_records)
        all_records = load_all_experiment_records()
        write_lessons(all_records)
        write_watchlist_and_blacklist(all_records)
        save_state(state)
    return {
        "write": write,
        "latest": latest,
        "files_seen": len(files),
        "records_extracted": len(new_records),
        "records_written": written,
        "files": summaries,
    }


def build_hermes_memory_candidate(records: list[dict[str, Any]]) -> str | None:
    """Return the legacy compact candidate text without writing agent memory."""
    if not records:
        return None
    watch = build_watchlist(records)
    blacklist = build_blacklist(records)
    best = watch.get("factors", [])[:1]
    rejected_count = len(blacklist.get("factors", []))
    if not best and rejected_count == 0:
        return None
    parts: list[str] = []
    if best:
        parts.append(f"Factor Lab: `{best[0].get('factor_name')}` is the best current watchlist candidate.")
    if rejected_count:
        parts.append(f"{rejected_count} factors are reject candidates; avoid unchanged retests.")
    return " ".join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Write repository knowledge files (default: dry-run).")
    parser.add_argument("--latest", action="store_true", help="Only process the newest unsynced result file.")
    parser.add_argument("--memory-candidate", action="store_true", help="Include the legacy compact candidate text.")
    args = parser.parse_args(argv)
    result = sync(write=args.write, latest=args.latest)
    if args.memory_candidate:
        result["hermes_memory_candidate"] = build_hermes_memory_candidate(load_all_experiment_records())
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
