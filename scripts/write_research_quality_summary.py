#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from factor_lab.research_quality_summary import write_research_quality_summary

ROOT = Path(__file__).resolve().parents[1]
KNOWLEDGE_DIR = ROOT / "knowledge"


def _write_knowledge_files(payload: dict) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    value = payload.get("value_research_routes", {})
    blocked = value.get("blocked", [])
    ready = value.get("ready", [])
    mechanism_lines = [
        "# Mechanism Lessons",
        "",
        "Generated from research quality summary.",
        "",
        f"- Ready value route candidates: {value.get('ready_count', 0)}",
        f"- Blocked value routes: {value.get('blocked_count', 0)}",
        "",
        "## Ready routes",
    ]
    for row in ready[:20]:
        mechanism_lines.append(f"- `{row.get('route_id')}` / `{row.get('mechanism_id')}`: {row.get('hypothesis')}")
    mechanism_lines.append("")
    mechanism_lines.append("## Blocked routes")
    for row in blocked[:20]:
        mechanism_lines.append(f"- `{row.get('route_id')}` blocked by missing fields: {row.get('missing_fields')}")
    (KNOWLEDGE_DIR / "mechanism_lessons.md").write_text("\n".join(mechanism_lines) + "\n", encoding="utf-8")
    blockers = {
        "updated_at_utc": payload.get("generated_at_utc"),
        "blocked_value_routes": blocked,
        "missing_fields": payload.get("data_coverage", {}).get("summary", {}).get("missing_fields", []),
    }
    (KNOWLEDGE_DIR / "data_blockers.json").write_text(__import__("json").dumps(blockers, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    gate = payload.get("gate_decisions", {})
    waste_lines = [
        "# Research Waste / Gate Summary",
        "",
        f"- Gate decisions observed: {gate.get('total', 0)}",
        f"- By decision: {gate.get('by_decision', {})}",
        f"- By budget bucket: {gate.get('by_budget_bucket', {})}",
        "",
        "## Top reasons",
    ]
    for row in gate.get("top_reasons", [])[:20]:
        waste_lines.append(f"- {row.get('reason')}: {row.get('count')}")
    (KNOWLEDGE_DIR / "research_waste.md").write_text("\n".join(waste_lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Factor Lab research quality summary artifacts")
    parser.add_argument("--json", default=str(ROOT / "artifacts" / "research_quality_summary.json"))
    parser.add_argument("--markdown", default=str(ROOT / "artifacts" / "research_quality_summary.md"))
    parser.add_argument("--controlled-ledger-summary", default=str(ROOT / "artifacts" / "controlled_run_ledger_summary.json"))
    args = parser.parse_args()
    payload = write_research_quality_summary(
        json_path=args.json,
        markdown_path=args.markdown,
        controlled_ledger_summary_path=args.controlled_ledger_summary,
    )
    _write_knowledge_files(payload)
    print(f"wrote {args.json}, {args.markdown}, and knowledge research-quality files; gate_decisions={payload['gate_decisions']['total']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
