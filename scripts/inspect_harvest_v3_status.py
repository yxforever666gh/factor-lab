#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def inspect_harvest_v3_status(*, root: str | Path = ROOT, latest: int = 5) -> dict[str, Any]:
    root = Path(root)
    base = root / "artifacts" / "harvest_agent"
    cycles = sorted([p for p in base.glob("cycle_*") if p.is_dir()])[-latest:] if base.exists() else []
    rows = []
    for cdir in cycles:
        route = _load(cdir / "mechanism_route.json")
        oos = _load(cdir / "oos_validation.json")
        decision = _load(cdir / "research_decision.json")
        route_state = _load(cdir / "route_state.json")
        v3_next = _load(cdir / "v3_next_cycle_plan.json")
        attr = _load(cdir / "failure_attribution.json")
        rows.append({
            "cycle_id": cdir.name,
            "mechanism_route": route.get("mechanism_id"),
            "oos_class": oos.get("oos_class"),
            "research_decision": decision.get("decision") or "missing_v3_decision",
            "route_state": route_state.get("current_route_status"),
            "repeated_blockers": attr.get("primary_blockers") or [],
            "next_plan_status": v3_next.get("plan_status"),
            "next_action_executable": v3_next.get("plan_status") == "planned",
        })
    out = {"schema_version": 1, "latest_cycle_id": rows[-1]["cycle_id"] if rows else None, "cycles": rows}
    base.mkdir(parents=True, exist_ok=True)
    (base / "v3_status.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md = "# Harvest v3 Status\n\n" + "\n".join(f"- {r['cycle_id']}: route={r['mechanism_route']} oos={r['oos_class']} decision={r['research_decision']}" for r in rows) + "\n"
    (base / "v3_status.md").write_text(md, encoding="utf-8")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Harvest v3 research intelligence status.")
    parser.add_argument("--latest", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps(inspect_harvest_v3_status(root=ROOT, latest=args.latest), ensure_ascii=False, indent=2))
