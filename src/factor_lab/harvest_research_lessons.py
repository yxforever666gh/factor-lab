from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from factor_lab.harvest_route_state import build_route_state


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_harvest_research_lessons(root: str | Path = ".", latest: int = 20) -> dict[str, Any]:
    root = Path(root)
    base = root / "artifacts" / "harvest_agent"
    cycles = sorted([p for p in base.glob("cycle_*") if p.is_dir()])[-latest:] if base.exists() else []
    blockers: Counter[str] = Counter()
    route_blockers: dict[str, Counter[str]] = defaultdict(Counter)
    data_requests: Counter[str] = Counter()
    for cdir in cycles:
        route = _load(cdir / "mechanism_route.json").get("mechanism_id") or "unknown"
        attr = _load(cdir / "failure_attribution.json")
        req = _load(cdir / "data_request.json")
        for blocker in attr.get("primary_blockers") or []:
            blockers[str(blocker)] += 1
            route_blockers[str(route)][str(blocker)] += 1
        for field in (req.get("missing_required_fields") or []) + (req.get("recommended_data") or []):
            data_requests[str(field)] += 1
    lessons = []
    for route, counts in route_blockers.items():
        for blocker, count in counts.most_common():
            lessons.append(f"{route} has repeated {blocker} evidence ({count} recent occurrence(s)).")
    for field, count in data_requests.most_common():
        lessons.append(f"Data field {field} is requested/recommended by Harvest v3 ({count} recent occurrence(s)).")
    return {
        "schema_version": 1,
        "cycle_count": len(cycles),
        "blockers": dict(blockers),
        "route_blockers": {k: dict(v) for k, v in route_blockers.items()},
        "data_requests": dict(data_requests),
        "lessons": lessons,
        "route_state": build_route_state(root),
    }


def write_harvest_research_lessons(root: str | Path = ".") -> dict[str, Any]:
    root = Path(root)
    out = build_harvest_research_lessons(root)
    kdir = root / "knowledge"
    kdir.mkdir(parents=True, exist_ok=True)
    (kdir / "harvest_research_lessons.md").write_text("# Harvest Research Lessons\n\n" + "\n".join(f"- {x}" for x in out.get("lessons") or []) + "\n", encoding="utf-8")
    (kdir / "harvest_route_state.json").write_text(json.dumps(out.get("route_state"), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (kdir / "harvest_data_requests.json").write_text(json.dumps(out.get("data_requests"), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out["written"] = True
    return out
