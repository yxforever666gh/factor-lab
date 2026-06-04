from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

HARVEST_ROOT = Path("artifacts/harvest_agent")

_SIGNAL_FAMILIES = {
    "industry_relative_earnings_yield": "industry_relative_value",
    "industry_relative_book_yield": "industry_relative_value",
    "earnings_yield": "raw_value",
    "book_yield": "raw_value",
    "pb": "raw_value",
    "pe_ttm": "raw_value",
    "momentum_20": "momentum",
    "momentum_60": "momentum",
    "momentum_120": "momentum",
    "turnover": "liquidity",
}


def _round_quantile(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(round(number / 0.1) * 0.1, 1)


def _signal_family(signal: str) -> str:
    return _SIGNAL_FAMILIES.get(str(signal), str(signal))


def _normalized_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for action in actions:
        atype = str(action.get("type") or "")
        if atype == "set_signal_columns":
            normalized.append(
                {
                    "type": atype,
                    "signal_families": sorted({_signal_family(s) for s in action.get("signal_columns") or []}),
                }
            )
        elif atype == "add_filter":
            normalized.append(
                {
                    "type": atype,
                    "field": action.get("field"),
                    "operator": action.get("operator"),
                    "quantile_band": _round_quantile(action.get("quantile")),
                }
            )
        elif atype == "restrict_costs":
            normalized.append(
                {
                    "type": atype,
                    "cost_bps_values": sorted(float(x) for x in action.get("cost_bps_values") or []),
                }
            )
        elif atype == "set_holding_counts":
            normalized.append(
                {
                    "type": atype,
                    "holding_count_bands": sorted(int(x) // 25 * 25 for x in action.get("holding_counts") or []),
                }
            )
        elif atype == "set_windows":
            windows = []
            for window in action.get("year_windows") or []:
                windows.append({"start_date": window.get("start_date"), "end_date": window.get("end_date")})
            normalized.append({"type": atype, "windows": sorted(windows, key=lambda w: (str(w.get("start_date")), str(w.get("end_date"))))})
        else:
            normalized.append({"type": atype})
    return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")))


def build_semantic_signature(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_path": plan.get("dataset_path"),
        "mechanism_id": (plan.get("mechanism_route") or {}).get("mechanism_id") or plan.get("mechanism_id"),
        "actions": _normalized_actions(plan.get("actions") or []),
    }


def semantic_hash(plan: dict[str, Any]) -> str:
    text = json.dumps(build_semantic_signature(plan), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def find_semantic_duplicate(root: str | Path, plan: dict[str, Any]) -> dict[str, Any] | None:
    target = semantic_hash(plan)
    base = Path(root) / HARVEST_ROOT
    for path in sorted(base.glob("cycle_*/semantic_signature.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("semantic_hash") == target:
            return {
                "cycle_id": path.parent.name,
                "path": str(path),
                "semantic_hash": target,
                "semantic_signature": payload.get("semantic_signature"),
            }
    return None


def write_semantic_signature(cycle_dir: str | Path, plan: dict[str, Any]) -> Path:
    cdir = Path(cycle_dir)
    cdir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "semantic_hash": semantic_hash(plan),
        "semantic_signature": build_semantic_signature(plan),
    }
    path = cdir / "semantic_signature.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
