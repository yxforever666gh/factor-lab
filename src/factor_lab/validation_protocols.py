from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROTOCOL_PATH = ROOT / "configs" / "validation_protocols.json"


def load_validation_protocols(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    payload = json.loads(Path(path or DEFAULT_PROTOCOL_PATH).read_text(encoding="utf-8"))
    return payload.get("protocols") or {}


def build_validation_matrix(
    *,
    factor: dict[str, Any],
    protocol_name: str = "alpha_candidate_default",
    protocols: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    registry = protocols or load_validation_protocols()
    protocol = registry[protocol_name]
    required_passes = set(protocol.get("required_passes") or [])
    recent_monitor_only = bool(protocol.get("recent_is_monitor_only", True))
    runs: list[dict[str, Any]] = []
    for window_name, bounds in (protocol.get("windows") or {}).items():
        for universe in protocol.get("universes") or []:
            for horizon in protocol.get("horizons") or []:
                promotion_eligible = window_name in required_passes
                if recent_monitor_only and window_name == "recent":
                    promotion_eligible = False
                runs.append(
                    {
                        "window_name": window_name,
                        "start_date": bounds[0],
                        "end_date": bounds[1],
                        "universe_limit": universe,
                        "horizon": horizon,
                        "factor": dict(factor),
                        "promotion_eligible": promotion_eligible,
                    }
                )
    return {"protocol_name": protocol_name, "protocol": protocol, "runs": runs}
