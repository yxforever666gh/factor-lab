from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read(default: Any = None) -> Any:
    if not STORE_PATH.exists():
        return default if default is not None else {"updated_at_utc": None, "opportunities": {}}
    return json.loads(STORE_PATH.read_text(encoding="utf-8"))


def _write(payload: dict[str, Any]) -> None:
    payload["updated_at_utc"] = _iso_now()
    STORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sync_opportunities(opportunities: list[dict[str, Any]]) -> dict[str, Any]:
    store = _read()
    items = dict(store.get("opportunities") or {})
    for row in opportunities:
        oid = row.get("opportunity_id")
        if not oid:
            continue
        current = dict(items.get(oid) or {})
        history = list(current.get("history") or [])
        if not current:
            history.append({"updated_at_utc": _iso_now(), "state": "proposed", "reason": "synced_from_engine"})
        current.update({
            **row,
            "state": current.get("state") or "proposed",
            "history": history[-30:],
            "last_synced_at_utc": _iso_now(),
        })
        items[oid] = current
    store["opportunities"] = items
    _write(store)
    return store


def update_opportunity_state(opportunity_id: str, state: str, *, reason: str | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    store = _read()
    items = dict(store.get("opportunities") or {})
    current = dict(items.get(opportunity_id) or {"opportunity_id": opportunity_id, "history": []})
    history = list(current.get("history") or [])
    history.append({"updated_at_utc": _iso_now(), "state": state, "reason": reason})
    current.update(extra or {})
    current["state"] = state
    current["history"] = history[-50:]
    current["updated_at_utc"] = _iso_now()
    items[opportunity_id] = current
    store["opportunities"] = items
    _write(store)
    return current


def get_opportunity(opportunity_id: str) -> dict[str, Any] | None:
    store = _read()
    return (store.get("opportunities") or {}).get(opportunity_id)
