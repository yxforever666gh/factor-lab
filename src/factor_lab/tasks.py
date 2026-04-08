from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


class TaskTracker:
    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.output_dir / "task_state.json"

    def start(self, config_path: str, output_dir: str) -> dict[str, Any]:
        payload = {
            "task_id": str(uuid4()),
            "config_path": config_path,
            "output_dir": output_dir,
            "status": "running",
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "finished_at_utc": None,
            "error": None,
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def update(self, payload: dict[str, Any], **fields: Any) -> dict[str, Any]:
        payload = dict(payload)
        payload.update(fields)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def finish(self, payload: dict[str, Any], status: str = "finished", error: str | None = None) -> dict[str, Any]:
        payload = dict(payload)
        payload["status"] = status
        payload["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        payload["error"] = error
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
