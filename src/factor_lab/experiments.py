from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class ExperimentLedger:
    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.output_dir / "experiment_ledger.json"

    def write(self, payload: Dict[str, Any]) -> None:
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
