from __future__ import annotations

import json
from pathlib import Path

from factor_lab.candidate_triage import build_candidate_triage_model

ROOT = Path(__file__).resolve().parents[1]
MEMORY_PATH = ROOT / "artifacts" / "research_memory.json"
OUTPUT_PATH = ROOT / "artifacts" / "candidate_triage_model.json"


def main() -> int:
    if not MEMORY_PATH.exists():
        raise SystemExit(f"missing memory file: {MEMORY_PATH}")
    payload = json.loads(MEMORY_PATH.read_text(encoding="utf-8"))
    model = build_candidate_triage_model(payload)
    OUTPUT_PATH.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output": str(OUTPUT_PATH), "generated_from_outcomes": model.get("generated_from_outcomes")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
