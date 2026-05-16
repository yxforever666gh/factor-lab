#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_gate import evaluate_research_gate_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Check a Factor Lab research hypothesis gate.")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    decision = evaluate_research_gate_file(args.hypothesis).to_dict()
    text = json.dumps(decision, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    return 0 if decision["decision"] == "allow_preflight" else 2


if __name__ == "__main__":
    raise SystemExit(main())
