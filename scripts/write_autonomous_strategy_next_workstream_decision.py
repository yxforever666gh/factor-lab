#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_next_workstream_decision import build_next_workstream_decision, write_next_workstream_decision

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write next workstream decision after proxy route report.")
    parser.add_argument("--run-id", default="next_workstream_decision")
    parser.add_argument("--proxy-report", default=str(ASL / "proxy_workstream_report.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)
    proxy_report = json.loads(Path(args.proxy_report).read_text(encoding="utf-8"))
    report = build_next_workstream_decision(run_id=args.run_id, proxy_report=proxy_report)
    paths = write_next_workstream_decision(report, args.output_dir)
    print(json.dumps({"decision": report["decision"], "recommended_next_step": report["recommended_next_step"], "json_path": str(paths["json"].relative_to(ROOT)), "markdown_path": str(paths["markdown"].relative_to(ROOT))}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
