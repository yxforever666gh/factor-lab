#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_proxy_workstream_report import build_proxy_workstream_report, write_proxy_workstream_report

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write proxy route workstream report.")
    parser.add_argument("--run-id", default="proxy_workstream_report")
    parser.add_argument("--phase6-final-verdict", default=str(ASL / "phase6_final_verdict.json"))
    parser.add_argument("--proxy-cheap-screen-result", default=str(ASL / "proxy_cheap_screen_result.json"))
    parser.add_argument("--proxy-route-verdict", default=str(ASL / "proxy_route_verdict.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    report = build_proxy_workstream_report(
        run_id=args.run_id,
        phase6_final_verdict=_read(Path(args.phase6_final_verdict)),
        proxy_cheap_screen_result=_read(Path(args.proxy_cheap_screen_result)),
        proxy_route_verdict=_read(Path(args.proxy_route_verdict)),
    )
    paths = write_proxy_workstream_report(report, args.output_dir)
    print(
        json.dumps(
            {
                "engineering_status": report["engineering_status"],
                "alpha_status": report["alpha_status"],
                "route_verdict": report["route_verdict"],
                "next_recommended_workstream": report["next_recommended_workstream"],
                "controlled_execution_allowed": report["controlled_execution_allowed"],
                "queue_write_allowed": report["queue_write_allowed"],
                "json_path": str(paths["json"].relative_to(ROOT)),
                "markdown_path": str(paths["markdown"].relative_to(ROOT)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
