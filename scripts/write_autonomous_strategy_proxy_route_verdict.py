#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_proxy_route_verdict import build_proxy_route_verdict, write_proxy_route_verdict

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write proxy route verdict from proxy cheap-screen result.")
    parser.add_argument("--run-id", default="proxy_route_verdict")
    parser.add_argument("--cheap-screen-result", default=str(ASL / "proxy_cheap_screen_result.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    cheap_screen = json.loads(Path(args.cheap_screen_result).read_text(encoding="utf-8"))
    verdict = build_proxy_route_verdict(run_id=args.run_id, cheap_screen_result=cheap_screen)
    paths = write_proxy_route_verdict(verdict, args.output_dir)
    print(
        json.dumps(
            {
                "verdict": verdict["verdict"],
                "reason_codes": verdict["reason_codes"],
                "recommended_next_step": verdict["recommended_next_step"],
                "controlled_execution_allowed": verdict["controlled_execution_allowed"],
                "queue_write_allowed": verdict["queue_write_allowed"],
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
