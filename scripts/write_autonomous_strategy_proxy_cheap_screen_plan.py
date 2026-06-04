#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_proxy_cheap_screen_plan import build_proxy_cheap_screen_plan, write_proxy_cheap_screen_plan

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Phase 7 proxy cheap-screen plan.")
    parser.add_argument("--run-id", default="proxy_cheap_screen_plan")
    parser.add_argument("--phase6-final-verdict", default=str(ASL / "phase6_final_verdict.json"))
    parser.add_argument("--proxy-pit-alignment", default=str(ASL / "proxy_pit_alignment.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    plan = build_proxy_cheap_screen_plan(
        run_id=args.run_id,
        phase6_final_verdict=_read(Path(args.phase6_final_verdict)),
        proxy_pit_alignment=_read(Path(args.proxy_pit_alignment)),
    )
    paths = write_proxy_cheap_screen_plan(plan, args.output_dir)
    print(
        json.dumps(
            {
                "decision": plan["decision"],
                "recommended_next_step": plan["recommended_next_step"],
                "candidate_count": len(plan["candidate_screens"]),
                "controlled_execution_allowed": plan["controlled_execution_allowed"],
                "queue_write_allowed": plan["queue_write_allowed"],
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
