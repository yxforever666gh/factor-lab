#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_phase6_final_verdict import build_phase6_final_verdict, write_phase6_final_verdict

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Phase 6 final verdict for Autonomous Strategy Lab.")
    parser.add_argument("--run-id", default="phase6_final_verdict")
    parser.add_argument("--controller-state", default=str(ASL / "controller_state.json"))
    parser.add_argument("--pit-overlay", default=str(ASL / "pit_overlay_diagnostic.json"))
    parser.add_argument("--proxy-pit-alignment", default=str(ASL / "proxy_pit_alignment.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    verdict = build_phase6_final_verdict(
        run_id=args.run_id,
        controller_state=_read(Path(args.controller_state)),
        pit_overlay_diagnostic=_read(Path(args.pit_overlay)),
        proxy_pit_alignment=_read(Path(args.proxy_pit_alignment)),
    )
    paths = write_phase6_final_verdict(verdict, args.output_dir)
    print(
        json.dumps(
            {
                "phase_status": verdict["phase_status"],
                "final_state": verdict["final_state"],
                "next_phase": verdict["next_phase"],
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
