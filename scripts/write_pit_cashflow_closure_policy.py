#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from factor_lab.pit_cashflow_closure_policy import write_cashflow_closure_policy

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Write PIT cashflow monitor-only closure policy")
    parser.add_argument("--json", default=str(ROOT / "artifacts" / "pit_cashflow_conditioning" / "cashflow_closure_policy.json"))
    parser.add_argument("--markdown", default=str(ROOT / "artifacts" / "pit_cashflow_conditioning" / "cashflow_closure_policy.md"))
    parser.add_argument("--diagnostics", default=str(ROOT / "artifacts" / "pit_cashflow_conditioning" / "cashflow_conditioning_diagnostics.json"))
    args = parser.parse_args()
    payload = write_cashflow_closure_policy(json_path=args.json, markdown_path=args.markdown, diagnostics_path=args.diagnostics)
    print(f"wrote {args.json} and {args.markdown}; decision={payload.get('decision')} status={payload.get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
