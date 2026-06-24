#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_worker_contract import (
    build_worker_contract,
    validate_worker_contract,
    write_worker_contract,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def read_text(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Write Hermes-native research worker contract artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    parser.add_argument("--agent-policy", default="artifacts/market_phenomena/agent_policy.json")
    parser.add_argument("--research-handoff", default="artifacts/market_phenomena/research_handoff.json")
    parser.add_argument("--phenomenon-verdict", default="artifacts/market_phenomena/phenomenon_verdict.json")
    parser.add_argument("--minimal-result", default="artifacts/market_phenomena/minimal_result.json")
    parser.add_argument("--lessons", default="artifacts/market_phenomena/lessons.md")
    parser.add_argument("--data-catalog-summary", default="artifacts/market_phenomena/data_catalog_summary.json")
    args = parser.parse_args()

    run_id = args.run_id or "worker_contract_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    contract = build_worker_contract(
        run_id=run_id,
        agent_policy=read_json(args.agent_policy),
        research_handoff=read_json(args.research_handoff),
        phenomenon_verdict=read_json(args.phenomenon_verdict),
        minimal_result=read_json(args.minimal_result),
        lessons_markdown=read_text(args.lessons),
        data_catalog_summary=read_json(args.data_catalog_summary),
    )
    validation = validate_worker_contract(contract)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid worker contract: {validation}")
    paths = write_worker_contract(contract, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")
    print(f"target_phenomena {len(contract['target_phenomena'])}")
    print(f"required_outputs {len(contract['required_output_artifacts'])}")


if __name__ == "__main__":
    main()
