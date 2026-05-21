#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.hermes_client import HermesClient, HermesRequest
from factor_lab.hermes_profiles import HERMES_PROFILE_SPECS
from factor_lab.hermes_router import HermesRouter
from factor_lab.hermes_briefings import build_hermes_prompt, write_hermes_briefing


def smoke_requests(artifact_dir: str | Path = "artifacts/hermes") -> list[HermesRequest]:
    router = HermesRouter(artifact_dir=artifact_dir)
    requests: list[HermesRequest] = []
    for key in HERMES_PROFILE_SPECS:
        route = router.route(key, {"request_id": f"smoke-{key}"})
        route.briefing_path.parent.mkdir(parents=True, exist_ok=True)
        route.briefing_path.write_text("{}", encoding="utf-8")
        requests.append(HermesRequest(
            request_id=route.request_id,
            profile_key=route.profile_key,
            profile_name=route.profile_name,
            session_name=route.session_name,
            toolsets=route.toolsets,
            skills=route.skills,
            briefing_path=route.briefing_path,
            response_path=route.response_path,
            timeout_seconds=300,
        ))
    return requests


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-artifacts", action="store_true")
    args = parser.parse_args()
    client = HermesClient()
    ok = True
    for req in smoke_requests():
        prompt = f'Return JSON only: {{"request_id":"{req.request_id}","profile_key":"{req.profile_key}","summary":"smoke","recommendation":"ok","confidence":1.0,"risks":[],"next_actions":[]}}'
        result = client.run(req, prompt)
        print(f"{req.profile_name}: {'ok' if result.ok else result.error}")
        ok = ok and result.ok
    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
