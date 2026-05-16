from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.decision_context_builder import (
    build_failure_decision_context,
    build_planner_decision_context,
)
from factor_lab.llm_provider_router import DecisionProviderRouter
from factor_lab.paths import artifacts_dir


def _artifacts_path() -> Path:
    return artifacts_dir()


def _planner_brief_path() -> Path:
    return _artifacts_path() / "planner_agent_brief.json"


def _failure_brief_path() -> Path:
    return _artifacts_path() / "failure_analyst_brief.json"


def _planner_context_path() -> Path:
    return _artifacts_path() / "planner_decision_context.json"


def _failure_context_path() -> Path:
    return _artifacts_path() / "failure_decision_context.json"


def _planner_response_path() -> Path:
    return _artifacts_path() / "planner_agent_response.json"


def _failure_response_path() -> Path:
    return _artifacts_path() / "failure_analyst_response.json"


def _agent_responses_path() -> Path:
    return _artifacts_path() / "agent_responses.json"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Factor Lab decision briefs")
    parser.add_argument(
        "--provider",
        default=(
            os.environ.get("FACTOR_LAB_AGENT_BRIEFS_PROVIDER")
            or os.environ.get("FACTOR_LAB_DECISION_PROVIDER_OVERRIDE")
            or None
        ),
        help="Override the decision provider for this brief run.",
    )
    return parser.parse_args()


def _effective_source(payload: dict) -> str | None:
    metadata = payload.get("decision_metadata") or {}
    return metadata.get("effective_source") or metadata.get("source") or payload.get("decision_source")


def main() -> int:
    args = _parse_args()
    planner_brief_path = _planner_brief_path()
    failure_brief_path = _failure_brief_path()
    planner_context_path = _planner_context_path()
    failure_context_path = _failure_context_path()
    planner_response_path = _planner_response_path()
    failure_response_path = _failure_response_path()
    agent_responses_path = _agent_responses_path()

    planner_brief = _read_json(planner_brief_path)
    failure_brief = _read_json(failure_brief_path)
    router = DecisionProviderRouter(provider=args.provider)

    planner_response = {}
    failure_response = {}
    planner_context = {}
    failure_context = {}

    if planner_brief:
        planner_context = build_planner_decision_context(planner_brief)
        _write_json(planner_context_path, planner_context)
        planner_response = router.generate("planner", planner_context)
        _write_json(planner_response_path, planner_response)

    if failure_brief:
        failure_context = build_failure_decision_context(failure_brief)
        _write_json(failure_context_path, failure_context)
        failure_response = router.generate("failure_analyst", failure_context)
        _write_json(failure_response_path, failure_response)

    _write_json(
        agent_responses_path,
        {
            "loaded_at_utc": _iso_now(),
            "planner": planner_response,
            "planner_errors": list((planner_response.get("decision_metadata") or {}).get("validation_errors") or []),
            "failure_analyst": failure_response,
            "failure_analyst_errors": list((failure_response.get("decision_metadata") or {}).get("validation_errors") or []),
        },
    )

    print(
        json.dumps(
            {
                "ok": True,
                "generated_at_utc": _iso_now(),
                "planner_written": planner_response_path.exists(),
                "failure_written": failure_response_path.exists(),
                "planner_context_id": planner_context.get("context_id"),
                "failure_context_id": failure_context.get("context_id"),
                "configured_provider": router.provider,
                "planner_source": _effective_source(planner_response) if planner_response else None,
                "failure_source": _effective_source(failure_response) if failure_response else None,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
