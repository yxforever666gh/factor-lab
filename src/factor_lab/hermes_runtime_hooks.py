from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.hermes_briefings import build_hermes_prompt, write_hermes_briefing
from factor_lab.hermes_client import HermesClient, HermesRequest
from factor_lab.hermes_router import HermesRouter
from factor_lab.paths import artifacts_dir


def _request_from_route(route, timeout_seconds: int = 300) -> HermesRequest:
    return HermesRequest(
        request_id=route.request_id,
        profile_key=route.profile_key,
        profile_name=route.profile_name,
        session_name=route.session_name,
        toolsets=route.toolsets,
        skills=route.skills,
        briefing_path=route.briefing_path,
        response_path=route.response_path,
        timeout_seconds=timeout_seconds,
    )


def run_hermes_profile(profile_key: str, context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    router = HermesRouter()
    route = router.route(profile_key, context)
    if output_path is not None:
        route = type(route)(
            request_id=route.request_id,
            profile_key=route.profile_key,
            profile_name=route.profile_name,
            session_name=route.session_name,
            toolsets=route.toolsets,
            skills=route.skills,
            briefing_path=route.briefing_path,
            response_path=Path(output_path),
            migration_alias_used=route.migration_alias_used,
            input_event_key=route.input_event_key,
        )
    write_hermes_briefing(route, task=f"Handle Factor Lab research event for {route.profile_name}.", context=context)
    result = HermesClient().run(_request_from_route(route), build_hermes_prompt(route))
    if not result.ok or result.payload is None:
        raise RuntimeError(result.error or "hermes_profile_failed")
    return result.payload


def run_researcher_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    return run_hermes_profile("researcher", context, output_path)


def run_diagnostician_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    return run_hermes_profile("diagnostician", context, output_path)


def run_reviewer_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    return run_hermes_profile("reviewer", context, output_path)


def run_data_steward_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any]:
    return run_hermes_profile("data_steward", context, output_path)


def _write_error(error_path: str | Path, *, profile_key: str, context: dict[str, Any], exc: Exception) -> None:
    path = Path(error_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"hermes_profile_key": profile_key, "context_id": context.get("context_id"), "updated_at_utc": datetime.now(timezone.utc).isoformat(), "error": str(exc)}, ensure_ascii=False, indent=2), encoding="utf-8")


def safe_run_data_steward_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any] | None:
    output = Path(output_path) if output_path else artifacts_dir() / "hermes" / "responses" / "data_steward_review.json"
    try:
        return run_data_steward_profile(context, output)
    except Exception as exc:
        _write_error(output.parent / "data_steward_review_error.json", profile_key="data_steward", context=context, exc=exc)
        return None


def safe_run_reviewer_profile(context: dict[str, Any], output_path: str | Path | None = None) -> dict[str, Any] | None:
    output = Path(output_path) if output_path else artifacts_dir() / "hermes" / "responses" / "reviewer_review.json"
    try:
        return run_reviewer_profile(context, output)
    except Exception as exc:
        _write_error(output.parent / "reviewer_review_error.json", profile_key="reviewer", context=context, exc=exc)
        return None
