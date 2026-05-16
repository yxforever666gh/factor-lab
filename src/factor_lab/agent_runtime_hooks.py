from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.llm_provider_router import DecisionProviderRouter
from factor_lab.paths import artifacts_dir


def _write_payload(payload: dict[str, Any], output_path: str | Path) -> dict[str, Any]:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _write_error(error_path: str | Path, *, agent_role: str, context: dict[str, Any], exc: Exception) -> None:
    path = Path(error_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "agent_role": agent_role,
                "context_id": context.get("context_id"),
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def run_data_quality_review(context: dict[str, Any], output_path: str | Path, provider: str | None = None) -> dict[str, Any]:
    payload = DecisionProviderRouter(provider=provider).generate("data_quality", context)
    return _write_payload(payload, output_path)


def run_reviewer_review(context: dict[str, Any], output_path: str | Path, provider: str | None = None) -> dict[str, Any]:
    payload = DecisionProviderRouter(provider=provider).generate("reviewer", context)
    return _write_payload(payload, output_path)


def safe_run_data_quality_review(context: dict[str, Any], output_path: str | Path | None = None, provider: str | None = None) -> dict[str, Any] | None:
    output = Path(output_path) if output_path else artifacts_dir() / "data_quality_review.json"
    try:
        return run_data_quality_review(context, output, provider=provider)
    except Exception as exc:  # pragma: no cover - defensive runtime hook
        _write_error(output.parent / "data_quality_review_error.json", agent_role="data_quality", context=context, exc=exc)
        return None


def safe_run_reviewer_review(context: dict[str, Any], output_path: str | Path | None = None, provider: str | None = None) -> dict[str, Any] | None:
    output = Path(output_path) if output_path else artifacts_dir() / "reviewer_review.json"
    try:
        return run_reviewer_review(context, output, provider=provider)
    except Exception as exc:  # pragma: no cover - defensive runtime hook
        _write_error(output.parent / "reviewer_review_error.json", agent_role="reviewer", context=context, exc=exc)
        return None
