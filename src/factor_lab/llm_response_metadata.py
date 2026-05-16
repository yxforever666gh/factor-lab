from __future__ import annotations

from typing import Any



def build_response_metadata(
    *,
    source: str,
    provider: str,
    model: str,
    fallback_reason: str | None,
    latency_ms: int,
    schema_valid: bool,
    validation_errors: list[str] | None = None,
    decision_context_id: str | None = None,
    effective_source: str | None = None,
    configured_provider: str | None = None,
    degraded_to_heuristic: bool = False,
    provider_latency_ms: int | None = None,
    session_mode: str | None = None,
    session_id: str | None = None,
    request_scope_id: str | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "provider": provider,
        "configured_provider": configured_provider or provider,
        "model": model,
        "effective_source": effective_source or source,
        "degraded_to_heuristic": bool(degraded_to_heuristic),
        "fallback_reason": fallback_reason,
        "latency_ms": int(latency_ms),
        "provider_latency_ms": int(provider_latency_ms if provider_latency_ms is not None else latency_ms),
        "schema_valid": bool(schema_valid),
        "validation_errors": list(validation_errors or []),
        "decision_context_id": decision_context_id,
        "session_mode": session_mode,
        "session_id": session_id,
        "request_scope_id": request_scope_id,
    }
