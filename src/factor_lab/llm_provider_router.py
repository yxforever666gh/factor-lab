from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from factor_lab.agent_roles import AgentRoleConfig, load_agent_roles, select_agent_role
from factor_lab.data_quality_decision_engine import build_data_quality_response
from factor_lab.failure_analyst_engine import build_failure_response
from factor_lab.llm_response_metadata import build_response_metadata
from factor_lab.llm_schema_validation import validate_decision_payload
from factor_lab.paths import artifacts_dir, env_file, project_root
from factor_lab.planner_decision_engine import build_planner_response
from factor_lab.reviewer_decision_engine import build_reviewer_response

DecisionBuilder = Callable[[dict[str, Any]], dict[str, Any]]
DECISION_SCHEMA_HINTS: dict[str, dict[str, Any]] = {
    "planner": {
        "mode": "explore|validate|recover|converge|harvest_exposure",
        "task_mix": {"baseline": 1, "validation": 1, "exploration": 1},
        "priority_families": ["family-name"],
        "suppress_families": ["family-name"],
        "recommended_actions": [
            {"type": "validation|exploration|diagnostic|suppress|promote", "target": "id", "reason": "why now"}
        ],
        "hypothesis_cards": [
            {
                "candidate_name": "id",
                "mechanism_note": "note",
                "target_window": "short_window_recheck|recent_extension|medium_horizon",
                "invalidation_signals": ["flag"],
                "incremental_value_thesis": "why",
            }
        ],
        "challenger_queue": ["candidate-name"],
        "confidence_score": 0.7,
        "rationale_markdown": "- summary",
    },
    "failure_analyst": {
        "failure_patterns": [
            {
                "pattern_id": "id",
                "scope": "candidate|family|branch|workflow",
                "symptom": "what failed",
                "likely_cause": "why",
                "recommended_action": "stop|diagnose|retry|deprioritize|reroute",
                "confidence_score": 0.7,
            }
        ],
        "should_stop": ["id"],
        "should_probe": ["id"],
        "should_reroute": ["id"],
        "summary_markdown": "- summary",
    },
    "reviewer": {
        "candidate_reviews": [
            {
                "candidate_name": "id",
                "quality_verdict": "promote|keep_validating|deprioritize|suppress|diagnose",
                "incremental_value_assessment": "strong|medium|weak|duplicate_like|unknown",
                "robustness_concerns": ["flag"],
                "evidence": ["metric or artifact reference"],
                "recommended_action": "what to do next",
                "confidence_score": 0.7,
            }
        ],
        "portfolio_level_notes": ["note"],
        "summary_markdown": "- summary",
    },
    "data_quality": {
        "data_quality_findings": [
            {
                "scope": "dataset|provider|cache|workflow|feature_store",
                "severity": "ok|warning|critical",
                "symptom": "what is observed",
                "likely_cause": "why",
                "recommended_action": "what to do next",
            }
        ],
        "dataset_health": {
            "dataset_rows": 0,
            "coverage_status": "ok|empty|partial|unknown",
            "token_status": "configured|missing|unknown",
        },
        "should_pause_research": False,
        "summary_markdown": "- summary",
    },
}
DECISION_SCHEMA_VERSIONS: dict[str, str] = {
    "planner": "factor_lab.planner_agent_response.v1",
    "failure_analyst": "factor_lab.failure_analyst_response.v1",
    "reviewer": "factor_lab.reviewer_agent_response.v1",
    "data_quality": "factor_lab.data_quality_agent_response.v1",
}
LEGACY_PROVIDER_ALIASES = {
    "openclaw_gateway": "legacy_openclaw_gateway",
    "openclaw_session": "legacy_openclaw_gateway",
    "openclaw_http": "legacy_openclaw_gateway",
    "openclaw_agent": "legacy_openclaw_agent",
    "openclaw_cli": "legacy_openclaw_agent",
    "openclaw_internal": "legacy_openclaw_agent",
    "openclaw": "legacy_openclaw_agent",
}
NORMALIZED_REMOTE_PROVIDER_SOURCES = {
    "real_llm",
    "legacy_openclaw_gateway",
    "legacy_openclaw_agent",
}
REMOTE_PROVIDER_SOURCES = {
    "real_llm",
    "openclaw_gateway",
    "openclaw_agent",
    "openclaw_cli",
    "openclaw_internal",
    *NORMALIZED_REMOTE_PROVIDER_SOURCES,
}



def _maybe_load_workspace_env() -> None:
    path = env_file()
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_maybe_load_workspace_env()


class DecisionProviderRouter:
    def __init__(self, provider: str | None = None, model: str | None = None) -> None:
        self.provider = (
            provider
            or os.environ.get("FACTOR_LAB_DECISION_PROVIDER")
            or os.environ.get("FACTOR_LAB_LLM_PROVIDER")
            or "heuristic"
        ).strip().lower()
        self.model = (
            model
            or os.environ.get("FACTOR_LAB_LLM_MODEL")
            or os.environ.get("FACTOR_LAB_OPENCLAW_AGENT_TARGET")
            or os.environ.get("OPENAI_MODEL")
            or "gpt-4o-mini"
        )

    def _context_request_scope_id(self, context: dict[str, Any] | None = None) -> str | None:
        raw = str((context or {}).get("context_id") or "").strip()
        if not raw:
            return None
        compact = "".join(ch if ch.isalnum() else "-" for ch in raw).strip("-")
        return compact[:64] if compact else None

    def _normalized_provider_name(self, provider: str | None = None) -> str:
        raw = (provider or self.provider or "heuristic").strip().lower()
        if raw in {"real", "real_llm", "openai", "openai_compatible"}:
            return "real_llm"
        if raw in LEGACY_PROVIDER_ALIASES:
            return LEGACY_PROVIDER_ALIASES[raw]
        if raw in {"heuristic", "mock", "auto"}:
            return raw
        return raw or "heuristic"

    def _provider_class(self, provider: str | None = None) -> str:
        normalized = self._normalized_provider_name(provider)
        if normalized.startswith("legacy_"):
            return "legacy"
        if normalized == "real_llm":
            return "primary"
        if normalized in {"heuristic", "mock"}:
            return "local"
        return "unknown"

    def _display_effective_source(self, provider: str) -> str:
        return self._normalized_provider_name(provider)

    def generate(self, decision_type: str, context: dict[str, Any]) -> dict[str, Any]:
        builders: dict[str, DecisionBuilder] = {
            "planner": lambda payload: build_planner_response(payload, source_label="heuristic"),
            "failure_analyst": lambda payload: build_failure_response(payload, source_label="heuristic"),
            "reviewer": lambda payload: build_reviewer_response(payload, source_label="heuristic"),
            "data_quality": lambda payload: build_data_quality_response(payload, source_label="heuristic"),
        }
        mock_builders: dict[str, DecisionBuilder] = {
            "planner": lambda payload: build_planner_response(payload, source_label="mock"),
            "failure_analyst": lambda payload: build_failure_response(payload, source_label="mock"),
            "reviewer": lambda payload: build_reviewer_response(payload, source_label="mock"),
            "data_quality": lambda payload: build_data_quality_response(payload, source_label="mock"),
        }
        if decision_type not in builders:
            raise ValueError(f"unsupported decision_type: {decision_type}")

        chain = self._provider_chain()
        agent_role = select_agent_role(decision_type)
        if agent_role is None and self._decision_type_is_configured_but_disabled(decision_type):
            raise RuntimeError(f"agent role for decision_type {decision_type} is disabled")
        fallback_reason: str | None = None
        last_error: str | None = None
        fallback_session_mode: str | None = None
        fallback_session_id: str | None = None
        fallback_request_scope_id: str | None = self._context_request_scope_id(context)

        for provider in chain:
            started = time.perf_counter()
            session_mode: str | None = None
            session_id: str | None = None
            request_scope_id: str | None = fallback_request_scope_id
            if provider in {"openclaw_gateway", "openclaw_agent"}:
                request_scope_id = self._openclaw_request_scope_id(context=context)
                session_mode = self._openclaw_session_mode()
                session_id = self._openclaw_session_id(
                    decision_type,
                    context=context,
                    request_scope_id=request_scope_id,
                )
            try:
                if provider == "real_llm":
                    payload = self._call_real_llm(decision_type, context, agent_role=agent_role)
                elif provider == "openclaw_gateway":
                    payload = self._call_openclaw_gateway(
                        decision_type,
                        context,
                        session_id=session_id,
                        request_scope_id=request_scope_id,
                    )
                elif provider in {"openclaw_agent", "openclaw_cli", "openclaw_internal"}:
                    payload = self._call_openclaw_agent(
                        decision_type,
                        context,
                        session_id=session_id,
                        request_scope_id=request_scope_id,
                    )
                elif provider == "heuristic":
                    payload = builders[decision_type](context)
                elif provider == "mock":
                    payload = mock_builders[decision_type](context)
                else:
                    last_error = f"unsupported_provider:{provider}"
                    continue

                latency_ms = int((time.perf_counter() - started) * 1000)
                errors = validate_decision_payload(decision_type, payload)
                schema_valid = not errors
                metadata_session_mode = session_mode or fallback_session_mode
                metadata_session_id = session_id or fallback_session_id
                metadata_request_scope_id = request_scope_id or fallback_request_scope_id
                payload["decision_metadata"] = build_response_metadata(
                    source=provider,
                    provider=provider,
                    configured_provider=self.provider,
                    model=self._metadata_model(provider, payload),
                    effective_source=provider,
                    degraded_to_heuristic=(
                        provider == "heuristic"
                        and self.provider not in {"heuristic", "mock"}
                        and fallback_reason is not None
                    ),
                    fallback_reason=fallback_reason,
                    latency_ms=latency_ms,
                    provider_latency_ms=latency_ms,
                    schema_valid=schema_valid,
                    validation_errors=errors,
                    decision_context_id=context.get("context_id"),
                    session_mode=metadata_session_mode,
                    session_id=metadata_session_id,
                    request_scope_id=metadata_request_scope_id,
                )
                self._attach_agent_role_metadata(payload, decision_type, agent_role)
                if provider not in REMOTE_PROVIDER_SOURCES:
                    payload.setdefault("decision_source", provider)
                    payload.setdefault("decision_context_id", context.get("context_id"))
                if schema_valid or (agent_role and not agent_role.strict_schema):
                    return payload
                fallback_reason = f"schema_invalid:{provider}"
                last_error = ";".join(errors)
                if session_mode or session_id or request_scope_id:
                    fallback_session_mode = session_mode
                    fallback_session_id = session_id
                    fallback_request_scope_id = request_scope_id
            except Exception as exc:  # pragma: no cover - guarded by fallback tests
                fallback_reason = f"provider_error:{provider}"
                last_error = str(exc)
                if session_mode or session_id or request_scope_id:
                    fallback_session_mode = session_mode
                    fallback_session_id = session_id
                    fallback_request_scope_id = request_scope_id

        payload = mock_builders[decision_type](context)
        payload["decision_metadata"] = build_response_metadata(
            source="mock",
            provider="mock",
            configured_provider=self.provider,
            model="mock",
            effective_source="mock",
            degraded_to_heuristic=False,
            fallback_reason=fallback_reason or last_error or "unknown",
            latency_ms=0,
            provider_latency_ms=0,
            schema_valid=True,
            validation_errors=[],
            decision_context_id=context.get("context_id"),
            session_mode=fallback_session_mode,
            session_id=fallback_session_id,
            request_scope_id=fallback_request_scope_id,
        )
        payload.setdefault("decision_source", "mock")
        payload.setdefault("decision_context_id", context.get("context_id"))
        self._attach_agent_role_metadata(payload, decision_type, agent_role)
        return payload

    def _attach_agent_role_metadata(self, payload: dict[str, Any], decision_type: str, agent_role: AgentRoleConfig | None) -> None:
        metadata = payload.setdefault("decision_metadata", {})
        metadata["agent_role"] = agent_role.name if agent_role else decision_type
        metadata["agent_role_source"] = "configured" if agent_role else "implicit"
        metadata["agent_role_enabled"] = bool(agent_role and agent_role.enabled)
        metadata["legacy_agent_id"] = agent_role.legacy_agent_id if agent_role else self._openclaw_agent_id(decision_type)
        if agent_role:
            metadata["agent_role_display_name"] = agent_role.display_name
            metadata["agent_role_decision_types"] = agent_role.decision_types
            metadata["agent_role_strict_schema"] = agent_role.strict_schema
            metadata["agent_role_timeout_seconds"] = agent_role.timeout_seconds
            metadata["agent_role_max_retries"] = agent_role.max_retries

    def _metadata_model(self, provider: str, payload: dict[str, Any]) -> str:
        if provider == "openclaw_gateway":
            meta = payload.get("openclaw_gateway_meta") or {}
            return str(meta.get("response_model") or meta.get("request_model") or self.model)
        if provider in REMOTE_PROVIDER_SOURCES:
            return self.model
        return provider

    def _provider_chain(self) -> list[str]:
        provider = self.provider
        normalized_provider = self._normalized_provider_name(provider)
        if normalized_provider == "auto":
            if self._real_provider_configured():
                return ["real_llm", "heuristic", "mock"]
            if self._openclaw_gateway_configured():
                return ["openclaw_gateway", "heuristic", "mock"]
            if self._openclaw_agent_configured():
                return ["openclaw_agent", "heuristic", "mock"]
            return ["heuristic", "mock"]
        if normalized_provider == "legacy_openclaw_gateway":
            return ["openclaw_gateway", "heuristic", "mock"]
        if normalized_provider == "legacy_openclaw_agent":
            return ["openclaw_agent", "heuristic", "mock"]
        if normalized_provider == "real_llm":
            return ["real_llm", "heuristic", "mock"]
        if normalized_provider == "heuristic":
            return ["heuristic", "mock"]
        if normalized_provider == "mock":
            return ["mock"]
        return ["heuristic", "mock"]

    def _real_provider_configured(self) -> bool:
        return bool(self._real_llm_profiles())

    def _openclaw_agent_configured(self) -> bool:
        explicit_provider = self.provider in {"openclaw_agent", "openclaw_cli", "openclaw_internal", "openclaw"}
        planner_env = os.environ.get("FACTOR_LAB_OPENCLAW_PLANNER_AGENT")
        failure_env = os.environ.get("FACTOR_LAB_OPENCLAW_FAILURE_AGENT")
        if not explicit_provider and not (planner_env and failure_env):
            return False
        planner_dir = Path.home() / ".openclaw" / "agents" / self._openclaw_agent_id("planner") / "agent"
        failure_dir = Path.home() / ".openclaw" / "agents" / self._openclaw_agent_id("failure_analyst") / "agent"
        return planner_dir.exists() and failure_dir.exists()

    def _openclaw_gateway_configured(self) -> bool:
        explicit_provider = self.provider in {"auto", "openclaw_gateway", "openclaw_session", "openclaw_http"}
        if not explicit_provider and not os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_URL"):
            return False
        return self._openclaw_agent_configured()

    def healthcheck(self, output_path: str | Path | None = None, *, probe: bool = True) -> dict[str, Any]:
        timeout = float(os.environ.get("FACTOR_LAB_LLM_TIMEOUT_SECONDS") or 20)
        payload: dict[str, Any] = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "configured_provider": self.provider,
            "normalized_provider": self._normalized_provider_name(),
            "provider_class": self._provider_class(),
            "configured_model": self.model,
            "real_provider_configured": self._real_provider_configured(),
            "openclaw_gateway_configured": self._openclaw_gateway_configured(),
            "openclaw_agent_configured": self._openclaw_agent_configured(),
            "gateway_url": self._openclaw_gateway_url(),
            "env": {
                "FACTOR_LAB_DECISION_PROVIDER": bool(os.environ.get("FACTOR_LAB_DECISION_PROVIDER")),
                "FACTOR_LAB_LLM_MODEL": bool(os.environ.get("FACTOR_LAB_LLM_MODEL")),
                "FACTOR_LAB_OPENCLAW_PLANNER_AGENT": bool(os.environ.get("FACTOR_LAB_OPENCLAW_PLANNER_AGENT")),
                "FACTOR_LAB_OPENCLAW_FAILURE_AGENT": bool(os.environ.get("FACTOR_LAB_OPENCLAW_FAILURE_AGENT")),
                "FACTOR_LAB_OPENCLAW_SESSION_PREFIX": bool(os.environ.get("FACTOR_LAB_OPENCLAW_SESSION_PREFIX")),
                "FACTOR_LAB_OPENCLAW_BACKEND_MODEL": bool(os.environ.get("FACTOR_LAB_OPENCLAW_BACKEND_MODEL")),
                "FACTOR_LAB_OPENCLAW_GATEWAY_URL": bool(os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_URL")),
                "FACTOR_LAB_OPENCLAW_GATEWAY_TOKEN": bool(os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_TOKEN") or os.environ.get("OPENCLAW_GATEWAY_TOKEN")),
            },
            "timeout_seconds": timeout,
            "probe": {
                "attempted": False,
                "skipped": False,
                "ok": False,
                "status_code": None,
                "latency_ms": None,
                "error": None,
            },
            "recommended_effective_source": "heuristic",
            "effective_source": "heuristic",
            "degraded_to_heuristic": False,
            "planner_agent": self._openclaw_agent_id("planner"),
            "failure_agent": self._openclaw_agent_id("failure_analyst"),
            "agent_roles": [
                {
                    "name": role.name,
                    "display_name": role.display_name,
                    "enabled": role.enabled,
                    "decision_types": role.decision_types,
                    "llm_fallback_order": role.llm_fallback_order,
                    "legacy_agent_id": role.legacy_agent_id,
                    "strict_schema": role.strict_schema,
                    "timeout_seconds": role.timeout_seconds,
                    "max_retries": role.max_retries,
                }
                for role in load_agent_roles()
            ],
        }
        if self.provider in {"auto", "openclaw_gateway", "openclaw_session", "openclaw_http"} and payload["openclaw_gateway_configured"]:
            payload["recommended_effective_source"] = "legacy_openclaw_gateway"
            if probe:
                payload["probe"] = self._probe_openclaw_gateway(timeout_seconds=timeout)
            else:
                payload["probe"]["skipped"] = True
                payload["probe"]["error"] = "probe_skipped"
            if payload["probe"].get("attempted") and not payload["probe"].get("ok"):
                payload["recommended_effective_source"] = "heuristic"
        elif self.provider in {"openclaw_agent", "openclaw_cli", "openclaw_internal", "openclaw"}:
            payload["recommended_effective_source"] = "legacy_openclaw_agent" if payload["openclaw_agent_configured"] else "heuristic"
        elif self.provider in {"real", "real_llm", "openai", "openai_compatible"} and payload["real_provider_configured"]:
            payload["recommended_effective_source"] = "real_llm"
        payload["effective_source"] = payload["recommended_effective_source"]
        payload["degraded_to_heuristic"] = (
            payload["effective_source"] == "heuristic"
            and self.provider not in {"heuristic", "mock"}
        )
        out = Path(output_path or (artifacts_dir() / "llm_provider_health.json"))
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def _probe_openclaw_gateway(self, timeout_seconds: float) -> dict[str, Any]:
        started = time.perf_counter()
        probe = {
            "attempted": True,
            "ok": False,
            "status_code": None,
            "latency_ms": None,
            "error": None,
        }
        req = urllib.request.Request(
            url=self._openclaw_gateway_health_url(),
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=max(5.0, min(timeout_seconds, 30.0))) as response:
                probe["status_code"] = getattr(response, "status", 200)
                response.read()
                probe["ok"] = 200 <= probe["status_code"] < 300
        except urllib.error.HTTPError as exc:
            probe["status_code"] = exc.code
            body = exc.read().decode("utf-8", errors="ignore")
            probe["error"] = f"http_error:{exc.code}:{body}"
        except Exception as exc:  # pragma: no cover - network dependent
            probe["error"] = str(exc)
        probe["latency_ms"] = int((time.perf_counter() - started) * 1000)
        return probe

    def _parse_json_content(self, content: str) -> dict[str, Any]:
        text = (content or "").strip()
        if not text:
            raise RuntimeError("empty content returned")
        try:
            return json.loads(text)
        except Exception:
            pass
        if text.startswith("```"):
            lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
            fenced = "\n".join(lines).strip()
            try:
                return json.loads(fenced)
            except Exception:
                text = fenced
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise RuntimeError("unable to parse json content")

    def _decision_schema_version(self, decision_type: str) -> str:
        return DECISION_SCHEMA_VERSIONS[decision_type]

    def _decision_schema_hint(self, decision_type: str) -> dict[str, Any]:
        return DECISION_SCHEMA_HINTS[decision_type]

    def _decision_type_is_configured_but_disabled(self, decision_type: str) -> bool:
        normalized = str(decision_type or "").strip()
        return any(normalized in role.decision_types and not role.enabled for role in load_agent_roles())

    def _openclaw_agent_id(self, decision_type: str) -> str:
        role = select_agent_role(decision_type)
        if role and role.legacy_agent_id:
            return role.legacy_agent_id
        if decision_type == "planner":
            return os.environ.get("FACTOR_LAB_OPENCLAW_PLANNER_AGENT") or "factor-lab-planner"
        if decision_type == "failure_analyst":
            return os.environ.get("FACTOR_LAB_OPENCLAW_FAILURE_AGENT") or "factor-lab-failure"
        env_key = f"FACTOR_LAB_OPENCLAW_{decision_type.upper()}_AGENT"
        return os.environ.get(env_key) or f"factor-lab-{decision_type.replace('_', '-')}"

    def _openclaw_session_base(self, decision_type: str) -> str:
        prefix = os.environ.get("FACTOR_LAB_OPENCLAW_SESSION_PREFIX") or "factor-lab-decision"
        suffix = decision_type.replace("_", "-") or "decision"
        return f"{prefix}-{suffix}"

    def _openclaw_session_mode(self, purpose: str = "decision") -> str:
        if purpose == "healthcheck":
            return "ephemeral"
        raw = (os.environ.get("FACTOR_LAB_OPENCLAW_SESSION_MODE") or "ephemeral").strip().lower()
        if raw not in {"ephemeral", "persistent"}:
            return "ephemeral"
        return raw

    def _openclaw_request_scope_id(self, context: dict[str, Any] | None = None, purpose: str = "decision") -> str:
        if purpose == "healthcheck":
            return uuid4().hex[:12]
        compact = self._context_request_scope_id(context)
        if compact:
            return compact
        return f"{int(time.time() * 1000)}-{uuid4().hex[:8]}"

    def _openclaw_session_id(
        self,
        decision_type: str,
        context: dict[str, Any] | None = None,
        purpose: str = "decision",
        request_scope_id: str | None = None,
    ) -> str:
        base = self._openclaw_session_base(decision_type)
        if self._openclaw_session_mode(purpose=purpose) == "persistent":
            return base
        scope_id = request_scope_id or self._openclaw_request_scope_id(context=context, purpose=purpose)
        return f"{base}-{scope_id}"

    def _trim_text(self, value: Any, max_chars: int = 400) -> Any:
        if not isinstance(value, str):
            return value
        text = value.strip()
        if len(text) <= max_chars:
            return text
        return f"{text[: max_chars - 19]}...[truncated {len(text) - max_chars + 19} chars]"

    def _compact_scalar_or_small(self, value: Any, *, max_chars: int = 240, list_limit: int = 5, dict_limit: int = 8) -> Any:
        if isinstance(value, str):
            return self._trim_text(value, max_chars=max_chars)
        if isinstance(value, list):
            compacted = [self._compact_scalar_or_small(item, max_chars=max_chars, list_limit=list_limit, dict_limit=dict_limit) for item in value[:list_limit]]
            if len(value) > list_limit:
                compacted.append({"omitted_count": len(value) - list_limit})
            return compacted
        if isinstance(value, dict):
            compacted = {}
            for key in list(value.keys())[:dict_limit]:
                compacted[key] = self._compact_scalar_or_small(value.get(key), max_chars=max_chars, list_limit=list_limit, dict_limit=dict_limit)
            if len(value) > dict_limit:
                compacted["omitted_key_count"] = len(value) - dict_limit
            return compacted
        return value

    def _summarize_candidate_pool_task(self, row: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key in [
            "task_type",
            "category",
            "priority_hint",
            "reason",
            "goal",
            "hypothesis",
            "branch_id",
            "expected_knowledge_gain",
            "family_focus",
            "representative_scope",
            "dedupe_signature",
        ]:
            if row.get(key) is not None:
                summary[key] = self._compact_scalar_or_small(row.get(key), max_chars=220)
        payload = row.get("payload") or {}
        if isinstance(payload, dict) and payload:
            payload_summary = {}
            for key in [
                "candidate_name",
                "family",
                "opportunity_type",
                "opportunity_id",
                "question_card_id",
                "workflow_name",
                "workflow_type",
                "research_pool",
                "exploration_pool",
                "window_key",
            ]:
                if payload.get(key) is not None:
                    payload_summary[key] = self._compact_scalar_or_small(payload.get(key), max_chars=180)
            if payload_summary:
                summary["payload_summary"] = payload_summary
        return summary

    def _summarize_stable_candidate(self, row: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key in [
            "factor_name",
            "family",
            "latest_recent_final_score",
            "latest_final_score",
            "promotion_decision",
            "quality_classification",
            "net_metric",
            "turnover_mean",
        ]:
            if row.get(key) is not None:
                summary[key] = row.get(key)
        return summary

    def _summarize_failure_dossier(self, row: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key in [
            "candidate_name",
            "family",
            "failure_cluster",
            "primary_failure_pattern",
            "question",
            "recommended_route",
            "action_bias",
        ]:
            if row.get(key) is not None:
                summary[key] = self._compact_scalar_or_small(row.get(key), max_chars=220)
        return summary

    def _summarize_question_card(self, row: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key in [
            "question_id",
            "family",
            "question",
            "failure_pattern",
            "target_pool",
            "route_type",
            "priority",
        ]:
            if row.get(key) is not None:
                summary[key] = self._compact_scalar_or_small(row.get(key), max_chars=220)
        return summary

    def _compact_planner_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        return {
            "latest_run": self._compact_scalar_or_small(inputs.get("latest_run") or {}, max_chars=180),
            "stable_candidates": [
                self._summarize_stable_candidate(row)
                for row in list(inputs.get("stable_candidates") or [])[:6]
                if isinstance(row, dict)
            ],
            "latest_candidates": self._compact_scalar_or_small(inputs.get("latest_candidates") or [], max_chars=160, list_limit=4),
            "latest_graveyard": self._compact_scalar_or_small(inputs.get("latest_graveyard") or [], max_chars=160, list_limit=6),
            "queue_budget": self._compact_scalar_or_small(inputs.get("queue_budget") or {}, max_chars=120),
            "failure_state": self._compact_scalar_or_small(inputs.get("failure_state") or {}, max_chars=160),
            "exploration_state": self._compact_scalar_or_small(inputs.get("exploration_state") or {}, max_chars=160),
            "research_flow_state": self._compact_scalar_or_small(inputs.get("research_flow_state") or {}, max_chars=180),
            "knowledge_gain_counter": self._compact_scalar_or_small(inputs.get("knowledge_gain_counter") or {}, max_chars=160),
            "candidate_pool_task_count": len(list(inputs.get("candidate_pool_tasks") or [])),
            "candidate_pool_tasks": [
                self._summarize_candidate_pool_task(row)
                for row in list(inputs.get("candidate_pool_tasks") or [])[:4]
                if isinstance(row, dict)
            ],
            "candidate_pool_suppressed": self._compact_scalar_or_small(inputs.get("candidate_pool_suppressed") or [], max_chars=180, list_limit=4),
            "branch_selected_families": self._compact_scalar_or_small(inputs.get("branch_selected_families") or [], max_chars=120, list_limit=6),
            "open_questions": self._compact_scalar_or_small(inputs.get("open_questions") or [], max_chars=220, list_limit=6),
            "candidate_hypothesis_cards": self._compact_scalar_or_small(inputs.get("candidate_hypothesis_cards") or [], max_chars=220, list_limit=6),
        }

    def _compact_failure_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        research_learning = inputs.get("research_learning") or {}
        analyst_feedback = inputs.get("analyst_feedback_context") or {}
        return {
            "failure_state": self._compact_scalar_or_small(inputs.get("failure_state") or {}, max_chars=160),
            "research_flow_state": self._compact_scalar_or_small(inputs.get("research_flow_state") or {}, max_chars=180),
            "knowledge_gain_counter": self._compact_scalar_or_small(inputs.get("knowledge_gain_counter") or {}, max_chars=160),
            "recent_failed_or_risky_tasks": self._compact_scalar_or_small(inputs.get("recent_failed_or_risky_tasks") or [], max_chars=180, list_limit=8),
            "open_questions": self._compact_scalar_or_small(inputs.get("open_questions") or [], max_chars=220, list_limit=6),
            "llm_diagnostics": self._compact_scalar_or_small(inputs.get("llm_diagnostics") or {}, max_chars=180),
            "latest_graveyard": self._compact_scalar_or_small(inputs.get("latest_graveyard") or [], max_chars=160, list_limit=6),
            "research_learning": {
                "updated_at_utc": research_learning.get("updated_at_utc"),
                "research_mode": research_learning.get("research_mode"),
                "autonomy_profile": self._compact_scalar_or_small(research_learning.get("autonomy_profile") or {}, max_chars=160),
                "failure_question_cards": [
                    self._summarize_question_card(row)
                    for row in list(research_learning.get("failure_question_cards") or [])[:8]
                    if isinstance(row, dict)
                ],
                "representative_failure_dossiers": [
                    self._summarize_failure_dossier(row)
                    for row in list(research_learning.get("representative_failure_dossiers") or [])[:6]
                    if isinstance(row, dict)
                ],
                "candidate_generation_history_tail": self._compact_scalar_or_small(
                    list(research_learning.get("candidate_generation_history") or [])[-5:],
                    max_chars=180,
                    list_limit=5,
                ),
            },
            "analyst_feedback_context": {
                "strategy_plan_summary": self._compact_scalar_or_small(analyst_feedback.get("strategy_plan_summary") or {}, max_chars=180),
                "injection_summary": self._compact_scalar_or_small(analyst_feedback.get("injection_summary") or {}, max_chars=180),
                "llm_execution_feedback": self._compact_scalar_or_small(analyst_feedback.get("llm_execution_feedback") or {}, max_chars=180),
                "analyst_learning_loop": self._compact_scalar_or_small(analyst_feedback.get("analyst_learning_loop") or {}, max_chars=180),
                "research_flow_state": self._compact_scalar_or_small(analyst_feedback.get("research_flow_state") or {}, max_chars=180),
                "research_memory_tail": self._compact_scalar_or_small(analyst_feedback.get("research_memory_tail") or {}, max_chars=180),
            },
        }

    def _compact_reviewer_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        return {
            "latest_run": self._compact_scalar_or_small(inputs.get("latest_run") or {}, max_chars=180),
            "promotion_scorecard": self._compact_scalar_or_small(inputs.get("promotion_scorecard") or {}, max_chars=220, list_limit=10),
            "candidate_pool": self._compact_scalar_or_small(inputs.get("candidate_pool") or {}, max_chars=220, list_limit=8),
            "research_attribution": self._compact_scalar_or_small(inputs.get("research_attribution") or {}, max_chars=220, list_limit=8),
            "stable_candidates": [
                self._summarize_stable_candidate(row)
                for row in list(inputs.get("stable_candidates") or [])[:8]
                if isinstance(row, dict)
            ],
            "latest_graveyard": self._compact_scalar_or_small(inputs.get("latest_graveyard") or [], max_chars=180, list_limit=8),
        }

    def _compact_data_quality_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        latest_run = inputs.get("latest_run") or {}
        return {
            "task_type": inputs.get("task_type"),
            "task_payload_summary": self._compact_scalar_or_small(inputs.get("task_payload_summary") or {}, max_chars=220),
            "latest_run": self._compact_scalar_or_small(latest_run, max_chars=220),
            "dataset_rows": latest_run.get("dataset_rows") if isinstance(latest_run, dict) else None,
            "last_error": self._trim_text(inputs.get("last_error") or "", max_chars=360),
            "data_source": latest_run.get("data_source") if isinstance(latest_run, dict) else None,
            "config_path": latest_run.get("config_path") if isinstance(latest_run, dict) else None,
            "output_dir": latest_run.get("output_dir") if isinstance(latest_run, dict) else None,
            "provider_health": self._compact_scalar_or_small(inputs.get("provider_health") or {}, max_chars=180),
            "cache_status": self._compact_scalar_or_small(inputs.get("cache_status") or {}, max_chars=180),
        }

    def _compact_context_for_prompt(self, decision_type: str, context: dict[str, Any]) -> dict[str, Any]:
        inputs = context.get("inputs") or {}
        if decision_type == "planner":
            compact_inputs = self._compact_planner_inputs(inputs)
        elif decision_type == "failure_analyst":
            compact_inputs = self._compact_failure_inputs(inputs)
        elif decision_type == "reviewer":
            compact_inputs = self._compact_reviewer_inputs(inputs)
        elif decision_type == "data_quality":
            compact_inputs = self._compact_data_quality_inputs(inputs)
        else:
            compact_inputs = self._compact_scalar_or_small(inputs, max_chars=220)
        return {
            "context_id": context.get("context_id"),
            "decision_type": decision_type,
            "summary": self._compact_scalar_or_small(context.get("summary") or {}, max_chars=200),
            "inputs": compact_inputs,
        }

    def _real_llm_context_mode(self) -> str:
        raw = (
            os.environ.get("FACTOR_LAB_REAL_LLM_CONTEXT_MODE")
            or os.environ.get("FACTOR_LAB_LLM_CONTEXT_MODE")
            or "compact"
        ).strip().lower()
        if raw in {"raw", "full", "full_context"}:
            return "raw"
        return "compact"

    def _real_llm_prompt_payload(
        self,
        decision_type: str,
        context: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        mode = self._real_llm_context_mode()
        raw_context_chars = len(json.dumps(context, ensure_ascii=False))

        if mode == "raw":
            prompt_context = context
        else:
            prompt_context = self._compact_context_for_prompt(decision_type, context)

        prompt_context_chars = len(json.dumps(prompt_context, ensure_ascii=False))
        prompt_meta = {
            "context_mode": mode,
            "raw_context_chars": raw_context_chars,
            "prompt_context_chars": prompt_context_chars,
            "estimated_raw_tokens_4c": raw_context_chars // 4,
            "estimated_prompt_tokens_4c": prompt_context_chars // 4,
            "reduction_ratio": round(
                1.0 - (prompt_context_chars / raw_context_chars),
                4,
            ) if raw_context_chars else 0.0,
        }

        return (
            {
                "decision_type": decision_type,
                "context_mode": mode,
                "context": prompt_context,
                "required_output_schema": self._decision_schema_hint(decision_type),
                "context_compaction": prompt_meta,
            },
            prompt_meta,
        )

    def _openclaw_prompt(self, decision_type: str, context: dict[str, Any]) -> str:
        compact_context = self._compact_context_for_prompt(decision_type, context)
        return (
            "You are the Factor Lab decision layer running as an internal OpenClaw decision session.\n"
            "Output exactly one minified JSON object and nothing else.\n"
            "No markdown, no prose, no explanation, no code fences, no tool use.\n"
            "Ground everything in the provided context summary; do not invent fields.\n"
            f"Decision type: {decision_type}\n"
            f"Required output schema: {json.dumps(self._decision_schema_hint(decision_type), ensure_ascii=False)}\n"
            f"Context summary: {json.dumps(compact_context, ensure_ascii=False)}\n"
        )

    def _extract_payload_text(self, raw: dict[str, Any]) -> str:
        payloads = list(raw.get("payloads") or [])
        for row in payloads:
            text = (row.get("text") or "").strip()
            if not text:
                continue
            try:
                self._parse_json_content(text)
                return text
            except Exception:
                continue
        if payloads:
            return (payloads[0].get("text") or "").strip()
        raise RuntimeError("no payload text returned")

    def _call_openclaw_agent(
        self,
        decision_type: str,
        context: dict[str, Any],
        *,
        session_id: str | None = None,
        request_scope_id: str | None = None,
    ) -> dict[str, Any]:
        agent_id = self._openclaw_agent_id(decision_type)
        session_id = session_id or self._openclaw_session_id(
            decision_type,
            context=context,
            request_scope_id=request_scope_id,
        )
        timeout = int(float(os.environ.get("FACTOR_LAB_LLM_TIMEOUT_SECONDS") or 20))
        thinking = os.environ.get("FACTOR_LAB_OPENCLAW_THINKING") or "low"
        prompt = self._openclaw_prompt(decision_type, context)
        command = [
            "openclaw",
            "agent",
            "--local",
            "--agent",
            agent_id,
            "--session-id",
            session_id,
            "--mode",
            "single_turn",
            "--message",
            prompt,
            "--json",
            "--thinking",
            thinking,
            "--timeout",
            str(max(timeout, 30)),
        ]
        completed = subprocess.run(
            command,
            cwd=str(project_root()),
            capture_output=True,
            text=True,
            timeout=max(timeout, 30) + 30,
            env=os.environ.copy(),
        )
        if completed.returncode != 0:
            raise RuntimeError((completed.stderr or completed.stdout or "openclaw agent failed").strip())
        raw = json.loads((completed.stdout or "{}").strip())
        text = self._extract_payload_text(raw)
        payload = self._parse_json_content(text)
        payload.setdefault("schema_version", self._decision_schema_version(decision_type))
        payload.setdefault("agent_name", f"{decision_type}-openclaw-agent")
        payload.setdefault("decision_source", "openclaw_agent")
        payload.setdefault("decision_context_id", context.get("context_id"))
        payload.setdefault(
            "openclaw_agent_meta",
            {
                "agent_id": agent_id,
                "session_id": session_id,
                "request_scope_id": request_scope_id or self._context_request_scope_id(context),
                "provider": (((raw.get("meta") or {}).get("agentMeta") or {}).get("provider")),
                "model": (((raw.get("meta") or {}).get("agentMeta") or {}).get("model")),
            },
        )
        return payload

    def _openclaw_gateway_url(self) -> str:
        raw = (
            os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_URL")
            or os.environ.get("OPENCLAW_GATEWAY_URL")
            or "http://127.0.0.1:18789/v1/chat/completions"
        ).rstrip("/")
        if raw.endswith("/chat/completions"):
            return raw
        if raw.endswith("/v1"):
            return f"{raw}/chat/completions"
        return f"{raw}/v1/chat/completions"

    def _openclaw_gateway_health_url(self) -> str:
        raw = (
            os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_URL")
            or os.environ.get("OPENCLAW_GATEWAY_URL")
            or "http://127.0.0.1:18789/v1/chat/completions"
        ).rstrip("/")
        if raw.endswith("/chat/completions"):
            raw = raw[: -len("/chat/completions")]
        if raw.endswith("/v1"):
            raw = raw[: -len("/v1")]
        return f"{raw}/readyz"

    def _openclaw_request_model(self, decision_type: str) -> str:
        return f"openclaw/{self._openclaw_agent_id(decision_type)}"

    def _openclaw_gateway_headers(self, session_id: str) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "x-openclaw-session-key": session_id,
            "x-openclaw-message-channel": os.environ.get("FACTOR_LAB_OPENCLAW_MESSAGE_CHANNEL") or "webchat",
        }
        token = os.environ.get("FACTOR_LAB_OPENCLAW_GATEWAY_TOKEN") or os.environ.get("OPENCLAW_GATEWAY_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        backend_model = os.environ.get("FACTOR_LAB_OPENCLAW_BACKEND_MODEL")
        if backend_model:
            headers["x-openclaw-model"] = backend_model
        return headers

    def _call_openclaw_gateway(
        self,
        decision_type: str,
        context: dict[str, Any],
        *,
        session_id: str | None = None,
        request_scope_id: str | None = None,
    ) -> dict[str, Any]:
        session_id = session_id or self._openclaw_session_id(
            decision_type,
            context=context,
            request_scope_id=request_scope_id,
        )
        request_model = self._openclaw_request_model(decision_type)
        prompt = self._openclaw_prompt(decision_type, context)
        request_body = {
            "model": request_model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an internal Factor Lab decision session. Return JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        req = urllib.request.Request(
            url=self._openclaw_gateway_url(),
            data=json.dumps(request_body).encode("utf-8"),
            headers=self._openclaw_gateway_headers(session_id),
            method="POST",
        )
        timeout = float(os.environ.get("FACTOR_LAB_LLM_TIMEOUT_SECONDS") or 20)
        try:
            with urllib.request.urlopen(req, timeout=max(timeout, 30.0)) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:  # pragma: no cover - network dependent
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"http_error:{exc.code}:{body}") from exc
        choices = raw.get("choices") or []
        if not choices:
            raise RuntimeError("no choices returned")
        message = choices[0].get("message") or {}
        content = (message.get("content") or "").strip()
        payload = self._parse_json_content(content)
        payload.setdefault("schema_version", self._decision_schema_version(decision_type))
        payload.setdefault("agent_name", f"{decision_type}-openclaw-gateway")
        payload.setdefault("decision_source", "openclaw_gateway")
        payload.setdefault("decision_context_id", context.get("context_id"))
        payload.setdefault(
            "openclaw_gateway_meta",
            {
                "agent_id": self._openclaw_agent_id(decision_type),
                "session_id": session_id,
                "request_scope_id": request_scope_id or self._context_request_scope_id(context),
                "request_model": request_model,
                "response_model": raw.get("model"),
                "finish_reason": (choices[0].get("finish_reason") or "stop"),
                "backend_model": os.environ.get("FACTOR_LAB_OPENCLAW_BACKEND_MODEL"),
            },
        )
        return payload

    def _real_llm_profiles(self, fallback_order: list[str] | None = None) -> list[dict[str, Any]]:
        raw_profiles = (os.environ.get("FACTOR_LAB_LLM_PROFILES_JSON") or "").strip()
        profiles: list[dict[str, Any]] = []
        if raw_profiles:
            try:
                parsed = json.loads(raw_profiles)
            except Exception:
                parsed = []
            if isinstance(parsed, list):
                for index, item in enumerate(parsed):
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or f"profile-{index + 1}").strip()
                    base_url = str(item.get("base_url") or "").strip().rstrip("/")
                    model = str(item.get("model") or self.model or "").strip()
                    api_key = str(item.get("api_key") or "").strip()
                    enabled = item.get("enabled", True)
                    if isinstance(enabled, str):
                        enabled = enabled.strip().lower() not in {"0", "false", "no", "off"}
                    api_format = str(
                        item.get("api_format")
                        or item.get("format")
                        or ("anthropic" if model.lower().startswith("claude") else "openai")
                    ).strip().lower()
                    if api_format in {"responses", "openai_response"}:
                        api_format = "openai_responses"
                    if api_format not in {"openai", "openai_responses", "anthropic"}:
                        api_format = "openai_responses" if model.lower().startswith("gpt-5") else "openai"
                    if name and base_url and model and api_key and bool(enabled):
                        profiles.append({
                            "name": name,
                            "base_url": base_url,
                            "model": model,
                            "api_key": api_key,
                            "api_format": api_format,
                            "enabled": True,
                        })
        if not profiles:
            base_url = (os.environ.get("FACTOR_LAB_LLM_BASE_URL") or os.environ.get("OPENAI_BASE_URL") or "").strip().rstrip("/")
            api_key = (os.environ.get("FACTOR_LAB_LLM_API_KEY") or os.environ.get("OPENAI_API_KEY") or "").strip()
            model = (os.environ.get("FACTOR_LAB_LLM_MODEL") or os.environ.get("OPENAI_MODEL") or self.model or "").strip()
            if base_url and api_key:
                api_format = (
                    os.environ.get("FACTOR_LAB_LLM_API_FORMAT")
                    or os.environ.get("OPENAI_API_FORMAT")
                    or ("anthropic" if (model or self.model or "").lower().startswith("claude") else "openai")
                ).strip().lower()
                if api_format in {"responses", "openai_response"}:
                    api_format = "openai_responses"
                if api_format not in {"openai", "openai_responses", "anthropic"}:
                    api_format = "openai_responses" if (model or self.model or "").lower().startswith("gpt-5") else "openai"
                profiles.append({
                    "name": os.environ.get("FACTOR_LAB_LLM_PROFILE_NAME") or "default",
                    "base_url": base_url,
                    "model": model or self.model,
                    "api_key": api_key,
                    "api_format": api_format,
                    "enabled": True,
                })
        order = list(fallback_order or [])
        if not order:
            order = [item.strip() for item in (os.environ.get("FACTOR_LAB_LLM_FALLBACK_ORDER") or "").split(",") if item.strip()]
        if not order:
            return profiles
        by_name = {profile["name"]: profile for profile in profiles}
        ordered: list[dict[str, Any]] = [by_name[name] for name in order if name in by_name]
        ordered_names = {profile["name"] for profile in ordered}
        ordered.extend(profile for profile in profiles if profile["name"] not in ordered_names)
        return ordered

    def _agent_system_prompt(self, decision_type: str, agent_role: AgentRoleConfig | None = None) -> str:
        if agent_role and agent_role.system_prompt.strip():
            return agent_role.system_prompt.strip()
        if decision_type == "planner":
            return "你是 Factor Lab 的规划 agent。必须基于输入 snapshot，不得编造指标。输出必须符合 planner schema。"
        if decision_type == "failure_analyst":
            return "你是 Factor Lab 的失败诊断 agent。优先定位根因：数据、配置、模型、schema、回测、候选质量。输出必须符合 failure_analyst schema。"
        return (
            "You are the Factor Lab decision layer. Respond with JSON only. "
            "Do not include markdown fences. Ground all decisions in the provided context. "
            "Keep execution/numeric logic deterministic and only output the requested decision object."
        )

    def _real_llm_headers(self, api_key: str, *, auth_scheme: str = "bearer") -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": os.environ.get("FACTOR_LAB_LLM_USER_AGENT") or "OpenAI/Python 1.0 FactorLab/1.0",
        }
        if auth_scheme == "anthropic":
            headers["x-api-key"] = api_key
            headers["anthropic-version"] = os.environ.get("FACTOR_LAB_ANTHROPIC_VERSION") or "2023-06-01"
        else:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _coerce_int_or_none(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _extract_llm_usage(self, raw: dict[str, Any], api_format: str) -> dict[str, Any]:
        usage = raw.get("usage") or {}
        if not isinstance(usage, dict):
            usage = {}
        prompt_tokens = self._coerce_int_or_none(usage.get("prompt_tokens") or usage.get("input_tokens"))
        completion_tokens = self._coerce_int_or_none(usage.get("completion_tokens") or usage.get("output_tokens"))
        total_tokens = self._coerce_int_or_none(usage.get("total_tokens"))
        if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
            total_tokens = prompt_tokens + completion_tokens
        if prompt_tokens is None and completion_tokens is None and total_tokens is None:
            source = "missing"
        elif prompt_tokens is None or completion_tokens is None or total_tokens is None:
            source = "partial"
        else:
            source = "provider"
        return {
            "api_format": api_format,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "usage_source": source,
            "raw_usage": usage,
        }

    def _llm_usage_ledger_path(self) -> Path:
        return artifacts_dir() / "llm_usage_ledger.jsonl"

    def _append_llm_usage_ledger(self, row: dict[str, Any]) -> None:
        path = self._llm_usage_ledger_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    def _try_append_llm_usage_ledger(self, row: dict[str, Any]) -> None:
        try:
            self._append_llm_usage_ledger(row)
        except Exception:
            return

    def _build_llm_usage_ledger_row(
        self,
        *,
        success: bool,
        decision_type: str,
        profile: dict[str, Any],
        api_format: str,
        prompt_meta: dict[str, Any],
        usage: dict[str, Any],
        latency_ms: int,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        return {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "success": success,
            "decision_type": decision_type,
            "provider": "real_llm",
            "profile_name": profile.get("name") or "default",
            "model": profile.get("model") or self.model,
            "base_url": profile.get("base_url"),
            "api_format": api_format,
            "context_mode": prompt_meta.get("context_mode"),
            "raw_context_chars": prompt_meta.get("raw_context_chars"),
            "prompt_context_chars": prompt_meta.get("prompt_context_chars"),
            "user_prompt_chars": prompt_meta.get("user_prompt_chars"),
            "estimated_user_prompt_tokens_4c": prompt_meta.get("estimated_user_prompt_tokens_4c"),
            "usage": {
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "usage_source": usage.get("usage_source"),
            },
            "latency_ms": latency_ms,
            "error_type": error_type,
            "error_message": error_message,
        }

    def _usage_from_http_error_body(self, body: str, api_format: str) -> dict[str, Any]:
        try:
            raw = json.loads(body)
        except Exception:
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        return self._extract_llm_usage(raw, api_format)

    def _record_real_llm_http_error(
        self,
        *,
        exc: urllib.error.HTTPError,
        body: str,
        decision_type: str,
        profile: dict[str, Any],
        api_format: str,
        prompt_meta: dict[str, Any],
        started: float,
    ) -> None:
        usage = self._usage_from_http_error_body(body, api_format)
        self._try_append_llm_usage_ledger(
            self._build_llm_usage_ledger_row(
                success=False,
                decision_type=decision_type,
                profile=profile,
                api_format=api_format,
                prompt_meta=prompt_meta,
                usage=usage,
                latency_ms=int((time.perf_counter() - started) * 1000),
                error_type=f"http_error:{exc.code}",
                error_message=body[:1000],
            )
        )

    def _real_llm_endpoint_url(self, base_url: str, api_format: str) -> str:
        raw = base_url.rstrip("/")
        if api_format == "anthropic":
            if raw.endswith("/messages"):
                return raw
            if raw.endswith("/v1"):
                return f"{raw}/messages"
            return f"{raw}/v1/messages"
        if api_format == "openai_responses":
            if raw.endswith("/responses"):
                return raw
            if raw.endswith("/v1"):
                return f"{raw}/responses"
            return f"{raw}/v1/responses"
        if raw.endswith("/chat/completions"):
            return raw
        if raw.endswith("/v1"):
            return f"{raw}/chat/completions"
        return f"{raw}/v1/chat/completions"

    def _call_real_llm_profile(
        self,
        decision_type: str,
        context: dict[str, Any],
        profile: dict[str, Any],
        agent_role: AgentRoleConfig | None = None,
    ) -> dict[str, Any]:
        base_url = str(profile.get("base_url") or "").rstrip("/")
        api_key = str(profile.get("api_key") or "")
        model = str(profile.get("model") or self.model)
        api_format = str(
            profile.get("api_format")
            or profile.get("format")
            or ("anthropic" if model.lower().startswith("claude") else "openai")
        ).strip().lower()
        if api_format in {"responses", "openai_response"}:
            api_format = "openai_responses"
        if api_format not in {"openai", "openai_responses", "anthropic"}:
            api_format = "openai_responses" if model.lower().startswith("gpt-5") else "openai"
        if not base_url or not api_key:
            raise RuntimeError("real provider profile not configured")

        system_prompt = self._agent_system_prompt(decision_type, agent_role)
        prompt_payload, prompt_meta = self._real_llm_prompt_payload(decision_type, context)
        user_prompt = json.dumps(prompt_payload, ensure_ascii=False)
        prompt_meta = dict(prompt_meta)
        prompt_meta.update({
            "user_prompt_chars": len(user_prompt),
            "estimated_user_prompt_tokens_4c": len(user_prompt) // 4,
        })
        started = time.perf_counter()
        timeout = float(agent_role.timeout_seconds if agent_role else (os.environ.get("FACTOR_LAB_LLM_TIMEOUT_SECONDS") or 20))
        if api_format == "anthropic":
            request_body = {
                "model": model,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
                "temperature": 0.1,
                "max_tokens": int(float(os.environ.get("FACTOR_LAB_LLM_MAX_TOKENS") or 2048)),
            }
            req = urllib.request.Request(
                url=self._real_llm_endpoint_url(base_url, api_format),
                data=json.dumps(request_body).encode("utf-8"),
                headers=self._real_llm_headers(api_key, auth_scheme="anthropic"),
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    raw = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:  # pragma: no cover - network dependent
                body = exc.read().decode("utf-8", errors="ignore")
                self._record_real_llm_http_error(
                    exc=exc,
                    body=body,
                    decision_type=decision_type,
                    profile=profile,
                    api_format=api_format,
                    prompt_meta=prompt_meta,
                    started=started,
                )
                raise RuntimeError(f"http_error:{exc.code}:{body}") from exc
            content_items = raw.get("content") or []
            content = "".join(str(item.get("text") or "") for item in content_items if isinstance(item, dict)).strip()
        elif api_format == "openai_responses":
            request_body = {
                "model": model,
                "input": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
            }
            req = urllib.request.Request(
                url=self._real_llm_endpoint_url(base_url, api_format),
                data=json.dumps(request_body).encode("utf-8"),
                headers=self._real_llm_headers(api_key),
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    raw = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:  # pragma: no cover - network dependent
                body = exc.read().decode("utf-8", errors="ignore")
                self._record_real_llm_http_error(
                    exc=exc,
                    body=body,
                    decision_type=decision_type,
                    profile=profile,
                    api_format=api_format,
                    prompt_meta=prompt_meta,
                    started=started,
                )
                raise RuntimeError(f"http_error:{exc.code}:{body}") from exc
            output_items = raw.get("output") or []
            content_parts: list[str] = []
            for output_item in output_items:
                if not isinstance(output_item, dict):
                    continue
                for item in output_item.get("content") or []:
                    if isinstance(item, dict):
                        content_parts.append(str(item.get("text") or ""))
            content = "".join(content_parts).strip()
        else:
            request_body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
            }
            req = urllib.request.Request(
                url=self._real_llm_endpoint_url(base_url, api_format),
                data=json.dumps(request_body).encode("utf-8"),
                headers=self._real_llm_headers(api_key),
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    raw = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:  # pragma: no cover - network dependent
                body = exc.read().decode("utf-8", errors="ignore")
                self._record_real_llm_http_error(
                    exc=exc,
                    body=body,
                    decision_type=decision_type,
                    profile=profile,
                    api_format=api_format,
                    prompt_meta=prompt_meta,
                    started=started,
                )
                raise RuntimeError(f"http_error:{exc.code}:{body}") from exc
            choices = raw.get("choices") or []
            if not choices:
                raise RuntimeError("no choices returned")
            content = ((choices[0].get("message") or {}).get("content") or "").strip()
        if not content:
            raise RuntimeError("no content returned")
        usage = self._extract_llm_usage(raw, api_format)
        latency_ms = int((time.perf_counter() - started) * 1000)
        payload = self._parse_json_content(content)
        payload.setdefault("schema_version", self._decision_schema_version(decision_type))
        payload.setdefault("agent_name", f"{decision_type}-real-llm")
        payload.setdefault("decision_source", "real_llm")
        payload.setdefault("decision_context_id", context.get("context_id"))
        payload.setdefault("real_llm_prompt_meta", prompt_meta)
        payload.setdefault("real_llm_usage", usage)
        self._try_append_llm_usage_ledger(
            self._build_llm_usage_ledger_row(
                success=True,
                decision_type=decision_type,
                profile=profile,
                api_format=api_format,
                prompt_meta=prompt_meta,
                usage=usage,
                latency_ms=latency_ms,
            )
        )
        return payload

    def _call_real_llm(
        self,
        decision_type: str,
        context: dict[str, Any],
        agent_role: AgentRoleConfig | None = None,
    ) -> dict[str, Any]:
        agent_role = agent_role or select_agent_role(decision_type)
        profiles = self._real_llm_profiles(agent_role.llm_fallback_order if agent_role else None)
        if not profiles:
            raise RuntimeError("real provider not configured")
        attempts: list[str] = []
        errors: dict[str, str] = {}
        last_error: Exception | None = None
        retry_count = max(0, agent_role.max_retries if agent_role else 0)
        for profile in profiles:
            name = str(profile.get("name") or "default")
            for attempt_index in range(retry_count + 1):
                attempt_name = name if attempt_index == 0 else f"{name}#retry{attempt_index}"
                attempts.append(attempt_name)
                try:
                    payload = self._call_real_llm_profile(decision_type, context, profile, agent_role=agent_role)
                    payload.setdefault("real_llm_profile", {})
                    payload["real_llm_profile"].update({
                        "name": name,
                        "model": profile.get("model"),
                        "base_url": profile.get("base_url"),
                        "fallback_attempts": attempts,
                        "fallback_errors": errors,
                    })
                    return payload
                except Exception as exc:
                    errors[attempt_name] = str(exc)
                    last_error = exc
                    continue
        raise RuntimeError(f"all real_llm profiles failed: {errors}") from last_error
