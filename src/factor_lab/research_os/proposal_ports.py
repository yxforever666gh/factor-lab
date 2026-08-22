"""Narrow model adapters for monthly hypothesis and typed Factor DSL proposals.

Production defaults to a credential-reference-backed direct model.  The model
sees only fixed Family metadata and may return only ``preregistration`` plus a
typed ``factor`` graph; the deterministic coordinator remains the sole
admission, budget, execution, and promotion authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlparse

import requests

from .credentials import CredentialResolutionError, resolve_credential_ref
from .fingerprint import canonical_json, content_fingerprint
from .proposals import HypothesisProposalPort


class ProposalPortError(RuntimeError):
    """A model proposal could not be obtained without weakening authority."""


@dataclass(frozen=True)
class DirectModelProfile:
    name: str
    base_url: str
    model: str
    api_format: str
    credential_ref: str
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.model.strip():
            raise ValueError("direct-model profile requires name and model")
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("production direct-model base_url must use HTTPS")
        if self.api_format not in {"openai", "openai_responses", "anthropic"}:
            raise ValueError("unsupported direct-model api_format")
        if not self.credential_ref.startswith("secret://"):
            raise ValueError("direct-model credential_ref must use secret://")
        if not 1.0 <= float(self.timeout_seconds) <= 120.0:
            raise ValueError("direct-model timeout must be between 1 and 120 seconds")

    @property
    def public_identity(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "model": self.model,
            "api_format": self.api_format,
            "credential_ref_hash": content_fingerprint(
                {"credential_ref": self.credential_ref},
                domain="factor-lab/research-os/v1/proposal-credential-ref",
            ),
            "timeout_seconds": float(self.timeout_seconds),
        }


def _normalise_format(value: Any, model: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"responses", "openai_response"}:
        raw = "openai_responses"
    if raw in {"chat", "chat_completions", "openai_chat"}:
        raw = "openai"
    if not raw:
        raw = "anthropic" if model.lower().startswith("claude") else "openai_responses"
    return raw


def _configured_profile(
    env: Mapping[str, str], *, profile_name: str | None, timeout_seconds: float
) -> DirectModelProfile:
    raw_profiles = str(env.get("FACTOR_LAB_LLM_PROFILES_JSON") or "").strip()
    profiles: list[Mapping[str, Any]] = []
    if raw_profiles:
        try:
            decoded = json.loads(raw_profiles)
        except json.JSONDecodeError as exc:
            raise ProposalPortError("LLM profile registry is invalid JSON") from exc
        if not isinstance(decoded, list):
            raise ProposalPortError("LLM profile registry must be a list")
        if any(not isinstance(item, Mapping) for item in decoded):
            raise ProposalPortError("LLM profile registry entries must be objects")
        profiles = [item for item in decoded if bool(item.get("enabled", True))]
    if profiles:
        by_name = {str(item.get("name") or ""): item for item in profiles}
        if len(by_name) != len(profiles) or "" in by_name:
            raise ProposalPortError("LLM profile names must be unique and non-empty")
        order = [
            item.strip()
            for item in str(env.get("FACTOR_LAB_LLM_FALLBACK_ORDER") or "").split(",")
            if item.strip()
        ]
        selected_name = str(profile_name or "").strip()
        if not selected_name:
            selected_name = next(
                (name for name in order if name in by_name),
                sorted(by_name)[0],
            )
        profile = by_name.get(selected_name)
        if profile is None:
            raise ProposalPortError(
                f"configured direct-model profile {selected_name!r} is unavailable"
            )
        if str(profile.get("api_key") or "").strip():
            raise ProposalPortError("direct-model profile embeds a raw credential")
        model = str(profile.get("model") or "").strip()
        try:
            return DirectModelProfile(
                name=selected_name,
                base_url=str(profile.get("base_url") or "").strip().rstrip("/"),
                model=model,
                api_format=_normalise_format(profile.get("api_format"), model),
                credential_ref=str(profile.get("credential_ref") or "").strip(),
                timeout_seconds=timeout_seconds,
            )
        except ValueError as exc:
            raise ProposalPortError(str(exc)) from exc

    if str(env.get("FACTOR_LAB_LLM_API_KEY") or "").strip():
        raise ProposalPortError("raw FACTOR_LAB_LLM_API_KEY is forbidden in production")
    model = str(env.get("FACTOR_LAB_LLM_MODEL") or "").strip()
    try:
        return DirectModelProfile(
            name=str(env.get("FACTOR_LAB_LLM_PROFILE_NAME") or "default"),
            base_url=str(env.get("FACTOR_LAB_LLM_BASE_URL") or "").strip().rstrip("/"),
            model=model,
            api_format=_normalise_format(env.get("FACTOR_LAB_LLM_API_FORMAT"), model),
            credential_ref=str(env.get("FACTOR_LAB_LLM_API_KEY_REF") or "").strip(),
            timeout_seconds=timeout_seconds,
        )
    except ValueError as exc:
        raise ProposalPortError(str(exc)) from exc


class DirectModelProposalPort(HypothesisProposalPort):
    """OpenAI/Anthropic-compatible proposal-only model boundary."""

    def __init__(
        self,
        profile: DirectModelProfile,
        *,
        env: Mapping[str, str],
        session: requests.Session | None = None,
    ) -> None:
        self.profile = profile
        try:
            self._api_key = resolve_credential_ref(
                profile.credential_ref,
                env=env,
                secrets_root=(
                    str(env.get("FACTOR_LAB_SECRETS_ROOT") or "").strip()
                    or str(env.get("FACTOR_LAB_SECRETS_DIR") or "").strip()
                    or None
                ),
                allow_plain_env=False,
            )
        except CredentialResolutionError as exc:
            raise ProposalPortError(
                "direct-model credential_ref could not be resolved"
            ) from exc
        self._session = session or requests.Session()

    @classmethod
    def from_environment(
        cls,
        env: Mapping[str, str],
        *,
        profile_name: str | None = None,
        timeout_seconds: float = 30.0,
        session: requests.Session | None = None,
    ) -> "DirectModelProposalPort":
        return cls(
            _configured_profile(
                env,
                profile_name=profile_name,
                timeout_seconds=float(timeout_seconds),
            ),
            env=env,
            session=session,
        )

    @property
    def public_identity(self) -> Mapping[str, Any]:
        return self.profile.public_identity

    @staticmethod
    def _prompt(context: Mapping[str, Any]) -> str:
        allowed = {
            "schema_version": context.get("schema_version"),
            "family_id": context.get("family_id"),
            "mechanism_key": context.get("mechanism_key"),
            "allowed_fields": list(context.get("allowed_fields") or ()),
            "field_registry_hash": context.get("field_registry_hash"),
        }
        return (
            "Propose exactly one falsifiable A-share cross-sectional hypothesis. "
            "Return one JSON object with exactly two top-level keys: "
            "preregistration and factor. The factor must use the typed "
            "research-os/factor-dsl/v1 DAG and only allowed_fields. Never return "
            "metrics, Sharpe, p-values, promotion, weights, positions, orders, or "
            "execution decisions. The deterministic research system will reject "
            "anything outside this proposal-only authority. Context: "
            + canonical_json(allowed)
        )

    @staticmethod
    def _text_from_response(api_format: str, payload: Mapping[str, Any]) -> str:
        if api_format == "anthropic":
            content = payload.get("content")
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, Mapping) and item.get("type") == "text":
                        return str(item.get("text") or "")
        elif api_format == "openai":
            choices = payload.get("choices")
            if isinstance(choices, list) and choices and isinstance(choices[0], Mapping):
                message = choices[0].get("message")
                if isinstance(message, Mapping):
                    return str(message.get("content") or "")
        else:
            if isinstance(payload.get("output_text"), str):
                return str(payload["output_text"])
            output = payload.get("output")
            if isinstance(output, list):
                for item in output:
                    if not isinstance(item, Mapping):
                        continue
                    content = item.get("content")
                    if not isinstance(content, list):
                        continue
                    for block in content:
                        if isinstance(block, Mapping) and block.get("type") in {
                            "output_text",
                            "text",
                        }:
                            return str(block.get("text") or "")
        raise ProposalPortError("direct-model response has no proposal text")

    def propose(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        prompt = self._prompt(context)
        base = self.profile.base_url.rstrip("/")
        headers = {"Content-Type": "application/json"}
        if self.profile.api_format == "anthropic":
            endpoint = f"{base}/messages"
            headers.update(
                {"x-api-key": self._api_key, "anthropic-version": "2023-06-01"}
            )
            request_payload = {
                "model": self.profile.model,
                "max_tokens": 2_048,
                "messages": [{"role": "user", "content": prompt}],
            }
        elif self.profile.api_format == "openai":
            endpoint = f"{base}/chat/completions"
            headers["Authorization"] = f"Bearer {self._api_key}"
            request_payload = {
                "model": self.profile.model,
                "messages": [
                    {"role": "system", "content": "Return strict JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "response_format": {"type": "json_object"},
            }
        else:
            endpoint = f"{base}/responses"
            headers["Authorization"] = f"Bearer {self._api_key}"
            request_payload = {
                "model": self.profile.model,
                "input": [
                    {"role": "system", "content": "Return strict JSON only."},
                    {"role": "user", "content": prompt},
                ],
            }
        try:
            response = self._session.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=float(self.profile.timeout_seconds),
            )
            response.raise_for_status()
            raw_response = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise ProposalPortError(
                f"direct-model proposal request failed ({type(exc).__name__})"
            ) from exc
        if not isinstance(raw_response, Mapping):
            raise ProposalPortError("direct-model response must be an object")
        text = self._text_from_response(self.profile.api_format, raw_response)
        if len(text.encode("utf-8")) > 1_000_000:
            raise ProposalPortError("direct-model proposal exceeds the size limit")
        try:
            proposal = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProposalPortError("direct-model proposal is not strict JSON") from exc
        if not isinstance(proposal, Mapping):
            raise ProposalPortError("direct-model proposal must be a JSON object")
        return dict(proposal)


class TestHeuristicProposalPort(HypothesisProposalPort):
    """Deterministic adapter available only to explicit test/legacy callers."""

    def propose(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        family_id = str(context.get("family_id") or "")
        fields = tuple(map(str, context.get("allowed_fields") or ()))
        if not family_id or not fields:
            raise ProposalPortError("test heuristic requires family and allowed fields")
        field = sorted(fields)[0]
        return {
            "preregistration": {
                "hypothesis_id": f"test-heuristic-{family_id}-{field}",
                "economic_mechanism": f"test-only heuristic over {field}",
                "direction": "positive",
                "falsification_criteria": ["outer OOS active return is non-positive"],
                "stop_rules": ["stop after the frozen confirmatory protocol"],
            },
            "factor": {
                "factor_id": f"test-heuristic-{family_id}-{field}",
                "family": family_id,
                "name": f"Test heuristic {field}",
                "mechanism": f"test-only heuristic over {field}",
                "expression": {
                    "schema_version": "research-os/factor-dsl/v1",
                    "output_id": "ranked",
                    "nodes": [
                        {"id": "raw", "op": "field", "field": field},
                        {"id": "ranked", "op": "rank", "input": "raw"},
                    ],
                },
                "direction": "higher_is_better",
                "falsification_criteria": ["outer OOS active return is non-positive"],
            },
        }


class MockProposalPort(HypothesisProposalPort):
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)

    def propose(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        del context
        return dict(self.payload)


def proposal_port_from_config(
    config: Mapping[str, Any] | None,
    *,
    env: Mapping[str, str],
    production: bool,
    session: requests.Session | None = None,
) -> HypothesisProposalPort:
    values = dict(config or {})
    provider = str(values.get("provider") or "direct_model").strip().lower()
    if provider == "direct_model":
        return DirectModelProposalPort.from_environment(
            env,
            profile_name=(
                None if not values.get("profile_name") else str(values["profile_name"])
            ),
            timeout_seconds=float(values.get("timeout_seconds", 30.0)),
            session=session,
        )
    if production:
        raise ProposalPortError(
            "production monthly proposals require provider=direct_model"
        )
    if provider == "heuristic":
        return TestHeuristicProposalPort()
    if provider == "mock":
        payload = values.get("payload")
        if not isinstance(payload, Mapping):
            raise ProposalPortError("test mock proposal requires an object payload")
        return MockProposalPort(payload)
    raise ProposalPortError(f"unknown monthly proposal provider {provider!r}")


__all__ = [
    "DirectModelProfile",
    "DirectModelProposalPort",
    "MockProposalPort",
    "ProposalPortError",
    "TestHeuristicProposalPort",
    "proposal_port_from_config",
]
