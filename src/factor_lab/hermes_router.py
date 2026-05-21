from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from factor_lab.hermes_profiles import canonical_profile_key, get_hermes_profile_spec
from factor_lab.paths import artifacts_dir


@dataclass(frozen=True)
class HermesRoute:
    request_id: str
    profile_key: str
    profile_name: str
    session_name: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    briefing_path: Path
    response_path: Path
    migration_alias_used: bool = False
    input_event_key: str | None = None


class HermesRouter:
    def __init__(self, profile_map: Mapping[str, str] | None = None, artifact_dir: str | Path | None = None):
        self.profile_map = {str(k): str(v) for k, v in (profile_map or {}).items()}
        self.artifact_dir = Path(artifact_dir) if artifact_dir is not None else artifacts_dir() / "hermes"

    def route(self, event_key: str, context: Mapping[str, Any] | None = None) -> HermesRoute:
        profile_key, alias_used = canonical_profile_key(event_key)
        spec = get_hermes_profile_spec(profile_key)
        if spec is None:
            raise ValueError(f"unsupported Hermes profile key: {profile_key}")
        request_id = self._request_id(profile_key, context or {})
        profile_name = self.profile_map.get(profile_key, spec.profile)
        return HermesRoute(
            request_id=request_id,
            profile_key=profile_key,
            profile_name=profile_name,
            session_name=spec.session,
            toolsets=spec.toolsets,
            skills=spec.skills,
            briefing_path=self.artifact_dir / "briefings" / spec.artifact_namespace / f"{request_id}.json",
            response_path=self.artifact_dir / "responses" / spec.artifact_namespace / f"{request_id}.json",
            migration_alias_used=alias_used,
            input_event_key=str(event_key or "").strip() if alias_used else None,
        )

    def _request_id(self, profile_key: str, context: Mapping[str, Any]) -> str:
        raw = str(context.get("request_id") or context.get("context_id") or "").strip()
        if raw:
            safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in raw).strip("-")
            if safe:
                return safe[:96]
        return f"{profile_key}-{uuid4().hex[:12]}"
