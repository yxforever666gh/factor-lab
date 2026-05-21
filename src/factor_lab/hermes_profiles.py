from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class HermesProfileSpec:
    key: str
    profile: str
    purpose: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    session: str
    artifact_namespace: str


HERMES_PROFILE_SPECS: dict[str, HermesProfileSpec] = {
    "researcher": HermesProfileSpec(
        key="researcher",
        profile="factor-lab-researcher",
        purpose="Create and prioritize Factor Lab research proposals.",
        toolsets=("file", "terminal", "skills", "session_search"),
        skills=("factor-lab",),
        session="factor-lab-researcher-main",
        artifact_namespace="researcher",
    ),
    "diagnostician": HermesProfileSpec(
        key="diagnostician",
        profile="factor-lab-diagnostician",
        purpose="Diagnose failed Factor Lab runs and propose repairs.",
        toolsets=("file", "terminal", "skills", "session_search"),
        skills=("factor-lab",),
        session="factor-lab-diagnostician-main",
        artifact_namespace="diagnostician",
    ),
    "reviewer": HermesProfileSpec(
        key="reviewer",
        profile="factor-lab-reviewer",
        purpose="Review Factor Lab candidates before promotion.",
        toolsets=("file", "terminal", "skills"),
        skills=("factor-lab",),
        session="factor-lab-reviewer-main",
        artifact_namespace="reviewer",
    ),
    "data_steward": HermesProfileSpec(
        key="data_steward",
        profile="factor-lab-data-steward",
        purpose="Check Factor Lab data availability, quality, and blockers.",
        toolsets=("file", "terminal", "skills"),
        skills=("factor-lab",),
        session="factor-lab-data-steward-main",
        artifact_namespace="data_steward",
    ),
}

# TODO(2026-06-30): remove after all persisted queues use canonical Hermes profile keys.
_LEGACY_EVENT_KEY_MAP = {
    "planner": "researcher",
    "diagnostician": "diagnostician",
    "data_steward": "data_steward",
}


def translate_legacy_event_key(event_key: str) -> str | None:
    return _LEGACY_EVENT_KEY_MAP.get(str(event_key or "").strip())


def canonical_profile_key(event_key: str) -> tuple[str, bool]:
    raw = str(event_key or "").strip()
    if raw in HERMES_PROFILE_SPECS:
        return raw, False
    translated = translate_legacy_event_key(raw)
    if translated:
        return translated, True
    raise ValueError(f"unsupported Hermes profile event key: {event_key}")


def get_hermes_profile_spec(key: str) -> HermesProfileSpec | None:
    return HERMES_PROFILE_SPECS.get(str(key or "").strip())


def hermes_profiles_config() -> dict[str, object]:
    return {
        "profiles": {
            spec.profile: {
                "workdir": "/home/admin/factor-lab",
                "toolsets": list(spec.toolsets),
                "skills": list(spec.skills),
            }
            for spec in HERMES_PROFILE_SPECS.values()
        }
    }


def hermes_profiles_to_json() -> str:
    return json.dumps(hermes_profiles_config(), ensure_ascii=False, indent=2)


def specs_asdict() -> list[dict[str, object]]:
    return [asdict(spec) for spec in HERMES_PROFILE_SPECS.values()]
