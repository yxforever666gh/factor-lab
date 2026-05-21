import json

from factor_lab.hermes_profiles import (
    HERMES_PROFILE_SPECS,
    HermesProfileSpec,
    get_hermes_profile_spec,
    hermes_profiles_to_json,
    translate_legacy_event_key,
)


def test_canonical_hermes_profiles_are_exact_four():
    assert list(HERMES_PROFILE_SPECS) == ["researcher", "diagnostician", "reviewer", "data_steward"]
    assert all(isinstance(spec, HermesProfileSpec) for spec in HERMES_PROFILE_SPECS.values())
    assert [spec.profile for spec in HERMES_PROFILE_SPECS.values()] == [
        "factor-lab-researcher",
        "factor-lab-diagnostician",
        "factor-lab-reviewer",
        "factor-lab-data-steward",
    ]


def test_profile_specs_have_hermes_runtime_shape():
    researcher = get_hermes_profile_spec("researcher")
    assert researcher.toolsets == ("file", "terminal", "skills", "session_search")
    assert researcher.skills == ("factor-lab",)
    assert researcher.session == "factor-lab-researcher-main"
    assert researcher.artifact_namespace == "researcher"


def test_legacy_translation_is_explicit_only():
    assert translate_legacy_event_key("planner") == "researcher"
    assert translate_legacy_event_key("diagnostician") == "diagnostician"
    assert translate_legacy_event_key("data_steward") == "data_steward"
    assert get_hermes_profile_spec("planner") is None


def test_hermes_profiles_json_uses_public_profile_names_only():
    payload = json.loads(hermes_profiles_to_json())
    assert sorted(payload["profiles"]) == [
        "factor-lab-data-steward",
        "factor-lab-diagnostician",
        "factor-lab-researcher",
        "factor-lab-reviewer",
    ]
    dumped = json.dumps(payload)
    assert "planner" not in dumped
    assert ("failure" + "_analyst") not in dumped
    assert "data_steward" not in dumped
