import json

from factor_lab.agent_roles import (
    AgentRoleConfig,
    agent_roles_to_json,
    load_agent_roles,
    select_agent_role,
)


def test_load_agent_roles_uses_defaults_when_env_missing(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "nowcoding,ai-continue,ccvibe")

    roles = load_agent_roles()

    assert [r.name for r in roles] == ["planner", "failure_analyst", "reviewer", "data_quality"]
    assert roles[0].llm_fallback_order == ["nowcoding", "ai-continue", "ccvibe"]
    assert roles[1].enabled is True
    assert roles[0].legacy_agent_id == "factor-lab-planner"
    assert roles[1].legacy_agent_id == "factor-lab-failure"
    assert roles[2].display_name == "质量复核 Agent"
    assert roles[3].display_name == "数据质量 Agent"
    assert roles[2].llm_fallback_order == ["nowcoding", "ai-continue", "ccvibe"]
    assert roles[3].llm_fallback_order == ["nowcoding", "ai-continue", "ccvibe"]


def test_select_agent_role_by_decision_type(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps(
            [
                {
                    "name": "planner",
                    "enabled": True,
                    "decision_types": ["planner"],
                    "llm_fallback_order": ["ccvibe"],
                },
                {
                    "name": "failure_analyst",
                    "enabled": True,
                    "decision_types": ["failure_analyst"],
                    "llm_fallback_order": ["nowcoding"],
                },
            ]
        ),
    )

    assert select_agent_role("planner").name == "planner"
    assert select_agent_role("failure_analyst").name == "failure_analyst"


def test_disabled_agent_role_is_not_selected(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps(
            [
                {
                    "name": "planner",
                    "enabled": False,
                    "decision_types": ["planner"],
                    "llm_fallback_order": ["ccvibe"],
                }
            ]
        ),
    )

    assert select_agent_role("planner") is None


def test_agent_roles_round_trip_to_json():
    roles = [
        AgentRoleConfig(
            name="planner",
            display_name="规划 Agent",
            enabled=True,
            decision_types=["planner"],
            purpose="plan",
            system_prompt="prompt",
            llm_fallback_order=["ccvibe", "nowcoding"],
            timeout_seconds=45,
            max_retries=2,
            strict_schema=True,
            legacy_agent_id="factor-lab-planner",
        )
    ]

    payload = json.loads(agent_roles_to_json(roles))

    assert payload[0]["name"] == "planner"
    assert payload[0]["llm_fallback_order"] == ["ccvibe", "nowcoding"]
    assert payload[0]["legacy_agent_id"] == "factor-lab-planner"


def test_invalid_roles_json_falls_back_to_defaults(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_AGENT_ROLES_JSON", "not json")
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "nowcoding")

    roles = load_agent_roles()

    assert [r.name for r in roles] == ["planner", "failure_analyst", "reviewer", "data_quality"]
    assert roles[0].llm_fallback_order == ["nowcoding"]
