from scripts.enable_hermes_native import desired_env_values


def test_only_new_hermes_env_keys_are_emitted():
    values = desired_env_values()
    assert values["FACTOR_LAB_AGENT_BACKEND"] == "hermes"
    assert values["FACTOR_LAB_HERMES_MODE"] == "native"
    forbidden = {
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
        "FACTOR_LAB_DECISION_PROVIDER",
        "FACTOR_LAB_LIVE_DECISION_PROVIDER",
        "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER",
    }
    assert forbidden.isdisjoint(values)
