from scripts.enable_hermes_native import desired_env_values


def test_enable_hermes_native_env_values_are_hermes_native():
    values = desired_env_values()
    assert values["FACTOR_LAB_AGENT_BACKEND"] == "hermes"
    assert values["FACTOR_LAB_HERMES_MODE"] == "native"
    assert "factor-lab-researcher" in values["FACTOR_LAB_HERMES_PROFILE_MAP_JSON"]
    assert values["FACTOR_LAB_HERMES_MODEL_SOURCE"] == "main"
    assert "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON" not in values
