from scripts.bootstrap_hermes_profiles import plan_profile_commands


def test_bootstrap_plans_create_for_missing_profiles():
    commands = plan_profile_commands(existing_profiles={"factor-lab-reviewer"})
    joined = [" ".join(cmd) for cmd in commands]
    assert "hermes profile create factor-lab-researcher" in joined
    assert "hermes profile create factor-lab-diagnostician" in joined
    assert "hermes profile create factor-lab-data-steward" in joined
    assert all("factor-lab-reviewer" not in c or "config set" in c for c in joined)
