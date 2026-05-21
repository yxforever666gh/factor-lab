from factor_lab.hermes_router import HermesRouter


def test_router_routes_canonical_event_to_profile(tmp_path):
    router = HermesRouter(artifact_dir=tmp_path)
    route = router.route("researcher", {"task": "find ideas"})
    assert route.profile_key == "researcher"
    assert route.profile_name == "factor-lab-researcher"
    assert route.session_name == "factor-lab-researcher-main"
    assert route.briefing_path.parent == tmp_path / "briefings" / "researcher"
    assert route.response_path.parent == tmp_path / "responses" / "researcher"
    assert route.migration_alias_used is False


def test_router_translates_old_event_key_once(tmp_path):
    router = HermesRouter(artifact_dir=tmp_path)
    route = router.route("planner", {"task": "legacy event"})
    assert route.profile_key == "researcher"
    assert route.profile_name == "factor-lab-researcher"
    assert route.migration_alias_used is True
    assert route.input_event_key == "planner"


def test_router_profile_map_overrides_profile_name(tmp_path):
    router = HermesRouter(profile_map={"reviewer": "custom-reviewer"}, artifact_dir=tmp_path)
    route = router.route("reviewer", {})
    assert route.profile_name == "custom-reviewer"
    assert route.profile_key == "reviewer"
