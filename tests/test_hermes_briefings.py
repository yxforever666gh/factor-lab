import json

from factor_lab.hermes_briefings import build_hermes_prompt, write_hermes_briefing
from factor_lab.hermes_router import HermesRouter


def test_write_hermes_briefing_creates_file_context(tmp_path):
    route = HermesRouter(artifact_dir=tmp_path).route("data_steward", {"task": "check data"})
    payload = write_hermes_briefing(route, task="Check data coverage", context={"x": 1}, workdir="/home/admin/factor-lab")
    assert route.briefing_path.exists()
    written = json.loads(route.briefing_path.read_text())
    assert written["profile_name"] == "factor-lab-data-steward"
    assert written["task"] == "Check data coverage"
    assert written["output_contract"]["profile_key"] == "data_steward"
    assert payload == written


def test_prompt_shape_mentions_profile_and_json_contract(tmp_path):
    route = HermesRouter(artifact_dir=tmp_path).route("researcher", {})
    prompt = build_hermes_prompt(route, workdir="/home/admin/factor-lab")
    assert "factor-lab-researcher" in prompt
    assert str(route.briefing_path) in prompt
    assert "Return one JSON object" in prompt
    assert "request_id=" in prompt
