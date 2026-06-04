from __future__ import annotations

import json

from factor_lab.autonomous_strategy_mechanism_request_pack import (
    build_mechanism_researcher_request_pack,
    mechanism_researcher_request_pack_to_markdown,
    write_mechanism_researcher_request_pack,
)


def request():
    return {
        "decision": "request_new_mechanism_or_external_distress_data",
        "candidate_next_mechanism_families": ["earnings_revision_valuation_repair"],
        "external_data_requests": ["forecast_eps"],
        "do_not_repeat": ["do not repeat static value"],
        "required_new_mechanism_properties": ["PIT-safe data"],
    }


def test_mechanism_request_pack_builds_worker_task_with_safety_flags():
    pack = build_mechanism_researcher_request_pack(run_id="x", new_mechanism_request=request())
    assert pack["decision"] == "send_to_mechanism_researcher"
    assert pack["worker_tasks"][0]["worker_key"] == "factor_lab_mechanism_researcher"
    assert pack["worker_tasks"][0]["max_candidate_routes"] == 3
    assert pack["controlled_execution_allowed"] is False
    assert pack["queue_write_allowed"] is False


def test_mechanism_request_pack_markdown_and_write(tmp_path):
    pack = build_mechanism_researcher_request_pack(run_id="x", new_mechanism_request=request())
    markdown = mechanism_researcher_request_pack_to_markdown(pack)
    assert "Mechanism Researcher Request Pack" in markdown
    assert "earnings_revision_valuation_repair" in markdown
    paths = write_mechanism_researcher_request_pack(pack, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["decision"] == "send_to_mechanism_researcher"
