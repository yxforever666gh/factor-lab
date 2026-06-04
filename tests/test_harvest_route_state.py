import json

from factor_lab.harvest_route_state import build_route_state


def _cycle(root, name, route="industry_relative_value", oos="fail", failures=None, sem="h"):
    c = root / "artifacts" / "harvest_agent" / name
    c.mkdir(parents=True)
    (c / "mechanism_route.json").write_text(json.dumps({"mechanism_id": route}))
    (c / "oos_validation.json").write_text(json.dumps({"oos_class": oos, "best_sharpe": 0.1, "worst_drawdown": -0.6}))
    (c / "diagnosis.json").write_text(json.dumps({"failure_classes": failures or ["drawdown_too_high"]}))
    (c / "semantic_signature.json").write_text(json.dumps({"semantic_hash": sem}))


def test_no_history_is_active(tmp_path):
    out = build_route_state(tmp_path, current_route="industry_relative_value")
    assert out["current_route_status"] == "active"
    assert out["routes"] == {}


def test_repeated_failures_demote_or_stop_route(tmp_path):
    _cycle(tmp_path, "cycle_0001", sem="same")
    _cycle(tmp_path, "cycle_0002", sem="same")
    _cycle(tmp_path, "cycle_0003", sem="same")
    out = build_route_state(tmp_path, current_route="industry_relative_value")
    assert out["current_route_status"] == "stop"
    assert out["routes"]["industry_relative_value"]["consecutive_failures"] == 3


def test_near_miss_keeps_route_active(tmp_path):
    _cycle(tmp_path, "cycle_0001", oos="fail", sem="a")
    _cycle(tmp_path, "cycle_0002", oos="near_miss", sem="b")
    out = build_route_state(tmp_path, current_route="industry_relative_value")
    assert out["current_route_status"] == "active"
