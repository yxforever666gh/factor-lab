import json
import pytest

from factor_lab.harvest_agent_policy import (
    DEFAULT_HARVEST_AGENT_POLICY,
    load_harvest_agent_policy,
    validate_mainline,
)


def test_missing_config_returns_safe_defaults(tmp_path):
    cfg = load_harvest_agent_policy(tmp_path / "missing.json")
    assert cfg["mode"] == "dry_run_first"
    assert cfg["max_experiments_per_cycle"] == 2
    assert cfg["max_cycles_per_day"] == 4
    assert cfg["cooldown_minutes"] == 180


def test_live_trading_cannot_be_enabled(tmp_path):
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({"live_trading_enabled": True}))
    with pytest.raises(ValueError, match="live_trading"):
        load_harvest_agent_policy(p)


def test_broad_daemon_restore_cannot_be_enabled(tmp_path):
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({"broad_daemon_restore_allowed": True}))
    with pytest.raises(ValueError, match="broad_daemon"):
        load_harvest_agent_policy(p)


def test_max_experiments_is_capped_in_v1(tmp_path):
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({"max_experiments_per_cycle": 99}))
    assert load_harvest_agent_policy(p)["max_experiments_per_cycle"] == 2


def test_unsupported_mainline_is_rejected():
    assert validate_mainline("bucket_aware_oos_followup", DEFAULT_HARVEST_AGENT_POLICY) is True
    with pytest.raises(ValueError, match="unsupported_mainline"):
        validate_mainline("arbitrary_factor_search", DEFAULT_HARVEST_AGENT_POLICY)


def test_promotion_thresholds_are_loaded(tmp_path):
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({"promotion_thresholds": {"min_rank_ic_mean": 0.03}}))
    cfg = load_harvest_agent_policy(p)
    assert cfg["promotion_thresholds"]["min_rank_ic_mean"] == 0.03
    assert "min_coverage" in cfg["promotion_thresholds"]
