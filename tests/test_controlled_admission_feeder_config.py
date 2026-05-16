from __future__ import annotations

import json

import pytest

from factor_lab.controlled_admission_feeder import load_feeder_config, resolve_feeder_policy


def test_load_feeder_config_missing_path_returns_conservative_defaults(tmp_path):
    cfg = load_feeder_config(tmp_path / "missing.json")

    assert cfg["profile"] == "conservative"
    assert cfg["limit"] == 1
    assert cfg["priority"] == 0
    assert cfg["cooldown_minutes"] == 60
    assert cfg["daily_budget"] == 3
    assert cfg["force_new"] is False


def test_load_feeder_config_reads_json_overrides(tmp_path):
    path = tmp_path / "feeder.json"
    path.write_text(json.dumps({"profile": "balanced", "cooldown_minutes": 30, "daily_budget": 8}), encoding="utf-8")

    cfg = load_feeder_config(path)

    assert cfg["profile"] == "balanced"
    assert cfg["cooldown_minutes"] == 30
    assert cfg["daily_budget"] == 8
    assert cfg["limit"] == 1


def test_load_feeder_config_rejects_invalid_json(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{bad", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid feeder config json"):
        load_feeder_config(path)


def test_resolve_feeder_policy_cli_overrides_config_values():
    policy = resolve_feeder_policy(
        {"profile": "balanced", "limit": 1, "priority": 0, "cooldown_minutes": 30, "daily_budget": 8, "force_new": False},
        cli_overrides={"cooldown_minutes": 15, "daily_budget": 4},
    )

    assert policy["profile"] == "balanced"
    assert policy["cooldown_minutes"] == 15
    assert policy["daily_budget"] == 4
    assert policy["limit"] == 1


def test_resolve_feeder_policy_rejects_force_new_without_probe_permission():
    with pytest.raises(ValueError, match="force_new is only allowed"):
        resolve_feeder_policy(
            {"profile": "conservative", "limit": 1, "priority": 0, "cooldown_minutes": 60, "daily_budget": 3, "force_new": True},
            allow_force_new_probe=False,
        )


def test_resolve_feeder_policy_allows_force_new_for_probe_profile_with_permission():
    policy = resolve_feeder_policy(
        {"profile": "probe", "limit": 1, "priority": 0, "cooldown_minutes": 1, "daily_budget": 1, "force_new": True},
        allow_force_new_probe=True,
    )

    assert policy["profile"] == "probe"
    assert policy["force_new"] is True
