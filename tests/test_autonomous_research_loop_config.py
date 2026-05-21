import json
import pytest
from factor_lab.autonomous_research_loop_config import load_autonomous_research_loop_config

def test_config_loader_defaults_when_missing(tmp_path):
    cfg = load_autonomous_research_loop_config(tmp_path/'missing.json')
    assert cfg['primary_mainline'] == 'defensive_quality_risk_layer'
    assert cfg['live_trading_enabled'] is False

def test_config_rejects_live_trading(tmp_path):
    p=tmp_path/'cfg.json'; p.write_text(json.dumps({'live_trading_enabled': True}))
    with pytest.raises(ValueError, match='live_trading'):
        load_autonomous_research_loop_config(p)

def test_config_rejects_broad_daemon_restore(tmp_path):
    p=tmp_path/'cfg.json'; p.write_text(json.dumps({'broad_daemon_restore_allowed': True}))
    with pytest.raises(ValueError, match='broad_daemon'):
        load_autonomous_research_loop_config(p)

def test_config_caps_experiments_to_three(tmp_path):
    p=tmp_path/'cfg.json'; p.write_text(json.dumps({'max_experiments_per_cycle': 99}))
    cfg=load_autonomous_research_loop_config(p)
    assert cfg['max_experiments_per_cycle'] == 3
