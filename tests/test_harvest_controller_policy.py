import pytest

from factor_lab.harvest_controller_policy import HarvestControllerPolicy


def test_controller_policy_defaults_are_dry_run_and_safe():
    policy = HarvestControllerPolicy()

    assert policy.max_cycles == 3
    assert policy.max_backtests == 300
    assert policy.allow_controlled_execution is False
    assert policy.no_timer is True
    assert policy.no_daemon is True
    assert policy.no_live_trading is True
    assert policy.no_automatic_promotion is True
    policy.validate()


@pytest.mark.parametrize("field", ["max_cycles", "max_backtests", "max_attempts_per_cycle"])
def test_controller_policy_rejects_negative_budgets(field):
    policy = HarvestControllerPolicy(**{field: -1})

    with pytest.raises(ValueError, match=field):
        policy.validate()


def test_controller_policy_rejects_live_trading_or_auto_promotion():
    with pytest.raises(ValueError, match="live trading"):
        HarvestControllerPolicy(no_live_trading=False).validate()

    with pytest.raises(ValueError, match="automatic promotion"):
        HarvestControllerPolicy(no_automatic_promotion=False).validate()
