import pandas as pd
import pytest

from factor_lab.research_os.shadow import (
    ShadowAccount,
    ShadowExecutionConfig,
    ShadowPortfolioEngine,
    assert_point_in_time_columns,
)


def _bars(
    session="2026-01-05",
    *,
    snapshot_id="snapshot-1",
    limit_up=False,
    limit_down=False,
    split=1.0,
    dividend=0.0,
):
    timing = {
        "trade_date": session,
        "execution_event_time": f"{session}T09:30:00+08:00",
        "execution_available_at": f"{session}T09:30:00+08:00",
        "mark_event_time": f"{session}T15:00:00+08:00",
        "mark_available_at": f"{session}T15:01:00+08:00",
        "gold_snapshot_id": snapshot_id,
    }
    return pd.DataFrame(
        [
            {
                **timing,
                "ticker": "000001.SZ",
                "open_adj": 10.0,
                "close_adj": 11.0,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": limit_up,
                "is_one_price_limit_down": limit_down,
                "is_suspended": False,
                "split_ratio": split,
                "cash_dividend": dividend,
            },
            {
                **timing,
                "ticker": "000002.SZ",
                "open_adj": 20.0,
                "close_adj": 20.0,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": False,
            },
        ]
    )


def test_shadow_rejects_forward_labels_and_same_day_execution():
    with pytest.raises(ValueError, match="forward"):
        assert_point_in_time_columns(["ticker", "forward_return_5d"])
    engine = ShadowPortfolioEngine(
        ShadowAccount("a", initial_capital=100_000),
        ShadowExecutionConfig(max_position_weight=0.5),
    )
    with pytest.raises(ValueError, match="after"):
        engine.execute_target(
            decision_date="2026-01-02",
            trade_date="2026-01-02",
            expected_next_session="2026-01-02",
            target_weights={"000001.SZ": 0.5},
            market_bars=_bars(),
            snapshot_id="s",
            model_version="m",
            trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
        )


@pytest.mark.parametrize(
    "column",
    [
        "next_return_5d",
        "fwd_ret",
        "lead_close",
        "vendor_next_day_alpha",
        "label",
        "target",
        "alpha_label",
        "return_5d",
        "y",
    ],
)
def test_shadow_forward_denylist_rejects_next_fwd_and_lead_aliases(column):
    with pytest.raises(ValueError, match="forward/label"):
        assert_point_in_time_columns(["ticker", column])


def test_shadow_requires_single_gold_snapshot_binding():
    engine = ShadowPortfolioEngine(
        ShadowAccount("binding", initial_capital=100_000),
        ShadowExecutionConfig(max_position_weight=0.5),
    )
    bars = _bars(snapshot_id="other-snapshot")
    with pytest.raises(ValueError, match="same authoritative Gold snapshot"):
        engine.execute_target(
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={},
            market_bars=bars,
            snapshot_id="expected-snapshot",
            model_version="m",
            trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
        )


def test_shadow_account_executes_at_next_open_and_balances_nav():
    account = ShadowAccount("a", initial_capital=100_000)
    engine = ShadowPortfolioEngine(account, ShadowExecutionConfig(max_position_weight=0.5))
    mark = engine.execute_target(
        decision_date="2026-01-02",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={"000001.SZ": 0.5},
        market_bars=_bars(),
        snapshot_id="snapshot-1",
        model_version="model-1",
        benchmark_return=0.01,
        trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
    )
    assert account.positions["000001.SZ"].quantity > 0
    assert mark["nav"] == pytest.approx(mark["cash"] + mark["positions_value"])
    assert mark["benchmark_nav"] == pytest.approx(101_000)
    assert account.validate_hash_chain() is True
    assert all(event.payload.get("price") in {None, 10.0} for event in account.events if event.event_type == "fill")


def test_shadow_accepts_prompt_fill_receipt_but_rejects_late_reconstruction():
    prompt = _bars()
    prompt["execution_available_at"] = "2026-01-05T09:34:00+08:00"
    engine = ShadowPortfolioEngine(
        ShadowAccount("prompt", initial_capital=100_000),
        ShadowExecutionConfig(max_position_weight=0.5),
    )
    engine.execute_target(
        decision_date="2026-01-02",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={"000001.SZ": 0.5},
        market_bars=prompt,
        snapshot_id="snapshot-1",
        model_version="model-1",
        trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
    )

    late = _bars()
    late["execution_available_at"] = "2026-01-05T09:36:00+08:00"
    with pytest.raises(ValueError, match="observation deadline"):
        ShadowPortfolioEngine(
            ShadowAccount("late", initial_capital=100_000),
            ShadowExecutionConfig(max_position_weight=0.5),
        ).execute_target(
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={"000001.SZ": 0.5},
            market_bars=late,
            snapshot_id="snapshot-1",
            model_version="model-1",
            trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
        )


def test_limit_rules_and_corporate_actions_are_evented():
    account = ShadowAccount("a", initial_capital=100_000)
    engine = ShadowPortfolioEngine(account, ShadowExecutionConfig(max_position_weight=0.5))
    engine.execute_target(
        decision_date="2026-01-02",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={"000001.SZ": 0.5},
        market_bars=_bars(limit_up=True, snapshot_id="s1"),
        snapshot_id="s1",
        model_version="m1",
        trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
    )
    assert "000001.SZ" not in account.positions
    assert any(event.event_type == "order_blocked" for event in account.events)

    engine.execute_target(
        decision_date="2026-01-05",
        trade_date="2026-01-06",
        expected_next_session="2026-01-06",
        target_weights={"000001.SZ": 0.5},
        market_bars=_bars("2026-01-06", snapshot_id="s2"),
        snapshot_id="s2",
        model_version="m1",
        trusted_calendar_sessions=("2026-01-05", "2026-01-06"),
    )
    quantity = account.positions["000001.SZ"].quantity
    cash = account.cash
    engine.mark_to_market(
        "2026-01-07",
        _bars("2026-01-07", split=2.0, dividend=0.1, snapshot_id="mark-snapshot"),
        snapshot_id="mark-snapshot",
    )
    # Corporate actions are applied before an execution cycle, not during a pure mark.
    engine.execute_target(
        decision_date="2026-01-07",
        trade_date="2026-01-08",
        expected_next_session="2026-01-08",
        target_weights={"000001.SZ": 0.5},
        market_bars=_bars("2026-01-08", split=2.0, dividend=0.1, snapshot_id="s3"),
        snapshot_id="s3",
        model_version="m1",
        trusted_calendar_sessions=("2026-01-07", "2026-01-08"),
    )
    assert any(event.event_type == "corporate_action_split" for event in account.events)
    assert any(event.event_type == "corporate_action_dividend" for event in account.events)
    assert account.cash != cash
    assert account.positions["000001.SZ"].quantity != quantity


def test_shadow_buys_track_fee_inclusive_average_cost():
    account = ShadowAccount("cost-basis", initial_capital=1_000_000)
    engine = ShadowPortfolioEngine(
        account,
        ShadowExecutionConfig(max_position_weight=1.0, lot_size=100),
    )
    bars = pd.DataFrame(
        [
            {
                "trade_date": "2026-01-06",
                "execution_event_time": "2026-01-06T09:30:00+08:00",
                "execution_available_at": "2026-01-06T09:30:00+08:00",
                "mark_event_time": "2026-01-06T15:00:00+08:00",
                "mark_available_at": "2026-01-06T15:01:00+08:00",
                "gold_snapshot_id": "snapshot",
                "ticker": "A",
                "open_adj": 10.0,
                "close_adj": 10.1,
                "adv_20": 1e9,
                "volatility_20": 0.02,
            }
        ]
    )
    engine.execute_target(
        decision_date="2026-01-05",
        trade_date="2026-01-06",
        expected_next_session="2026-01-06",
        target_weights={"A": 0.5},
        market_bars=bars,
        snapshot_id="snapshot",
        model_version="champion-v1",
        trusted_calendar_sessions=("2026-01-05", "2026-01-06"),
    )
    position = account.positions["A"]
    assert position.average_cost > 10.0
    assert position.last_price == 10.1
