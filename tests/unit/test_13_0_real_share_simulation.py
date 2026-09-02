from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research.pit_stock import (
    PITStockContractError,
    STRATEGY_ID,
    canonical_sha256,
)
from factor_lab.research.pit_stock_real_account import PnlPosting
from factor_lab.research import pit_stock_real_simulation as simulation
from factor_lab.research.pit_stock_minute_execution import (
    MINUTE_EXECUTION_BAR_COLUMNS,
    MINUTE_EXECUTION_CONTEXT_COLUMNS,
)


def test_target_map_rejects_duplicate_stored_identity_before_dict_collapse() -> None:
    panel = pd.DataFrame(
        [{"signal_date": "2021-03-31", "ticker": "000001.SZ"}]
    )
    stored = pd.DataFrame(
        [
            {
                "strategy_id": STRATEGY_ID,
                "signal_date": "2021-03-31",
                "ticker": "000001.SZ",
                "target_weight": 0.5,
            },
            {
                "strategy_id": STRATEGY_ID,
                "signal_date": "2021-03-31",
                "ticker": "000001.SZ",
                "target_weight": 0.5,
            },
        ]
    )
    with pytest.raises(PITStockContractError, match="duplicate"):
        simulation._target_maps(panel, stored)


def test_period_reconciliation_fails_closed() -> None:
    period = {
        "signal_date": "2020-12-31",
        "execution_date": "2021-01-04",
        "start_nav": 100.0,
        "groups": {},
        "market_state": "both_nonpositive",
        "start_weights": {},
    }
    postings = [
        PnlPosting(
            date="2021-03-31",
            phase="new_period_close",
            period_signal="2020-12-31",
            ticker="000001.SZ",
            kind="raw_mark",
            amount=1.0,
        )
    ]
    with pytest.raises(PITStockContractError, match="does not reconcile"):
        simulation._finalize_period(
            role="candidate_base",
            period=period,
            end_date=pd.Timestamp("2021-04-01"),
            end_nav=102.0,
            postings=postings,
            period_rows=[],
            ticker_rows=[],
            group_rows=[],
        )


class _CutoffStore:
    sessions = tuple(
        pd.to_datetime(
            [
                "2020-12-31",
                "2021-01-04",
                "2021-03-31",
                "2021-04-01",
                "2021-06-30",
                "2021-07-01",
            ]
        )
    )
    market_sessions = sessions
    maximum_read_date = pd.Timestamp("2021-04-01")


def test_simulation_rejects_final_next_open_after_cutoff(monkeypatch) -> None:
    signals = tuple(
        pd.to_datetime(["2020-12-31", "2021-03-31", "2021-06-30"])
    )
    targets = {signal: {} for signal in signals}
    empty_sha = canonical_sha256([])
    decisions = {
        signal: {"target_sha256": empty_sha} for signal in signals
    }
    monkeypatch.setattr(
        simulation,
        "_target_maps",
        lambda panel, stored: (
            targets,
            decisions,
            simulation.DEVELOPMENT_TARGET_PAYLOAD_SHA256,
        ),
    )
    panel = pd.DataFrame(
        [{"signal_date": signal, "ticker": "000001.SZ"} for signal in signals]
    )
    with pytest.raises(PITStockContractError, match="after cutoff"):
        simulation.simulate_candidate_real_share_accounts(
            store=_CutoffStore(),
            panel=panel,
            stored_targets=pd.DataFrame(),
            resolved_actions=pd.DataFrame(),
            suspension_events=pd.DataFrame(),
            include_benchmark=False,
        )


class _MinuteStore(_CutoffStore):
    maximum_read_date = pd.Timestamp("2021-07-01")
    security_master = pd.DataFrame(columns=["ts_code", "delist_date"])

    def read_market(self, session):
        return pd.DataFrame(
            columns=[
                "ts_code",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "adj_factor",
            ]
        )


class _Projection:
    def __init__(self, *args, **kwargs):
        pass

    def advance(self, session, observed_tickers):
        return set()


def test_simulation_replays_sequential_minute_mode_without_daily_fallback(
    monkeypatch,
) -> None:
    signals = tuple(
        pd.to_datetime(["2020-12-31", "2021-03-31", "2021-06-30"])
    )
    targets = {signal: {} for signal in signals}
    empty_sha = canonical_sha256([])
    decisions = {
        signal: {"target_sha256": empty_sha} for signal in signals
    }
    monkeypatch.setattr(
        simulation,
        "_target_maps",
        lambda panel, stored: (
            targets,
            decisions,
            simulation.DEVELOPMENT_TARGET_PAYLOAD_SHA256,
        ),
    )
    monkeypatch.setattr(simulation, "SuspensionProjection", _Projection)
    panel = pd.DataFrame(
        [
            {
                "signal_date": signal,
                "ticker": "000001.SZ",
                "universe_member": True,
                "mom12": 1.0,
                "mom6": 1.0,
                "adv20": 1_000_000.0,
                "vol63": 0.2,
                "industry": "bank",
                "size_bucket": "large",
            }
            for signal in signals
        ]
    )
    action_columns = [
        "action_id",
        "ticker",
        "available_date",
        "record_date",
        "ex_date",
        "pay_date",
        "share_arrival_date",
        "stock_dividend_per_share",
        "cash_dividend_before_tax_per_share",
    ]
    class _MinuteProvider:
        def __init__(self):
            self.calls = []

        def build_auction_anchors(
            self, *, signal_date, execution_date, required_tickers
        ):
            self.calls.append((signal_date, "anchor", set(required_tickers)))
            return (
                pd.DataFrame(
                    columns=(
                        "ticker",
                        "trade_time",
                        "observable_at",
                        "open",
                        "zero_liquidity_flat_price",
                    )
                ),
                set(required_tickers),
            )

        def build_context(
            self,
            *,
            signal_date,
            execution_date,
            required_tickers,
            signal_snapshot,
        ):
            self.calls.append((signal_date, "context", set(required_tickers)))
            return pd.DataFrame(columns=MINUTE_EXECUTION_CONTEXT_COLUMNS)

        def build_window(
            self,
            *,
            signal_date,
            execution_date,
            required_tickers,
            window,
        ):
            self.calls.append((signal_date, window, set(required_tickers)))
            return (
                pd.DataFrame(columns=MINUTE_EXECUTION_BAR_COLUMNS),
                set(required_tickers),
            )

    minute_provider = _MinuteProvider()

    result = simulation.simulate_candidate_real_share_accounts(
        store=_MinuteStore(),
        panel=panel,
        stored_targets=pd.DataFrame(),
        resolved_actions=pd.DataFrame(columns=action_columns),
        suspension_events=pd.DataFrame(
            [
                {
                    "ticker": "999999.SZ",
                    "date": "2021-01-04",
                    "suspend_type": "S",
                    "suspend_timing": pd.NA,
                }
            ]
        ),
        include_benchmark=False,
        execution_mode="sequential_minute",
        minute_market_provider=minute_provider,
    )
    assert [phase for _, phase, _ in minute_provider.calls] == [
        "anchor",
        "context",
        "context",
        "A",
        "B",
        "C",
        "anchor",
        "context",
        "context",
        "A",
        "B",
        "C",
        "anchor",
    ]
    assert result.boundaries.execution_mode.eq("sequential_minute").all()
    assert result.metrics["phase_gate"]["complete"] is False
    assert result.metrics["phase_gate"]["stage_1_passed"] is False
    assert result.metrics["phase_gate"]["future_input_violation_count"] == 0
    assert result.orders.empty


def test_minute_provider_trace_rejects_per_role_future_order() -> None:
    signals = tuple(pd.to_datetime(["2020-12-31", "2021-03-31"]))
    roles = ("candidate_base", "candidate_stress")
    calls = []
    for signal, role, phase in (
        (signals[0], "all_roles_boundary_proof", "09:30_anchor"),
        (signals[0], "candidate_base", "context"),
        (signals[0], "all_roles", "A"),
        (signals[0], "all_roles", "B"),
        (signals[0], "all_roles", "C"),
        (signals[0], "candidate_stress", "context"),
        (signals[1], "all_roles_boundary_proof", "09:30_anchor"),
    ):
        calls.append(
            {
                "signal_date": signal.date().isoformat(),
                "role": role,
                "phase": phase,
                "ticker_count": 0,
                "ticker_payload_sha256": canonical_sha256([]),
            }
        )
    violations = simulation._minute_provider_trace_violations(
        calls, signals=signals, roles=roles
    )
    assert violations == [
        "minute provider call order differs from anchor/all-context/A/B/C lockstep"
    ]


def test_intraday_resume_carries_only_when_0930_anchor_is_absent() -> None:
    assert simulation._intraday_resume_carry_tickers(
        minute_no_anchor={"NO_ANCHOR", "SUSPENDED"},
        session_resume_open_blocks={"NO_ANCHOR", "HAS_ANCHOR"},
    ) == {"NO_ANCHOR"}


def test_zero_liquidity_preclose_placeholder_does_not_override_daily_open() -> None:
    assert simulation._auction_anchor_matches_daily_open(
        auction_open=10.80,
        daily_open=10.72,
        daily_pre_close=10.80,
        zero_liquidity_flat_price=True,
    )
    assert not simulation._auction_anchor_matches_daily_open(
        auction_open=10.80,
        daily_open=10.72,
        daily_pre_close=10.80,
        zero_liquidity_flat_price=False,
    )
    assert not simulation._auction_anchor_matches_daily_open(
        auction_open=10.79,
        daily_open=10.72,
        daily_pre_close=10.80,
        zero_liquidity_flat_price=True,
    )


def test_role_metrics_fill_denominator_includes_full_target_gap() -> None:
    daily = pd.DataFrame(
        [
            {
                "trade_date": pd.Timestamp("2021-01-04"),
                "nav": 100.0,
                "net_return": 0.0,
                "cash": 100.0,
                "gross_stock_exposure": 0.0,
                "reconciliation_error": 0.0,
            },
            {
                "trade_date": pd.Timestamp("2021-04-01"),
                "nav": 100.0,
                "net_return": 0.0,
                "cash": 100.0,
                "gross_stock_exposure": 0.0,
                "reconciliation_error": 0.0,
            },
        ]
    )
    periods = pd.DataFrame(
        [
            {
                "signal_date": pd.Timestamp("2020-12-31"),
                "period_return": 0.0,
                "reconciliation_error": 0.0,
            },
            {
                "signal_date": pd.Timestamp("2021-03-31"),
                "period_return": 0.0,
                "reconciliation_error": 0.0,
            },
        ]
    )
    orders = pd.DataFrame(
        [
            {
                "requested_notional": 0.0,
                "target_gap_notional": 1_000.0,
                "executed_notional": 0.0,
                "filled_decision_notional": 0.0,
                "capacity_limited": False,
                "status": "blocked",
            }
        ]
    )
    metrics = simulation._role_metrics(
        daily, periods, orders, initial_nav=100.0
    )
    assert metrics["requested_notional"] == 0.0
    assert metrics["target_gap_notional"] == 1_000.0
    assert metrics["target_gap_fill_ratio"] == 0.0
