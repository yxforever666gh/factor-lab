import numpy as np
import pandas as pd

from factor_lab.research_os.champion import ChampionChallengePolicy, evaluate_challenger


def test_challenger_needs_both_stitched_oos_and_sixty_new_shadow_sessions():
    index = pd.date_range("2021-01-01", periods=5 * 52, freq="W-FRI")
    champion = pd.Series(np.full(len(index), 0.001), index=index)
    challenger = pd.Series(np.full(len(index), 0.002), index=index)
    short_shadow = pd.Series(np.full(59, 0.001), index=pd.bdate_range("2026-01-01", periods=59))
    shadow_base = pd.Series(np.zeros(59), index=short_shadow.index)
    not_ready = evaluate_challenger(
        challenger,
        champion,
        shadow_challenger_returns=short_shadow,
        shadow_champion_returns=shadow_base,
        policy=ChampionChallengePolicy(min_positive_outer_years=3),
    )
    assert not_ready.decision == "retain_champion"
    assert not_ready.checks["shadow_observation"] is False

    ready_shadow = pd.Series(np.full(60, 0.001), index=pd.bdate_range("2026-01-01", periods=60))
    ready_base = pd.Series(np.zeros(60), index=ready_shadow.index)
    ready = evaluate_challenger(
        challenger,
        champion,
        shadow_challenger_returns=ready_shadow,
        shadow_champion_returns=ready_base,
        policy=ChampionChallengePolicy(min_positive_outer_years=3),
    )
    assert ready.decision == "challenger_research_recovered"
    assert all(ready.checks.values())


def test_challenger_falls_back_when_it_only_beats_market_not_champion():
    index = pd.bdate_range("2021-01-01", periods=260)
    champion = pd.Series(np.full(len(index), 0.002), index=index)
    challenger = pd.Series(np.full(len(index), 0.001), index=index)
    shadow_index = pd.bdate_range("2026-01-01", periods=60)
    decision = evaluate_challenger(
        challenger,
        champion,
        shadow_challenger_returns=pd.Series(0.001, index=shadow_index),
        shadow_champion_returns=pd.Series(0.002, index=shadow_index),
    )
    assert decision.decision == "retain_champion"
    assert decision.fallback == "static_champion"
