# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### hybrid_liquidity_turnover_shock_mom_20 [PASS]
- Expression: `(turnover_shock_5_20) + (momentum_20)`
- RankIC mean: 0.339161
- RankIC IR: 1.360975
- Top-bottom spread mean: 0.028584
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_mom_20 [PASS] | RankIC=0.033433 | IR=0.179121 | Spread=0.003104 | Reason=n/a

## Time Split Robustness

- hybrid_liquidity_turnover_shock_mom_20 / first_half [PASS] | RankIC=0.159091 | Spread=0.015611 | Reason=n/a
- hybrid_liquidity_turnover_shock_mom_20 / second_half [PASS] | RankIC=0.493506 | Spread=0.039704 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_mom_20 | score=1.452348 | rawIC=0.339161 | neutralIC=0.033433 | peers=none

## Candidate Pool

- hybrid_liquidity_turnover_shock_mom_20 | rawIC=0.339161 | neutralIC=0.033433 | peers=none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_mom_20 | score=1.452348 | cluster=hybrid_liquidity_turnover_shock_mom_20

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.25801
- Annual volatility: 0.194761
- Sharpe: 6.45926
- Max drawdown: -0.017451
- Avg turnover: 0.388889
- Observations: 13

### long_short_top_bottom_candidates_only
- Annual return: 1.25801
- Annual volatility: 0.194761
- Sharpe: 6.45926
- Max drawdown: -0.017451
- Avg turnover: 0.388889
- Observations: 13

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.334905
- Annual volatility: 0.190075
- Sharpe: 1.761967
- Max drawdown: -0.046801
- Avg turnover: 1.270833
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.25801
- Annual volatility: 0.194761
- Sharpe: 6.45926
- Max drawdown: -0.017451
- Avg turnover: 0.388889
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.334905
- Annual volatility: 0.190075
- Sharpe: 1.761967
- Max drawdown: -0.046801
- Avg turnover: 1.270833
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.334905
- Annual volatility: 0.190075
- Sharpe: 1.761967
- Max drawdown: -0.046801
- Avg turnover: 1.270833
- Observations: 13
