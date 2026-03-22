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
- RankIC mean: 0.309486
- RankIC IR: 1.34473
- Top-bottom spread mean: 0.034256
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_mom_20 [PASS] | RankIC=0.081574 | IR=0.301021 | Spread=0.00249 | Reason=n/a

## Time Split Robustness

- hybrid_liquidity_turnover_shock_mom_20 / first_half [PASS] | RankIC=0.268595 | Spread=0.039271 | Reason=n/a
- hybrid_liquidity_turnover_shock_mom_20 / second_half [PASS] | RankIC=0.34697 | Spread=0.02966 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_mom_20 | score=1.520146 | rawIC=0.309486 | neutralIC=0.081574 | peers=none

## Candidate Pool

- hybrid_liquidity_turnover_shock_mom_20 | rawIC=0.309486 | neutralIC=0.081574 | peers=none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_mom_20 | score=1.520146 | cluster=hybrid_liquidity_turnover_shock_mom_20

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.342906
- Annual volatility: 0.167202
- Sharpe: 8.031649
- Max drawdown: -0.017451
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.342906
- Annual volatility: 0.167202
- Sharpe: 8.031649
- Max drawdown: -0.017451
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.343569
- Annual volatility: 0.189822
- Sharpe: 1.80996
- Max drawdown: -0.062818
- Avg turnover: 1.315909
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.342906
- Annual volatility: 0.167202
- Sharpe: 8.031649
- Max drawdown: -0.017451
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.343569
- Annual volatility: 0.189822
- Sharpe: 1.80996
- Max drawdown: -0.062818
- Avg turnover: 1.315909
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.343569
- Annual volatility: 0.189822
- Sharpe: 1.80996
- Max drawdown: -0.062818
- Avg turnover: 1.315909
- Observations: 23
