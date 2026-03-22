# Workflow Summary

- Data source: tushare
- Total factors: 3
- Passed: 3
- Failed: 0
- Candidate pool size: 3
- Graveyard size: 0
- Cluster representative count: 2

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

### hybrid_mom_20_liquidity_turnover_shock [PASS]
- Expression: `(momentum_20) + (turnover_shock_5_20)`
- RankIC mean: 0.309486
- RankIC IR: 1.34473
- Top-bottom spread mean: 0.034256
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.25336
- RankIC IR: 0.996098
- Top-bottom spread mean: 0.026287
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_mom_20_liquidity_turnover_shock [PASS] | RankIC=0.081574 | IR=0.301021 | Spread=0.00249 | Reason=n/a
- liquidity_turnover_shock [PASS] | RankIC=0.07224 | IR=0.284445 | Spread=0.008624 | Reason=n/a
- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a
- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.252273 | Spread=0.017869 | Reason=n/a
- hybrid_mom_20_liquidity_turnover_shock / first_half [PASS] | RankIC=0.268595 | Spread=0.039271 | Reason=n/a
- hybrid_mom_20_liquidity_turnover_shock / second_half [PASS] | RankIC=0.34697 | Spread=0.02966 | Reason=n/a

## Factor Scores

- hybrid_mom_20_liquidity_turnover_shock | score=1.420146 | rawIC=0.309486 | neutralIC=0.081574 | peers=liquidity_turnover_shock
- mom_20 | score=1.37731 | rawIC=0.326877 | neutralIC=0.024715 | peers=none
- liquidity_turnover_shock | score=1.199535 | rawIC=0.25336 | neutralIC=0.07224 | peers=hybrid_mom_20_liquidity_turnover_shock

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=none
- liquidity_turnover_shock | rawIC=0.25336 | neutralIC=0.07224 | peers=hybrid_mom_20_liquidity_turnover_shock
- hybrid_mom_20_liquidity_turnover_shock | rawIC=0.309486 | neutralIC=0.081574 | peers=liquidity_turnover_shock

## Cluster Representatives

- hybrid_mom_20_liquidity_turnover_shock | score=1.420146 | cluster=hybrid_mom_20_liquidity_turnover_shock, liquidity_turnover_shock
- mom_20 | score=1.37731 | cluster=mom_20

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.221359
- Annual volatility: 0.155844
- Sharpe: 7.837043
- Max drawdown: -0.010113
- Avg turnover: 0.393939
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.221359
- Annual volatility: 0.155844
- Sharpe: 7.837043
- Max drawdown: -0.010113
- Avg turnover: 0.393939
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.603942
- Annual volatility: 0.192799
- Sharpe: 3.132486
- Max drawdown: -0.063111
- Avg turnover: 1.272727
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.372347
- Annual volatility: 0.1975
- Sharpe: 6.9486
- Max drawdown: -0.019324
- Avg turnover: 0.348485
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.258712
- Annual volatility: 0.2059
- Sharpe: 1.256494
- Max drawdown: -0.093241
- Avg turnover: 1.382576
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.603942
- Annual volatility: 0.192799
- Sharpe: 3.132486
- Max drawdown: -0.063111
- Avg turnover: 1.272727
- Observations: 23
