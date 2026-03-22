# Workflow Summary

- Data source: tushare
- Total factors: 2
- Passed: 2
- Failed: 0
- Candidate pool size: 2
- Graveyard size: 0
- Cluster representative count: 2

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.25336
- RankIC IR: 0.996098
- Top-bottom spread mean: 0.026287
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.07224 | IR=0.284445 | Spread=0.008624 | Reason=n/a
- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a
- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.252273 | Spread=0.017869 | Reason=n/a

## Factor Scores

- mom_20 | score=1.37731 | rawIC=0.326877 | neutralIC=0.024715 | peers=none
- liquidity_turnover_shock | score=1.299535 | rawIC=0.25336 | neutralIC=0.07224 | peers=none

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=none
- liquidity_turnover_shock | rawIC=0.25336 | neutralIC=0.07224 | peers=none

## Cluster Representatives

- liquidity_turnover_shock | score=1.299535 | cluster=liquidity_turnover_shock
- mom_20 | score=1.37731 | cluster=mom_20

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.270751
- Annual volatility: 0.190998
- Sharpe: 6.65321
- Max drawdown: -0.010113
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.270751
- Annual volatility: 0.190998
- Sharpe: 6.65321
- Max drawdown: -0.010113
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.59135
- Annual volatility: 0.174406
- Sharpe: 3.390644
- Max drawdown: -0.047222
- Avg turnover: 1.333333
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.270751
- Annual volatility: 0.190998
- Sharpe: 6.65321
- Max drawdown: -0.010113
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.59135
- Annual volatility: 0.174406
- Sharpe: 3.390644
- Max drawdown: -0.047222
- Avg turnover: 1.333333
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.59135
- Annual volatility: 0.174406
- Sharpe: 3.390644
- Max drawdown: -0.047222
- Avg turnover: 1.333333
- Observations: 23
