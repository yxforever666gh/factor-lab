# Workflow Summary

- Data source: tushare
- Total factors: 4
- Passed: 4
- Failed: 0
- Candidate pool size: 2
- Graveyard size: 2
- Cluster representative count: 3

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.327686
- RankIC IR: 1.21569
- Top-bottom spread mean: 0.032816
- Fail reason: n/a

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.281405
- RankIC IR: 1.080412
- Top-bottom spread mean: 0.019764
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.254545
- RankIC IR: 0.978997
- Top-bottom spread mean: 0.02472
- Fail reason: n/a

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.233884
- RankIC IR: 1.198217
- Top-bottom spread mean: 0.020628
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_20 [PASS] | RankIC=0.065516 | IR=0.280807 | Spread=0.006955 | Reason=n/a
- mom_plus_value [PASS] | RankIC=0.024927 | IR=0.110529 | Spread=0.006892 | Reason=n/a
- size_small [FAIL] | RankIC=-0.017024 | IR=-0.069919 | Spread=-0.009697 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=-0.022004 | IR=-0.078614 | Spread=-0.000168 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.339669 | Spread=0.041942 | Reason=n/a
- mom_plus_value / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.317355 | Spread=0.026058 | Reason=n/a
- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.254545 | Spread=0.013971 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.209091 | Spread=0.025039 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.258678 | Spread=0.016217 | Reason=n/a

## Factor Scores

- mom_20 | score=1.398116 | rawIC=0.327686 | neutralIC=0.065516 | peers=mom_plus_value
- mom_plus_value | score=1.140355 | rawIC=0.281405 | neutralIC=0.024927 | peers=mom_20
- liquidity_turnover_shock | score=0.5172 | rawIC=0.254545 | neutralIC=-0.022004 | peers=none
- size_small | score=0.513371 | rawIC=0.233884 | neutralIC=-0.017024 | peers=none

## Candidate Pool

- mom_20 | rawIC=0.327686 | neutralIC=0.065516 | peers=mom_plus_value
- mom_plus_value | rawIC=0.281405 | neutralIC=0.024927 | peers=mom_20

## Cluster Representatives

- liquidity_turnover_shock | score=0.5172 | cluster=liquidity_turnover_shock
- mom_20 | score=1.398116 | cluster=mom_20, mom_plus_value
- size_small | score=0.513371 | cluster=size_small

## Graveyard

- liquidity_turnover_shock | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.584423
- Annual volatility: 0.159848
- Sharpe: 9.912092
- Max drawdown: -0.010113
- Avg turnover: 0.31746
- Observations: 22

### long_short_top_bottom_candidates_only
- Annual return: 1.213329
- Annual volatility: 0.222964
- Sharpe: 5.441824
- Max drawdown: -0.034234
- Avg turnover: 0.428571
- Observations: 22

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.155334
- Annual volatility: 0.17455
- Sharpe: 0.889911
- Max drawdown: -0.074308
- Avg turnover: 1.460317
- Observations: 22

### long_short_top_bottom_cluster_representatives
- Annual return: 1.381271
- Annual volatility: 0.136086
- Sharpe: 10.150001
- Max drawdown: -0.013906
- Avg turnover: 0.190476
- Observations: 22

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.142237
- Annual volatility: 0.140178
- Sharpe: 1.014694
- Max drawdown: -0.064025
- Avg turnover: 1.349206
- Observations: 22

### long_short_top_bottom_neutralized
- Annual return: -0.059999
- Annual volatility: 0.188031
- Sharpe: -0.319091
- Max drawdown: -0.143191
- Avg turnover: 1.333333
- Observations: 22
