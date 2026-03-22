# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.154574
- RankIC IR: 0.408926
- Top-bottom spread mean: 0.02216
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=0.005525 | IR=0.020276 | Spread=-0.000811 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.036872 | Spread=0.010548 | Reason=rank_ic_mean<0.02
- mom_20 / second_half [PASS] | RankIC=0.334759 | Spread=0.033088 | Reason=n/a

## Factor Scores

- mom_20 | score=0.064349 | rawIC=0.154574 | neutralIC=0.005525 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=0.064349 | cluster=mom_20

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.830045
- Annual volatility: 0.238491
- Sharpe: 3.480408
- Max drawdown: -0.178253
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.830045
- Annual volatility: 0.238491
- Sharpe: 3.480408
- Max drawdown: -0.178253
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.052229
- Annual volatility: 0.18208
- Sharpe: -0.286849
- Max drawdown: -0.136574
- Avg turnover: 1.448958
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: -0.052229
- Annual volatility: 0.18208
- Sharpe: -0.286849
- Max drawdown: -0.136574
- Avg turnover: 1.448958
- Observations: 33
