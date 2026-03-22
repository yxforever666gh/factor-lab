# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_value_ep [FAIL]
- Expression: `(momentum_20) + (earnings_yield)`
- RankIC mean: -0.022877
- RankIC IR: -0.063207
- Top-bottom spread mean: -0.006049
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- hybrid_mom_20_value_ep [FAIL] | RankIC=0.000964 | IR=0.003347 | Spread=-0.001378 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_mom_20_value_ep / first_half [FAIL] | RankIC=-0.137888 | Spread=-0.015462 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_value_ep / second_half [PASS] | RankIC=0.089107 | Spread=0.003115 | Reason=n/a

## Factor Scores

- hybrid_mom_20_value_ep | score=-0.463782 | rawIC=-0.022877 | neutralIC=0.000964 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_mom_20_value_ep | score=-0.463782 | cluster=hybrid_mom_20_value_ep

## Graveyard

- hybrid_mom_20_value_ep | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.002714
- Annual volatility: 0.288172
- Sharpe: 0.009418
- Max drawdown: -0.553185
- Avg turnover: 0.371622
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.002714
- Annual volatility: 0.288172
- Sharpe: 0.009418
- Max drawdown: -0.553185
- Avg turnover: 0.371622
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.031157
- Annual volatility: 0.180865
- Sharpe: -0.172265
- Max drawdown: -0.186539
- Avg turnover: 1.407658
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: -0.031157
- Annual volatility: 0.180865
- Sharpe: -0.172265
- Max drawdown: -0.186539
- Avg turnover: 1.407658
- Observations: 75
