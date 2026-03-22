# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.018454
- RankIC IR: -0.045048
- Top-bottom spread mean: -0.001331
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=0.014449 | IR=0.050922 | Spread=0.001609 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.246046 | Spread=-0.024239 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.200709 | Spread=0.020728 | Reason=n/a

## Factor Scores

- mom_20 | score=-0.409545 | rawIC=-0.018454 | neutralIC=0.014449 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=-0.409545 | cluster=mom_20

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.105946
- Annual volatility: 0.291822
- Sharpe: -0.363051
- Max drawdown: -0.51544
- Avg turnover: 0.423077
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: -0.105946
- Annual volatility: 0.291822
- Sharpe: -0.363051
- Max drawdown: -0.51544
- Avg turnover: 0.423077
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.046214
- Annual volatility: 0.181435
- Sharpe: -0.254713
- Max drawdown: -0.182071
- Avg turnover: 1.421154
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: -0.046214
- Annual volatility: 0.181435
- Sharpe: -0.254713
- Max drawdown: -0.182071
- Avg turnover: 1.421154
- Observations: 53
