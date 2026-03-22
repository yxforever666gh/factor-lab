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
- RankIC mean: -0.003892
- RankIC IR: -0.010087
- Top-bottom spread mean: 0.002109
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- mom_20 [PASS] | RankIC=0.020075 | IR=0.069231 | Spread=0.001827 | Reason=n/a

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.114304 | Spread=-0.010425 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.103614 | Spread=0.014313 | Reason=n/a

## Factor Scores

- mom_20 | score=0.148982 | rawIC=-0.003892 | neutralIC=0.020075 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=0.148982 | cluster=mom_20

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.155324
- Annual volatility: 0.308029
- Sharpe: 0.50425
- Max drawdown: -0.535036
- Avg turnover: 0.385135
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.155324
- Annual volatility: 0.308029
- Sharpe: 0.50425
- Max drawdown: -0.535036
- Avg turnover: 0.385135
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.020852
- Annual volatility: 0.224827
- Sharpe: -0.092746
- Max drawdown: -0.274116
- Avg turnover: 1.43964
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: -0.020852
- Annual volatility: 0.224827
- Sharpe: -0.092746
- Max drawdown: -0.274116
- Avg turnover: 1.43964
- Observations: 75
