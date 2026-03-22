# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.118708
- RankIC IR: 0.382127
- Top-bottom spread mean: 0.009516
- Fail reason: n/a

## Neutralized Results (industry + size)

- size_small [FAIL] | RankIC=-0.040073 | IR=-0.149368 | Spread=-0.00438 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- size_small / first_half [FAIL] | RankIC=-0.035671 | Spread=-0.004922 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / second_half [PASS] | RankIC=0.267368 | Spread=0.023419 | Reason=n/a

## Factor Scores

- size_small | score=-0.16613 | rawIC=0.118708 | neutralIC=-0.040073 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- size_small | score=-0.16613 | cluster=size_small

## Graveyard

- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.178786
- Annual volatility: 0.210172
- Sharpe: 0.850665
- Max drawdown: -0.15432
- Avg turnover: 0.044872
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.178786
- Annual volatility: 0.210172
- Sharpe: 0.850665
- Max drawdown: -0.15432
- Avg turnover: 0.044872
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.020163
- Annual volatility: 0.127539
- Sharpe: -0.158092
- Max drawdown: -0.092417
- Avg turnover: 1.214194
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: -0.020163
- Annual volatility: 0.127539
- Sharpe: -0.158092
- Max drawdown: -0.092417
- Avg turnover: 1.214194
- Observations: 53
