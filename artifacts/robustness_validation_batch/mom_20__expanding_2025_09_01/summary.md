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
- RankIC mean: -0.02216
- RankIC IR: -0.060153
- Top-bottom spread mean: -0.001575
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=-0.020537 | IR=-0.067198 | Spread=-0.001743 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.040988 | Spread=-0.007083 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.003631 | Spread=0.003845 | Reason=rank_ic_mean<0.02

## Factor Scores

- mom_20 | score=-0.726034 | rawIC=-0.02216 | neutralIC=-0.020537 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=-0.726034 | cluster=mom_20

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.010698
- Annual volatility: 0.397494
- Sharpe: 0.026915
- Max drawdown: -0.683855
- Avg turnover: 0.406504
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: 0.010698
- Annual volatility: 0.397494
- Sharpe: 0.026915
- Max drawdown: -0.683855
- Avg turnover: 0.406504
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.071286
- Annual volatility: 0.30289
- Sharpe: -0.235352
- Max drawdown: -0.587518
- Avg turnover: 1.203523
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: -0.071286
- Annual volatility: 0.30289
- Sharpe: -0.235352
- Max drawdown: -0.587518
- Avg turnover: 1.203523
- Observations: 124
