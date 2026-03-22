# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### quality_minus_value [FAIL]
- Expression: `roe - pb`
- RankIC mean: 0.015415
- RankIC IR: 0.046651
- Top-bottom spread mean: 0.006746
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- quality_minus_value [FAIL] | RankIC=-0.006735 | IR=-0.032528 | Spread=0.002178 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- quality_minus_value / first_half [FAIL] | RankIC=-0.202479 | Spread=-0.010568 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- quality_minus_value / second_half [PASS] | RankIC=0.215152 | Spread=0.022616 | Reason=n/a

## Factor Scores

- quality_minus_value | score=-0.37467 | rawIC=0.015415 | neutralIC=-0.006735 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- quality_minus_value | score=-0.37467 | cluster=quality_minus_value

## Graveyard

- quality_minus_value | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:rank_ic_mean<0.02; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.00697
- Annual volatility: 0.190817
- Sharpe: -0.036528
- Max drawdown: -0.181611
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: -0.00697
- Annual volatility: 0.190817
- Sharpe: -0.036528
- Max drawdown: -0.181611
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.099058
- Annual volatility: 0.156538
- Sharpe: 0.632807
- Max drawdown: -0.11157
- Avg turnover: 1.522727
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.099058
- Annual volatility: 0.156538
- Sharpe: 0.632807
- Max drawdown: -0.11157
- Avg turnover: 1.522727
- Observations: 23
