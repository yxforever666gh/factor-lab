# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: 0.015415
- RankIC IR: 0.046651
- Top-bottom spread mean: 0.006746
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- value_bp [FAIL] | RankIC=0.06531 | IR=0.216249 | Spread=0.000247 | Reason=top_bottom_spread<0.0005

## Time Split Robustness

- value_bp / first_half [FAIL] | RankIC=-0.202479 | Spread=-0.010568 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.215152 | Spread=0.022616 | Reason=n/a

## Factor Scores

- value_bp | score=-0.158535 | rawIC=0.015415 | neutralIC=0.06531 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_bp | score=-0.158535 | cluster=value_bp

## Graveyard

- value_bp | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1

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
- Annual return: -0.00178
- Annual volatility: 0.228253
- Sharpe: -0.007799
- Max drawdown: -0.133535
- Avg turnover: 1.458333
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: -0.00178
- Annual volatility: 0.228253
- Sharpe: -0.007799
- Max drawdown: -0.133535
- Avg turnover: 1.458333
- Observations: 23
