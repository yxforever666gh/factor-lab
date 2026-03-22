# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_size_small_value_bp [PASS]
- Expression: `(size_inv) + (book_yield)`
- RankIC mean: 0.314625
- RankIC IR: 1.311615
- Top-bottom spread mean: 0.013115
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=-0.043739 | IR=-0.135223 | Spread=0.000361 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [PASS] | RankIC=0.235537 | Spread=0.010396 | Reason=n/a
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.387121 | Spread=0.015607 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=0.652088 | rawIC=0.314625 | neutralIC=-0.043739 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=0.652088 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.151497
- Annual volatility: 0.238346
- Sharpe: 4.831209
- Max drawdown: -0.049408
- Avg turnover: 0.015152
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.151497
- Annual volatility: 0.238346
- Sharpe: 4.831209
- Max drawdown: -0.049408
- Avg turnover: 0.015152
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.247808
- Annual volatility: 0.17357
- Sharpe: -1.427708
- Max drawdown: -0.158753
- Avg turnover: 1.210155
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: -0.247808
- Annual volatility: 0.17357
- Sharpe: -1.427708
- Max drawdown: -0.158753
- Avg turnover: 1.210155
- Observations: 23
