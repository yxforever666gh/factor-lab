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
- RankIC mean: 0.119679
- RankIC IR: 0.370409
- Top-bottom spread mean: 0.003845
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=0.001873 | IR=0.006047 | Spread=-0.00795 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [PASS] | RankIC=0.076598 | Spread=0.003701 | Reason=n/a
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.162076 | Spread=0.003986 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=0.160378 | rawIC=0.119679 | neutralIC=0.001873 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=0.160378 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.314617
- Annual volatility: 0.332925
- Sharpe: 0.945011
- Max drawdown: -0.451031
- Avg turnover: 0.02439
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: 0.314617
- Annual volatility: 0.332925
- Sharpe: 0.945011
- Max drawdown: -0.451031
- Avg turnover: 0.02439
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.002083
- Annual volatility: 0.237758
- Sharpe: 0.008762
- Max drawdown: -0.39361
- Avg turnover: 1.202074
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: 0.002083
- Annual volatility: 0.237758
- Sharpe: 0.008762
- Max drawdown: -0.39361
- Avg turnover: 1.202074
- Observations: 124
