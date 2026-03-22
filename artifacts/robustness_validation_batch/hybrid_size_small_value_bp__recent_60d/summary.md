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
- RankIC mean: 0.310635
- RankIC IR: 1.288631
- Top-bottom spread mean: 0.011787
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=-0.069675 | IR=-0.232591 | Spread=-0.000411 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [PASS] | RankIC=0.227617 | Spread=0.003549 | Reason=n/a
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.38877 | Spread=0.01954 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=0.560857 | rawIC=0.310635 | neutralIC=-0.069675 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=0.560857 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.94622
- Annual volatility: 0.24675
- Sharpe: 3.834723
- Max drawdown: -0.115124
- Avg turnover: 0.010417
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.94622
- Annual volatility: 0.24675
- Sharpe: 3.834723
- Max drawdown: -0.115124
- Avg turnover: 0.010417
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.191197
- Annual volatility: 0.182485
- Sharpe: -1.047741
- Max drawdown: -0.242665
- Avg turnover: 1.164521
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: -0.191197
- Annual volatility: 0.182485
- Sharpe: -1.047741
- Max drawdown: -0.242665
- Avg turnover: 1.164521
- Observations: 33
