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
- RankIC mean: 0.173177
- RankIC IR: 0.515116
- Top-bottom spread mean: 0.005315
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=-0.049623 | IR=-0.163495 | Spread=-0.002346 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [FAIL] | RankIC=-0.006637 | Spread=-0.00572 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.346332 | Spread=0.015941 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=-0.038659 | rawIC=0.173177 | neutralIC=-0.049623 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=-0.038659 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.439928
- Annual volatility: 0.242068
- Sharpe: 1.817369
- Max drawdown: -0.243878
- Avg turnover: 0.00641
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.439928
- Annual volatility: 0.242068
- Sharpe: 1.817369
- Max drawdown: -0.243878
- Avg turnover: 0.00641
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.18815
- Annual volatility: 0.176893
- Sharpe: -1.063636
- Max drawdown: -0.274134
- Avg turnover: 1.18647
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: -0.18815
- Annual volatility: 0.176893
- Sharpe: -1.063636
- Max drawdown: -0.274134
- Avg turnover: 1.18647
- Observations: 53
