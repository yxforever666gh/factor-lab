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
- RankIC mean: 0.140548
- RankIC IR: 0.437562
- Top-bottom spread mean: 0.005977
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=-0.029716 | IR=-0.096676 | Spread=-0.001315 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [FAIL] | RankIC=-0.037613 | Spread=-0.002571 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.314021 | Spread=0.014299 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=-0.072144 | rawIC=0.140548 | neutralIC=-0.029716 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=-0.072144 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.503005
- Annual volatility: 0.232637
- Sharpe: 2.162188
- Max drawdown: -0.256656
- Avg turnover: 0.022523
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.503005
- Annual volatility: 0.232637
- Sharpe: 2.162188
- Max drawdown: -0.256656
- Avg turnover: 0.022523
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.040657
- Annual volatility: 0.19208
- Sharpe: -0.211664
- Max drawdown: -0.315676
- Avg turnover: 1.186122
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: -0.040657
- Annual volatility: 0.19208
- Sharpe: -0.211664
- Max drawdown: -0.315676
- Avg turnover: 1.186122
- Observations: 75
