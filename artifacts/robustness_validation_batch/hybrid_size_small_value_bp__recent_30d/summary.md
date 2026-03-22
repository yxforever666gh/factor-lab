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
- RankIC mean: 0.379021
- RankIC IR: 1.823376
- Top-bottom spread mean: 0.016082
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_size_small_value_bp [FAIL] | RankIC=-0.126772 | IR=-0.568898 | Spread=3.4e-05 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_size_small_value_bp / first_half [PASS] | RankIC=0.412121 | Spread=0.012927 | Reason=n/a
- hybrid_size_small_value_bp / second_half [PASS] | RankIC=0.350649 | Spread=0.018787 | Reason=n/a

## Factor Scores

- hybrid_size_small_value_bp | score=0.640743 | rawIC=0.379021 | neutralIC=-0.126772 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_size_small_value_bp | score=0.640743 | cluster=hybrid_size_small_value_bp

## Graveyard

- hybrid_size_small_value_bp | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.153537
- Annual volatility: 0.267338
- Sharpe: 4.314908
- Max drawdown: -0.049408
- Avg turnover: 0.027778
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.153537
- Annual volatility: 0.267338
- Sharpe: 4.314908
- Max drawdown: -0.049408
- Avg turnover: 0.027778
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.389864
- Annual volatility: 0.194826
- Sharpe: -2.001093
- Max drawdown: -0.152792
- Avg turnover: 1.227646
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: -0.389864
- Annual volatility: 0.194826
- Sharpe: -2.001093
- Max drawdown: -0.152792
- Avg turnover: 1.227646
- Observations: 13
