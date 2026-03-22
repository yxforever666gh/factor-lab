# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### quality_minus_value [PASS]
- Expression: `roe - pb`
- RankIC mean: 0.027047
- RankIC IR: 0.083513
- Top-bottom spread mean: 0.00716
- Fail reason: n/a

## Neutralized Results (industry + size)

- quality_minus_value [FAIL] | RankIC=-0.013387 | IR=-0.054308 | Spread=0.003254 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- quality_minus_value / first_half [PASS] | RankIC=0.026793 | Spread=0.006463 | Reason=n/a
- quality_minus_value / second_half [PASS] | RankIC=0.027291 | Spread=0.007831 | Reason=n/a

## Factor Scores

- quality_minus_value | score=-0.160017 | rawIC=0.027047 | neutralIC=-0.013387 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- quality_minus_value | score=-0.160017 | cluster=quality_minus_value

## Graveyard

- quality_minus_value | reason=neutral_fail:rank_ic_mean<0.02

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.136105
- Annual volatility: 0.153434
- Sharpe: 0.887059
- Max drawdown: -0.141525
- Avg turnover: 1.483013
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.136105
- Annual volatility: 0.153434
- Sharpe: 0.887059
- Max drawdown: -0.141525
- Avg turnover: 1.483013
- Observations: 53
