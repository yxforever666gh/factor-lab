# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.281028
- RankIC IR: 1.103186
- Top-bottom spread mean: 0.019939
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_plus_value [PASS] | RankIC=0.036979 | IR=0.206675 | Spread=0.005281 | Reason=n/a

## Time Split Robustness

- mom_plus_value / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.313636 | Spread=0.025869 | Reason=n/a

## Factor Scores

- mom_plus_value | score=1.278985 | rawIC=0.281028 | neutralIC=0.036979 | peers=none

## Candidate Pool

- mom_plus_value | rawIC=0.281028 | neutralIC=0.036979 | peers=none

## Cluster Representatives

- mom_plus_value | score=1.278985 | cluster=mom_plus_value

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.142231
- Annual volatility: 0.1858
- Sharpe: 6.147649
- Max drawdown: -0.017559
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.142231
- Annual volatility: 0.1858
- Sharpe: 6.147649
- Max drawdown: -0.017559
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.008589
- Annual volatility: 0.132347
- Sharpe: 0.064894
- Max drawdown: -0.084474
- Avg turnover: 1.409091
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.142231
- Annual volatility: 0.1858
- Sharpe: 6.147649
- Max drawdown: -0.017559
- Avg turnover: 0.378788
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.008589
- Annual volatility: 0.132347
- Sharpe: 0.064894
- Max drawdown: -0.084474
- Avg turnover: 1.409091
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.008589
- Annual volatility: 0.132347
- Sharpe: 0.064894
- Max drawdown: -0.084474
- Avg turnover: 1.409091
- Observations: 23
