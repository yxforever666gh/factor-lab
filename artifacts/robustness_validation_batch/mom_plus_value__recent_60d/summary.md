# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.171479
- RankIC IR: 0.514258
- Top-bottom spread mean: 0.008658
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_plus_value [PASS] | RankIC=0.023883 | IR=0.117015 | Spread=0.002809 | Reason=n/a

## Time Split Robustness

- mom_plus_value / first_half [FAIL] | RankIC=0.011062 | Spread=-0.010499 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.32246 | Spread=0.026688 | Reason=n/a

## Factor Scores

- mom_plus_value | score=0.677485 | rawIC=0.171479 | neutralIC=0.023883 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_plus_value | score=0.677485 | cluster=mom_plus_value

## Graveyard

- mom_plus_value | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.762081
- Annual volatility: 0.22164
- Sharpe: 3.438374
- Max drawdown: -0.144102
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.762081
- Annual volatility: 0.22164
- Sharpe: 3.438374
- Max drawdown: -0.144102
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.003567
- Annual volatility: 0.129084
- Sharpe: -0.027633
- Max drawdown: -0.104982
- Avg turnover: 1.351562
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: -0.003567
- Annual volatility: 0.129084
- Sharpe: -0.027633
- Max drawdown: -0.104982
- Avg turnover: 1.351562
- Observations: 33
