# Workflow Summary

- Data source: tushare
- Total factors: 2
- Passed: 2
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 2
- Cluster representative count: 1

## Main Results

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.171479
- RankIC IR: 0.514258
- Top-bottom spread mean: 0.008658
- Fail reason: n/a

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.154574
- RankIC IR: 0.408926
- Top-bottom spread mean: 0.02216
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_plus_value [PASS] | RankIC=0.023883 | IR=0.117015 | Spread=0.002809 | Reason=n/a
- mom_20 [FAIL] | RankIC=0.005525 | IR=0.020276 | Spread=-0.000811 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.036872 | Spread=0.010548 | Reason=rank_ic_mean<0.02
- mom_20 / second_half [PASS] | RankIC=0.334759 | Spread=0.033088 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=0.011062 | Spread=-0.010499 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.32246 | Spread=0.026688 | Reason=n/a

## Factor Scores

- mom_plus_value | score=0.577485 | rawIC=0.171479 | neutralIC=0.023883 | peers=mom_20
- mom_20 | score=-0.035651 | rawIC=0.154574 | neutralIC=0.005525 | peers=mom_plus_value

## Candidate Pool

- none

## Cluster Representatives

- mom_plus_value | score=0.577485 | cluster=mom_20, mom_plus_value

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.820677
- Annual volatility: 0.263293
- Sharpe: 3.116969
- Max drawdown: -0.175923
- Avg turnover: 0.40625
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
- Annual return: 0.220994
- Annual volatility: 0.154966
- Sharpe: 1.426082
- Max drawdown: -0.10123
- Avg turnover: 1.380208
- Observations: 33
