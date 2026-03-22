# Workflow Summary

- Data source: tushare
- Total factors: 3
- Passed: 3
- Failed: 0
- Candidate pool size: 3
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

### hybrid_mom_20_mom_plus_value [PASS]
- Expression: `(momentum_20) + (momentum_20 + earnings_yield)`
- RankIC mean: 0.312648
- RankIC IR: 1.138553
- Top-bottom spread mean: 0.027513
- Fail reason: n/a

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.281028
- RankIC IR: 1.103186
- Top-bottom spread mean: 0.019939
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_mom_20_mom_plus_value [PASS] | RankIC=0.093651 | IR=0.417609 | Spread=0.007218 | Reason=n/a
- mom_plus_value [PASS] | RankIC=0.036979 | IR=0.206675 | Spread=0.005281 | Reason=n/a
- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a
- mom_plus_value / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.313636 | Spread=0.025869 | Reason=n/a
- hybrid_mom_20_mom_plus_value / first_half [PASS] | RankIC=0.280992 | Spread=0.017777 | Reason=n/a
- hybrid_mom_20_mom_plus_value / second_half [PASS] | RankIC=0.341667 | Spread=0.036437 | Reason=n/a

## Factor Scores

- hybrid_mom_20_mom_plus_value | score=1.333356 | rawIC=0.312648 | neutralIC=0.093651 | peers=mom_20, mom_plus_value
- mom_20 | score=1.17731 | rawIC=0.326877 | neutralIC=0.024715 | peers=hybrid_mom_20_mom_plus_value, mom_plus_value
- mom_plus_value | score=1.078985 | rawIC=0.281028 | neutralIC=0.036979 | peers=hybrid_mom_20_mom_plus_value, mom_20

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=hybrid_mom_20_mom_plus_value, mom_plus_value
- mom_plus_value | rawIC=0.281028 | neutralIC=0.036979 | peers=hybrid_mom_20_mom_plus_value, mom_20
- hybrid_mom_20_mom_plus_value | rawIC=0.312648 | neutralIC=0.093651 | peers=mom_20, mom_plus_value

## Cluster Representatives

- hybrid_mom_20_mom_plus_value | score=1.333356 | cluster=hybrid_mom_20_mom_plus_value, mom_20, mom_plus_value

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.13173
- Annual volatility: 0.243129
- Sharpe: 4.654863
- Max drawdown: -0.07239
- Avg turnover: 0.424242
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.13173
- Annual volatility: 0.243129
- Sharpe: 4.654863
- Max drawdown: -0.07239
- Avg turnover: 0.424242
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.369946
- Annual volatility: 0.164607
- Sharpe: 2.247453
- Max drawdown: -0.074092
- Avg turnover: 1.484848
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.150172
- Annual volatility: 0.219657
- Sharpe: 5.236223
- Max drawdown: -0.040389
- Avg turnover: 0.439394
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.238772
- Annual volatility: 0.138763
- Sharpe: 1.720711
- Max drawdown: -0.063134
- Avg turnover: 1.457576
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.369946
- Annual volatility: 0.164607
- Sharpe: 2.247453
- Max drawdown: -0.074092
- Avg turnover: 1.484848
- Observations: 23
