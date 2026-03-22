# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 1
- Failed: 5
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.09862
- RankIC IR: 0.336968
- Top-bottom spread mean: 0.008121
- Fail reason: n/a

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.053537
- RankIC IR: 0.200914
- Top-bottom spread mean: -0.002764
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.021855
- RankIC IR: -0.059101
- Top-bottom spread mean: -0.000907
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.034972
- RankIC IR: -0.102032
- Top-bottom spread mean: -0.006829
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.044702
- RankIC IR: -0.14346
- Top-bottom spread mean: -0.016163
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.04986
- RankIC IR: -0.150833
- Top-bottom spread mean: -0.013827
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.036128 | IR=0.110341 | Spread=0.001096 | Reason=n/a
- value_ep [FAIL] | RankIC=0.026185 | IR=0.080276 | Spread=-0.004468 | Reason=top_bottom_spread<0.0005
- value_bp [FAIL] | RankIC=0.024272 | IR=0.073573 | Spread=-0.00542 | Reason=top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.017577 | IR=-0.057266 | Spread=-0.001989 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.031159 | IR=-0.100039 | Spread=-0.007293 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.052254 | IR=-0.188862 | Spread=-0.003546 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.03391 | Spread=-0.005508 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.009995 | Spread=0.003619 | Reason=rank_ic_mean<0.02
- value_ep / first_half [FAIL] | RankIC=0.0083 | Spread=-0.014963 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.098044 | Spread=0.009239 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.114164 | Spread=-0.034014 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.02364 | Spread=0.001401 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.062624 | Spread=0.002742 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.134035 | Spread=0.013413 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.146131 | Spread=-0.030039 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.044857 | Spread=0.002125 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.050884 | Spread=-0.008321 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.019316 | Spread=-0.005361 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- liquidity_turnover_shock | score=0.061109 | rawIC=-0.04986 | neutralIC=0.036128 | peers=none
- size_small | score=-0.059667 | rawIC=0.09862 | neutralIC=-0.052254 | peers=none
- value_ep | score=-0.157465 | rawIC=0.053537 | neutralIC=0.026185 | peers=none
- value_bp | score=-0.460458 | rawIC=-0.044702 | neutralIC=0.024272 | peers=none
- mom_20 | score=-0.816234 | rawIC=-0.021855 | neutralIC=-0.017577 | peers=mom_plus_value
- mom_plus_value | score=-0.896212 | rawIC=-0.034972 | neutralIC=-0.031159 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.061109 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.816234 | cluster=mom_20, mom_plus_value
- size_small | score=-0.059667 | cluster=size_small
- value_bp | score=-0.460458 | cluster=value_bp
- value_ep | score=-0.157465 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- value_ep | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.247157
- Annual volatility: 0.372424
- Sharpe: -0.663644
- Max drawdown: -0.841892
- Avg turnover: 0.260331
- Observations: 122

### long_short_top_bottom_cluster_representatives
- Annual return: -0.463893
- Annual volatility: 0.347253
- Sharpe: -1.335895
- Max drawdown: -0.8741
- Avg turnover: 0.250689
- Observations: 122

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.26356
- Annual volatility: 0.333794
- Sharpe: -0.789591
- Max drawdown: -0.641608
- Avg turnover: 1.210744
- Observations: 122

### long_short_top_bottom_neutralized
- Annual return: -0.143637
- Annual volatility: 0.330848
- Sharpe: -0.434147
- Max drawdown: -0.639034
- Avg turnover: 1.294766
- Observations: 122
