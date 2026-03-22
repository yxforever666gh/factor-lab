# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 4
- Failed: 2
- Candidate pool size: 1
- Graveyard size: 5
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.118708
- RankIC IR: 0.382127
- Top-bottom spread mean: 0.009516
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.105497
- RankIC IR: 0.462393
- Top-bottom spread mean: 0.008146
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.041927
- RankIC IR: 0.127263
- Top-bottom spread mean: 0.003058
- Fail reason: n/a

### value_bp [PASS]
- Expression: `book_yield`
- RankIC mean: 0.027047
- RankIC IR: 0.083513
- Top-bottom spread mean: 0.00716
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.018454
- RankIC IR: -0.045048
- Top-bottom spread mean: -0.001331
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.030055
- RankIC IR: -0.076528
- Top-bottom spread mean: -0.007053
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_bp [PASS] | RankIC=0.05901 | IR=0.182456 | Spread=0.003883 | Reason=n/a
- liquidity_turnover_shock [PASS] | RankIC=0.039025 | IR=0.128312 | Spread=0.005952 | Reason=n/a
- value_ep [PASS] | RankIC=0.034893 | IR=0.106874 | Spread=0.001275 | Reason=n/a
- mom_20 [FAIL] | RankIC=0.014449 | IR=0.050922 | Spread=0.001609 | Reason=rank_ic_mean<0.02
- mom_plus_value [FAIL] | RankIC=0.011813 | IR=0.043223 | Spread=-0.000582 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.040073 | IR=-0.149368 | Spread=-0.00438 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.246046 | Spread=-0.024239 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.200709 | Spread=0.020728 | Reason=n/a
- value_ep / first_half [FAIL] | RankIC=0.020287 | Spread=-0.00738 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.187551 | Spread=0.023098 | Reason=n/a
- value_bp / first_half [PASS] | RankIC=0.026793 | Spread=0.006463 | Reason=n/a
- value_bp / second_half [PASS] | RankIC=0.027291 | Spread=0.007831 | Reason=n/a
- size_small / first_half [FAIL] | RankIC=-0.035671 | Spread=-0.004922 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / second_half [PASS] | RankIC=0.267368 | Spread=0.023419 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.097193 | Spread=-0.011376 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.175895 | Spread=0.016957 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.252688 | Spread=-0.025889 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.184333 | Spread=0.011086 | Reason=n/a

## Factor Scores

- value_bp | score=0.557174 | rawIC=0.027047 | neutralIC=0.05901 | peers=none
- value_ep | score=0.53778 | rawIC=0.105497 | neutralIC=0.034893 | peers=none
- liquidity_turnover_shock | score=0.340982 | rawIC=0.041927 | neutralIC=0.039025 | peers=none
- size_small | score=-0.16613 | rawIC=0.118708 | neutralIC=-0.040073 | peers=none
- mom_20 | score=-0.509545 | rawIC=-0.018454 | neutralIC=0.014449 | peers=mom_plus_value
- mom_plus_value | score=-0.551178 | rawIC=-0.030055 | neutralIC=0.011813 | peers=mom_20

## Candidate Pool

- value_bp | rawIC=0.027047 | neutralIC=0.05901 | peers=none

## Cluster Representatives

- liquidity_turnover_shock | score=0.340982 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.509545 | cluster=mom_20, mom_plus_value
- size_small | score=-0.16613 | cluster=size_small
- value_bp | score=0.557174 | cluster=value_bp
- value_ep | score=0.53778 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- liquidity_turnover_shock | reason=split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.069495
- Annual volatility: 0.283388
- Sharpe: 0.245228
- Max drawdown: -0.521357
- Avg turnover: 0.224359
- Observations: 53

### long_short_top_bottom_candidates_only
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.301426
- Annual volatility: 0.221123
- Sharpe: 1.363157
- Max drawdown: -0.133535
- Avg turnover: 1.395833
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: -0.048268
- Annual volatility: 0.262983
- Sharpe: -0.18354
- Max drawdown: -0.554679
- Avg turnover: 0.230769
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.099089
- Annual volatility: 0.186617
- Sharpe: 0.530973
- Max drawdown: -0.204052
- Avg turnover: 1.25
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.084736
- Annual volatility: 0.189352
- Sharpe: 0.447506
- Max drawdown: -0.149252
- Avg turnover: 1.230769
- Observations: 53
