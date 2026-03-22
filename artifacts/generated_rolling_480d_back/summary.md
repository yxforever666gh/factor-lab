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
- RankIC mean: 0.042112
- RankIC IR: 0.129015
- Top-bottom spread mean: 0.003067
- Fail reason: n/a

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.014725
- RankIC IR: 0.044949
- Top-bottom spread mean: -0.005979
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.00066
- RankIC IR: -0.001963
- Top-bottom spread mean: -0.005816
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.02674
- RankIC IR: -0.084068
- Top-bottom spread mean: -0.004582
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.02728
- RankIC IR: -0.082714
- Top-bottom spread mean: 3.3e-05
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.028247
- RankIC IR: -0.082223
- Top-bottom spread mean: 0.003556
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- value_bp [PASS] | RankIC=0.029236 | IR=0.092293 | Spread=0.001887 | Reason=n/a
- value_ep [FAIL] | RankIC=0.026281 | IR=0.080318 | Spread=-0.000222 | Reason=top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=0.002099 | IR=0.006761 | Spread=-0.001441 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.011205 | IR=-0.036263 | Spread=-0.001346 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.015955 | IR=-0.052204 | Spread=-0.002524 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.021069 | IR=-0.071631 | Spread=-0.003766 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.059315 | Spread=2.9e-05 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=0.002821 | Spread=0.007083 | Reason=rank_ic_mean<0.02
- value_ep / first_half [FAIL] | RankIC=0.017158 | Spread=-0.006608 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [FAIL] | RankIC=0.012293 | Spread=-0.005349 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / first_half [FAIL] | RankIC=0.009978 | Spread=-0.002749 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=-0.011298 | Spread=-0.008884 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / first_half [PASS] | RankIC=0.054544 | Spread=0.004649 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.02968 | Spread=0.001485 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.014375 | Spread=0.003183 | Reason=rank_ic_mean<0.02
- liquidity_turnover_shock / second_half [FAIL] | RankIC=-0.039104 | Spread=-0.012347 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / first_half [FAIL] | RankIC=-0.035409 | Spread=-0.000162 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.01915 | Spread=0.000228 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- value_bp | score=-0.014236 | rawIC=-0.00066 | neutralIC=0.029236 | peers=none
- size_small | score=-0.138575 | rawIC=0.042112 | neutralIC=-0.021069 | peers=none
- value_ep | score=-0.477602 | rawIC=0.014725 | neutralIC=0.026281 | peers=none
- liquidity_turnover_shock | score=-0.673163 | rawIC=-0.02674 | neutralIC=0.002099 | peers=none
- mom_20 | score=-0.816566 | rawIC=-0.028247 | neutralIC=-0.011205 | peers=mom_plus_value
- mom_plus_value | score=-0.828472 | rawIC=-0.02728 | neutralIC=-0.015955 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.673163 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.816566 | cluster=mom_20, mom_plus_value
- size_small | score=-0.138575 | cluster=size_small
- value_bp | score=-0.014236 | cluster=value_bp
- value_ep | score=-0.477602 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- value_ep | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:2
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.183628
- Annual volatility: 0.332996
- Sharpe: -0.551442
- Max drawdown: -0.939638
- Avg turnover: 0.285256
- Observations: 365

### long_short_top_bottom_cluster_representatives
- Annual return: -0.345613
- Annual volatility: 0.314825
- Sharpe: -1.097794
- Max drawdown: -0.97798
- Avg turnover: 0.291667
- Observations: 365

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.043257
- Annual volatility: 0.31728
- Sharpe: 0.136337
- Max drawdown: -0.641608
- Avg turnover: 1.35119
- Observations: 365

### long_short_top_bottom_neutralized
- Annual return: -0.040169
- Annual volatility: 0.318502
- Sharpe: -0.126117
- Max drawdown: -0.743263
- Avg turnover: 1.387363
- Observations: 365
