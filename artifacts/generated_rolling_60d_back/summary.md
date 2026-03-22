# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 2
- Failed: 4
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.12577
- RankIC IR: 0.427964
- Top-bottom spread mean: 0.011068
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.084848
- RankIC IR: 0.352361
- Top-bottom spread mean: 0.006008
- Fail reason: n/a

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.029943
- RankIC IR: 0.08943
- Top-bottom spread mean: -0.006037
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.01218
- RankIC IR: 0.032195
- Top-bottom spread mean: 0.005066
- Fail reason: rank_ic_mean<0.02

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.001261
- RankIC IR: 0.003534
- Top-bottom spread mean: -0.001492
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.043669
- RankIC IR: -0.139232
- Top-bottom spread mean: -0.010926
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_bp [FAIL] | RankIC=0.033319 | IR=0.100306 | Spread=-0.000854 | Reason=top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.021499 | IR=0.065431 | Spread=-0.00149 | Reason=top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.008782 | IR=-0.029061 | Spread=-0.002602 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=-0.012769 | IR=-0.038339 | Spread=-0.003162 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.023799 | IR=-0.078768 | Spread=-0.006458 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.029611 | IR=-0.104625 | Spread=0.000207 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.03211 | Spread=0.006153 | Reason=rank_ic_mean<0.02
- mom_20 / second_half [PASS] | RankIC=0.05647 | Spread=0.003979 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.067576 | Spread=0.001931 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.10212 | Spread=0.010085 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.1068 | Spread=-0.030867 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=0.019462 | Spread=0.009014 | Reason=rank_ic_mean<0.02
- size_small / first_half [PASS] | RankIC=0.04518 | Spread=0.007288 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.20636 | Spread=0.014848 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.009757 | Spread=-0.016972 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.069642 | Spread=0.004898 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.029501 | Spread=0.00167 | Reason=rank_ic_mean<0.02
- mom_plus_value / second_half [FAIL] | RankIC=0.032023 | Spread=-0.004654 | Reason=top_bottom_spread<0.0005

## Factor Scores

- value_ep | score=0.129471 | rawIC=0.084848 | neutralIC=0.021499 | peers=none
- size_small | score=0.089787 | rawIC=0.12577 | neutralIC=-0.029611 | peers=none
- liquidity_turnover_shock | score=-0.350035 | rawIC=0.029943 | neutralIC=-0.012769 | peers=none
- mom_20 | score=-0.491067 | rawIC=0.01218 | neutralIC=-0.008782 | peers=mom_plus_value
- value_bp | score=-0.6301 | rawIC=-0.043669 | neutralIC=0.033319 | peers=none
- mom_plus_value | score=-0.767714 | rawIC=0.001261 | neutralIC=-0.023799 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.350035 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.491067 | cluster=mom_20, mom_plus_value
- size_small | score=0.089787 | cluster=size_small
- value_bp | score=-0.6301 | cluster=value_bp
- value_ep | score=0.129471 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- value_ep | reason=neutral_fail:top_bottom_spread<0.0005
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.252412
- Annual volatility: 0.289556
- Sharpe: 0.871723
- Max drawdown: -0.534953
- Avg turnover: 0.264706
- Observations: 86

### long_short_top_bottom_cluster_representatives
- Annual return: 0.08369
- Annual volatility: 0.278036
- Sharpe: 0.301005
- Max drawdown: -0.576392
- Avg turnover: 0.235294
- Observations: 86

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.270278
- Annual volatility: 0.314264
- Sharpe: -0.860036
- Max drawdown: -0.506729
- Avg turnover: 1.313725
- Observations: 86

### long_short_top_bottom_neutralized
- Annual return: -0.199917
- Annual volatility: 0.316144
- Sharpe: -0.632361
- Max drawdown: -0.5199
- Avg turnover: 1.331373
- Observations: 86
