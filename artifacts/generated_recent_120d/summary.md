# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 3
- Failed: 3
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.119732
- RankIC IR: 0.398788
- Top-bottom spread mean: 0.012238
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.066019
- RankIC IR: 0.28552
- Top-bottom spread mean: 0.00589
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.045728
- RankIC IR: 0.140603
- Top-bottom spread mean: 0.000566
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.003892
- RankIC IR: -0.010087
- Top-bottom spread mean: 0.002109
- Fail reason: rank_ic_mean<0.02

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.018801
- RankIC IR: -0.058883
- Top-bottom spread mean: -0.002617
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.022877
- RankIC IR: -0.063207
- Top-bottom spread mean: -0.006049
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_bp [PASS] | RankIC=0.041377 | IR=0.120181 | Spread=0.002741 | Reason=n/a
- value_ep [PASS] | RankIC=0.033652 | IR=0.099877 | Spread=0.0015 | Reason=n/a
- mom_20 [PASS] | RankIC=0.020075 | IR=0.069231 | Spread=0.001827 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.000964 | IR=0.003347 | Spread=-0.001378 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=-0.023103 | IR=-0.0777 | Spread=-0.000208 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.028211 | IR=-0.105371 | Spread=0.002459 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.114304 | Spread=-0.010425 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.103614 | Spread=0.014313 | Reason=n/a
- value_ep / first_half [FAIL] | RankIC=-0.030557 | Spread=-0.009033 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.160054 | Spread=0.02042 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.087018 | Spread=-0.015452 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.047621 | Spread=0.00988 | Reason=n/a
- size_small / first_half [FAIL] | RankIC=-0.001793 | Spread=0.007806 | Reason=rank_ic_mean<0.02
- size_small / second_half [PASS] | RankIC=0.238058 | Spread=0.016554 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-3.7e-05 | Spread=-0.004745 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.090289 | Spread=0.005736 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.137888 | Spread=-0.015462 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.089107 | Spread=0.003115 | Reason=n/a

## Factor Scores

- value_ep | score=0.408831 | rawIC=0.066019 | neutralIC=0.033652 | peers=none
- value_bp | score=0.168296 | rawIC=-0.018801 | neutralIC=0.041377 | peers=none
- mom_20 | score=0.048982 | rawIC=-0.003892 | neutralIC=0.020075 | peers=mom_plus_value
- size_small | score=-0.125485 | rawIC=0.119732 | neutralIC=-0.028211 | peers=none
- liquidity_turnover_shock | score=-0.333899 | rawIC=0.045728 | neutralIC=-0.023103 | peers=none
- mom_plus_value | score=-0.563782 | rawIC=-0.022877 | neutralIC=0.000964 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.333899 | cluster=liquidity_turnover_shock
- mom_20 | score=0.048982 | cluster=mom_20, mom_plus_value
- size_small | score=-0.125485 | cluster=size_small
- value_bp | score=0.168296 | cluster=value_bp
- value_ep | score=0.408831 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- liquidity_turnover_shock | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.136916
- Annual volatility: 0.294864
- Sharpe: 0.464336
- Max drawdown: -0.534953
- Avg turnover: 0.268018
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: -0.005443
- Annual volatility: 0.280506
- Sharpe: -0.019403
- Max drawdown: -0.566806
- Avg turnover: 0.247748
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.035843
- Annual volatility: 0.233508
- Sharpe: -0.153498
- Max drawdown: -0.204052
- Avg turnover: 1.313063
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: 0.03453
- Annual volatility: 0.227743
- Sharpe: 0.151618
- Max drawdown: -0.167598
- Avg turnover: 1.301802
- Observations: 75
