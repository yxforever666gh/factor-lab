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
- RankIC mean: 0.080675
- RankIC IR: 0.26247
- Top-bottom spread mean: 0.008305
- Fail reason: n/a

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.055046
- RankIC IR: 0.211556
- Top-bottom spread mean: -0.002349
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.001735
- RankIC IR: 0.004755
- Top-bottom spread mean: 0.001371
- Fail reason: rank_ic_mean<0.02

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.012015
- RankIC IR: -0.034852
- Top-bottom spread mean: -0.004682
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.041353
- RankIC IR: -0.137158
- Top-bottom spread mean: -0.014708
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.056391
- RankIC IR: -0.170243
- Top-bottom spread mean: -0.012503
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.052725 | IR=0.154618 | Spread=0.005736 | Reason=n/a
- mom_20 [PASS] | RankIC=0.032995 | IR=0.103766 | Spread=0.001723 | Reason=n/a
- value_ep [FAIL] | RankIC=0.00667 | IR=0.019632 | Spread=-0.004868 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp [FAIL] | RankIC=0.006484 | IR=0.019353 | Spread=-0.006201 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.003259 | IR=-0.011296 | Spread=0.000511 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.028774 | IR=-0.099155 | Spread=-0.00077 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=0.009327 | Spread=0.000217 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.005857 | Spread=0.002525 | Reason=rank_ic_mean<0.02
- value_ep / first_half [FAIL] | RankIC=0.034426 | Spread=-0.012278 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.075666 | Spread=0.007579 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.090877 | Spread=-0.029521 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=0.008172 | Spread=0.000106 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / first_half [PASS] | RankIC=0.051564 | Spread=0.006269 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.109785 | Spread=0.01034 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.175458 | Spread=-0.028044 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.062676 | Spread=0.003038 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.003578 | Spread=-0.002095 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.020453 | Spread=-0.007269 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- liquidity_turnover_shock | score=0.091661 | rawIC=-0.056391 | neutralIC=0.052725 | peers=none
- size_small | score=-0.045264 | rawIC=0.080675 | neutralIC=-0.028774 | peers=none
- mom_20 | score=-0.095964 | rawIC=0.001735 | neutralIC=0.032995 | peers=mom_plus_value
- value_ep | score=-0.210642 | rawIC=0.055046 | neutralIC=0.00667 | peers=none
- value_bp | score=-0.704504 | rawIC=-0.041353 | neutralIC=0.006484 | peers=none
- mom_plus_value | score=-0.745042 | rawIC=-0.012015 | neutralIC=-0.003259 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.091661 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.095964 | cluster=mom_20, mom_plus_value
- size_small | score=-0.045264 | cluster=size_small
- value_bp | score=-0.704504 | cluster=value_bp
- value_ep | score=-0.210642 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:2
- value_ep | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.060319
- Annual volatility: 0.375209
- Sharpe: -0.160763
- Max drawdown: -0.819919
- Avg turnover: 0.244949
- Observations: 133

### long_short_top_bottom_cluster_representatives
- Annual return: -0.444067
- Annual volatility: 0.33951
- Sharpe: -1.307963
- Max drawdown: -0.88461
- Avg turnover: 0.267677
- Observations: 133

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.146055
- Annual volatility: 0.281886
- Sharpe: 0.518134
- Max drawdown: -0.384856
- Avg turnover: 1.241162
- Observations: 133

### long_short_top_bottom_neutralized
- Annual return: 0.061372
- Annual volatility: 0.282149
- Sharpe: 0.217517
- Max drawdown: -0.497966
- Avg turnover: 1.330808
- Observations: 133
