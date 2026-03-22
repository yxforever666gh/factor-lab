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
- RankIC mean: 0.121829
- RankIC IR: 0.412814
- Top-bottom spread mean: 0.011251
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.075406
- RankIC IR: 0.300594
- Top-bottom spread mean: 0.00334
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.001033
- RankIC IR: -0.002714
- Top-bottom spread mean: -0.000673
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.006899
- RankIC IR: -0.019568
- Top-bottom spread mean: -0.007473
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.023973
- RankIC IR: -0.070569
- Top-bottom spread mean: -0.011049
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.056383
- RankIC IR: -0.18462
- Top-bottom spread mean: -0.015204
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_bp [FAIL] | RankIC=0.040497 | IR=0.120294 | Spread=-0.002082 | Reason=top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.036495 | IR=0.109792 | Spread=-0.00146 | Reason=top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=0.015009 | IR=0.043684 | Spread=-0.001156 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.025844 | IR=-0.082982 | Spread=-0.004193 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.042282 | IR=-0.135426 | Spread=-0.010656 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.046538 | IR=-0.162874 | Spread=-0.000259 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=0.00711 | Spread=-0.000553 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.009019 | Spread=-0.000791 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / first_half [FAIL] | RankIC=0.050322 | Spread=-0.001266 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.100008 | Spread=0.007857 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.13467 | Spread=-0.037839 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.020399 | Spread=0.006996 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.114129 | Spread=0.012175 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.129382 | Spread=0.010345 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.0927 | Spread=-0.025446 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.043433 | Spread=0.003071 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=0.014269 | Spread=-0.008015 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.027661 | Spread=-0.006943 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- size_small | score=0.026881 | rawIC=0.121829 | neutralIC=-0.046538 | peers=none
- value_ep | score=-0.056911 | rawIC=0.075406 | neutralIC=0.036495 | peers=none
- liquidity_turnover_shock | score=-0.425491 | rawIC=-0.023973 | neutralIC=0.015009 | peers=none
- value_bp | score=-0.44716 | rawIC=-0.056383 | neutralIC=0.040497 | peers=none
- mom_20 | score=-0.780522 | rawIC=-0.001033 | neutralIC=-0.025844 | peers=mom_plus_value
- mom_plus_value | score=-0.847029 | rawIC=-0.006899 | neutralIC=-0.042282 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.425491 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.780522 | cluster=mom_20, mom_plus_value
- size_small | score=0.026881 | cluster=size_small
- value_bp | score=-0.44716 | cluster=value_bp
- value_ep | score=-0.056911 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- value_ep | reason=neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.078514
- Annual volatility: 0.363536
- Sharpe: -0.215972
- Max drawdown: -0.674554
- Avg turnover: 0.268977
- Observations: 102

### long_short_top_bottom_cluster_representatives
- Annual return: -0.147242
- Annual volatility: 0.349114
- Sharpe: -0.421759
- Max drawdown: -0.694088
- Avg turnover: 0.250825
- Observations: 102

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.256604
- Annual volatility: 0.323518
- Sharpe: -0.793168
- Max drawdown: -0.611247
- Avg turnover: 1.288779
- Observations: 102

### long_short_top_bottom_neutralized
- Annual return: -0.258302
- Annual volatility: 0.324077
- Sharpe: -0.797038
- Max drawdown: -0.639034
- Avg turnover: 1.339934
- Observations: 102
