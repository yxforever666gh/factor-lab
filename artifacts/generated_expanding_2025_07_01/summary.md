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
- RankIC mean: 0.082493
- RankIC IR: 0.268699
- Top-bottom spread mean: 0.007071
- Fail reason: n/a

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.028435
- RankIC IR: 0.104564
- Top-bottom spread mean: -0.004959
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.006296
- RankIC IR: 0.017554
- Top-bottom spread mean: 0.004036
- Fail reason: rank_ic_mean<0.02

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.020234
- RankIC IR: -0.060352
- Top-bottom spread mean: -0.003308
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.047363
- RankIC IR: -0.156317
- Top-bottom spread mean: -0.01306
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.050253
- RankIC IR: -0.154431
- Top-bottom spread mean: -0.010471
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.046976 | IR=0.151806 | Spread=0.00782 | Reason=n/a
- mom_20 [PASS] | RankIC=0.03149 | IR=0.099422 | Spread=0.001811 | Reason=n/a
- value_bp [FAIL] | RankIC=0.016336 | IR=0.049889 | Spread=-0.004817 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.012685 | IR=0.037886 | Spread=-0.004298 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=0.007055 | IR=0.02449 | Spread=0.002022 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.037742 | IR=-0.13175 | Spread=0.000701 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.026201 | Spread=0.006919 | Reason=n/a
- mom_20 / second_half [FAIL] | RankIC=-0.013609 | Spread=0.001153 | Reason=rank_ic_mean<0.02
- value_ep / first_half [FAIL] | RankIC=-0.004513 | Spread=-0.015413 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.061383 | Spread=0.005496 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.080873 | Spread=-0.024439 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=-0.013854 | Spread=-0.00168 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / first_half [PASS] | RankIC=0.04765 | Spread=0.00139 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.117336 | Spread=0.012751 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.148777 | Spread=-0.02175 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.048271 | Spread=0.000809 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.00942 | Spread=0.000225 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.031049 | Spread=-0.00684 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- mom_20 | score=0.112843 | rawIC=0.006296 | neutralIC=0.03149 | peers=mom_plus_value
- liquidity_turnover_shock | score=0.092131 | rawIC=-0.050253 | neutralIC=0.046976 | peers=none
- size_small | score=-0.066689 | rawIC=0.082493 | neutralIC=-0.037742 | peers=none
- value_ep | score=-0.275173 | rawIC=0.028435 | neutralIC=0.012685 | peers=none
- value_bp | score=-0.692847 | rawIC=-0.047363 | neutralIC=0.016336 | peers=none
- mom_plus_value | score=-0.738473 | rawIC=-0.020234 | neutralIC=0.007055 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.092131 | cluster=liquidity_turnover_shock
- mom_20 | score=0.112843 | cluster=mom_20, mom_plus_value
- size_small | score=-0.066689 | cluster=size_small
- value_bp | score=-0.692847 | cluster=value_bp
- value_ep | score=-0.275173 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.098901
- Annual volatility: 0.363383
- Sharpe: -0.272168
- Max drawdown: -0.819919
- Avg turnover: 0.253425
- Observations: 147

### long_short_top_bottom_cluster_representatives
- Annual return: -0.497206
- Annual volatility: 0.335991
- Sharpe: -1.479822
- Max drawdown: -0.915865
- Avg turnover: 0.267123
- Observations: 147

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.152003
- Annual volatility: 0.29727
- Sharpe: 0.51133
- Max drawdown: -0.487194
- Avg turnover: 1.295662
- Observations: 147

### long_short_top_bottom_neutralized
- Annual return: 0.124373
- Annual volatility: 0.293659
- Sharpe: 0.423528
- Max drawdown: -0.402424
- Avg turnover: 1.364155
- Observations: 147
