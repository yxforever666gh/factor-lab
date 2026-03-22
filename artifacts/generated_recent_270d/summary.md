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
- RankIC mean: 0.066698
- RankIC IR: 0.213928
- Top-bottom spread mean: 0.005932
- Fail reason: n/a

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.024332
- RankIC IR: 0.09074
- Top-bottom spread mean: -0.006942
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.003663
- RankIC IR: 0.010291
- Top-bottom spread mean: 0.004487
- Fail reason: rank_ic_mean<0.02

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.022773
- RankIC IR: -0.068737
- Top-bottom spread mean: -0.003301
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.044728
- RankIC IR: -0.148965
- Top-bottom spread mean: -0.012908
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.057901
- RankIC IR: -0.173077
- Top-bottom spread mean: -0.012028
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.046559 | IR=0.145365 | Spread=0.010381 | Reason=n/a
- value_bp [FAIL] | RankIC=0.028003 | IR=0.084657 | Spread=-0.004254 | Reason=top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.026034 | IR=0.076512 | Spread=-0.003356 | Reason=top_bottom_spread<0.0005
- mom_20 [PASS] | RankIC=0.025657 | IR=0.080195 | Spread=0.001258 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.013834 | IR=0.047895 | Spread=0.003005 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.038838 | IR=-0.135038 | Spread=0.000563 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=0.01691 | Spread=0.009253 | Reason=rank_ic_mean<0.02
- mom_20 / second_half [FAIL] | RankIC=-0.009415 | Spread=-0.000219 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / first_half [FAIL] | RankIC=-0.024297 | Spread=-0.020339 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.072338 | Spread=0.006283 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.05736 | Spread=-0.01766 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=-0.032258 | Spread=-0.008218 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small / first_half [PASS] | RankIC=0.027872 | Spread=0.002038 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.105025 | Spread=0.009777 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.135908 | Spread=-0.01785 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [FAIL] | RankIC=0.019105 | Spread=-0.00628 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / first_half [FAIL] | RankIC=-0.022259 | Spread=0.001228 | Reason=rank_ic_mean<0.02
- mom_plus_value / second_half [FAIL] | RankIC=-0.023279 | Spread=-0.007773 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- mom_20 | score=-0.112328 | rawIC=0.003663 | neutralIC=0.025657 | peers=mom_plus_value
- size_small | score=-0.11768 | rawIC=0.066698 | neutralIC=-0.038838 | peers=none
- liquidity_turnover_shock | score=-0.131037 | rawIC=-0.057901 | neutralIC=0.046559 | peers=none
- value_ep | score=-0.247457 | rawIC=0.024332 | neutralIC=0.026034 | peers=none
- value_bp | score=-0.650156 | rawIC=-0.044728 | neutralIC=0.028003 | peers=none
- mom_plus_value | score=-0.725741 | rawIC=-0.022773 | neutralIC=0.013834 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.131037 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.112328 | cluster=mom_20, mom_plus_value
- size_small | score=-0.11768 | cluster=size_small
- value_bp | score=-0.650156 | cluster=value_bp
- value_ep | score=-0.247457 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:2
- value_ep | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.097001
- Annual volatility: 0.357989
- Sharpe: -0.27096
- Max drawdown: -0.819919
- Avg turnover: 0.254902
- Observations: 154

### long_short_top_bottom_cluster_representatives
- Annual return: -0.524465
- Annual volatility: 0.336086
- Sharpe: -1.560505
- Max drawdown: -0.919861
- Avg turnover: 0.272331
- Observations: 154

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.172945
- Annual volatility: 0.302448
- Sharpe: 0.571816
- Max drawdown: -0.438289
- Avg turnover: 1.298475
- Observations: 154

### long_short_top_bottom_neutralized
- Annual return: 0.255192
- Annual volatility: 0.282194
- Sharpe: 0.904313
- Max drawdown: -0.366032
- Avg turnover: 1.356209
- Observations: 154
