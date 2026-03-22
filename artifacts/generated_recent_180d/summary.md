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
- RankIC mean: 0.139053
- RankIC IR: 0.469715
- Top-bottom spread mean: 0.013812
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.079335
- RankIC IR: 0.325436
- Top-bottom spread mean: 0.005694
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.018772
- RankIC IR: 0.048624
- Top-bottom spread mean: 0.006331
- Fail reason: rank_ic_mean<0.02

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.012603
- RankIC IR: 0.037254
- Top-bottom spread mean: -0.007775
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.010844
- RankIC IR: 0.029824
- Top-bottom spread mean: -0.00029
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.050918
- RankIC IR: -0.164041
- Top-bottom spread mean: -0.011721
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=0.02501 | IR=0.076408 | Spread=0.00035 | Reason=top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.003885 | IR=0.011883 | Spread=-0.003137 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp [FAIL] | RankIC=0.003332 | IR=0.009692 | Spread=-0.004168 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.004398 | IR=-0.014557 | Spread=-0.001011 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.014736 | IR=-0.048496 | Spread=-0.003626 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=-0.024904 | IR=-0.075127 | Spread=-0.005035 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.002546 | Spread=0.009632 | Reason=rank_ic_mean<0.02
- mom_20 / second_half [PASS] | RankIC=0.039617 | Spread=0.003105 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.063152 | Spread=0.001899 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.095157 | Spread=0.009404 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.114781 | Spread=-0.031672 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [FAIL] | RankIC=0.011526 | Spread=0.007786 | Reason=rank_ic_mean<0.02
- size_small / first_half [PASS] | RankIC=0.101703 | Spread=0.014513 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.175573 | Spread=0.013127 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.053723 | Spread=-0.019829 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.077456 | Spread=0.004012 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=0.009854 | Spread=0.004848 | Reason=rank_ic_mean<0.02
- mom_plus_value / second_half [FAIL] | RankIC=0.011812 | Spread=-0.005313 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- size_small | score=0.173882 | rawIC=0.139053 | neutralIC=-0.014736 | peers=none
- value_ep | score=0.058808 | rawIC=0.079335 | neutralIC=0.003885 | peers=none
- mom_20 | score=-0.370746 | rawIC=0.018772 | neutralIC=0.02501 | peers=mom_plus_value
- liquidity_turnover_shock | score=-0.437616 | rawIC=0.012603 | neutralIC=-0.024904 | peers=none
- mom_plus_value | score=-0.68161 | rawIC=0.010844 | neutralIC=-0.004398 | peers=mom_20
- value_bp | score=-0.741905 | rawIC=-0.050918 | neutralIC=0.003332 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.437616 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.370746 | cluster=mom_20, mom_plus_value
- size_small | score=0.173882 | cluster=size_small
- value_bp | score=-0.741905 | cluster=value_bp
- value_ep | score=0.058808 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.321854
- Annual volatility: 0.293907
- Sharpe: 1.095087
- Max drawdown: -0.534953
- Avg turnover: 0.266284
- Observations: 88

### long_short_top_bottom_cluster_representatives
- Annual return: 0.173041
- Annual volatility: 0.300258
- Sharpe: 0.576306
- Max drawdown: -0.595183
- Avg turnover: 0.235632
- Observations: 88

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.045983
- Annual volatility: 0.249546
- Sharpe: -0.184268
- Max drawdown: -0.381661
- Avg turnover: 1.402299
- Observations: 88

### long_short_top_bottom_neutralized
- Annual return: 0.04484
- Annual volatility: 0.262071
- Sharpe: 0.171098
- Max drawdown: -0.268551
- Avg turnover: 1.369732
- Observations: 88
