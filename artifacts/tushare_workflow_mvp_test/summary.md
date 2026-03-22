# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 4
- Failed: 2
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.20636
- RankIC IR: 0.787172
- Top-bottom spread mean: 0.014848
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.10212
- RankIC IR: 0.422366
- Top-bottom spread mean: 0.010085
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.069642
- RankIC IR: 0.217067
- Top-bottom spread mean: 0.004898
- Fail reason: n/a

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.05647
- RankIC IR: 0.144243
- Top-bottom spread mean: 0.003979
- Fail reason: n/a

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.032023
- RankIC IR: 0.08133
- Top-bottom spread mean: -0.004654
- Fail reason: top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: 0.019462
- RankIC IR: 0.061247
- Top-bottom spread mean: 0.009014
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.053968 | IR=0.193837 | Spread=0.007453 | Reason=n/a
- value_bp [PASS] | RankIC=0.040301 | IR=0.137833 | Spread=0.00286 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.013324 | IR=0.060168 | Spread=0.000187 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.011664 | IR=0.041078 | Spread=0.000433 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=0.010007 | IR=0.035956 | Spread=0.001051 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.025524 | IR=-0.095899 | Spread=-0.008465 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.249738 | Spread=-0.029192 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.34876 | Spread=0.035642 | Reason=n/a
- value_ep / first_half [FAIL] | RankIC=0.020791 | Spread=-0.003593 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.179752 | Spread=0.023142 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=0.014742 | Spread=0.010499 | Reason=rank_ic_mean<0.02
- value_bp / second_half [PASS] | RankIC=0.023967 | Spread=0.007598 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.19614 | Spread=0.012202 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.216116 | Spread=0.017374 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.127962 | Spread=-0.018104 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.258264 | Spread=0.026854 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.249148 | Spread=-0.032689 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.300413 | Spread=0.022107 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.468569 | rawIC=0.069642 | neutralIC=0.053968 | peers=none
- size_small | score=0.357404 | rawIC=0.20636 | neutralIC=-0.025524 | peers=none
- value_bp | score=0.278745 | rawIC=0.019462 | neutralIC=0.040301 | peers=none
- value_ep | score=-0.046353 | rawIC=0.10212 | neutralIC=0.011664 | peers=none
- mom_20 | score=-0.307168 | rawIC=0.05647 | neutralIC=0.010007 | peers=mom_plus_value
- mom_plus_value | score=-0.367771 | rawIC=0.032023 | neutralIC=0.013324 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.468569 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.307168 | cluster=mom_20, mom_plus_value
- size_small | score=0.357404 | cluster=size_small
- value_bp | score=0.278745 | cluster=value_bp
- value_ep | score=-0.046353 | cluster=value_ep

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=split_fail_count:1
- mom_plus_value | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.303097
- Annual volatility: 0.299114
- Sharpe: 1.013319
- Max drawdown: -0.401967
- Avg turnover: 0.198413
- Observations: 43

### long_short_top_bottom_cluster_representatives
- Annual return: 0.156428
- Annual volatility: 0.273089
- Sharpe: 0.57281
- Max drawdown: -0.443068
- Avg turnover: 0.214286
- Observations: 43

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.087521
- Annual volatility: 0.181813
- Sharpe: 0.481376
- Max drawdown: -0.204052
- Avg turnover: 1.230159
- Observations: 43

### long_short_top_bottom_neutralized
- Annual return: 0.098638
- Annual volatility: 0.179979
- Sharpe: 0.548054
- Max drawdown: -0.149252
- Avg turnover: 1.230159
- Observations: 43
