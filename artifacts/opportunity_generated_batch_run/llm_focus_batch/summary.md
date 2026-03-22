# Workflow Summary

- Data source: tushare
- Total factors: 4
- Passed: 3
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 4
- Cluster representative count: 3

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.20636
- RankIC IR: 0.787172
- Top-bottom spread mean: 0.014848
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

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.053968 | IR=0.193837 | Spread=0.007453 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.013324 | IR=0.060168 | Spread=0.000187 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=0.010007 | IR=0.035956 | Spread=0.001051 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.025524 | IR=-0.095899 | Spread=-0.008465 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.249738 | Spread=-0.029192 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.34876 | Spread=0.035642 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.249148 | Spread=-0.032689 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.300413 | Spread=0.022107 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.127962 | Spread=-0.018104 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.258264 | Spread=0.026854 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.19614 | Spread=0.012202 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.216116 | Spread=0.017374 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.468569 | rawIC=0.069642 | neutralIC=0.053968 | peers=none
- size_small | score=0.357404 | rawIC=0.20636 | neutralIC=-0.025524 | peers=none
- mom_20 | score=-0.307168 | rawIC=0.05647 | neutralIC=0.010007 | peers=mom_plus_value
- mom_plus_value | score=-0.367771 | rawIC=0.032023 | neutralIC=0.013324 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.468569 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.307168 | cluster=mom_20, mom_plus_value
- size_small | score=0.357404 | cluster=size_small

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- mom_plus_value | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- liquidity_turnover_shock | reason=split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.340339
- Annual volatility: 0.296706
- Sharpe: 1.14706
- Max drawdown: -0.390072
- Avg turnover: 0.373016
- Observations: 43

### long_short_top_bottom_cluster_representatives
- Annual return: 0.304654
- Annual volatility: 0.266061
- Sharpe: 1.145053
- Max drawdown: -0.354495
- Avg turnover: 0.293651
- Observations: 43

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.497589
- Annual volatility: 0.156168
- Sharpe: 3.186232
- Max drawdown: -0.039481
- Avg turnover: 1.388889
- Observations: 43

### long_short_top_bottom_neutralized
- Annual return: 0.254141
- Annual volatility: 0.184445
- Sharpe: 1.37787
- Max drawdown: -0.081883
- Avg turnover: 1.325397
- Observations: 43
