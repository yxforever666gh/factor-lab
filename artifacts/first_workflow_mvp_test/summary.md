# Workflow Summary

- Data source: sample
- Total factors: 5
- Passed: 4
- Failed: 1
- Candidate pool size: 4
- Graveyard size: 1
- Cluster representative count: 4

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.981143
- RankIC IR: 64.992119
- Top-bottom spread mean: 0.762779
- Fail reason: n/a

### quality_roe [PASS]
- Expression: `roe`
- RankIC mean: 0.736473
- RankIC IR: 19.14013
- Top-bottom spread mean: 0.569508
- Fail reason: n/a

### quality_minus_value [PASS]
- Expression: `roe - pb`
- RankIC mean: 0.643788
- RankIC IR: 15.313021
- Top-bottom spread mean: 0.488012
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.575457
- RankIC IR: 12.79664
- Top-bottom spread mean: 0.436563
- Fail reason: n/a

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.009888
- RankIC IR: 0.08047
- Top-bottom spread mean: 0.016568
- Fail reason: rank_ic_mean<0.03

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.974702 | Spread=0.638487 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.987525 | Spread=0.88592 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.560469 | Spread=0.358154 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.590307 | Spread=0.514246 | Reason=n/a
- quality_roe / first_half [PASS] | RankIC=0.741887 | Spread=0.487331 | Reason=n/a
- quality_roe / second_half [PASS] | RankIC=0.731109 | Spread=0.650925 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=0.007422 | Spread=0.005028 | Reason=rank_ic_mean<0.03
- liquidity_turnover_shock / second_half [FAIL] | RankIC=0.012308 | Spread=0.027895 | Reason=rank_ic_mean<0.03
- quality_minus_value / first_half [PASS] | RankIC=0.629036 | Spread=0.396337 | Reason=n/a
- quality_minus_value / second_half [PASS] | RankIC=0.658403 | Spread=0.578839 | Reason=n/a

## Factor Scores

- mom_20 | score=12.001675 | rawIC=0.981143 | neutralIC=None | peers=none
- quality_roe | score=4.512202 | rawIC=0.736473 | neutralIC=None | peers=none
- quality_minus_value | score=3.606423 | rawIC=0.643788 | neutralIC=None | peers=value_ep
- value_ep | score=3.058138 | rawIC=0.575457 | neutralIC=None | peers=quality_minus_value
- liquidity_turnover_shock | score=-0.56321 | rawIC=0.009888 | neutralIC=None | peers=none

## Candidate Pool

- mom_20 | rawIC=0.981143 | neutralIC=None | peers=none
- value_ep | rawIC=0.575457 | neutralIC=None | peers=quality_minus_value
- quality_roe | rawIC=0.736473 | neutralIC=None | peers=none
- quality_minus_value | rawIC=0.643788 | neutralIC=None | peers=value_ep

## Cluster Representatives

- liquidity_turnover_shock | score=-0.56321 | cluster=liquidity_turnover_shock
- mom_20 | score=12.001675 | cluster=mom_20
- quality_minus_value | score=3.606423 | cluster=quality_minus_value, value_ep
- quality_roe | score=4.512202 | cluster=quality_roe

## Graveyard

- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.03; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 33.086334
- Annual volatility: 1.132936
- Sharpe: 29.204067
- Max drawdown: 0.0
- Avg turnover: 0.343458
- Observations: 215

### long_short_top_bottom_candidates_only
- Annual return: 34.75283
- Annual volatility: 1.166194
- Sharpe: 29.800205
- Max drawdown: 0.0
- Avg turnover: 0.262461
- Observations: 215

### long_short_top_bottom_cluster_representatives
- Annual return: 33.492212
- Annual volatility: 1.120587
- Sharpe: 29.888105
- Max drawdown: 0.0
- Avg turnover: 0.385514
- Observations: 215
