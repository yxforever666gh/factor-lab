# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 5
- Failed: 1
- Candidate pool size: 3
- Graveyard size: 3
- Cluster representative count: 5

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.281028
- RankIC IR: 1.103186
- Top-bottom spread mean: 0.019939
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.25336
- RankIC IR: 0.996098
- Top-bottom spread mean: 0.026287
- Fail reason: n/a

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.228458
- RankIC IR: 1.186231
- Top-bottom spread mean: 0.019391
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.192885
- RankIC IR: 1.201206
- Top-bottom spread mean: 0.023122
- Fail reason: n/a

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: 0.015415
- RankIC IR: 0.046651
- Top-bottom spread mean: 0.006746
- Fail reason: rank_ic_mean<0.02

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.07224 | IR=0.284445 | Spread=0.008624 | Reason=n/a
- value_bp [FAIL] | RankIC=0.06531 | IR=0.216249 | Spread=0.000247 | Reason=top_bottom_spread<0.0005
- mom_plus_value [PASS] | RankIC=0.036979 | IR=0.206675 | Spread=0.005281 | Reason=n/a
- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a
- value_ep [FAIL] | RankIC=0.0001 | IR=0.000361 | Spread=-0.002148 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.064864 | IR=-0.248092 | Spread=-0.018327 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.216529 | Spread=0.016736 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.171212 | Spread=0.028975 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.202479 | Spread=-0.010568 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.215152 | Spread=0.022616 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.209091 | Spread=0.025039 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.246212 | Spread=0.014214 | Reason=n/a
- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.252273 | Spread=0.017869 | Reason=n/a
- mom_plus_value / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.313636 | Spread=0.025869 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=1.299535 | rawIC=0.25336 | neutralIC=0.07224 | peers=none
- mom_20 | score=1.27731 | rawIC=0.326877 | neutralIC=0.024715 | peers=mom_plus_value
- mom_plus_value | score=1.178985 | rawIC=0.281028 | neutralIC=0.036979 | peers=mom_20
- value_ep | score=0.462693 | rawIC=0.192885 | neutralIC=0.0001 | peers=none
- size_small | score=0.354488 | rawIC=0.228458 | neutralIC=-0.064864 | peers=none
- value_bp | score=-0.158535 | rawIC=0.015415 | neutralIC=0.06531 | peers=none

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=mom_plus_value
- liquidity_turnover_shock | rawIC=0.25336 | neutralIC=0.07224 | peers=none
- mom_plus_value | rawIC=0.281028 | neutralIC=0.036979 | peers=mom_20

## Cluster Representatives

- liquidity_turnover_shock | score=1.299535 | cluster=liquidity_turnover_shock
- mom_20 | score=1.27731 | cluster=mom_20, mom_plus_value
- size_small | score=0.354488 | cluster=size_small
- value_bp | score=-0.158535 | cluster=value_bp
- value_ep | score=0.462693 | cluster=value_ep

## Graveyard

- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.637281
- Annual volatility: 0.15239
- Sharpe: 10.744009
- Max drawdown: -0.009419
- Avg turnover: 0.151515
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.44484
- Annual volatility: 0.194052
- Sharpe: 7.445619
- Max drawdown: -0.019324
- Avg turnover: 0.30303
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.311981
- Annual volatility: 0.146696
- Sharpe: 2.126716
- Max drawdown: -0.045615
- Avg turnover: 1.454545
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.528357
- Annual volatility: 0.150501
- Sharpe: 10.155104
- Max drawdown: -0.009419
- Avg turnover: 0.121212
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.304807
- Annual volatility: 0.153856
- Sharpe: -1.98112
- Max drawdown: -0.204052
- Avg turnover: 1.242424
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: -0.170917
- Annual volatility: 0.131956
- Sharpe: -1.295258
- Max drawdown: -0.149252
- Avg turnover: 1.242424
- Observations: 23
