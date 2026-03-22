# Workflow Summary

- Data source: tushare
- Total factors: 3
- Passed: 3
- Failed: 0
- Candidate pool size: 2
- Graveyard size: 1
- Cluster representative count: 2

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

### hybrid_mom_20_value_ep [PASS]
- Expression: `(momentum_20) + (earnings_yield)`
- RankIC mean: 0.281028
- RankIC IR: 1.103186
- Top-bottom spread mean: 0.019939
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.192885
- RankIC IR: 1.201206
- Top-bottom spread mean: 0.023122
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_mom_20_value_ep [PASS] | RankIC=0.036979 | IR=0.206675 | Spread=0.005281 | Reason=n/a
- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a
- value_ep [FAIL] | RankIC=0.0001 | IR=0.000361 | Spread=-0.002148 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.216529 | Spread=0.016736 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.171212 | Spread=0.028975 | Reason=n/a
- hybrid_mom_20_value_ep / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- hybrid_mom_20_value_ep / second_half [PASS] | RankIC=0.313636 | Spread=0.025869 | Reason=n/a

## Factor Scores

- mom_20 | score=1.27731 | rawIC=0.326877 | neutralIC=0.024715 | peers=hybrid_mom_20_value_ep
- hybrid_mom_20_value_ep | score=1.178985 | rawIC=0.281028 | neutralIC=0.036979 | peers=mom_20
- value_ep | score=0.462693 | rawIC=0.192885 | neutralIC=0.0001 | peers=none

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=hybrid_mom_20_value_ep
- hybrid_mom_20_value_ep | rawIC=0.281028 | neutralIC=0.036979 | peers=mom_20

## Cluster Representatives

- mom_20 | score=1.27731 | cluster=hybrid_mom_20_value_ep, mom_20
- value_ep | score=0.462693 | cluster=value_ep

## Graveyard

- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.193344
- Annual volatility: 0.179843
- Sharpe: 6.635497
- Max drawdown: -0.017559
- Avg turnover: 0.348485
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.275793
- Annual volatility: 0.222125
- Sharpe: 5.743572
- Max drawdown: -0.034234
- Avg turnover: 0.424242
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.241998
- Annual volatility: 0.167963
- Sharpe: 1.440781
- Max drawdown: -0.10123
- Avg turnover: 1.454545
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.20928
- Annual volatility: 0.179644
- Sharpe: 6.731531
- Max drawdown: -0.017559
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.043007
- Annual volatility: 0.19661
- Sharpe: 0.218743
- Max drawdown: -0.131083
- Avg turnover: 1.363636
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.159929
- Annual volatility: 0.183118
- Sharpe: 0.87337
- Max drawdown: -0.104033
- Avg turnover: 1.473485
- Observations: 23
