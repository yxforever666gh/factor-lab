# Workflow Summary

- Data source: tushare
- Total factors: 2
- Passed: 2
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 2
- Cluster representative count: 2

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.373427
- RankIC IR: 1.84399
- Top-bottom spread mean: 0.043199
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.18951
- RankIC IR: 1.150538
- Top-bottom spread mean: 0.031375
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_ep [FAIL] | RankIC=0.034718 | IR=0.147384 | Spread=-0.000197 | Reason=top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.016911 | IR=-0.077933 | Spread=0.006331 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.427273 | Spread=0.048109 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.327273 | Spread=0.038991 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.227273 | Spread=0.036792 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.157143 | Spread=0.026732 | Reason=n/a

## Factor Scores

- mom_20 | score=0.959433 | rawIC=0.373427 | neutralIC=-0.016911 | peers=none
- value_ep | score=0.55051 | rawIC=0.18951 | neutralIC=0.034718 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=0.959433 | cluster=mom_20
- value_ep | score=0.55051 | cluster=value_ep

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02
- value_ep | reason=neutral_fail:top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.621951
- Annual volatility: 0.179382
- Sharpe: 9.041887
- Max drawdown: -0.017559
- Avg turnover: 0.444444
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.621951
- Annual volatility: 0.179382
- Sharpe: 9.041887
- Max drawdown: -0.017559
- Avg turnover: 0.444444
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.12196
- Annual volatility: 0.217796
- Sharpe: 0.55997
- Max drawdown: -0.101526
- Avg turnover: 1.472222
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.12196
- Annual volatility: 0.217796
- Sharpe: 0.55997
- Max drawdown: -0.101526
- Avg turnover: 1.472222
- Observations: 13
