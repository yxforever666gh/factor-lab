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

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.241958
- RankIC IR: 0.934412
- Top-bottom spread mean: 0.01588
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.035689 | IR=0.153709 | Spread=0.01327 | Reason=n/a
- mom_20 [FAIL] | RankIC=-0.016911 | IR=-0.077933 | Spread=0.006331 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.427273 | Spread=0.048109 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.327273 | Spread=0.038991 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=0.071212 | Spread=-0.011109 | Reason=top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.388312 | Spread=0.039014 | Reason=n/a

## Factor Scores

- mom_20 | score=0.959433 | rawIC=0.373427 | neutralIC=-0.016911 | peers=none
- liquidity_turnover_shock | score=0.952124 | rawIC=0.241958 | neutralIC=0.035689 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.952124 | cluster=liquidity_turnover_shock
- mom_20 | score=0.959433 | cluster=mom_20

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02
- liquidity_turnover_shock | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.503848
- Annual volatility: 0.193852
- Sharpe: 7.757717
- Max drawdown: -0.010113
- Avg turnover: 0.333333
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.503848
- Annual volatility: 0.193852
- Sharpe: 7.757717
- Max drawdown: -0.010113
- Avg turnover: 0.333333
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.66702
- Annual volatility: 0.198012
- Sharpe: 3.368591
- Max drawdown: -0.047222
- Avg turnover: 1.305556
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.66702
- Annual volatility: 0.198012
- Sharpe: 3.368591
- Max drawdown: -0.047222
- Avg turnover: 1.305556
- Observations: 13
