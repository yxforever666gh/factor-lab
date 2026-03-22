# Workflow Summary

- Data source: tushare
- Total factors: 2
- Passed: 2
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.373427
- RankIC IR: 1.84399
- Top-bottom spread mean: 0.043199
- Fail reason: n/a

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.360839
- RankIC IR: 1.562532
- Top-bottom spread mean: 0.030046
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_plus_value [PASS] | RankIC=0.029915 | IR=0.216665 | Spread=0.010638 | Reason=n/a
- mom_20 [FAIL] | RankIC=-0.016911 | IR=-0.077933 | Spread=0.006331 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.427273 | Spread=0.048109 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.327273 | Spread=0.038991 | Reason=n/a
- mom_plus_value / first_half [PASS] | RankIC=0.431818 | Spread=0.034079 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.3 | Spread=0.02659 | Reason=n/a

## Factor Scores

- mom_plus_value | score=1.426222 | rawIC=0.360839 | neutralIC=0.029915 | peers=mom_20
- mom_20 | score=0.859433 | rawIC=0.373427 | neutralIC=-0.016911 | peers=mom_plus_value

## Candidate Pool

- mom_plus_value | rawIC=0.360839 | neutralIC=0.029915 | peers=mom_20

## Cluster Representatives

- mom_plus_value | score=1.426222 | cluster=mom_20, mom_plus_value

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.618822
- Annual volatility: 0.22458
- Sharpe: 7.208236
- Max drawdown: -0.017559
- Avg turnover: 0.5
- Observations: 13

### long_short_top_bottom_candidates_only
- Annual return: 1.503326
- Annual volatility: 0.195115
- Sharpe: 7.704806
- Max drawdown: -0.017559
- Avg turnover: 0.472222
- Observations: 13

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.230106
- Annual volatility: 0.126886
- Sharpe: 1.813482
- Max drawdown: -0.049138
- Avg turnover: 1.444444
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.503326
- Annual volatility: 0.195115
- Sharpe: 7.704806
- Max drawdown: -0.017559
- Avg turnover: 0.472222
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.230106
- Annual volatility: 0.126886
- Sharpe: 1.813482
- Max drawdown: -0.049138
- Avg turnover: 1.444444
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.46617
- Annual volatility: 0.156952
- Sharpe: 2.970147
- Max drawdown: -0.041361
- Avg turnover: 1.5
- Observations: 13
