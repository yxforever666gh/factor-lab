# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.373427
- RankIC IR: 1.84399
- Top-bottom spread mean: 0.043199
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=-0.016911 | IR=-0.077933 | Spread=0.006331 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.427273 | Spread=0.048109 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.327273 | Spread=0.038991 | Reason=n/a

## Factor Scores

- mom_20 | score=0.959433 | rawIC=0.373427 | neutralIC=-0.016911 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=0.959433 | cluster=mom_20

## Graveyard

- mom_20 | reason=neutral_fail:rank_ic_mean<0.02

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.412435
- Annual volatility: 0.193369
- Sharpe: 7.304352
- Max drawdown: -0.017559
- Avg turnover: 0.416667
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 1.412435
- Annual volatility: 0.193369
- Sharpe: 7.304352
- Max drawdown: -0.017559
- Avg turnover: 0.416667
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.222133
- Annual volatility: 0.159
- Sharpe: 1.397064
- Max drawdown: -0.051468
- Avg turnover: 1.483333
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.222133
- Annual volatility: 0.159
- Sharpe: 1.397064
- Max drawdown: -0.051468
- Avg turnover: 1.483333
- Observations: 13
