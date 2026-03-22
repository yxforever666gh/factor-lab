# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.360839
- RankIC IR: 1.562532
- Top-bottom spread mean: 0.030046
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_plus_value [PASS] | RankIC=0.029915 | IR=0.216665 | Spread=0.010638 | Reason=n/a

## Time Split Robustness

- mom_plus_value / first_half [PASS] | RankIC=0.431818 | Spread=0.034079 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.3 | Spread=0.02659 | Reason=n/a

## Factor Scores

- mom_plus_value | score=1.526222 | rawIC=0.360839 | neutralIC=0.029915 | peers=none

## Candidate Pool

- mom_plus_value | rawIC=0.360839 | neutralIC=0.029915 | peers=none

## Cluster Representatives

- mom_plus_value | score=1.526222 | cluster=mom_plus_value

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.503326
- Annual volatility: 0.195115
- Sharpe: 7.704806
- Max drawdown: -0.017559
- Avg turnover: 0.472222
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
- Annual return: 0.230106
- Annual volatility: 0.126886
- Sharpe: 1.813482
- Max drawdown: -0.049138
- Avg turnover: 1.444444
- Observations: 13
