# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.192885
- RankIC IR: 1.201206
- Top-bottom spread mean: 0.023122
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_ep [FAIL] | RankIC=0.0001 | IR=0.000361 | Spread=-0.002148 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- value_ep / first_half [PASS] | RankIC=0.216529 | Spread=0.016736 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.171212 | Spread=0.028975 | Reason=n/a

## Factor Scores

- value_ep | score=0.462693 | rawIC=0.192885 | neutralIC=0.0001 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_ep | score=0.462693 | cluster=value_ep

## Graveyard

- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.677025
- Annual volatility: 0.094452
- Sharpe: 7.167898
- Max drawdown: -0.01328
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 0.677025
- Annual volatility: 0.094452
- Sharpe: 7.167898
- Max drawdown: -0.01328
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.236425
- Annual volatility: 0.184866
- Sharpe: -1.278905
- Max drawdown: -0.191745
- Avg turnover: 1.348485
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: -0.236425
- Annual volatility: 0.184866
- Sharpe: -1.278905
- Max drawdown: -0.191745
- Avg turnover: 1.348485
- Observations: 23
