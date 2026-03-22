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
- RankIC mean: 0.066019
- RankIC IR: 0.28552
- Top-bottom spread mean: 0.00589
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_ep [PASS] | RankIC=0.033652 | IR=0.099877 | Spread=0.0015 | Reason=n/a

## Time Split Robustness

- value_ep / first_half [FAIL] | RankIC=-0.030557 | Spread=-0.009033 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.160054 | Spread=0.02042 | Reason=n/a

## Factor Scores

- value_ep | score=0.408831 | rawIC=0.066019 | neutralIC=0.033652 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_ep | score=0.408831 | cluster=value_ep

## Graveyard

- value_ep | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.182214
- Annual volatility: 0.153301
- Sharpe: 1.188605
- Max drawdown: -0.271241
- Avg turnover: 0.022523
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.182214
- Annual volatility: 0.153301
- Sharpe: 1.188605
- Max drawdown: -0.271241
- Avg turnover: 0.022523
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.067718
- Annual volatility: 0.252855
- Sharpe: 0.267813
- Max drawdown: -0.197736
- Avg turnover: 1.43018
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: 0.067718
- Annual volatility: 0.252855
- Sharpe: 0.267813
- Max drawdown: -0.197736
- Avg turnover: 1.43018
- Observations: 75
