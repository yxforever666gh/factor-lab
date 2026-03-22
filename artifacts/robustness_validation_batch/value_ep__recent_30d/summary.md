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
- RankIC mean: 0.18951
- RankIC IR: 1.150538
- Top-bottom spread mean: 0.031375
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_ep [FAIL] | RankIC=0.034718 | IR=0.147384 | Spread=-0.000197 | Reason=top_bottom_spread<0.0005

## Time Split Robustness

- value_ep / first_half [PASS] | RankIC=0.227273 | Spread=0.036792 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.157143 | Spread=0.026732 | Reason=n/a

## Factor Scores

- value_ep | score=0.55051 | rawIC=0.18951 | neutralIC=0.034718 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_ep | score=0.55051 | cluster=value_ep

## Graveyard

- value_ep | reason=neutral_fail:top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.976395
- Annual volatility: 0.085716
- Sharpe: 11.391007
- Max drawdown: 0.0
- Avg turnover: 0.0
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 0.976395
- Annual volatility: 0.085716
- Sharpe: 11.391007
- Max drawdown: 0.0
- Avg turnover: 0.0
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.24981
- Annual volatility: 0.211484
- Sharpe: -1.181223
- Max drawdown: -0.113771
- Avg turnover: 1.444444
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: -0.24981
- Annual volatility: 0.211484
- Sharpe: -1.181223
- Max drawdown: -0.113771
- Avg turnover: 1.444444
- Observations: 13
