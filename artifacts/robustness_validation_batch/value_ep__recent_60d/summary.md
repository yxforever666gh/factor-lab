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
- RankIC mean: 0.19863
- RankIC IR: 1.277065
- Top-bottom spread mean: 0.023033
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_ep [FAIL] | RankIC=0.008184 | IR=0.027017 | Spread=-0.000247 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- value_ep / first_half [PASS] | RankIC=0.204561 | Spread=0.016515 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.193048 | Spread=0.029168 | Reason=n/a

## Factor Scores

- value_ep | score=0.512687 | rawIC=0.19863 | neutralIC=0.008184 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_ep | score=0.512687 | cluster=value_ep

## Graveyard

- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.68818
- Annual volatility: 0.092103
- Sharpe: 7.471886
- Max drawdown: -0.01328
- Avg turnover: 0.0
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.68818
- Annual volatility: 0.092103
- Sharpe: 7.471886
- Max drawdown: -0.01328
- Avg turnover: 0.0
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.118396
- Annual volatility: 0.221047
- Sharpe: -0.535612
- Max drawdown: -0.197736
- Avg turnover: 1.367188
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: -0.118396
- Annual volatility: 0.221047
- Sharpe: -0.535612
- Max drawdown: -0.197736
- Avg turnover: 1.367188
- Observations: 33
