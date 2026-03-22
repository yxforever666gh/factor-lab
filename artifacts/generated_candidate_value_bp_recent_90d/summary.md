# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### value_bp [PASS]
- Expression: `book_yield`
- RankIC mean: 0.027047
- RankIC IR: 0.083513
- Top-bottom spread mean: 0.00716
- Fail reason: n/a

## Neutralized Results (industry + size)

- value_bp [PASS] | RankIC=0.05901 | IR=0.182456 | Spread=0.003883 | Reason=n/a

## Time Split Robustness

- value_bp / first_half [PASS] | RankIC=0.026793 | Spread=0.006463 | Reason=n/a
- value_bp / second_half [PASS] | RankIC=0.027291 | Spread=0.007831 | Reason=n/a

## Factor Scores

- value_bp | score=0.557174 | rawIC=0.027047 | neutralIC=0.05901 | peers=none

## Candidate Pool

- value_bp | rawIC=0.027047 | neutralIC=0.05901 | peers=none

## Cluster Representatives

- value_bp | score=0.557174 | cluster=value_bp

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_candidates_only
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.301426
- Annual volatility: 0.221123
- Sharpe: 1.363157
- Max drawdown: -0.133535
- Avg turnover: 1.395833
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.148136
- Annual volatility: 0.163909
- Sharpe: 0.903766
- Max drawdown: -0.216953
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.301426
- Annual volatility: 0.221123
- Sharpe: 1.363157
- Max drawdown: -0.133535
- Avg turnover: 1.395833
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.301426
- Annual volatility: 0.221123
- Sharpe: 1.363157
- Max drawdown: -0.133535
- Avg turnover: 1.395833
- Observations: 53
