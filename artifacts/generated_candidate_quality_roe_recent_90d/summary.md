# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### quality_roe [PASS]
- Expression: `roe`
- RankIC mean: 0.105497
- RankIC IR: 0.462393
- Top-bottom spread mean: 0.008146
- Fail reason: n/a

## Neutralized Results (industry + size)

- quality_roe [PASS] | RankIC=0.034893 | IR=0.106874 | Spread=0.001275 | Reason=n/a

## Time Split Robustness

- quality_roe / first_half [FAIL] | RankIC=0.020287 | Spread=-0.00738 | Reason=top_bottom_spread<0.0005
- quality_roe / second_half [PASS] | RankIC=0.187551 | Spread=0.023098 | Reason=n/a

## Factor Scores

- quality_roe | score=0.53778 | rawIC=0.105497 | neutralIC=0.034893 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- quality_roe | score=0.53778 | cluster=quality_roe

## Graveyard

- quality_roe | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.261868
- Annual volatility: 0.167236
- Sharpe: 1.565856
- Max drawdown: -0.271241
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.261868
- Annual volatility: 0.167236
- Sharpe: 1.565856
- Max drawdown: -0.271241
- Avg turnover: 0.0
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.010748
- Annual volatility: 0.220158
- Sharpe: 0.048821
- Max drawdown: -0.197736
- Avg turnover: 1.419872
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.010748
- Annual volatility: 0.220158
- Sharpe: 0.048821
- Max drawdown: -0.197736
- Avg turnover: 1.419872
- Observations: 53
