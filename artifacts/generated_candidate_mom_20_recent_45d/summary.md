# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.326877
- RankIC IR: 1.239817
- Top-bottom spread mean: 0.032424
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_20 [PASS] | RankIC=0.024715 | IR=0.09713 | Spread=0.005112 | Reason=n/a

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.337121 | Spread=0.04043 | Reason=n/a

## Factor Scores

- mom_20 | score=1.37731 | rawIC=0.326877 | neutralIC=0.024715 | peers=none

## Candidate Pool

- mom_20 | rawIC=0.326877 | neutralIC=0.024715 | peers=none

## Cluster Representatives

- mom_20 | score=1.37731 | cluster=mom_20

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.248567
- Annual volatility: 0.205801
- Sharpe: 6.066854
- Max drawdown: -0.035216
- Avg turnover: 0.393939
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.248567
- Annual volatility: 0.205801
- Sharpe: 6.066854
- Max drawdown: -0.035216
- Avg turnover: 0.393939
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.118519
- Annual volatility: 0.154894
- Sharpe: 0.765163
- Max drawdown: -0.075456
- Avg turnover: 1.42197
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.248567
- Annual volatility: 0.205801
- Sharpe: 6.066854
- Max drawdown: -0.035216
- Avg turnover: 0.393939
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.118519
- Annual volatility: 0.154894
- Sharpe: 0.765163
- Max drawdown: -0.075456
- Avg turnover: 1.42197
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.118519
- Annual volatility: 0.154894
- Sharpe: 0.765163
- Max drawdown: -0.075456
- Avg turnover: 1.42197
- Observations: 23
