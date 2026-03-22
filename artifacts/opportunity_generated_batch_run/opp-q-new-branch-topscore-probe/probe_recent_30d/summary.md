# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.241958
- RankIC IR: 0.934412
- Top-bottom spread mean: 0.01588
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.035689 | IR=0.153709 | Spread=0.01327 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=0.071212 | Spread=-0.011109 | Reason=top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.388312 | Spread=0.039014 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.952124 | rawIC=0.241958 | neutralIC=0.035689 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.952124 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.849303
- Annual volatility: 0.30513
- Sharpe: 2.783416
- Max drawdown: -0.090345
- Avg turnover: 0.388889
- Observations: 13

### long_short_top_bottom_cluster_representatives
- Annual return: 0.849303
- Annual volatility: 0.30513
- Sharpe: 2.783416
- Max drawdown: -0.090345
- Avg turnover: 0.388889
- Observations: 13

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.703112
- Annual volatility: 0.21274
- Sharpe: 3.305032
- Max drawdown: -0.0361
- Avg turnover: 1.291667
- Observations: 13

### long_short_top_bottom_neutralized
- Annual return: 0.703112
- Annual volatility: 0.21274
- Sharpe: 3.305032
- Max drawdown: -0.0361
- Avg turnover: 1.291667
- Observations: 13
