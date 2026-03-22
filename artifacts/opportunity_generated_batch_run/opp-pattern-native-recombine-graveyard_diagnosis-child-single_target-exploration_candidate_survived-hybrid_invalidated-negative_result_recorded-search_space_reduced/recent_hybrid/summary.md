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
- RankIC mean: 0.041927
- RankIC IR: 0.127263
- Top-bottom spread mean: 0.003058
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.039025 | IR=0.128312 | Spread=0.005952 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.097193 | Spread=-0.011376 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.175895 | Spread=0.016957 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.340982 | rawIC=0.041927 | neutralIC=0.039025 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.340982 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.184787
- Annual volatility: 0.225942
- Sharpe: 0.817852
- Max drawdown: -0.291482
- Avg turnover: 0.391026
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.184787
- Annual volatility: 0.225942
- Sharpe: 0.817852
- Max drawdown: -0.291482
- Avg turnover: 0.391026
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.457704
- Annual volatility: 0.200198
- Sharpe: 2.286252
- Max drawdown: -0.091543
- Avg turnover: 1.390705
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.457704
- Annual volatility: 0.200198
- Sharpe: 2.286252
- Max drawdown: -0.091543
- Avg turnover: 1.390705
- Observations: 53
