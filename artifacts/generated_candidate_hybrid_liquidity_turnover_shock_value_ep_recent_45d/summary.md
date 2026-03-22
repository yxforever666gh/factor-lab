# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### hybrid_liquidity_turnover_shock_value_ep [PASS]
- Expression: `(turnover_shock_5_20) + (earnings_yield)`
- RankIC mean: 0.264822
- RankIC IR: 1.113596
- Top-bottom spread mean: 0.025738
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_value_ep [PASS] | RankIC=0.07192 | IR=0.259704 | Spread=0.011437 | Reason=n/a

## Time Split Robustness

- hybrid_liquidity_turnover_shock_value_ep / first_half [PASS] | RankIC=0.266942 | Spread=0.038268 | Reason=n/a
- hybrid_liquidity_turnover_shock_value_ep / second_half [PASS] | RankIC=0.262879 | Spread=0.014253 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_value_ep | score=1.344854 | rawIC=0.264822 | neutralIC=0.07192 | peers=none

## Candidate Pool

- hybrid_liquidity_turnover_shock_value_ep | rawIC=0.264822 | neutralIC=0.07192 | peers=none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_value_ep | score=1.344854 | cluster=hybrid_liquidity_turnover_shock_value_ep

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.056899
- Annual volatility: 0.210332
- Sharpe: 5.024902
- Max drawdown: -0.090345
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.056899
- Annual volatility: 0.210332
- Sharpe: 5.024902
- Max drawdown: -0.090345
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.51082
- Annual volatility: 0.189245
- Sharpe: 2.699255
- Max drawdown: -0.070182
- Avg turnover: 1.393182
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.056899
- Annual volatility: 0.210332
- Sharpe: 5.024902
- Max drawdown: -0.090345
- Avg turnover: 0.363636
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.51082
- Annual volatility: 0.189245
- Sharpe: 2.699255
- Max drawdown: -0.070182
- Avg turnover: 1.393182
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.51082
- Annual volatility: 0.189245
- Sharpe: 2.699255
- Max drawdown: -0.070182
- Avg turnover: 1.393182
- Observations: 23
