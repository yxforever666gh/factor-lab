# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_liquidity_turnover_shock_value_ep [PASS]
- Expression: `(turnover_shock_5_20) + (earnings_yield)`
- RankIC mean: 0.037637
- RankIC IR: 0.110974
- Top-bottom spread mean: 0.003216
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_value_ep [PASS] | RankIC=0.052292 | IR=0.159716 | Spread=0.008347 | Reason=n/a

## Time Split Robustness

- hybrid_liquidity_turnover_shock_value_ep / first_half [FAIL] | RankIC=-0.120273 | Spread=-0.011331 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_liquidity_turnover_shock_value_ep / second_half [PASS] | RankIC=0.1897 | Spread=0.017224 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_value_ep | score=0.367615 | rawIC=0.037637 | neutralIC=0.052292 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_value_ep | score=0.367615 | cluster=hybrid_liquidity_turnover_shock_value_ep

## Graveyard

- hybrid_liquidity_turnover_shock_value_ep | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.165258
- Annual volatility: 0.210695
- Sharpe: 0.784347
- Max drawdown: -0.307315
- Avg turnover: 0.365385
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.165258
- Annual volatility: 0.210695
- Sharpe: 0.784347
- Max drawdown: -0.307315
- Avg turnover: 0.365385
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.454944
- Annual volatility: 0.205175
- Sharpe: 2.217346
- Max drawdown: -0.073973
- Avg turnover: 1.369872
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.454944
- Annual volatility: 0.205175
- Sharpe: 2.217346
- Max drawdown: -0.073973
- Avg turnover: 1.369872
- Observations: 53
