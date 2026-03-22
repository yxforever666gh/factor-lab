# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_liquidity_turnover_shock_mom_20 [PASS]
- Expression: `(turnover_shock_5_20) + (momentum_20)`
- RankIC mean: 0.026816
- RankIC IR: 0.074696
- Top-bottom spread mean: 0.001501
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_mom_20 [PASS] | RankIC=0.053361 | IR=0.186562 | Spread=0.001578 | Reason=n/a

## Time Split Robustness

- hybrid_liquidity_turnover_shock_mom_20 / first_half [FAIL] | RankIC=-0.162072 | Spread=-0.01889 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_liquidity_turnover_shock_mom_20 / second_half [PASS] | RankIC=0.208708 | Spread=0.021137 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_mom_20 | score=0.338327 | rawIC=0.026816 | neutralIC=0.053361 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_mom_20 | score=0.338327 | cluster=hybrid_liquidity_turnover_shock_mom_20

## Graveyard

- hybrid_liquidity_turnover_shock_mom_20 | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.170572
- Annual volatility: 0.232061
- Sharpe: 0.735031
- Max drawdown: -0.369458
- Avg turnover: 0.384615
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: 0.170572
- Annual volatility: 0.232061
- Sharpe: 0.735031
- Max drawdown: -0.369458
- Avg turnover: 0.384615
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.215561
- Annual volatility: 0.193805
- Sharpe: 1.112259
- Max drawdown: -0.109682
- Avg turnover: 1.373947
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: 0.215561
- Annual volatility: 0.193805
- Sharpe: 1.112259
- Max drawdown: -0.109682
- Avg turnover: 1.373947
- Observations: 53
