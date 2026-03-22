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
- RankIC mean: 0.029445
- RankIC IR: 0.08278
- Top-bottom spread mean: 0.001267
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_liquidity_turnover_shock_mom_20 [FAIL] | RankIC=0.034427 | IR=0.122399 | Spread=0.000183 | Reason=top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_liquidity_turnover_shock_mom_20 / first_half [FAIL] | RankIC=-0.050109 | Spread=-0.00576 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_liquidity_turnover_shock_mom_20 / second_half [PASS] | RankIC=0.106905 | Spread=0.008109 | Reason=n/a

## Factor Scores

- hybrid_liquidity_turnover_shock_mom_20 | score=-0.21069 | rawIC=0.029445 | neutralIC=0.034427 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_liquidity_turnover_shock_mom_20 | score=-0.21069 | cluster=hybrid_liquidity_turnover_shock_mom_20

## Graveyard

- hybrid_liquidity_turnover_shock_mom_20 | reason=neutral_fail:top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.186362
- Annual volatility: 0.236959
- Sharpe: 0.786473
- Max drawdown: -0.400873
- Avg turnover: 0.400901
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.186362
- Annual volatility: 0.236959
- Sharpe: 0.786473
- Max drawdown: -0.400873
- Avg turnover: 0.400901
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.063839
- Annual volatility: 0.204289
- Sharpe: 0.312491
- Max drawdown: -0.235274
- Avg turnover: 1.434846
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: 0.063839
- Annual volatility: 0.204289
- Sharpe: 0.312491
- Max drawdown: -0.235274
- Avg turnover: 1.434846
- Observations: 75
