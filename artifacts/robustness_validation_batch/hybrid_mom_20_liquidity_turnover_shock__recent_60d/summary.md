# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_liquidity_turnover_shock [PASS]
- Expression: `(momentum_20) + (turnover_shock_5_20)`
- RankIC mean: 0.161119
- RankIC IR: 0.507199
- Top-bottom spread mean: 0.014207
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_mom_20_liquidity_turnover_shock [PASS] | RankIC=0.074239 | IR=0.275121 | Spread=0.004071 | Reason=n/a

## Time Split Robustness

- hybrid_mom_20_liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.040419 | Spread=-0.005393 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_liquidity_turnover_shock / second_half [PASS] | RankIC=0.350802 | Spread=0.032653 | Reason=n/a

## Factor Scores

- hybrid_mom_20_liquidity_turnover_shock | score=0.801594 | rawIC=0.161119 | neutralIC=0.074239 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_mom_20_liquidity_turnover_shock | score=0.801594 | cluster=hybrid_mom_20_liquidity_turnover_shock

## Graveyard

- hybrid_mom_20_liquidity_turnover_shock | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.667073
- Annual volatility: 0.233258
- Sharpe: 2.859811
- Max drawdown: -0.207583
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.667073
- Annual volatility: 0.233258
- Sharpe: 2.859811
- Max drawdown: -0.207583
- Avg turnover: 0.375
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.328171
- Annual volatility: 0.173584
- Sharpe: 1.890566
- Max drawdown: -0.062818
- Avg turnover: 1.326042
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: 0.328171
- Annual volatility: 0.173584
- Sharpe: 1.890566
- Max drawdown: -0.062818
- Avg turnover: 1.326042
- Observations: 33
