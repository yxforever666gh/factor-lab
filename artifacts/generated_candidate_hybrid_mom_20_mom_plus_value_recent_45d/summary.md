# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_mom_plus_value [PASS]
- Expression: `(momentum_20) + (momentum_20 + earnings_yield)`
- RankIC mean: 0.312648
- RankIC IR: 1.138553
- Top-bottom spread mean: 0.027513
- Fail reason: n/a

## Neutralized Results (industry + size)

- hybrid_mom_20_mom_plus_value [PASS] | RankIC=0.093651 | IR=0.417609 | Spread=0.007218 | Reason=n/a

## Time Split Robustness

- hybrid_mom_20_mom_plus_value / first_half [PASS] | RankIC=0.280992 | Spread=0.017777 | Reason=n/a
- hybrid_mom_20_mom_plus_value / second_half [PASS] | RankIC=0.341667 | Spread=0.036437 | Reason=n/a

## Factor Scores

- hybrid_mom_20_mom_plus_value | score=1.533356 | rawIC=0.312648 | neutralIC=0.093651 | peers=none

## Candidate Pool

- hybrid_mom_20_mom_plus_value | rawIC=0.312648 | neutralIC=0.093651 | peers=none

## Cluster Representatives

- hybrid_mom_20_mom_plus_value | score=1.533356 | cluster=hybrid_mom_20_mom_plus_value

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.150172
- Annual volatility: 0.219657
- Sharpe: 5.236223
- Max drawdown: -0.040389
- Avg turnover: 0.439394
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.150172
- Annual volatility: 0.219657
- Sharpe: 5.236223
- Max drawdown: -0.040389
- Avg turnover: 0.439394
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.238772
- Annual volatility: 0.138763
- Sharpe: 1.720711
- Max drawdown: -0.063134
- Avg turnover: 1.457576
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.150172
- Annual volatility: 0.219657
- Sharpe: 5.236223
- Max drawdown: -0.040389
- Avg turnover: 0.439394
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.238772
- Annual volatility: 0.138763
- Sharpe: 1.720711
- Max drawdown: -0.063134
- Avg turnover: 1.457576
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.238772
- Annual volatility: 0.138763
- Sharpe: 1.720711
- Max drawdown: -0.063134
- Avg turnover: 1.457576
- Observations: 23
