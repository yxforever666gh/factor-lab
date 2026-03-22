# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_mom_plus_value [FAIL]
- Expression: `(momentum_20) + (momentum_20 + earnings_yield)`
- RankIC mean: -0.029456
- RankIC IR: -0.070675
- Top-bottom spread mean: -0.003775
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- hybrid_mom_20_mom_plus_value [PASS] | RankIC=0.027923 | IR=0.100772 | Spread=0.000822 | Reason=n/a

## Time Split Robustness

- hybrid_mom_20_mom_plus_value / first_half [FAIL] | RankIC=-0.267024 | Spread=-0.024831 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_mom_plus_value / second_half [PASS] | RankIC=0.199314 | Spread=0.016502 | Reason=n/a

## Factor Scores

- hybrid_mom_20_mom_plus_value | score=0.099528 | rawIC=-0.029456 | neutralIC=0.027923 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_mom_20_mom_plus_value | score=0.099528 | cluster=hybrid_mom_20_mom_plus_value

## Graveyard

- hybrid_mom_20_mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.241621
- Annual volatility: 0.29361
- Sharpe: -0.822931
- Max drawdown: -0.571957
- Avg turnover: 0.455128
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: -0.241621
- Annual volatility: 0.29361
- Sharpe: -0.822931
- Max drawdown: -0.571957
- Avg turnover: 0.455128
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.002516
- Annual volatility: 0.173599
- Sharpe: -0.014495
- Max drawdown: -0.255223
- Avg turnover: 1.460897
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: -0.002516
- Annual volatility: 0.173599
- Sharpe: -0.014495
- Max drawdown: -0.255223
- Avg turnover: 1.460897
- Observations: 53
