# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_value_ep [FAIL]
- Expression: `(momentum_20) + (earnings_yield)`
- RankIC mean: -0.036958
- RankIC IR: -0.107631
- Top-bottom spread mean: -0.007371
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- hybrid_mom_20_value_ep [FAIL] | RankIC=-0.034661 | IR=-0.111709 | Spread=-0.006834 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_mom_20_value_ep / first_half [FAIL] | RankIC=-0.059594 | Spread=-0.009716 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_value_ep / second_half [FAIL] | RankIC=-0.014681 | Spread=-0.005063 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- hybrid_mom_20_value_ep | score=-0.812523 | rawIC=-0.036958 | neutralIC=-0.034661 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_mom_20_value_ep | score=-0.812523 | cluster=hybrid_mom_20_value_ep

## Graveyard

- hybrid_mom_20_value_ep | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.193875
- Annual volatility: 0.351461
- Sharpe: -0.551626
- Max drawdown: -0.769614
- Avg turnover: 0.388889
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: -0.193875
- Annual volatility: 0.351461
- Sharpe: -0.551626
- Max drawdown: -0.769614
- Avg turnover: 0.388889
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.230845
- Annual volatility: 0.275783
- Sharpe: -0.837052
- Max drawdown: -0.738812
- Avg turnover: 1.195122
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: -0.230845
- Annual volatility: 0.275783
- Sharpe: -0.837052
- Max drawdown: -0.738812
- Avg turnover: 1.195122
- Observations: 124
