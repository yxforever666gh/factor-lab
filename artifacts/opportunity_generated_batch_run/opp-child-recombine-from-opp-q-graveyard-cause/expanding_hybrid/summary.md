# Workflow Summary

- Data source: tushare
- Total factors: 3
- Passed: 0
- Failed: 3
- Candidate pool size: 0
- Graveyard size: 3
- Cluster representative count: 1

## Main Results

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.021855
- RankIC IR: -0.059101
- Top-bottom spread mean: -0.000907
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### hybrid_mom_20_mom_plus_value [FAIL]
- Expression: `(momentum_20) + (momentum_20 + earnings_yield)`
- RankIC mean: -0.026979
- RankIC IR: -0.074313
- Top-bottom spread mean: -0.003358
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.034972
- RankIC IR: -0.102032
- Top-bottom spread mean: -0.006829
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_20 [FAIL] | RankIC=-0.017577 | IR=-0.057266 | Spread=-0.001989 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.031159 | IR=-0.100039 | Spread=-0.007293 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_mom_plus_value [FAIL] | RankIC=-0.040393 | IR=-0.133449 | Spread=-0.006901 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.03391 | Spread=-0.005508 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.009995 | Spread=0.003619 | Reason=rank_ic_mean<0.02
- mom_plus_value / first_half [FAIL] | RankIC=-0.050884 | Spread=-0.008321 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.019316 | Spread=-0.005361 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_mom_plus_value / first_half [FAIL] | RankIC=-0.032898 | Spread=-0.005408 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_mom_plus_value / second_half [FAIL] | RankIC=-0.021155 | Spread=-0.001341 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- mom_20 | score=-0.916234 | rawIC=-0.021855 | neutralIC=-0.017577 | peers=hybrid_mom_20_mom_plus_value, mom_plus_value
- mom_plus_value | score=-0.996212 | rawIC=-0.034972 | neutralIC=-0.031159 | peers=hybrid_mom_20_mom_plus_value, mom_20
- hybrid_mom_20_mom_plus_value | score=-0.999773 | rawIC=-0.026979 | neutralIC=-0.040393 | peers=mom_20, mom_plus_value

## Candidate Pool

- none

## Cluster Representatives

- mom_20 | score=-0.916234 | cluster=hybrid_mom_20_mom_plus_value, mom_20, mom_plus_value

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- hybrid_mom_20_mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.100978
- Annual volatility: 0.37882
- Sharpe: -0.266558
- Max drawdown: -0.712553
- Avg turnover: 0.442149
- Observations: 122

### long_short_top_bottom_cluster_representatives
- Annual return: 0.019323
- Annual volatility: 0.399214
- Sharpe: 0.048403
- Max drawdown: -0.683855
- Avg turnover: 0.407713
- Observations: 122

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.055733
- Annual volatility: 0.302647
- Sharpe: -0.184153
- Max drawdown: -0.587518
- Avg turnover: 1.220661
- Observations: 122

### long_short_top_bottom_neutralized
- Annual return: -0.097128
- Annual volatility: 0.285546
- Sharpe: -0.340149
- Max drawdown: -0.672696
- Avg turnover: 1.219008
- Observations: 122
