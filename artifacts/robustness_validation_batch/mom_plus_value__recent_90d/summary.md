# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.030055
- RankIC IR: -0.076528
- Top-bottom spread mean: -0.007053
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_plus_value [FAIL] | RankIC=0.011813 | IR=0.043223 | Spread=-0.000582 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_plus_value / first_half [FAIL] | RankIC=-0.252688 | Spread=-0.025889 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.184333 | Spread=0.011086 | Reason=n/a

## Factor Scores

- mom_plus_value | score=-0.451178 | rawIC=-0.030055 | neutralIC=0.011813 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- mom_plus_value | score=-0.451178 | cluster=mom_plus_value

## Graveyard

- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.184258
- Annual volatility: 0.275636
- Sharpe: -0.668483
- Max drawdown: -0.55203
- Avg turnover: 0.352564
- Observations: 53

### long_short_top_bottom_cluster_representatives
- Annual return: -0.184258
- Annual volatility: 0.275636
- Sharpe: -0.668483
- Max drawdown: -0.55203
- Avg turnover: 0.352564
- Observations: 53

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.012323
- Annual volatility: 0.146404
- Sharpe: -0.084174
- Max drawdown: -0.131846
- Avg turnover: 1.38141
- Observations: 53

### long_short_top_bottom_neutralized
- Annual return: -0.012323
- Annual volatility: 0.146404
- Sharpe: -0.084174
- Max drawdown: -0.131846
- Avg turnover: 1.38141
- Observations: 53
