# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.228458
- RankIC IR: 1.186231
- Top-bottom spread mean: 0.019391
- Fail reason: n/a

## Neutralized Results (industry + size)

- size_small [FAIL] | RankIC=-0.064864 | IR=-0.248092 | Spread=-0.018327 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- size_small / first_half [PASS] | RankIC=0.209091 | Spread=0.025039 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.246212 | Spread=0.014214 | Reason=n/a

## Factor Scores

- size_small | score=0.354488 | rawIC=0.228458 | neutralIC=-0.064864 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- size_small | score=0.354488 | cluster=size_small

## Graveyard

- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.42415
- Annual volatility: 0.194142
- Sharpe: 2.18474
- Max drawdown: -0.112992
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 0.42415
- Annual volatility: 0.194142
- Sharpe: 2.18474
- Max drawdown: -0.112992
- Avg turnover: 0.0
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.06746
- Annual volatility: 0.129849
- Sharpe: -0.519528
- Max drawdown: -0.073065
- Avg turnover: 1.172348
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: -0.06746
- Annual volatility: 0.129849
- Sharpe: -0.519528
- Max drawdown: -0.073065
- Avg turnover: 1.172348
- Observations: 23
