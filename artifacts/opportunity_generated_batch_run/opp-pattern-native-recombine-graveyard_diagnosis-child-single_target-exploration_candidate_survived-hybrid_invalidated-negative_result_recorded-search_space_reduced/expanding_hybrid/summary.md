# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.04986
- RankIC IR: -0.150833
- Top-bottom spread mean: -0.013827
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.036128 | IR=0.110341 | Spread=0.001096 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.146131 | Spread=-0.030039 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.044857 | Spread=0.002125 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.061109 | rawIC=-0.04986 | neutralIC=0.036128 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.061109 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.624202
- Annual volatility: 0.346946
- Sharpe: -1.799134
- Max drawdown: -0.9001
- Avg turnover: 0.407713
- Observations: 122

### long_short_top_bottom_cluster_representatives
- Annual return: -0.624202
- Annual volatility: 0.346946
- Sharpe: -1.799134
- Max drawdown: -0.9001
- Avg turnover: 0.407713
- Observations: 122

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.155529
- Annual volatility: 0.344217
- Sharpe: 0.451833
- Max drawdown: -0.632507
- Avg turnover: 1.279063
- Observations: 122

### long_short_top_bottom_neutralized
- Annual return: 0.155529
- Annual volatility: 0.344217
- Sharpe: 0.451833
- Max drawdown: -0.632507
- Avg turnover: 1.279063
- Observations: 122
