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
- RankIC mean: -0.050154
- RankIC IR: -0.15282
- Top-bottom spread mean: -0.012827
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.031332 | IR=0.095798 | Spread=0.001421 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.155211 | Spread=-0.028427 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.053236 | Spread=0.002525 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.045688 | rawIC=-0.050154 | neutralIC=0.031332 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.045688 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.598021
- Annual volatility: 0.346065
- Sharpe: -1.728056
- Max drawdown: -0.9001
- Avg turnover: 0.403794
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: -0.598021
- Annual volatility: 0.346065
- Sharpe: -1.728056
- Max drawdown: -0.9001
- Avg turnover: 0.403794
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.136568
- Annual volatility: 0.344019
- Sharpe: 0.39698
- Max drawdown: -0.632507
- Avg turnover: 1.260976
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: 0.136568
- Annual volatility: 0.344019
- Sharpe: 0.39698
- Max drawdown: -0.632507
- Avg turnover: 1.260976
- Observations: 124
