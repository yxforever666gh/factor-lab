# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.045728
- RankIC IR: 0.140603
- Top-bottom spread mean: 0.000566
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [FAIL] | RankIC=-0.023103 | IR=-0.0777 | Spread=-0.000208 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=-3.7e-05 | Spread=-0.004745 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.090289 | Spread=0.005736 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=-0.333899 | rawIC=0.045728 | neutralIC=-0.023103 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.333899 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.128742
- Annual volatility: 0.232468
- Sharpe: 0.553807
- Max drawdown: -0.336694
- Avg turnover: 0.412162
- Observations: 75

### long_short_top_bottom_cluster_representatives
- Annual return: 0.128742
- Annual volatility: 0.232468
- Sharpe: 0.553807
- Max drawdown: -0.336694
- Avg turnover: 0.412162
- Observations: 75

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.103188
- Annual volatility: 0.21381
- Sharpe: 0.482617
- Max drawdown: -0.331536
- Avg turnover: 1.450676
- Observations: 75

### long_short_top_bottom_neutralized
- Annual return: 0.103188
- Annual volatility: 0.21381
- Sharpe: 0.482617
- Max drawdown: -0.331536
- Avg turnover: 1.450676
- Observations: 75
