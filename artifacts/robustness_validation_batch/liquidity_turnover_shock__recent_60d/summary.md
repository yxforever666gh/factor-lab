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
- RankIC mean: 0.143363
- RankIC IR: 0.482484
- Top-bottom spread mean: 0.011475
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.090477 | IR=0.387245 | Spread=0.007923 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.000337 | Spread=-0.001158 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.27861 | Spread=0.023365 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=0.802211 | rawIC=0.143363 | neutralIC=0.090477 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.802211 | cluster=liquidity_turnover_shock

## Graveyard

- liquidity_turnover_shock | reason=split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.511324
- Annual volatility: 0.249955
- Sharpe: 2.045669
- Max drawdown: -0.142564
- Avg turnover: 0.40625
- Observations: 33

### long_short_top_bottom_cluster_representatives
- Annual return: 0.511324
- Annual volatility: 0.249955
- Sharpe: 2.045669
- Max drawdown: -0.142564
- Avg turnover: 0.40625
- Observations: 33

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.768183
- Annual volatility: 0.176212
- Sharpe: 4.35943
- Max drawdown: -0.072267
- Avg turnover: 1.346875
- Observations: 33

### long_short_top_bottom_neutralized
- Annual return: 0.768183
- Annual volatility: 0.176212
- Sharpe: 4.35943
- Max drawdown: -0.072267
- Avg turnover: 1.346875
- Observations: 33
