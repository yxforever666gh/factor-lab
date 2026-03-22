# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 1
- Failed: 0
- Candidate pool size: 1
- Graveyard size: 0
- Cluster representative count: 1

## Main Results

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.25336
- RankIC IR: 0.996098
- Top-bottom spread mean: 0.026287
- Fail reason: n/a

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.07224 | IR=0.284445 | Spread=0.008624 | Reason=n/a

## Time Split Robustness

- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.252273 | Spread=0.017869 | Reason=n/a

## Factor Scores

- liquidity_turnover_shock | score=1.299535 | rawIC=0.25336 | neutralIC=0.07224 | peers=none

## Candidate Pool

- liquidity_turnover_shock | rawIC=0.25336 | neutralIC=0.07224 | peers=none

## Cluster Representatives

- liquidity_turnover_shock | score=1.299535 | cluster=liquidity_turnover_shock

## Graveyard

- none

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 1.061375
- Annual volatility: 0.247255
- Sharpe: 4.292628
- Max drawdown: -0.092552
- Avg turnover: 0.409091
- Observations: 23

### long_short_top_bottom_candidates_only
- Annual return: 1.061375
- Annual volatility: 0.247255
- Sharpe: 4.292628
- Max drawdown: -0.092552
- Avg turnover: 0.409091
- Observations: 23

### long_short_top_bottom_candidates_only_neutralized
- Annual return: 0.652608
- Annual volatility: 0.194491
- Sharpe: 3.355464
- Max drawdown: -0.072267
- Avg turnover: 1.315909
- Observations: 23

### long_short_top_bottom_cluster_representatives
- Annual return: 1.061375
- Annual volatility: 0.247255
- Sharpe: 4.292628
- Max drawdown: -0.092552
- Avg turnover: 0.409091
- Observations: 23

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.652608
- Annual volatility: 0.194491
- Sharpe: 3.355464
- Max drawdown: -0.072267
- Avg turnover: 1.315909
- Observations: 23

### long_short_top_bottom_neutralized
- Annual return: 0.652608
- Annual volatility: 0.194491
- Sharpe: 3.355464
- Max drawdown: -0.072267
- Avg turnover: 1.315909
- Observations: 23
