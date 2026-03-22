# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### hybrid_mom_20_liquidity_turnover_shock [FAIL]
- Expression: `(momentum_20) + (turnover_shock_5_20)`
- RankIC mean: -0.05912
- RankIC IR: -0.167775
- Top-bottom spread mean: -0.009728
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- hybrid_mom_20_liquidity_turnover_shock [FAIL] | RankIC=0.04911 | IR=0.157289 | Spread=0.000335 | Reason=top_bottom_spread<0.0005

## Time Split Robustness

- hybrid_mom_20_liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.154128 | Spread=-0.022377 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- hybrid_mom_20_liquidity_turnover_shock / second_half [PASS] | RankIC=0.03438 | Spread=0.002721 | Reason=n/a

## Factor Scores

- hybrid_mom_20_liquidity_turnover_shock | score=-0.425636 | rawIC=-0.05912 | neutralIC=0.04911 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- hybrid_mom_20_liquidity_turnover_shock | score=-0.425636 | cluster=hybrid_mom_20_liquidity_turnover_shock

## Graveyard

- hybrid_mom_20_liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.345119
- Annual volatility: 0.363709
- Sharpe: -0.948889
- Max drawdown: -0.872995
- Avg turnover: 0.409214
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: -0.345119
- Annual volatility: 0.363709
- Sharpe: -0.948889
- Max drawdown: -0.872995
- Avg turnover: 0.409214
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.09854
- Annual volatility: 0.344724
- Sharpe: 0.285853
- Max drawdown: -0.610027
- Avg turnover: 1.219609
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: 0.09854
- Annual volatility: 0.344724
- Sharpe: 0.285853
- Max drawdown: -0.610027
- Avg turnover: 1.219609
- Observations: 124
