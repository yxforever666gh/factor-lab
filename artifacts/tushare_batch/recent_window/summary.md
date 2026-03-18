# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 6
- Failed: 0
- Candidate pool size: 2
- Graveyard size: 4

## Main Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.327686
- RankIC IR: 1.21569
- Top-bottom spread mean: 0.032816
- Fail reason: n/a

### mom_plus_value [PASS]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: 0.281405
- RankIC IR: 1.080412
- Top-bottom spread mean: 0.019764
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.254545
- RankIC IR: 0.978997
- Top-bottom spread mean: 0.02472
- Fail reason: n/a

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.233884
- RankIC IR: 1.198217
- Top-bottom spread mean: 0.020628
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.193388
- RankIC IR: 1.177993
- Top-bottom spread mean: 0.023535
- Fail reason: n/a

### value_bp [PASS]
- Expression: `book_yield`
- RankIC mean: 0.027273
- RankIC IR: 0.08189
- Top-bottom spread mean: 0.007854
- Fail reason: n/a

## Neutralized Results (industry + size)

- mom_20 [PASS] | RankIC=0.065516 | IR=0.280807 | Spread=0.006955 | Reason=n/a
- value_bp [FAIL] | RankIC=0.026398 | IR=0.082663 | Spread=-6.6e-05 | Reason=top_bottom_spread<0.0005
- mom_plus_value [PASS] | RankIC=0.024927 | IR=0.110529 | Spread=0.006892 | Reason=n/a
- size_small [FAIL] | RankIC=-0.017024 | IR=-0.069919 | Spread=-0.009697 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock [FAIL] | RankIC=-0.022004 | IR=-0.078614 | Spread=-0.000168 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=-0.022959 | IR=-0.082217 | Spread=-0.002173 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [PASS] | RankIC=0.315702 | Spread=0.023691 | Reason=n/a
- mom_20 / second_half [PASS] | RankIC=0.339669 | Spread=0.041942 | Reason=n/a
- value_ep / first_half [PASS] | RankIC=0.216529 | Spread=0.016736 | Reason=n/a
- value_ep / second_half [PASS] | RankIC=0.170248 | Spread=0.030334 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.202479 | Spread=-0.010568 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.257025 | Spread=0.026275 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.209091 | Spread=0.025039 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.258678 | Spread=0.016217 | Reason=n/a
- liquidity_turnover_shock / first_half [PASS] | RankIC=0.254545 | Spread=0.035469 | Reason=n/a
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.254545 | Spread=0.013971 | Reason=n/a
- mom_plus_value / first_half [PASS] | RankIC=0.245455 | Spread=0.013469 | Reason=n/a
- mom_plus_value / second_half [PASS] | RankIC=0.317355 | Spread=0.026058 | Reason=n/a

## Candidate Pool

- mom_20 | rawIC=0.327686 | neutralIC=0.065516 | peers=mom_plus_value
- mom_plus_value | rawIC=0.281405 | neutralIC=0.024927 | peers=mom_20

## Graveyard

- value_ep | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp | reason=neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005

## Portfolio Results

### long_short_top_bottom
- Annual return: 1.670311
- Annual volatility: 0.154128
- Sharpe: 10.837132
- Max drawdown: -0.009419
- Avg turnover: 0.15873
- Observations: 22

### long_short_top_bottom_neutralized
- Annual return: 0.050627
- Annual volatility: 0.123753
- Sharpe: 0.4091
- Max drawdown: -0.094694
- Avg turnover: 1.380952
- Observations: 22
