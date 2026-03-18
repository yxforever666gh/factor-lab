# Workflow Summary

- Total factors: 6
- Passed: 6
- Failed: 0

## Results

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

## Time Split Robustness

- mom_20 / first_half: PASS, RankIC=0.315702, Spread=0.023691
- mom_20 / second_half: PASS, RankIC=0.339669, Spread=0.041942
- value_ep / first_half: PASS, RankIC=0.216529, Spread=0.016736
- value_ep / second_half: PASS, RankIC=0.170248, Spread=0.030334
- value_bp / first_half: FAIL, RankIC=-0.202479, Spread=-0.010568
- value_bp / second_half: PASS, RankIC=0.257025, Spread=0.026275
- size_small / first_half: PASS, RankIC=0.209091, Spread=0.025039
- size_small / second_half: PASS, RankIC=0.258678, Spread=0.016217
- liquidity_turnover_shock / first_half: PASS, RankIC=0.254545, Spread=0.035469
- liquidity_turnover_shock / second_half: PASS, RankIC=0.254545, Spread=0.013971
- mom_plus_value / first_half: PASS, RankIC=0.245455, Spread=0.013469
- mom_plus_value / second_half: PASS, RankIC=0.317355, Spread=0.026058
