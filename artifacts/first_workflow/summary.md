# First Workflow Summary

- Total factors: 5
- Passed: 4
- Failed: 1

## Results

### mom_20 [PASS]
- Expression: `momentum_20`
- RankIC mean: 0.981143
- RankIC IR: 64.992119
- Top-bottom spread mean: 0.762779
- Fail reason: n/a

### quality_roe [PASS]
- Expression: `roe`
- RankIC mean: 0.736473
- RankIC IR: 19.14013
- Top-bottom spread mean: 0.569508
- Fail reason: n/a

### quality_minus_value [PASS]
- Expression: `roe - pb`
- RankIC mean: 0.643788
- RankIC IR: 15.313021
- Top-bottom spread mean: 0.488012
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.575457
- RankIC IR: 12.79664
- Top-bottom spread mean: 0.436563
- Fail reason: n/a

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.009888
- RankIC IR: 0.08047
- Top-bottom spread mean: 0.016568
- Fail reason: rank_ic_mean<0.03
