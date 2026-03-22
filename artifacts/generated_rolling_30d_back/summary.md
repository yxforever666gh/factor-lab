# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 3
- Failed: 3
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.118618
- RankIC IR: 0.384808
- Top-bottom spread mean: 0.011297
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.086946
- RankIC IR: 0.370167
- Top-bottom spread mean: 0.008311
- Fail reason: n/a

### liquidity_turnover_shock [PASS]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.061108
- RankIC IR: 0.185326
- Top-bottom spread mean: 0.003854
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: 0.001795
- RankIC IR: 0.004687
- Top-bottom spread mean: 0.004142
- Fail reason: rank_ic_mean<0.02

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: 0.00157
- RankIC IR: 0.004921
- Top-bottom spread mean: -0.000426
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.013809
- RankIC IR: -0.038021
- Top-bottom spread mean: -0.005694
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_bp [PASS] | RankIC=0.048011 | IR=0.136999 | Spread=0.004081 | Reason=n/a
- value_ep [PASS] | RankIC=0.03095 | IR=0.092556 | Spread=0.002442 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.019769 | IR=0.070583 | Spread=0.000562 | Reason=rank_ic_mean<0.02
- mom_20 [FAIL] | RankIC=0.017908 | IR=0.062925 | Spread=0.000839 | Reason=rank_ic_mean<0.02
- liquidity_turnover_shock [FAIL] | RankIC=-0.001997 | IR=-0.006687 | Spread=0.001905 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.025014 | IR=-0.095029 | Spread=-0.005441 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.155758 | Spread=-0.014438 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.154574 | Spread=0.02216 | Reason=n/a
- value_ep / first_half [FAIL] | RankIC=-0.028229 | Spread=-0.006871 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.19863 | Spread=0.023033 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.054497 | Spread=-0.013112 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.055938 | Spread=0.011876 | Reason=n/a
- size_small / first_half [FAIL] | RankIC=0.011373 | Spread=0.006066 | Reason=rank_ic_mean<0.02
- size_small / second_half [PASS] | RankIC=0.222613 | Spread=0.016369 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.023717 | Spread=-0.004006 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.143363 | Spread=0.011475 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.204888 | Spread=-0.020495 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.171479 | Spread=0.008658 | Reason=n/a

## Factor Scores

- value_ep | score=0.46574 | rawIC=0.086946 | neutralIC=0.03095 | peers=none
- value_bp | score=0.248696 | rawIC=0.00157 | neutralIC=0.048011 | peers=none
- size_small | score=-0.120776 | rawIC=0.118618 | neutralIC=-0.025014 | peers=none
- liquidity_turnover_shock | score=-0.225422 | rawIC=0.061108 | neutralIC=-0.001997 | peers=none
- mom_20 | score=-0.441085 | rawIC=0.001795 | neutralIC=0.017908 | peers=mom_plus_value
- mom_plus_value | score=-0.480919 | rawIC=-0.013809 | neutralIC=0.019769 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.225422 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.441085 | cluster=mom_20, mom_plus_value
- size_small | score=-0.120776 | cluster=size_small
- value_bp | score=0.248696 | cluster=value_bp
- value_ep | score=0.46574 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- liquidity_turnover_shock | reason=neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.323576
- Annual volatility: 0.288336
- Sharpe: 1.12222
- Max drawdown: -0.534953
- Avg turnover: 0.239583
- Observations: 65

### long_short_top_bottom_cluster_representatives
- Annual return: 0.135446
- Annual volatility: 0.275429
- Sharpe: 0.491762
- Max drawdown: -0.566806
- Avg turnover: 0.234375
- Observations: 65

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.078002
- Annual volatility: 0.210902
- Sharpe: 0.369852
- Max drawdown: -0.204052
- Avg turnover: 1.302083
- Observations: 65

### long_short_top_bottom_neutralized
- Annual return: 0.145182
- Annual volatility: 0.205098
- Sharpe: 0.707864
- Max drawdown: -0.149252
- Avg turnover: 1.286458
- Observations: 65
