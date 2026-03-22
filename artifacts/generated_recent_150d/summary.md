# Workflow Summary

- Data source: tushare
- Total factors: 6
- Passed: 2
- Failed: 4
- Candidate pool size: 0
- Graveyard size: 6
- Cluster representative count: 5

## Main Results

### size_small [PASS]
- Expression: `size_inv`
- RankIC mean: 0.117792
- RankIC IR: 0.388507
- Top-bottom spread mean: 0.010725
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.066242
- RankIC IR: 0.270241
- Top-bottom spread mean: 0.007231
- Fail reason: n/a

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: 0.028072
- RankIC IR: 0.082577
- Top-bottom spread mean: -0.00361
- Fail reason: top_bottom_spread<0.0005

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.016723
- RankIC IR: -0.043097
- Top-bottom spread mean: 0.002628
- Fail reason: rank_ic_mean<0.02

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.018821
- RankIC IR: -0.059187
- Top-bottom spread mean: -0.003524
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.032756
- RankIC IR: -0.090179
- Top-bottom spread mean: -0.00595
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- mom_20 [PASS] | RankIC=0.043776 | IR=0.137776 | Spread=0.000787 | Reason=n/a
- value_ep [PASS] | RankIC=0.030699 | IR=0.094295 | Spread=0.002125 | Reason=n/a
- value_bp [PASS] | RankIC=0.02288 | IR=0.064592 | Spread=0.000814 | Reason=n/a
- mom_plus_value [FAIL] | RankIC=0.019763 | IR=0.071241 | Spread=0.000896 | Reason=rank_ic_mean<0.02
- liquidity_turnover_shock [FAIL] | RankIC=-0.001522 | IR=-0.005198 | Spread=0.001798 | Reason=rank_ic_mean<0.02
- size_small [FAIL] | RankIC=-0.020862 | IR=-0.071242 | Spread=0.002601 | Reason=rank_ic_mean<0.02

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.126798 | Spread=-0.007765 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [PASS] | RankIC=0.090456 | Spread=0.012748 | Reason=n/a
- value_ep / first_half [FAIL] | RankIC=-0.011679 | Spread=-0.004866 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.142112 | Spread=0.019011 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.094184 | Spread=-0.017891 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.054559 | Spread=0.010466 | Reason=n/a
- size_small / first_half [FAIL] | RankIC=0.007789 | Spread=0.004649 | Reason=rank_ic_mean<0.02
- size_small / second_half [PASS] | RankIC=0.224901 | Spread=0.016642 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.036564 | Spread=-0.011997 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.091007 | Spread=0.004556 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.145628 | Spread=-0.014023 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [PASS] | RankIC=0.077146 | Spread=0.00191 | Reason=n/a

## Factor Scores

- value_ep | score=0.398238 | rawIC=0.066242 | neutralIC=0.030699 | peers=none
- value_bp | score=0.112709 | rawIC=-0.018821 | neutralIC=0.02288 | peers=none
- mom_20 | score=0.083056 | rawIC=-0.016723 | neutralIC=0.043776 | peers=mom_plus_value
- size_small | score=-0.10983 | rawIC=0.117792 | neutralIC=-0.020862 | peers=none
- liquidity_turnover_shock | score=-0.321999 | rawIC=0.028072 | neutralIC=-0.001522 | peers=none
- mom_plus_value | score=-0.536128 | rawIC=-0.032756 | neutralIC=0.019763 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=-0.321999 | cluster=liquidity_turnover_shock
- mom_20 | score=0.083056 | cluster=mom_20, mom_plus_value
- size_small | score=-0.10983 | cluster=size_small
- value_bp | score=0.112709 | cluster=value_bp
- value_ep | score=0.398238 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; split_fail_count:1
- value_ep | reason=split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- liquidity_turnover_shock | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: 0.134831
- Annual volatility: 0.291707
- Sharpe: 0.462212
- Max drawdown: -0.534953
- Avg turnover: 0.276256
- Observations: 74

### long_short_top_bottom_cluster_representatives
- Annual return: -0.043361
- Annual volatility: 0.293904
- Sharpe: -0.147536
- Max drawdown: -0.566806
- Avg turnover: 0.260274
- Observations: 74

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.115365
- Annual volatility: 0.226906
- Sharpe: 0.508428
- Max drawdown: -0.156254
- Avg turnover: 1.363014
- Observations: 74

### long_short_top_bottom_neutralized
- Annual return: 0.071152
- Annual volatility: 0.222655
- Sharpe: 0.319559
- Max drawdown: -0.136232
- Avg turnover: 1.349315
- Observations: 74
