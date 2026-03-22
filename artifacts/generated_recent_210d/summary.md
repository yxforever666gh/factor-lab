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
- RankIC mean: 0.120021
- RankIC IR: 0.414056
- Top-bottom spread mean: 0.011914
- Fail reason: n/a

### value_ep [PASS]
- Expression: `earnings_yield`
- RankIC mean: 0.076652
- RankIC IR: 0.303743
- Top-bottom spread mean: 0.002319
- Fail reason: n/a

### mom_20 [FAIL]
- Expression: `momentum_20`
- RankIC mean: -0.03063
- RankIC IR: -0.082483
- Top-bottom spread mean: -0.006398
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### mom_plus_value [FAIL]
- Expression: `momentum_20 + earnings_yield`
- RankIC mean: -0.038001
- RankIC IR: -0.108743
- Top-bottom spread mean: -0.012226
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### value_bp [FAIL]
- Expression: `book_yield`
- RankIC mean: -0.039244
- RankIC IR: -0.125183
- Top-bottom spread mean: -0.013471
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

### liquidity_turnover_shock [FAIL]
- Expression: `turnover_shock_5_20`
- RankIC mean: -0.039931
- RankIC IR: -0.119186
- Top-bottom spread mean: -0.012081
- Fail reason: rank_ic_mean<0.02; top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- liquidity_turnover_shock [PASS] | RankIC=0.051407 | IR=0.15679 | Spread=0.004275 | Reason=n/a
- value_bp [FAIL] | RankIC=0.047387 | IR=0.140256 | Spread=-0.002414 | Reason=top_bottom_spread<0.0005
- value_ep [FAIL] | RankIC=0.045955 | IR=0.134973 | Spread=-0.000264 | Reason=top_bottom_spread<0.0005
- mom_20 [FAIL] | RankIC=-0.003349 | IR=-0.010244 | Spread=-0.004162 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value [FAIL] | RankIC=-0.011574 | IR=-0.039147 | Spread=-0.002287 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- size_small [FAIL] | RankIC=-0.040037 | IR=-0.134783 | Spread=-0.003278 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Time Split Robustness

- mom_20 / first_half [FAIL] | RankIC=-0.048178 | Spread=-0.012893 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_20 / second_half [FAIL] | RankIC=-0.013082 | Spread=9.7e-05 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_ep / first_half [FAIL] | RankIC=0.068393 | Spread=-0.003076 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.08491 | Spread=0.007714 | Reason=n/a
- value_bp / first_half [FAIL] | RankIC=-0.105709 | Spread=-0.03201 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- value_bp / second_half [PASS] | RankIC=0.027221 | Spread=0.005067 | Reason=n/a
- size_small / first_half [PASS] | RankIC=0.104642 | Spread=0.01431 | Reason=n/a
- size_small / second_half [PASS] | RankIC=0.1354 | Spread=0.009518 | Reason=n/a
- liquidity_turnover_shock / first_half [FAIL] | RankIC=-0.10964 | Spread=-0.025087 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock / second_half [PASS] | RankIC=0.029778 | Spread=0.000926 | Reason=n/a
- mom_plus_value / first_half [FAIL] | RankIC=-0.045284 | Spread=-0.01671 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005
- mom_plus_value / second_half [FAIL] | RankIC=-0.030717 | Spread=-0.007742 | Reason=rank_ic_mean<0.02; top_bottom_spread<0.0005

## Factor Scores

- liquidity_turnover_shock | score=0.136516 | rawIC=-0.039931 | neutralIC=0.051407 | peers=none
- size_small | score=0.04205 | rawIC=0.120021 | neutralIC=-0.040037 | peers=none
- value_ep | score=-0.024944 | rawIC=0.076652 | neutralIC=0.045955 | peers=none
- value_bp | score=-0.374726 | rawIC=-0.039244 | neutralIC=0.047387 | peers=none
- mom_20 | score=-0.798994 | rawIC=-0.03063 | neutralIC=-0.003349 | peers=mom_plus_value
- mom_plus_value | score=-0.846036 | rawIC=-0.038001 | neutralIC=-0.011574 | peers=mom_20

## Candidate Pool

- none

## Cluster Representatives

- liquidity_turnover_shock | score=0.136516 | cluster=liquidity_turnover_shock
- mom_20 | score=-0.798994 | cluster=mom_20, mom_plus_value
- size_small | score=0.04205 | cluster=size_small
- value_bp | score=-0.374726 | cluster=value_bp
- value_ep | score=-0.024944 | cluster=value_ep

## Graveyard

- mom_20 | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2
- value_ep | reason=neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- value_bp | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1
- size_small | reason=neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005
- liquidity_turnover_shock | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:1
- mom_plus_value | reason=raw_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; neutral_fail:rank_ic_mean<0.02; top_bottom_spread<0.0005; split_fail_count:2

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.272692
- Annual volatility: 0.380475
- Sharpe: -0.716717
- Max drawdown: -0.769683
- Avg turnover: 0.254545
- Observations: 111

### long_short_top_bottom_cluster_representatives
- Annual return: -0.261202
- Annual volatility: 0.355083
- Sharpe: -0.735609
- Max drawdown: -0.745962
- Avg turnover: 0.239394
- Observations: 111

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: 0.224108
- Annual volatility: 0.245656
- Sharpe: 0.912286
- Max drawdown: -0.355283
- Avg turnover: 1.269697
- Observations: 111

### long_short_top_bottom_neutralized
- Annual return: -0.010194
- Annual volatility: 0.253287
- Sharpe: -0.040248
- Max drawdown: -0.485401
- Avg turnover: 1.316667
- Observations: 111
