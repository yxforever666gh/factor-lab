# Workflow Summary

- Data source: tushare
- Total factors: 1
- Passed: 0
- Failed: 1
- Candidate pool size: 0
- Graveyard size: 1
- Cluster representative count: 1

## Main Results

### value_ep [FAIL]
- Expression: `earnings_yield`
- RankIC mean: 0.058135
- RankIC IR: 0.216613
- Top-bottom spread mean: -0.001942
- Fail reason: top_bottom_spread<0.0005

## Neutralized Results (industry + size)

- value_ep [FAIL] | RankIC=0.029009 | IR=0.089237 | Spread=-0.004068 | Reason=top_bottom_spread<0.0005

## Time Split Robustness

- value_ep / first_half [FAIL] | RankIC=0.022242 | Spread=-0.013096 | Reason=top_bottom_spread<0.0005
- value_ep / second_half [PASS] | RankIC=0.093458 | Spread=0.009035 | Reason=n/a

## Factor Scores

- value_ep | score=-0.135144 | rawIC=0.058135 | neutralIC=0.029009 | peers=none

## Candidate Pool

- none

## Cluster Representatives

- value_ep | score=-0.135144 | cluster=value_ep

## Graveyard

- value_ep | reason=raw_fail:top_bottom_spread<0.0005; neutral_fail:top_bottom_spread<0.0005; split_fail_count:1

## Portfolio Results

### long_short_top_bottom_all_factors
- Annual return: -0.152446
- Annual volatility: 0.239916
- Sharpe: -0.635416
- Max drawdown: -0.669801
- Avg turnover: 0.052846
- Observations: 124

### long_short_top_bottom_cluster_representatives
- Annual return: -0.152446
- Annual volatility: 0.239916
- Sharpe: -0.635416
- Max drawdown: -0.669801
- Avg turnover: 0.052846
- Observations: 124

### long_short_top_bottom_cluster_representatives_neutralized
- Annual return: -0.171923
- Annual volatility: 0.296364
- Sharpe: -0.580108
- Max drawdown: -0.545692
- Avg turnover: 1.159892
- Observations: 124

### long_short_top_bottom_neutralized
- Annual return: -0.171923
- Annual volatility: 0.296364
- Sharpe: -0.580108
- Max drawdown: -0.545692
- Avg turnover: 1.159892
- Observations: 124
