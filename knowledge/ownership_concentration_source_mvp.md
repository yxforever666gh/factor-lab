# 第 8 轮：十大股东 / ownership concentration read-only diagnostic

Scope: bounded top10_holders/top10_floatholders sample + PIT daily as-of diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.

## Decision
- Decision: `stop_ownership_concentration_not_incremental`
- Reasons: best_signal_not_above_value_quality_benchmark
- Best signal: `conf_low_top10_float_hkscc_ratio_change` spread=0.0030241217
- Benchmark value_quality_no_distress bucket spread: 0.0062253011

## Coverage
- Rows: 32642
- Dates: 845
- Tickers: 57
- Signal columns: 24

## Key spreads
- Local baseline Q3-Q0: 0.0012177106 (obs=845)
- conf_high_top10_float_fund_like_ratio_sum: spread=0.0009166951, positive_rate=0.5018, obs=845
- conf_high_top10_float_fund_like_ratio_sum_change: spread=-0.000659037, positive_rate=0.4923, obs=845
- conf_high_top10_float_hkscc_ratio: spread=-0.0005752623, positive_rate=0.4769, obs=845
- conf_high_top10_float_hkscc_ratio_change: spread=0.0021916521, positive_rate=0.5302, obs=845
- conf_high_top10_float_ratio_sum: spread=0.0009769129, positive_rate=0.5314, obs=845
- conf_high_top10_float_ratio_sum_change: spread=-6.37729e-05, positive_rate=0.497, obs=845
- conf_low_top10_float_fund_like_ratio_sum: spread=0.000279564, positive_rate=0.5231, obs=845
- conf_low_top10_float_fund_like_ratio_sum_change: spread=0.0011204119, positive_rate=0.5124, obs=845
- conf_low_top10_float_hkscc_ratio: spread=0.0002354882, positive_rate=0.5041, obs=845
- conf_low_top10_float_hkscc_ratio_change: spread=0.0030241217, positive_rate=0.5444, obs=845
- conf_low_top10_float_ratio_sum: spread=0.0005111211, positive_rate=0.5018, obs=845
- conf_low_top10_float_ratio_sum_change: spread=0.0010454326, positive_rate=0.503, obs=845
- high_top10_float_fund_like_ratio_sum: spread=-0.0019943976, positive_rate=0.4675, obs=845
- high_top10_float_fund_like_ratio_sum_change: spread=-0.0018144353, positive_rate=0.4982, obs=845
- high_top10_float_hkscc_ratio: spread=-0.0033902191, positive_rate=0.4568, obs=845
- high_top10_float_hkscc_ratio_change: spread=-2.59401e-05, positive_rate=0.5243, obs=845
- high_top10_float_ratio_sum: spread=0.0005890499, positive_rate=0.4994, obs=845
- high_top10_float_ratio_sum_change: spread=0.0013738373, positive_rate=0.5254, obs=845
- low_top10_float_fund_like_ratio_sum: spread=0.000118904, positive_rate=0.5373, obs=845
- low_top10_float_fund_like_ratio_sum_change: spread=0.0014924066, positive_rate=0.5207, obs=845
- low_top10_float_hkscc_ratio: spread=0.0015655113, positive_rate=0.5444, obs=845
- low_top10_float_hkscc_ratio_change: spread=0.0001022446, positive_rate=0.5219, obs=845
- low_top10_float_ratio_sum: spread=0.0003534876, positive_rate=0.497, obs=845
- low_top10_float_ratio_sum_change: spread=0.0026939249, positive_rate=0.5432, obs=845

## PIT / source preflight
- Raw source rows: 19010
- Raw source tickers: 77
- PIT-safe endpoints: 1

## Interpretation
- 若 best signal 未超过 0.0062253011 benchmark，则该 ownership-concentration 路线停止，不进入 controlled workflow。
- 本轮所有持仓数据均要求 `ann_date <= trade_date` 的 daily as-of；未使用 end_date 直接前视。
