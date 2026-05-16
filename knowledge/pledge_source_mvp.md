# pledge / 股权质押 bounded PIT read-only source MVP

Scope: bounded Tushare pledge_stat/pledge_detail sample + PIT daily as-of read-only diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.

## Decision
- Decision: `proceed_controlled_pledge_probe_plan`
- Reasons: pledge_readonly_signal_passed
- Best signal: `high_pledge_record_count` spread=0.0074866435, positive_rate=0.5567, obs=203
- Benchmark value_quality_no_distress bucket spread: 0.0062253011
- Local baseline Q3-Q0: 0.0028484279 (obs=845)

## Coverage
- Raw source rows: 8841
- Raw source tickers: 60
- PIT-safe endpoints: 1
- Statement rows: 1031
- Daily as-of rows before dropna: 20002
- Diagnostic rows: 12370
- Diagnostic dates: 845
- Diagnostic tickers: 24
- Signal columns: 32

## Top spreads
- high_pledge_record_count: spread=0.0074866435, positive_rate=0.5567, obs=203
- conf_high_pledge_ratio_max_change: spread=0.0064725825, positive_rate=0.5657, obs=845
- conf_high_pledge_amount_sum_change: spread=0.006324932, positive_rate=0.5562, obs=845
- conf_low_pledge_amount_sum: spread=0.0061715698, positive_rate=0.5598, obs=845
- conf_high_pledge_ratio_mean_change: spread=0.0054891349, positive_rate=0.5657, obs=845
- high_pledge_ratio_max_change: spread=0.0047633451, positive_rate=0.5243, obs=845
- high_pledge_ratio_mean_change: spread=0.0046263, positive_rate=0.5325, obs=845
- high_pledge_record_count_change: spread=0.0046064314, positive_rate=0.5414, obs=495
- high_pledge_amount_sum_change: spread=0.0027258146, positive_rate=0.5219, obs=845
- conf_low_pledge_ratio_mean_change: spread=0.0024690408, positive_rate=0.5278, obs=845
- high_pledge_ratio_mean: spread=0.0018673885, positive_rate=0.5479, obs=845
- low_pledge_amount_sum_change: spread=0.0017032662, positive_rate=0.5148, obs=845

## PIT interpretation
- 质押数据只用 `ann_date <= feature date` 的 daily as-of；无公告日时 detail 仅用 start_date 作为保守可见日期，不使用未来 release/end date。
- 若 best spread 未同时超过 local baseline 与 benchmark 0.0062253011，则停止该路线，不进入 controlled workflow。
