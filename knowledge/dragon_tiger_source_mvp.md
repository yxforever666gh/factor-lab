# dragon_tiger / 龙虎榜事件流 bounded read-only source MVP

Scope: bounded Tushare top_list sample + event-date PIT/read-only diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.

## Decision
- Decision: `stop_dragon_tiger_not_incremental`
- Reasons: best_signal_not_above_value_quality_benchmark
- Best signal: `high_dt_buy_sum_20d` spread=0.0022231093, positive_rate=0.5455, obs=11
- Benchmark value_quality_no_distress bucket spread: 0.0062253011
- Local baseline Q3-Q0: 0.0022064017 (obs=845)

## Coverage
- Diagnostic rows: 41669
- Dates: 845
- Tickers: 72
- Event-active rows: 204
- Signal columns: 48
- Raw rows: 54
- Raw dates: 34
- Raw tickers: 20

## Top spreads
- high_dt_buy_sum_20d: spread=0.0022231093, positive_rate=0.5455, obs=11
- high_dt_sell_sum_20d: spread=0.0022231093, positive_rate=0.5455, obs=11
- conf_high_dt_event_count_20d: spread=0.0009864002, positive_rate=0.5952, obs=84
- conf_high_dt_sell_sum_20d: spread=0.0003155781, positive_rate=0.5714, obs=84
- conf_high_dt_buy_sum_20d: spread=7.95776e-05, positive_rate=0.5714, obs=84
- high_dt_net_amount_sum_20d: spread=-0.0013921845, positive_rate=0.4545, obs=11
- low_dt_net_amount_sum_20d: spread=-0.0015332669, positive_rate=0.6364, obs=11
- conf_low_dt_event_count_20d: spread=-0.0017696085, positive_rate=0.4881, obs=84
- conf_low_dt_buy_sum_20d: spread=-0.0019709656, positive_rate=0.5, obs=84
- conf_low_dt_net_amount_sum_5d: spread=-0.0020133949, positive_rate=0.5, obs=64
- conf_high_dt_net_amount_sum_20d: spread=-0.0021513007, positive_rate=0.4643, obs=84
- conf_high_dt_sell_sum_5d: spread=-0.0025591359, positive_rate=0.4375, obs=64

## PIT interpretation
- 龙虎榜是 trade_date 事件流；本诊断只把 `trade_date <= feature date` 的事件滚动进入 5/20 日窗口，未使用未来事件。
- 若 best spread 未同时超过 local baseline 与 benchmark 0.0062253011，则停止该路线，不进入 controlled workflow。
