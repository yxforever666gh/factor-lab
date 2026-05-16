# 第 13 轮：earnings event failure diagnosis

Scope: read-only diagnosis only. No workflow enqueue, no daemon start, no new API pull.

## Decision
- Decision: `stop_earnings_event_not_robust`
- Reasons: controlled_overlay_active_tickers_too_few, signal_coverage_depends_on_few_tickers, leave_one_ticker_out_not_stably_above_benchmark, rolling_workflow_all_failed_standard_gate, split_workflow_all_failed_standard_gate, standard_gate_failed_sharpe_net<1.0
- Benchmark: 0.0062253011

## Coverage
- Dataset rows: 50431
- Dataset dates: 871
- Dataset tickers: 77
- Non-null signal rows: 2999
- Non-null signal tickers: 6
- Active diagnostic rows: 2999
- Active diagnostic dates: 840
- Active diagnostic tickers: 6

## Bucket spreads
- Q3-Q0: spread=0.0066274031, positive_rate=0.5547, obs=402
- Q3-Q1: spread=0.0012175759, positive_rate=0.4826, obs=402
- Q4-Q0: spread=0.0002104814, positive_rate=0.51, obs=402
- Q4-Q1: spread=-0.0051993458, positive_rate=0.4602, obs=402
- Best pair: Q3-Q0 spread=0.0066274031

## Ticker concentration / leave-one-out
- Min leave-one-ticker-out Q3-Q0 spread: 2.93608e-05
- All leave-one-out above benchmark: False
- Top active tickers:
  - 600644.SH: rows=801, row_share=0.2671, mean_return=0.0026710832
  - 600606.SH: rows=674, row_share=0.2247, mean_return=-0.0058183147
  - 600639.SH: rows=443, row_share=0.1477, mean_return=-0.0022489732
  - 600612.SH: rows=442, row_share=0.1474, mean_return=0.0040768143
  - 600648.SH: rows=441, row_share=0.147, mean_return=-0.0021692857
  - 600663.SH: rows=198, row_share=0.066, mean_return=-0.0020864549

## Workflow instability evidence
- Standard pass_gate: False
- Standard fail_reason: sharpe_net<1.0
- Standard sharpe_net: -2.1968
- Rolling pass count: 0 / 6
- Split pass count: 0 / 2

## Interpretation
- 虽然早前 bucket-aware Q3-Q0 略高于 benchmark，但本诊断发现信号覆盖极窄且 standard/rolling/split workflow gate 全部失败。
- 因此 earnings event 当前不能进入扩展或 controlled workflow；应停止该路线并切换下一数据源/机制。
