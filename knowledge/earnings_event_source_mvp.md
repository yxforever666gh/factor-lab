# 第 9 轮：业绩预告 / 业绩快报事件数据源 read-only diagnostic

Scope: bounded per-ticker/cache reuse + PIT daily as-of diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.

## Decision
- Decision: `proceed_earnings_event_controlled_probe_plan`
- Reasons: earnings_event_readonly_signal_passed
- Best signal: `high_express_diluted_roe_yoy` spread=0.0066274031
- Benchmark value_quality_no_distress bucket spread: 0.0062253011

## Coverage
- Rows: 30499
- Dates: 845
- Tickers: 51
- Signal columns: 76

## Key spreads
- Local baseline Q3-Q0: 0.0004834922 (obs=845)
- high_forecast_type_score: spread=None, positive_rate=None, obs=None
- low_forecast_type_score: spread=None, positive_rate=None, obs=None
- high_express_diluted_roe_yoy: spread=0.0066274031, positive_rate=0.5547, obs=402
- low_forecast_type_score_qoq: spread=0.0062716595, positive_rate=0.6047, obs=253
- conf_high_express_revenue_log: spread=0.004869458, positive_rate=0.5207, obs=845
- high_express_n_income_log_qoq: spread=0.0047078586, positive_rate=0.5183, obs=845
- high_express_n_income_log_qoq_calc: spread=0.0047078586, positive_rate=0.5183, obs=845
- low_express_diluted_roe_yoy: spread=0.0046620207, positive_rate=0.5323, obs=402
- high_express_revenue_log_qoq: spread=0.0028753342, positive_rate=0.5207, obs=845
- high_express_revenue_log_qoq_calc: spread=0.0028753342, positive_rate=0.5207, obs=845
- conf_low_forecast_p_change_mid_qoq: spread=0.0025170244, positive_rate=0.5325, obs=845
- low_forecast_p_change_mid: spread=0.0022703626, positive_rate=0.5337, obs=845
- conf_high_express_n_income_log: spread=0.0022161326, positive_rate=0.4817, obs=845
- conf_low_forecast_net_profit_mid: spread=0.0021987719, positive_rate=0.5467, obs=845
- conf_high_express_revenue_log_qoq: spread=0.0017941749, positive_rate=0.4675, obs=845
- conf_high_express_revenue_log_qoq_calc: spread=0.0017941749, positive_rate=0.4675, obs=845
- conf_low_forecast_type_score: spread=0.0016940432, positive_rate=0.5101, obs=845
- conf_high_express_diluted_roe: spread=0.0016109664, positive_rate=0.5148, obs=845
- conf_low_forecast_p_change_mid: spread=0.0016005759, positive_rate=0.5432, obs=845
- low_express_diluted_roe_qoq: spread=0.0015869229, positive_rate=0.4769, obs=845

## PIT / source preflight
- Raw source rows: 2804
- Raw source tickers: 71
- PIT-safe endpoints: 2
- Daily as-of rows before feature dropna: 45519

## Interpretation
- 所有事件/财务数据均要求 `ann_date <= trade_date` 的 daily as-of；未使用 end_date 直接前视。
- 若 best signal 未超过 0.0062253011 benchmark，则事件路线不进入 controlled workflow。
