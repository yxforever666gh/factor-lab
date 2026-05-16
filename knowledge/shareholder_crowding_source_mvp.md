# Shareholder Crowding Source MVP

Scope: bounded Tushare `stk_holdernumber` sample + PIT daily as-of read-only diagnostic. No workflow enqueue, no daemon start.

## Decision
- Decision: `stop_shareholder_crowding_not_incremental`
- Reasons: confirmation_not_above_value_quality_benchmark

## Preflight
- Rows: 2961
- Tickers: 60
- Required fields present: True
- Missing fields: []
- ann_date range: 20180102 to 20231229

## Daily as-of coverage
- Rows: 22281
- Dates: 871
- Tickers: 39

## Bucket diagnostics
- baseline: spread=0.0007056237, observations=871, positive_rate=0.5109
- low_shareholder_crowding: spread=0.0031225567, observations=871, positive_rate=0.5557
- confirmation: spread=0.0022390601, observations=871, positive_rate=0.5327

## Correlations
- low_shareholder_vs_baseline: 0.057347
- confirmation_vs_baseline: 0.785304
- low_shareholder_vs_turnover: -0.005886
- low_shareholder_vs_turnover_shock_5_20: 0.00243
