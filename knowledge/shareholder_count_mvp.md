# Shareholder Count / Ownership-crowding MVP

Scope: bounded Tushare stk_holdernumber sample + daily as-of read-only diagnostic. No queue write, no daemon start, no full workflow.

Decision: `stop_shareholder_count_not_incremental`
Reasons: confirmation_not_above_value_quality_benchmark

## Raw source
- Rows: 3565
- Tickers: 77
- Date fields: ann_date, end_date
- Ann date range: 20180102 to 20231229

## Daily as-of feature coverage
- Rows: 30359
- Dates: 871
- Tickers: 53
- QoQ non-null rate: 1.0
- YoY non-null rate: 1.0

## Bucket diagnostics
| Score | Spread mean | Positive rate | Observations |
|---|---:|---:|---:|
| baseline | 0.0009996388 | 0.5109 | 871 |
| low_shareholder_crowding_qoq | 0.0019091035 | 0.5212 | 871 |
| shareholder_confirmation_qoq | 0.0021776636 | 0.5316 | 871 |
| low_shareholder_crowding_yoy | 0.0026462653 | 0.5316 | 871 |
| shareholder_confirmation_yoy | 0.0033320347 | 0.5683 | 871 |
