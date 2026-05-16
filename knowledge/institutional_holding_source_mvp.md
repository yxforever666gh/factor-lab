# Institutional Holding / Top10 Holders Source MVP

Scope: bounded Tushare top10_holders/top10_floatholders sample only. No workflow enqueue, no daemon start, no full-market backfill.

## Decision
- Decision: `proceed_institutional_holding_readonly_feature_plan`
- Reasons: bounded_sample_has_pit_safe_rows

## Coverage
- Rows: 21746
- Tickers: 20
- Endpoints with rows: 2

## Endpoint schema / PIT audit
### top10_holders
- Rows: 5585
- Tickers: 20
- Date fields: ann_date, end_date
- PIT control: `announcement_date_pit`
- Columns: ts_code, ann_date, end_date, holder_name, hold_amount, hold_ratio, hold_float_ratio, hold_change, holder_type, endpoint
### top10_floatholders
- Rows: 16161
- Tickers: 20
- Date fields: ann_date, end_date
- PIT control: `announcement_date_pit`
- Columns: ts_code, ann_date, end_date, holder_name, hold_amount, hold_ratio, hold_float_ratio, hold_change, holder_type, endpoint

## Interpretation
- 若只有 `end_date` 而没有 `ann_date`/`f_ann_date`，该源不能直接用于 PIT 因子研究。
- 本轮为只读 MVP；未写队列、未启动 daemon、未运行 workflow。
