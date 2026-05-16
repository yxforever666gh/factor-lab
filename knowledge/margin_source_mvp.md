# Margin Source MVP

Scope: bounded sample data quality only. No factor run, no queue write, no daemon start.

## Decision
- Decision: `proceed_margin_factor_probe_plan`
- Reasons: margin_data_has_stock_coverage_fields_overlap_and_not_highly_redundant

## Coverage
- Rows: 14288
- Unique tickers: 4001
- Stock-like tickers: 3675
- Stock-like ratio: 0.9185
- Feature overlap rows: 112
- Feature cache: artifacts/tushare_cache/tushare_2019-12-02_2024-01-05_80.csv

## Sample date coverage
| trade_date | rows | unique tickers | stock-like ratio |
|---|---:|---:|---:|
| 20181228 | 994 | 994 | 0.9547 |
| 20191231 | 1738 | 1738 | 0.9603 |
| 20201231 | 1992 | 1992 | 0.9428 |
| 20211231 | 2407 | 2407 | 0.9302 |
| 20221230 | 3316 | 3316 | 0.9379 |
| 20231229 | 3841 | 3841 | 0.9268 |

## Redundancy pre-check
- Available: True
- Rows: 112
- Max abs turnover-like corr: 0.8353
- Redundancy flag: high

## Interpretation
融资融券数据通过 MVP 质量门槛。下一轮可以写 controlled probe 计划，把 low-crowding 从 turnover 弱代理升级为 margin-based 机制。
