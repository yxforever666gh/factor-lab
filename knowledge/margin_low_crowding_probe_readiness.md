# Margin Low-crowding Probe Readiness

Scope: read-only diagnostic. No workflow run, no queue write, no daemon start.

## Decision
- Decision: `proceed_controlled_margin_low_crowding_probe`
- Reasons: readonly_signal_passed_preliminary_checks

## Coverage
- Feature rows after dropna: 942
- Dates: 43
- Tickers: 35
- Raw margin rows: 115371
- Merged overlap before dropna: 1614
- Feature cache: artifacts/tushare_cache/tushare_2019-12-02_2024-01-05_80.csv

## Bucket diagnostics
| Score | Spread mean | Positive rate | Observations |
|---|---:|---:|---:|
| baseline | 0.0057829537 | 0.6279 | 43 |
| low_margin_crowding | 0.0028764243 | 0.5581 | 43 |
| confirmation | 0.0070747769 | 0.6047 | 43 |

## Correlations
- low_margin_vs_baseline: -0.150952
- confirmation_vs_baseline: 0.641089
- low_margin_vs_turnover: -0.229506
- low_margin_vs_turnover_shock_5_20: 0.014744

## Interpretation
融资余额低拥挤特征在只读样本上有正向且非重复迹象，可以进入下一轮 controlled workflow 计划。
