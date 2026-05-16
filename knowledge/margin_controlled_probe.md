# Margin Controlled Probe

Scope: held-out diagnostic from bounded margin feature sample. No queue write, no daemon start.

## Decision
- Decision: `fail_stop_margin_low_crowding_probe`
- Reasons: holdout_confirmation_not_incremental_vs_baseline, holdout_confirmation_not_above_benchmark, holdout_positive_rate_below_threshold

## Coverage
- rows: 942
- dates: 43
- tickers: 35
- train_rows: 687
- train_dates: 31
- holdout_rows: 255
- holdout_dates: 12

## Score diagnostics
| Split | Score | Spread mean | Positive rate | Observations | Rank IC |
|---|---|---:|---:|---:|---:|
| full_sample | value_quality_baseline | 0.0057829537 | 0.6279 | 43 | 0.07489 |
| full_sample | low_margin_crowding | 0.0028764243 | 0.5581 | 43 | 0.034766 |
| full_sample | margin_low_crowding_confirmation | 0.0070747769 | 0.6047 | 43 | 0.072489 |
| train | value_quality_baseline | 0.0060197948 | 0.6129 | 31 | 0.057383 |
| train | low_margin_crowding | 0.0053856048 | 0.5806 | 31 | 0.068219 |
| train | margin_low_crowding_confirmation | 0.0094596857 | 0.6774 | 31 | 0.08378 |
| holdout | value_quality_baseline | 0.0051711144 | 0.6667 | 12 | 0.116544 |
| holdout | low_margin_crowding | -0.0036056252 | 0.5 | 12 | -0.04849 |
| holdout | margin_low_crowding_confirmation | 0.0009137623 | 0.4167 | 12 | 0.046385 |
