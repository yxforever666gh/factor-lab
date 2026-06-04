# Mechanism Lessons

Generated from research quality summary.

- Ready value route candidates: 8
- Blocked value routes: 1

## Ready routes
- `industry_relative_value` / `industry_relative_value`: 行业内相对低估值比全市场裸低估值更能代表定价错误，并能减少行业结构暴露。
- `industry_relative_value` / `industry_relative_value`: 行业内相对低估值比全市场裸低估值更能代表定价错误，并能减少行业结构暴露。
- `industry_relative_value` / `industry_relative_value`: 行业内相对低估值比全市场裸低估值更能代表定价错误，并能减少行业结构暴露。
- `value_quality_no_distress` / `value_quality_no_distress`: 便宜且盈利质量尚可的公司比单纯便宜公司更可能发生估值修复。
- `value_quality_no_distress` / `value_quality_no_distress`: 便宜且盈利质量尚可的公司比单纯便宜公司更可能发生估值修复。
- `value_momentum_confirmation` / `value_momentum_confirmation`: 价值需要价格确认；价值加中期动量确认比裸价值更少接飞刀。
- `value_momentum_confirmation` / `value_momentum_confirmation`: 价值需要价格确认；价值加中期动量确认比裸价值更少接飞刀。
- `value_trap_exclusion` / `value_trap_exclusion`: 排除低质量、弱动量和高风险样本后，价值信号应改善回撤和稳定性。

## Blocked routes
- `historical_valuation_percentile` blocked by missing fields: ['pb_history_756d', 'pe_ttm_history_756d']

## Harvest cycle_0001
- No new durable mechanism lesson.

## Harvest cycle_0002
- No new durable mechanism lesson.

## Harvest cycle_0003
- No new durable mechanism lesson.

## Harvest cycle_0004
- value_quality_no_distress: positive_progress in cycle_0004 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0005
- No new durable mechanism lesson.

## Harvest cycle_0006
- value_quality_no_distress: positive_progress in cycle_0006 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0007
- No new durable mechanism lesson.

## Harvest cycle_0008
- No new durable mechanism lesson.

## Harvest cycle_0009
- No new durable mechanism lesson.

## Harvest cycle_0010
- No new durable mechanism lesson.

## Harvest cycle_0011
- No new durable mechanism lesson.

## Harvest cycle_0012
- No new durable mechanism lesson.

## Harvest cycle_0013
- No new durable mechanism lesson.

## Harvest cycle_0014
- No new durable mechanism lesson.

## Harvest cycle_0015
- No new durable mechanism lesson.

## Harvest cycle_0016
- No new durable mechanism lesson.

## Harvest cycle_0017
- No new durable mechanism lesson.

## Harvest cycle_0018
- No new durable mechanism lesson.

## Harvest cycle_0019
- No new durable mechanism lesson.

## Harvest cycle_0020
- No new durable mechanism lesson.

## Harvest cycle_0021
- No new durable mechanism lesson.

## Harvest cycle_0022
- value_quality_no_distress: positive_progress in cycle_0022 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0023
- value_quality_no_distress: positive_progress in cycle_0023 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0024
- value_quality_no_distress: positive_progress in cycle_0024 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0025
- value_quality_no_distress: positive_progress in cycle_0025 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0026
- value_quality_no_distress: positive_progress in cycle_0026 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0027
- value_quality_no_distress: positive_progress in cycle_0027 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0028
- value_quality_no_distress: positive_progress in cycle_0028 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0029
- value_quality_no_distress: positive_progress in cycle_0029 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0030
- value_quality_no_distress: positive_progress in cycle_0030 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0031
- value_quality_no_distress: positive_progress in cycle_0031 (value_quality_cost_sensitivity_v1).

## Harvest cycle_0032
- No new durable mechanism lesson.

## Harvest cycle_0033
- No new durable mechanism lesson.

## Harvest cycle_0034
- No new durable mechanism lesson.

## Harvest cycle_0035
- No new durable mechanism lesson.

## Harvest cycle_0036
- No new durable mechanism lesson.

## Harvest cycle_0037
- No new durable mechanism lesson.

## Harvest cycle_0038
- No new durable mechanism lesson.

## Harvest cycle_0039
- No new durable mechanism lesson.

## Harvest cycle_0040
- No new durable mechanism lesson.

## Harvest cycle_0041
- No new durable mechanism lesson.

## Harvest cycle_0042
- No new durable mechanism lesson.

## Harvest cycle_0046
- No new durable mechanism lesson.

## Autonomous Strategy Lab 2026-06-03
- `industry_cycle_inflection_value_anchor_v1`: stopped after cheap-screen risk gate. `industry_return_60d_positive` improved mean daily spread to `0.0026386024146501305` but max drawdown stayed unacceptable at `-1.691301239110362`; cheap + industry-cycle momentum is insufficient without stronger cashflow/quality/balance-sheet or earnings-confirmation filters.
- New mechanism requested: `quality_cashflow_value_repair_v1`. Hypothesis: cheap stocks are more likely valuation dislocations when cashflow resilience, quality, balance-sheet stress control, and earnings non-deterioration confirm the valuation signal; next gate is field resolution/PIT safety before any execution.
- `quality_cashflow_value_repair_v1` field resolution is blocked by missing full-quality fields (`gross_margin`, `current_ratio`, `quick_ratio`, `interest_coverage`) and non-equivalent proxy fields (`ocfps`, `operating_cashflow_yoy`). Proxy revision created as `quality_profit_proxy_value_repair_v1`, explicitly limited to `roe`, `profit_yoy`, `debt_to_asset`, and `operating_cashflow_to_profit` with PIT/proxy caveats.
- `quality_profit_proxy_value_repair_v1` field resolution is blocked by low coverage: `profit_yoy`, `debt_to_asset`, and `operating_cashflow_to_profit` exist as columns but are 0.0 coverage in scanned Tushare caches; only `roe` has full coverage and still requires PIT alignment. Do not proceed to proxy cheap screen until cache/data coverage is fixed.
- PIT overlay diagnostic shows the issue is integration, not a total external data-source failure: base feature cache has 0-coverage placeholder financial columns, but PIT financial cache has data. Overlay covers about 40.06% of base rows, still below the 60% gate because PIT cache only covers 77 tickers and 2020-06-02..2023-12-28 versus base cache's 96 tickers and 2017-03-15..2023-12-22.
