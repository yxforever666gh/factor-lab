# 第 21 轮总结：ownership concentration/top10 PIT daily as-of 复核仍未超过 benchmark，路线停止

## 本轮执行范围

本轮按用户要求先读取了最新总结与指定计划：

- 最新总结：`knowledge/autonomous_alpha_loop_latest_summary.md`
- 指定计划：`.hermes/plans/2026-05-09_ownership_concentration_source_plan.md`

同时写出本轮执行计划：

- `.hermes/plans/2026-05-13_ownership_concentration_rerun_execution_plan.md`

硬性边界保持：

- 未启动 broad daemon。
- 未 enqueue workflow。
- 未恢复 old broad/generated/recent/rolling 路径。
- 未做无界 API 拉取。
- 优先复用本地 cache：`artifacts/ownership_top10_mvp/top10_floatholders_raw.csv`。
- 持仓数据使用 `ann_date <= trade_date` 的 PIT daily as-of，不使用 `end_date` 前视。

## 本轮执行内容

复核并运行已有 ownership concentration read-only diagnostic：

- 模块：`src/factor_lab/ownership_concentration_source.py`
- 脚本：`scripts/write_ownership_concentration_source_mvp.py`
- 测试：`tests/test_ownership_concentration_source.py`

本轮 artifact：

- `artifacts/ownership_concentration_source_mvp/ownership_concentration_source_mvp.json`
- `artifacts/ownership_concentration_source_mvp/ownership_concentration_source_mvp.md`
- `artifacts/ownership_concentration_source_mvp/ownership_top10_raw_sample.csv`
- `artifacts/ownership_concentration_source_mvp/ownership_top10_statement_features.csv`
- `artifacts/ownership_concentration_source_mvp/ownership_top10_daily_asof_features.csv`
- `knowledge/ownership_concentration_source_mvp.md`

## 数据源与 PIT preflight

本轮复用 legacy bounded cache：

- Source: `legacy_cache`
- Path: `artifacts/ownership_top10_mvp/top10_floatholders_raw.csv`

Preflight：

- Raw source rows: `19010`
- Raw source tickers: `77`
- PIT-safe endpoints: `1`
- `top10_floatholders`: required fields present，`ann_date_nonnull_rate=1.0`，`end_date_nonnull_rate=1.0`
- `top10_holders`: 本地无可用 rows，因此未贡献本轮信号

Feature construction：

- Statement feature rows: `1897`
- Daily as-of rows before feature dropna: `48779`
- Final diagnostic coverage rows: `32642`
- Dates: `845`
- Tickers: `57`
- Signal columns: `24`

## 关键结果

Local baseline：

- `value_quality_baseline` Q3-Q0 spread: `0.0012177106`
- Observations: `845`
- Positive rate: `0.5077`

Best ownership signal：

- Signal: `conf_low_top10_float_hkscc_ratio_change`
- Q3-Q0 spread: `0.0030241217`
- Observations: `845`
- Positive rate: `0.5444`
- Bucket means: Q0 `-0.0007973956`，Q3 `0.0022267261`，Q4 `-0.0010124197`

Benchmark：

- `value_quality_no_distress` bucket spread: `0.0062253011`

是否超过 benchmark：`false`。

## 本轮 decision

`stop_ownership_concentration_not_incremental`

原因：

- `best_signal_not_above_value_quality_benchmark`
- best ownership signal 虽高于本轮 local baseline，但只有 `0.0030241217`，不到 benchmark `0.0062253011` 的一半。
- `top10_floatholders` 本地 PIT 数据可用，但 alpha 增量不足。
- `top10_holders` 本地 bounded cache 未覆盖，当前没有足够证据支持为此路线做新的无界 API 回填。

因此：

- 不写 controlled probe plan。
- 不 dry-run admission。
- 不 enqueue workflow。
- 不继续 top10 ownership-concentration 变体。

## 验证

已执行：

```bash
PYTHONPATH=src .venv/bin/python scripts/write_ownership_concentration_source_mvp.py
PYTHONPATH=src python3 -m py_compile \
  src/factor_lab/ownership_concentration_source.py \
  scripts/write_ownership_concentration_source_mvp.py \
  scripts/dry_run_controlled_restart.py
PYTHONPATH=src .venv/bin/python -m pytest tests/test_ownership_concentration_source.py -q
PYTHONPATH=src python3 scripts/dry_run_controlled_restart.py
PYTHONPATH=src python3 scripts/audit_runtime_takeover.py
```

结果：

- Targeted tests: `4 passed, 8 warnings`
- Controlled restart dry-run: `pending_count=4`, `would_run_count=0`, `blocked_count=0`
- Runtime takeover audit recommendations: `['pause_broad_daemon', 'allow_controlled_only_daemon']`

说明：首次尝试 `uv run --with ...` 因 PyPI 网络超时失败；随后改用项目本地 `.venv/bin/python` 完成脚本与测试。

## 下一步

下一轮计划：

- `.hermes/plans/2026-05-13_pledge_followup_write_admission_after_ownership_stop_plan.md`

理由：ownership concentration 已再次停止；当前唯一已有明确正向证据且已通过 dry-run admission 的路线是 pledge follow-up：

- Route: `value_quality_high_pledge_record_count_confirmation`
- Parent bucket-aware Q3-Q0 spread: `0.015417`
- Benchmark: `0.0062253011`
- Follow-up: `cost_sensitivity_20bps`
- Prior dry-run admission: `allow`，`would_enqueue_count=1`，`enqueued_count=0`

下一轮只允许 exactly-one controlled write admission；仍禁止 broad daemon、无界 API、超过一条 workflow enqueue、以及 production 默认 `--force-new`。
