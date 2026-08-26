# Factor Lab

Factor Lab 是一条本地、可复现的 A 股因子研究主线：Parquet 数据 → 因子计算 → 训练段冻结方向 → 验证段筛选 → 真实成本多头回测 → Markdown/JSON 报告。

项目不再依赖 WebUI、Docker、PostgreSQL、MinIO、Dagster、Hermes 或自治 Agent。旧 Research OS 已完整归档在 Git tag `research-os-final-20260826`，不再进入当前主线。

> 当前结果全部属于 `historical_diagnostic`。本项目不连接券商、不下单，也不保证未来收益。

## 安装

```powershell
python -m pip install -e ".[dev]"

# 需要从 Tushare/AkShare 更新数据时
python -m pip install -e ".[data,dev]"
```

数据源凭据继续使用本机环境变量，例如 `TUSHARE_TOKEN`；运行数据、密钥和报告位于已忽略的 `runtime/`，不会进入 Git。

## 唯一 CLI

```powershell
# 查看 canonical Parquet 是否就绪
factor-lab data status

# 首次将现有冻结数据复制并校验到 runtime/data/top500
factor-lab data build --full --apply-migration --hash

# 增量同步三类 Tushare 日分区
factor-lab data sync --from 2026-08-14 --to 2026-08-26 --resume

# 50 只股票 / 20 个交易日 smoke test
factor-lab research run --suite next --canary --resume

# 固定的下一轮价值族研究；失败时自动运行有限稳健性矩阵
factor-lab research run --suite next --full --resume

# 旧八因子数值回归
factor-lab research run --suite legacy-regression --full --resume

factor-lab research status
factor-lab report --run latest
```

## 研究协议

- 训练：2017–2022，只在这里确定因子方向。
- 验证：2023–2024，用于 Stage A 放行和成本后组合硬门槛。
- 审计：2025 至数据截止日，单独报告，不伪装成盲测。
- Stage A：每 5 个交易日计算一次非重叠 Rank IC，检查方向和覆盖率，最多放行 3 个 Challenger。
- Stage B：5000 万资金、周频、多头 Top-50、单股最多 2%、ADV 最多 5%，包含真实换手成本和涨跌停/停牌约束。
- 没有因子通过时输出 0 个 validated，并在有限稳健性矩阵结束后停止，不降低门槛。

当前固定的新变体位于 `configs/factors.json`：

- `value_rank`
- `value_quality_rank`
- `value_defensive_rank`
- `value_low_turnover_rank`

控制项为 `earnings_yield_over_pb`。

## 项目结构

```text
src/factor_lab/
  cli.py
  data/          本地路径、Parquet 审计、Tushare 分区同步
  research/      因子合同、表达式、Stage A/B、断点续跑、报告
  portfolio/     唯一多头执行、费用和账户核算
configs/         数据、因子和研究协议
tests/           单元、数据与集成测试
runtime/         本地数据和运行结果（Git ignored）
```

每次研究结果写入：

```text
runtime/runs/<run-id>/
  manifest.json
  summary.json
  factors/<factor>.json
  robustness.json      # 仅在触发时存在
  report.md
runtime/runs/latest.json
```

## 测试

```powershell
python -m pytest tests/unit tests/data tests/integration -q
python -m compileall -q src/factor_lab
```

Windows 上若系统 `%TEMP%` 的 pytest 目录 ACL 异常，可显式指定：

```powershell
python -m pytest tests/unit tests/data tests/integration -q `
  --basetemp runtime/pytest-temp/local -p no:cacheprovider
```

## 数据边界

当前冻结样本为 2017-01-03 至 2026-08-13，历史 ST 仍标记为 `st_history_unverified`。它可以用于交易引擎回归和历史因子比较，但报告会保留该警告；不能据此宣称实盘盈利能力。
