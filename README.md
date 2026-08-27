# Factor Lab

Factor Lab 是一条本地、可复现的 A 股历史成绩优化主线：Parquet 数据 → 因子计算 →
有向截面排名 → control/challenger 权重混合 → 真实成本多头回测 → 全历史成绩榜。

项目不再依赖 WebUI、Docker、PostgreSQL、MinIO、Dagster、Hermes 或自治 Agent。旧 Research OS 已完整归档在 Git tag `research-os-final-20260826`，不再进入当前主线。

> `results-first` 会直接使用训练、验证和审计全部已观察历史来选择最佳构造；它追求当前
> 回测成绩，不把结果包装成独立 OOS。本项目不连接券商、不下单，也不保证未来收益。

历史版本与当前未发布改动见 [CHANGELOG.md](CHANGELOG.md)。正式发布、Git tag 与 GitHub
同步规则见 [RELEASING.md](RELEASING.md)。

未来 tag 统一通过 `./scripts/publish-tag.ps1 -Tag <major.minor>` 发布；大方向递增 major，
小方向递增 minor。脚本会确认
GitHub CI 成功并在推送后核对远端 tag SHA。

## 安装

```powershell
python -m pip install -e ".[dev]"

# 需要从 Tushare/AkShare 更新数据时
python -m pip install -e ".[data,dev]"
```

数据源凭据可使用本机环境变量 `TUSHARE_TOKEN`，也可放在配置指定的
`runtime/secrets/settings/tushare_token`；运行数据、密钥和报告位于已忽略的
`runtime/`，不会进入 Git。

## 唯一 CLI

```powershell
# 查看 canonical Parquet 是否就绪
factor-lab data status

# 首次将现有冻结数据复制并校验到 runtime/data/top500
factor-lab data build --full --apply-migration --hash

# 增量同步三类 Tushare 日分区
factor-lab data sync --from 2026-08-14 --to 2026-08-26 --resume

# 续传 PIT 财务指标和历史月末名称/行业，并原子更新 canonical Parquet
factor-lab data enrich --from 2017-01-01 --to 2026-08-13 --resume

# 默认主线：control + challenger 多权重组合的快速 smoke test
factor-lab research run --suite results-first --canary --resume

# 全样本真实成本成绩优化与排行榜
factor-lab research run --suite results-first --full --resume

# 旧保守 recovery 协议，仅保留为历史诊断
factor-lab research run --suite recovery --full --resume

# 已冻结的旧价值族实验，仅用于复现既有研究
factor-lab research run --suite next --full --resume

# 旧八因子数值回归
factor-lab research run --suite legacy-regression --full --resume

factor-lab research status
factor-lab report --run latest
```

## 当前主线：Results-first

- 控制信号和 challenger 直接用全部已观察历史确定方向；这是有意的样本内成绩优化。
- 对每个 challenger 先构造 30%、70% 两档粗网格有向秩混合；challenger 缺失时回退
  control，control 缺失时不产生组合信号。
- 单体 challenger 只负责方向和信号诊断；冠军榜仅回测 control 与完整 fallback-control
  混合策略，确保日期覆盖和资金暴露可比。
- 使用全部已观察历史的成本后年化收益、Sharpe、IR 与最大回撤百分位形成排行榜，权重
  分别为 50%、25%、15%、10%；不使用
  `validated` 标签，也不因旧 promotion gate 失败而停止搜索。
- Canary 只检查执行链，不输出“最佳策略”；只有 full 模式生成历史榜单。
- 当前 suite 搜索 PIT 现金流质量、防御价值、60 日动量/反转、低波动和低换手对价值控制
  的增量组合。

## 保留的旧研究协议

- 训练：2017–2022，只在这里确定因子方向。
- 验证：2023–2024，只用于 Stage B 成本后组合硬门槛，不参与 Stage A 排序。
- 审计：2025 至数据截止日，单独报告，不伪装成盲测。
- Stage A：每 5 个交易日计算一次非重叠 Rank IC，只根据训练段冻结方向、检查覆盖率、相关性去重，最多放行 3 个 Challenger。
- Stage B：5000 万资金、周频、多头 Top-50（Top-75 留仓缓冲）、单股最多 2%、ADV 最多 5%，包含真实换手成本和涨跌停/停牌约束。开盘成交使用该股票上一可见交易日的 ADV/波动率，不读取当日收盘后信息。
- 晋级还要求验证期主动收益循环区块 Bootstrap 的 95% 下界不小于 0，且基准成分收益覆盖率不低于 95%。
- 没有因子通过时输出 0 个 validated，并在有限稳健性矩阵结束后停止，不降低门槛。

旧 `recovery` 只注册一个与旧价值信号低相关的机制 Challenger：

- `pit_cashflow_quality`：使用公告后下一交易日起可见的 ROIC、单季经营现金流/收入和资产负债率；至少两个分量有效，并做 PIT 行业与规模中性化。

控制项仍为 `earnings_yield_over_pb`。旧四个价值变体保留在 `next` suite，旧八因子保留在 `legacy-regression` suite；它们不再自动扩展新变体。

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

当前 canonical 样本为 2017-01-03 至 2026-08-13。ST 与名称状态来自当时可见的月末
`bak_basic.name`，日内 ST 事件历史仍不可用，因此报告固定标记
`monthly_name_verified_daily_events_unavailable`。缺失历史参考记录的股票/月会被明确排除，
不会静默当作普通股票。当前 3 组经正式公告确认的证券代码迁移使用左闭右开的 PIT
有效区间解析，已恢复 24 个 member-month；不按名称或上市日期模糊猜测。最后 6 个交易日的
execution 尾部只用于最后一批信号的退出估值，不产生新信号。全部结果仍属于已观察历史诊断，
不能据此宣称未来盈利能力。
