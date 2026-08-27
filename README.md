# Factor Lab

Factor Lab 是一条本地、可复现的 A 股组合研究链：Parquet 数据 → 固定方向截面排名 →
逐日成交与账户核算 → fresh equal-AUM 比较 → 10 个相关调仓相位稳健性汇总 → 协议冻结。

项目不再依赖 WebUI、Docker、PostgreSQL、MinIO、Dagster、Hermes 或自治 Agent。旧 Research OS 已完整归档在 Git tag `research-os-final-20260826`，不再进入当前主线。

> 默认 `walk-forward` 在每个历史决策点严格排除尚未结束的持有期，但候选库和协议是在
> 看过既有历史后设计的，因此证据等级只能是 `post_selection_causal_simulation`，不是
> 独立 OOS。项目不连接券商、不下单，也不保证未来收益。

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

# 下载并校验官方停复牌历史快照
factor-lab data suspensions --from 2017-01-01 --to 2026-08-21 --resume

# 续传 PIT 财务指标和历史月末名称/行业，并原子更新 canonical Parquet
factor-lab data enrich --from 2017-01-01 --to 2026-08-13 --resume

# 默认主线：固定方向、selector 配置与静态执行链 smoke（不执行 selector）
factor-lab research run --canary --resume

# 全历史因果模拟、10 个调仓 offset 与相位汇总
factor-lab research run --suite walk-forward --full --resume

# 旧全历史方向/冠军榜，仅用于复现历史诊断
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

## 当前主线：校正后的 Walk-forward 4.1

4.1 首先修复了 4.0 最关键的研究错误：旧 dynamic/static 账户在公共评分起点前已经拥有
财富和持仓，而 fixed 账户从 5000 万现金开始，跨策略排名并不等资金可比。旧运行
`97840d20b4a2ff71` 的 selector cutoff 仍没有读取未来收益，但绩效、排名和 gate 已作废。

校正协议有三条硬约束：

- 全历史、连续、含成本的影子账户只给 selector 提供 `end_date < signal_date` 的成熟反馈；
  评分账户一律从同一个 `2019-08-16` 以 5000 万现金、空仓启动。
- 七个静态候选、fixed comparator 与 dynamic 在十个 offset 上形成 90 个 fresh equal-AUM
  账户。收益、年度/半年度指标和最大回撤来自完整逐日 NAV，不能用稀疏调仓边界替代。
- 每个 execution session 都处理停复牌、退市和估值。生产研究只接受
  `adjusted_total_return/open_adj`，拆股/分红影响已嵌入调整价格，不再叠加显式事件；
  退市默认零回收，不允许脏停牌行情成交或估值。本次 90 个账户没有实际触发退市冲销，
  因此该分支仍只有合同与测试证据。

完整校正运行 `6462d5550b459fb2` 的 90/90 个评分账户均通过起点、逐日路径、执行覆盖和
期末复利对账；每个账户有 2,028 个 daily NAV 观测，未来输入违规和容量违规均为 0，
benchmark 收益覆盖率最低 98.996%。manifest 的 181 个文件、大小、SHA-256 与自哈希也已
独立复核。

结果否决了 hard selector：dynamic 年化收益 Q20 / median / worst 为 8.90% / 9.25% /
6.93%，相对 fixed 的配对年化、Sharpe、IR、最大回撤 Q20 分别为 -1.41 个百分点、
-0.092、-0.045、-2.91 个百分点，只在 5/10 offset 年化更高。因此
`historical_diagnostic_passed=false`。

70% 防御价值静态候选在九个策略中排名第一，年化收益 Q20 / median / worst 为 11.16% /
11.58% / 10.53%，Sharpe Q20 为 0.707、IR Q20 为 0.181、最大回撤 Q20 为 -18.29%。它
相对 fixed 的配对年化和 Sharpe Q20 为 +1.22 个百分点和 +0.053，且 10/10 offset 年化
改善，因此基于已观察的 4.1 结果被选作下一阶段固定核心。这个选择不是预注册 gate；它
只用于冻结后续协议，仍是看过 2017–2026 历史后的 post-selection 结论，不是独立 OOS，
也不证明未来盈利。

当前 PIT lineage 会递归列出未验证 vintage 的 feature、execution、universe 和停复牌依赖，
并保持 `investment_claim_allowed=false`。十个 offset 共用同一市场路径、不是十份独立样本；
财务 revision vintage 和日内 ST 历史也不完整。详见 [CHANGELOG.md](CHANGELOG.md) 的 4.1
Known limitations。

## 保留的旧研究协议

`results-first` 保留为旧诊断 suite：它直接用全部已观察历史确定方向并生成冠军榜，能够
复现当时“哪种构造历史成绩最好”的问题，但不能用于证明任一历史决策是因果选择，也不再
是 CLI 默认入口。

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
  walk-forward/
    walk-forward-summary.json
    offset-00/ ... offset-09/
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
