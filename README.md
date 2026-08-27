# Factor Lab

Factor Lab 是一条本地、可复现的 A 股 selector 内部因果 walk-forward 研究 framework：Parquet 数据 →
固定方向截面排名 → 预注册 control/challenger 候选 → 仅使用已成熟成本后收益轮换 →
真实成本多头回测 → 10 个调仓相位汇总。

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

## 当前主线：Walk-forward 4.0

`walk-forward` 是默认研究 framework；`causal_walk_forward_dynamic` 只是其中的动态实验
账户，不是默认获胜策略，也不会因为 framework 升级而自动晋级。

- 控制项仍是 `earnings_yield_over_pb`。候选只包括 `value_defensive_rank`、
  `low_volatility` 和 `low_turnover`；控制项与三个候选的方向全部预注册为固定 `+1`，
  历史收益不能翻转任何较早时点的方向。
- 每个 challenger 只生成 30%、70% 两档预注册有向秩混合。challenger 缺失时回退
  control，control 缺失时不产生组合信号；不连续优化因子权重。
- 每个候选维护独立、连续、包含成本的影子账户。决策日只能使用
  `end_date < signal_date` 的完整持有期；决策日当天结束的收益也明确排除。
- 另设不读取收益、始终对同一 candidate registry 等权的 `fixed_registry_equal_weight`
  成本后无择时基准；动态轮换的同样本历史阈值诊断必须相对它达标，不能只和较弱
  control 比，但达标也不构成证明或独立验证。这里等权的是 control 加六个预注册混合
  策略，不是四个原始因子等权。
- 选择器回看 756 个交易日，必须有 60 个共同完成的 10 日持有期；至少间隔 63 个交易日，
  并在该 offset 的下一个调仓信号日更新，只比较成本后 Sharpe。只有至少领先 control `0.10` 的 challenger 才能入选，
  control 始终进入合格集合，再与越过 guard 的 challenger 一起按成绩取最多前三名等权；
  若无 challenger 合格，则只使用 control。
- 部署组合固定为 5000 万、Top-10、留仓缓冲 5、每 10 个交易日调仓。完整运行必须覆盖
  offset `0..9`，从十个相位都完成 warmup 后的共同日期开始比较，并报告 Q20、median、
  worst 与 IQR；不得选择表现最好的 offset。
- 该协议保证模拟内部的选择只读取当时已成熟的收益，不会把已经反复看过的 2017–2026
  数据重新变成盲测。真正更干净的确认只能来自协议冻结后的新增市场数据。

这里的“因果”边界仅覆盖 selector 的成熟收益 cutoff 与预注册方向，并不自动证明输入数据
在每个时点都保留了原始 revision vintage。退市/吸收合并的 ghost position 处置、候选间
等 AUM 可比性和历史分段资本重置也仍需加固；十个 offset 共用同一市场路径，不是十份
独立样本。详见 [CHANGELOG.md](CHANGELOG.md) 的 4.0 Known limitations。

当前完整运行 `97840d20b4a2ff71` 在共同区间 2019-08-16 至 feature 决策数据末端
2026-08-13（退出估值行情延伸至 2026-08-21）的十相位结果为：动态实验账户成本后年化
收益 Q20 / median / worst 为 8.88% / 9.11% / 6.85%，Sharpe Q20 为 0.561、最大回撤
Q20 为 -18.15%；选择违规为 0、周期覆盖为 100%，十个 offset 的年化均高于各自 control。

但 fixed comparator 的年化 Q20 / median / worst 为 9.50% / 10.22% / 8.84%，Sharpe
Q20 为 0.587、最大回撤 Q20 为 -16.59%。逐 offset 配对后，dynamic − fixed 的年化、
Sharpe、IR、最大回撤 Q20 分别为 -1.35 个百分点、-0.098、-0.043、-2.98 个百分点；
动态仅在 5/10 个 offset 年化更高。因此 `historical_diagnostic_passed=false`，动态账户只
排第 7，固定基准排第 4；当前证据支持候选篮子，不支持这套轮换逻辑。事后 phase 排名
最强的 70% 防御价值静态候选 Q20 年化为 11.13%，但它同样不是独立 OOS 或可直接晋级的
赢家。

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
