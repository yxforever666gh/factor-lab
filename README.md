# Factor Lab

Factor Lab 是一条本地、可复现的 A 股组合研究链：Parquet 数据 → 固定方向截面排名 →
逐日成交与账户核算 → fresh equal-AUM 比较 → 10 个相关调仓相位稳健性汇总 → 协议冻结。

项目不再依赖 WebUI、Docker、PostgreSQL、MinIO、Dagster、Hermes 或自治 Agent。旧 Research OS 已完整归档在 Git tag `research-os-final-20260826`，不再进入当前主线。

> 5.4 运行实现执行 5.0 已冻结、由 5.2 协议完整定义的 `fixed_core_full` 路线：历史只负责
> 确定这一条待检验假设，新的
> signal、targets、十个虚拟 sleeve、执行快照和 outcome 从 2026-08-21 之后开始不可回填地
> 积累。confirmed outcome 达到预注册门槛前仍不能称为独立 OOS 验证。项目不连接券商、
> 不下真实订单，也不保证未来收益。

历史版本与当前未发布改动见 [CHANGELOG.md](CHANGELOG.md)。正式发布、Git tag 与 GitHub
同步规则见 [RELEASING.md](RELEASING.md)。

未来 tag 统一通过 `./scripts/publish-tag.ps1 -Tag <major.minor>` 发布；大方向递增 major，
小方向递增 minor。脚本会确认
GitHub CI 成功并在推送后核对远端 tag SHA。

## 安装

普通历史诊断可以直接安装开发依赖：

```powershell
python -m pip install -e ".[dev]"

# 需要从 Tushare/AkShare 更新数据时
python -m pip install -e ".[data,dev]"
```

5.4 的发布、前瞻 decision、execution、outcome、replay 和 evaluate 必须使用项目内的专用
运行环境 `runtime/environments/5.4`。该环境固定为当前发布主机的 CPython 3.10.16，并从
`protocols/5.2-runtime-lock.txt` 与项目内 wheelhouse 按逐文件 SHA-256 离线安装；随后用
同一 lock 中的项目 wheel 安装 Factor Lab 本身。不要使用 editable install，也不要让系统
Python 或用户级 site-packages 参与前瞻证据：

```powershell
$factorLabPython = (Resolve-Path `
  "runtime/environments/5.4/Scripts/python.exe").Path
$wheelhouse = (Resolve-Path `
  "runtime/environments/5.4/wheelhouse").Path

& $factorLabPython -m pip install --no-index --find-links $wheelhouse `
  --require-hashes -r protocols/5.2-runtime-lock.txt
& $factorLabPython -c `
  "import factor_lab; assert factor_lab.__version__ == '5.4.0'"
```

下文的 `python -m factor_lab.cli prospective ...` 表示应由 `$factorLabPython` 执行；发布
胶囊还会再次核对完整 CPython build 字符串、平台标签、全部已安装 distributions、lock 和
全部 `src/factor_lab/**/*.py` 的发布 Git blob，任一不一致都会停止。

数据源凭据可使用本机环境变量 `TUSHARE_TOKEN`，也可放在配置指定的
`runtime/secrets/settings/tushare_token`；运行数据、密钥和报告位于已忽略的
`runtime/`，不会进入 Git。

## 唯一 CLI

```powershell
# 查看 canonical Parquet 是否就绪
python -m factor_lab.cli data status

# 首次将现有冻结数据复制并校验到 runtime/data/top500
python -m factor_lab.cli data build --full --apply-migration --hash

# 增量同步三类 Tushare 日分区，并预存覆盖持有期的官方日历
python -m factor_lab.cli data sync --from 2026-08-22 --to 2026-08-28 `
  --calendar-to 2026-09-15 --resume

# 下载并校验官方停复牌历史快照
python -m factor_lab.cli data suspensions --from 2017-01-01 --to 2026-08-21 --resume

# 续传 PIT 财务指标和历史月末名称/行业，并原子更新 canonical Parquet
python -m factor_lab.cli data enrich --from 2017-01-01 --to 2026-08-13 --resume

# 默认主线 canary：冻结协议、四专家信号与执行入口 smoke
python -m factor_lab.cli research run --canary --resume

# 5.0 全历史：40 个因果影子账户、50 个共同起点评分账户、冻结 gate 与路由
python -m factor_lab.cli research run --suite adaptive --full --resume

# 4.1 hard-selector 纠正基线，仅用于复现历史诊断
python -m factor_lab.cli research run --suite walk-forward --full --resume

# 旧全历史方向/冠军榜，仅用于复现历史诊断
python -m factor_lab.cli research run --suite results-first --full --resume

# 旧保守 recovery 协议，仅保留为历史诊断
python -m factor_lab.cli research run --suite recovery --full --resume

# 已冻结的旧价值族实验，仅用于复现既有研究
python -m factor_lab.cli research run --suite next --full --resume

# 旧八因子数值回归
python -m factor_lab.cli research run --suite legacy-regression --full --resume

python -m factor_lab.cli research status
python -m factor_lab.cli report --run latest

# 5.0 tag 发布且权威 full run 绑定完成后，激活不可回填的前瞻账本
python -m factor_lab.cli prospective activate `
  --run <authoritative-run-id> --release-tag 5.0
# 5.1 修复后恢复已成功的精确远端 run；不得重新 dispatch activation canary
python -m factor_lab.cli prospective attest --purpose activation_canary `
  --release-tag 5.0 --workflow-run-id 33132845922

# 5.4 tag 与 GitHub 同步后，绑定并见证完整运行实现
python -m factor_lab.cli prospective upgrade `
  --manifest protocols/5.2-target-generator.json --release-tag 5.4
# 为保持 2026-08-31 是第一条真实前瞻 signal，本次 implementation canary 的可信 Tlog
# 必须晚于 2026-08-28 15:00 Asia/Shanghai，且早于 2026-08-31 15:00。
python -m factor_lab.cli prospective attest `
  --purpose implementation_upgrade_canary --release-tag 5.0

# 为 signal 的“下一官方交易日所在月份”生成 Top500；input 不会隐式选择“最新”快照
# 2026-08-31 的下一官方交易日在 9 月，因此首条 signal 就必须显式绑定 2026-09 membership
$membershipSnapshot = python -m factor_lab.cli prospective membership `
  --month 2026-09 | ConvertFrom-Json
python -m factor_lab.cli prospective input --signal-date 2026-08-31 `
  --membership-snapshot $($membershipSnapshot.membership_path)

# 对上一步 JSON 返回的 directory 生成 create-only 计划，再封存并见证 decision
python -m factor_lab.cli prospective plan --input <input-snapshot-directory>
python -m factor_lab.cli prospective seal --plan <stored-plan-path>
python -m factor_lab.cli prospective attest --purpose decision_anchor --release-tag 5.0 `
  --decision-record-sha256 <decision-sha> --admission-deadline-utc <deadline-utc>

# i+11开盘及停复牌资料到齐后，只能从decision哈希重建执行和结算
python -m factor_lab.cli prospective execution --decision <decision-sha>
python -m factor_lab.cli prospective outcome `
  --decision <decision-sha> --execution <execution-snapshot-sha>
python -m factor_lab.cli prospective status
python -m factor_lab.cli prospective audit
python -m factor_lab.cli prospective evaluate
```

上例的 2026-09 membership 可以把 effective/state date 记为 `2026-09-01`，但其
`as_of_date` 必须不晚于 `2026-08-31` signal，且构建完成与全部输入可用时间仍必须早于
该 decision 的 pretrade deadline；`input` 会验证这些边界并 fail-closed。

## 当前主线：5.2 前瞻执行闭环

5.0 不再让一组高度相关的价值信号做 hard switch。系统固定保留 4.1 事后观察到最稳健的
70% 防御价值核心，同时把两个可能增加复杂度的机制隔离成挑战者：市场风险覆盖层和因果
在线分配。协议 `protocols/5.0.json` 在首次历史执行前冻结十个 offset、五类账户、四组配对
gate 和三分支路由；看到结果后不能改阈值、挑最好相位或重写路由。

权威运行 `88009f1e5309b268` 建立 40 个连续独立成本影子账户和 50 个从
`2018-09-03` 以 5000 万现金、空仓开始的 fresh equal-AUM 账户。40/40 影子与 50/50
评分账户通过状态、目标 cohort、完整逐日 NAV、执行输入、容量、未来输入和期末复利对账；
feedback/overlay 未来违规均为 0。该运行只用于提交前审计，不能成为发布权威；正式身份只
认 clean `5.0` tag target 上的完整运行，并由 activation record 同时绑定 run fingerprint、
manifest、adaptive summary 与重算路由，避免在 tracked 文档里预填一个循环失效的 run id。

冻结 gate 全部拒绝新增复杂度，历史路由为 `fixed_core_full`：

- 固定核心全仓年化收益 Q20 / median / worst 为 10.62% / 11.07% / 10.11%，Sharpe
  Q20 为 0.674、IR Q20 为 0.162、最大回撤 Q20 为 -18.30%。
- 核心风险覆盖层在 89.28% 的信号日降低暴露，最大回撤配对 Q20 改善 2.31 个百分点，
  但年化收益和 Sharpe 配对 Q20 分别损失 6.49 个百分点和 0.216，0/10 offset 改善年化；
  因此覆盖层 gate 失败。
- 静态分散相对固定核心的年化 / Sharpe 配对 Q20 为 -1.14 个百分点 / -0.056，
  0/10 offset 改善年化。在线分配又相对静态先验略差：年化 / Sharpe 配对 Q20 为
  -0.04 个百分点 / -0.002，只有 1/10 offset 改善年化；因此在线挑战者 gate 失败。

这不是“稳定盈利已经证明”。它表示在当前历史证据下，可靠方向反而是删去不能证明增益的
覆盖层、分散 sleeve 和在线权重，把可执行路线收缩为单一固定核心。正式 5.0 激活后，前瞻
账本从数据截止日之后的第一条新决策开始，确认观察数从 0 起步，历史记录不得回填；只有
前瞻证据才能决定固定核心是否真的值得继续。

5.2 补齐了此前真正阻塞第一条 decision 的缺口。route→targets 只能由已发布 commit 的隔离
runner 从内容寻址 PIT 输入重建；十个 500 万元 sleeve 从现金独立启动，选择状态与实际账户
状态分离；执行和 outcome 只能读取封存的市场、日历、membership、停复牌和退市证据。
发布后的主分支可以继续修 adapter 或增加下一版本，旧周期仍用旧 tag 胶囊重放，不靠冻结
当前 checkout 维持一致性。

真实 11-session 持有窗与 10-session 同 offset 间隔会在共同开盘边界短暂形成两代在途周期，
所以每个 offset 最多允许两个 open cycle；第三代 admission 前必须先封存最老 outcome。同一
offset 的 execution 和 outcome 也只能按 calendar index 从老到新关闭，不能挂起亏损旧周期、
选择性结算较新的幸存周期。日常 status/写入仍逐条重放结构、收据及全部内容寻址 artifact，
但可复用已经过胶囊验证且仍与完整递归 CAS 树一致的前缀，只让 release runner 计算新增 suffix；
`audit` 永远绕过该缓存做全历史胶囊重放并刷新前缀。

前瞻评价不会每日改门槛：10 条且每 offset 至少一条只说明机械闭环可运行；60 条且每 offset
至少六条时只允许在绝对和主动复利都明显普遍失败时提前否决，不能提前宣布成功；250 条且
每 offset 至少 25 条后才运行约一年的方向 gate。正式 gate 除了逐 offset 的绝对/主动 CAGR、
Sharpe、完整持有期日频回撤和相位一致性，还要求真实 5,000 万元 master portfolio 的终值、
绝对/主动 CAGR、日频 Sharpe 和日频最大回撤全部过线。未启动 sleeve 按现金计，benchmark
冻结 decision-time roster：起点缺价留现金、停牌沿用最后价格、退市归零，不能事后删除或
重配幸存者。即使全部通过，也只表示这一年没有否决该方向，不能宣称稳定盈利或晋级实盘；
若失败则封存终止状态，下一步按版本规则发布新的 major 方向，而不是在同一段历史上微调
5.x 因子。

当前科学信任边界仍是假定自动化使用唯一默认账本并及时执行一次官方数据采集。人为复制隐藏
ledger root，或在首次 outcome 前反复替换原始 checkpoint、生成多个自洽 execution snapshot
再择优提交，属于本地对抗性操作，5.2 不用远端全局 registry 阻止。工作日自动化会固定使用
默认根并在证据首次可得时立即构建；若将来需要抵抗恶意操作者，应另发版本增加远端唯一性或
execution-binding 记录，而不是把这层安全取证继续塞进当前结果优先的主线。

## 4.1 纠正基线：否决 hard selector

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
  adaptive/
    adaptive-summary.json
    offset-00/ ... offset-09/
  robustness.json      # 仅在触发时存在
  report.md
runtime/runs/latest.json

runtime/prospective/5.0/
  records/             # create-only canonical JSON hash chain
  snapshots/           # 可提交 GitHub attestation 的 immutable snapshot
  bundles/             # 已验证的远端 attestation bundle
  source-artifacts/    # 原始分区/日历/成员等逐字节SHA-256副本
  membership/          # forward-only月度Top500快照
  inputs/              # 单日窄PIT signal snapshot
  executions/          # i+1..i+11 source-backed执行证据
  release-runners/     # 对应发布commit的隔离源码胶囊
```

## 测试

```powershell
$factorLabPython = (Resolve-Path `
  "runtime/environments/5.4/Scripts/python.exe").Path
$localTestRun = "local-" + [guid]::NewGuid().ToString("N")
& $factorLabPython -m pytest tests/unit tests/data tests/integration -q `
  --basetemp "runtime/test-tmp/$localTestRun" -p no:cacheprovider
& $factorLabPython -m compileall -q src/factor_lab
```

## 数据边界

冻结历史桥接样本截至 2026-08-21；此后的 signal 只由新同步且带可用时间的分区扩展。ST 与
名称状态来自当时可见的月末
`bak_basic.name`，日内 ST 事件历史仍不可用，因此报告固定标记
`monthly_name_verified_daily_events_unavailable`。缺失历史参考记录的股票/月会被明确排除，
不会静默当作普通股票。当前 3 组经正式公告确认的证券代码迁移使用左闭右开的 PIT
有效区间解析，已恢复 24 个 member-month；不按名称或上市日期模糊猜测。最后 6 个交易日的
execution 尾部只用于最后一批信号的退出估值，不产生新信号。全部结果仍属于已观察历史诊断，
不能据此宣称未来盈利能力。
