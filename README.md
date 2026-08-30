# Factor Lab 6.1

Factor Lab 是一条本地、可复现的 A 股截面策略研究链：canonical Parquet → PIT 信号 →
次日开盘成交 → 全成本逐日账户核算 → 10 个绝对日历调仓相位比较。

6.1 的主问题不再是“怎样继续组合旧因子”，而是“过去的 Top500 研究边界是否过早排除了
fixed-core 真正可交易的机会”。它保持信号、Top10/exit25、十相位、成本、次日开盘和容量模型
不变，只改变逐日因果机会集。正式结果尚未打开，因此当前仍不能声称找到稳定 alpha。

6.0 的结论不是“已经找到稳定 alpha”，而是两件更具体的事：

1. 停止继续为同一批弱相关信号建设 selector、overlay、前瞻账本和调度平台。
2. 把当前最可靠的可运行路线收缩为低换手 fixed-core，并让新机制必须先证明能在选择期
   稳定胜过它。

旧 Research OS 保存在 annotated tag `research-os-final-20260826`；完整 5.x 前瞻/影子实现
保存在已经同步 GitHub 的 annotated tag `5.9`。它们不再进入当前主线。

版本历史见 [CHANGELOG.md](CHANGELOG.md)，发布规则见 [RELEASING.md](RELEASING.md)。

## 为什么判断原方向错了

Git 历史显示，项目长期把主要增量投向研究平台与控制层，而不是新的信息源：`1.0 → 1.2`
增加约 49 万行；Research OS 归档到 `3.0` 时一次删除约 75.6 万行；但 `5.0 → 5.9` 又在
23 个提交中增加约 5.85 万行前瞻账本、attestation、controller、watchdog 和影子锦标赛代码。
这十个 5.x tag 在约 46 小时内发布，却一直是 0 个新增 market outcome。

与此同时，4.0 的 selector 结果被 4.1 的 equal-AUM/核算修正改变，5.0 的 overlay、静态分配和
在线分配都没有稳定胜过 fixed-core，5.9 的低换手/低波动 challenger 又是 0/10 offset 胜出。
因此错误不在“回测工程还不够复杂”，而在“同一组弱且高度相关的特征上叠了太多决策层”。
6.0 保留能防止虚假成绩的 PIT、执行和核算内核，删除不能创造信息优势的运行层。

## 6.0 基线

`fixed_core_top10_exit25` 保持旧 fixed-core 的信号定义、Top10、等权和十个 sleeve 不变：

```text
control   = rank(earnings_yield / pb)
defensive = rank(rank(book_yield) + rank(earnings_yield) + rank(-volatility_20))
score     = (1 - 0.70) * control + 0.70 * defensive
```

每个交易日只更新其绝对日历对应的一个 sleeve。新持仓从 Top10 进入，旧持仓在当日排名不差于
25 时保留，空位再按排名补齐。成交使用下一官方交易日 `open_adj`，并计入佣金、印花税、滑点、
冲击、涨跌停、停复牌、退市与容量约束。

相对旧 `exit15` 的 exact paired 结果如下；数值是年化收益率差的百分点，不是未来收益承诺：

| 区间 | active CAGR q20 | 中位数 | 最差 offset | 正 offset |
| --- | ---: | ---: | ---: | ---: |
| train（持有期末不晚于 2022-12-31） | +0.334 | +0.891 | -0.246 | 8/10 |
| validation（2023–2024） | +1.030 | +2.278 | +0.172 | 10/10 |
| audit（2025+） | -0.821 | +0.003 | -1.947 | 5/10 |
| full | +0.728 | +1.045 | +0.399 | 10/10 |

全区间 `exit25` 每个 10-session 调仓周期的平均换手中位数约 11.717%，比 `exit15` 的
18.216% 下降约 6.50 个百分点；十相位最差
日频最大回撤约 -27.047%。因此目前可靠的结论是“降低了换手并改善了历史全区间成本后结果”，
不是“发现了新的独立 alpha”。尤其 2025+ 的主动收益中位数接近零。

## 新信号搜索结果

6.0 首轮研究了 6 个只在公告可见后使用的 PIT 定义：增长加速度、现金质量变化、增长与现金流
确认，以及三者各自的 30% fixed-core 混合。选择输入在物理文件层截断至 2024-12-31，财务
available/report/announcement 时点违规均为 0。

结果是 6/6 都没有同时通过 train 和 validation 门槛，`selected_candidate_id=null`，所以没有把
任何公告机制接入正式分数，也没有为了挑赢家而查看 2025+。三个独立 event 组合的换手中位数
约 51.7%–54.7%，最差日频回撤约 -64.8% 至 -74.4%；最接近的现金质量混合又与 fixed-core
分数高度相关（约 0.976）。详细定义和负面结果位于：

- [protocols/6.0-pit-event-search.json](protocols/6.0-pit-event-search.json)
- [protocols/evidence/6.0/pit-event-negative.json](protocols/evidence/6.0/pit-event-negative.json)

这类失败会被保留为搜索边界，避免换个名字再次消耗计算量；它不会进入正式 registry。

随后又在同一物理截止上冻结并穷尽了 129 个旧数据候选，覆盖行业/规模中性动量、52 周高位、
趋势稳定、量价确认、残差趋势和质量×趋势。结果仍是 train q20 为正 `0/129`、validation q20
为正 `0/129`，没有 formal exact finalist，也没有打开 2025+。最优联合失败者与 fixed-core 的
score 相关仅 0.226，说明它确实正交，但 train / validation 主动 CAGR q20 约 -15.4% / -17.4%。
紧凑负面证据见
[orthogonal-canonical-negative.json](protocols/evidence/6.0/orthogonal-canonical-negative.json)。

6.0 当时还定义了一条卖方盈利预测修正数据线：按 `report_date` 后
首个官方交易日可用，先取每家券商对 ticker/目标季度的最新预测，再构造至少三家券商覆盖的
20/60 日 EPS、净利润共识修正与 revision breadth。供应商 `create_time` 已实测存在多年后回填，
只保留为 lineage，禁止参与 PIT availability。

6.0 已实现这条数据线的不可变 ingestion 闭包与 feature prototype：每个报告日以官方 3000 行上限做显式 `limit/offset`
分页，只有短页或空页才算完成；重复页、超页、第二页失败、跨页 identity 冲突或连续 100 个
满页都会阻断，且不会发布部分分区。Parquet 与逐页 hash manifest 在同一目录原子发布，已发布
日期不可覆盖；为避开供应商 19–22 点当日更新窗口，只允许发布上海时区昨天及更早的日期。
每个规范化 page 也单独保存并在 resume 时重算文件与内容 hash。特征构造器不读取价格、收益或
label，并再次验证 availability 必须恰好是 `report_date` 后第一个官方开市日；同券商同日多报告
按 EPS/NP 中位数聚合，不用 `create_time` 或内容 hash 决定先后，只输出信号年 FY0/FY1 的 Q4
预测，过期季度、非 Q4 和 FY2+ 不进入面板。

当前状态是 `ingestion_implemented_research_spec_permission_and_vintage_blocked`，不是 alpha 通过。
现有 Tushare 凭据只有试用权限
（每天 10 次）；[官方 `report_rc` 文档](https://tushare.pro/document/2?doc_id=292)要求 8000
积分取得正式日 100000 次权限，10000 积分以上无总量限制。
因此 2017–2024 全量回填与冻结后的 exact return validation 仍未执行，任何旧的单页 3886/5000
响应都被视为不完整证据。2017/2020 样本的 `create_time` 还分别晚了 1831/732 天；打开收益前
必须另购 [`research_report`](https://tushare.pro/document/2?doc_id=415)/原始研报档案抽样核验历史 timestamp 与预测值，或把严格证据限制在
首次捕获之后的前瞻样本。

打开任何新收益前，协议一次冻结三类有经济含义的小候选族：FY0/FY1 NP 共识修正（EPS 只作
拆送股稳健性对照）、FY1 相对 FY0 的预测增长，以及实际公告相对公告前共识的 earnings surprise。
coverage/initiations、dispersion 与 active-reviser breadth 先只做诊断，不再建 selector。

## 6.1 当前主线：扩大因果机会集

6.1 比较三个逐日重建的 universe：因果 ADV20 Top500 control、ADV20 至少人民币 1 亿元、ADV20
Top1500。证券必须是沪深人民币普通股、已上市至少 120 个官方交易日、当日非 ST；缺 bar 只有
同日或尚未清除的显式整日停牌证据才可解释。Train、validation、audit 的停复牌与 `stock_st`
分区拥有互不复用的物理私有根，不能靠同一文件换路径伪装成隔离。

流程只有一条前进路径：截至 2022 的 train 门不过就不打开 validation；截至 2024 的 validation
只运行 train 通过者；两段都过才按预注册 q20、中位主动 CAGR、容量和换手率排序冻结至多一个
winner；2025-01-01 至 2026-08-21 的历史 audit 只运行该 winner，失败后不换 runner-up。即使
audit 通过，也只记为“需要新鲜未来样本”，不允许盈利承诺。基础协议与红队前置修订见：

- [protocols/6.1-wide-universe.json](protocols/6.1-wide-universe.json)
- [protocols/6.1-wide-universe-amendment-1.json](protocols/6.1-wide-universe-amendment-1.json)
- [protocols/6.1-runtime.json](protocols/6.1-runtime.json)

## 安装

```powershell
python -m pip install -e ".[dev]"

# 需要从 Tushare/AkShare 更新数据时
python -m pip install -e ".[data,dev]"
```

凭据可由环境变量 `TUSHARE_TOKEN` 提供，也可放在配置指定的
`runtime/secrets/settings/tushare_token`。运行数据、密钥、临时结果与报告不进入 Git。

## 数据

```powershell
# 查看 canonical Parquet readiness、键和覆盖范围
python -m factor_lab.cli data status --deep --hash

# 首次采用现有 frozen store
python -m factor_lab.cli data build --full --apply-migration --hash

# 增量同步日行情、daily_basic、复权因子和官方日历
python -m factor_lab.cli data sync --from 2026-08-22 --to 2026-08-28 `
  --calendar-to 2026-09-30 --resume

# 全量原子更新停复牌历史
python -m factor_lab.cli data suspensions --from 2017-01-01 --to 2026-08-28 --no-resume

# 同步并应用 PIT 财务/历史名称行业数据
python -m factor_lab.cli data enrich --from 2017-01-01 --to 2026-08-28 --resume

# 新方向：同步不可变的卖方盈利预测报告日分区；完整回填前须先升级 report_rc 权限
python scripts/sync-analyst-reports.py `
  --start-date 2017-01-01 --end-date 2024-12-31
```

canonical 数据默认位于 `runtime/data/top500/`：

- `features.parquet`：信号日 PIT 特征与 lineage；
- `execution.parquet`：逐日 next-open 执行与账户核算输入；
- `membership.parquet`：月度 Top500 membership；
- `suspensions.parquet`：停复牌事件。

`report_rc` 原始分区独立放在
`runtime/data/raw/report_rc/report_date=YYYY-MM-DD/`，每个日期包含 `part-000.parquet` 和
逐页完整性 `manifest.json`；`pages/` 保存可在 resume 时独立重算的规范化 page。它尚未并入
正式 fixed-core score，FY0/FY1 如何汇成一维股票分数也尚未冻结。协议与 feature-only 可行性
证据见 [protocols/6.0-analyst-revisions.json](protocols/6.0-analyst-revisions.json) 和
[analyst-scout.json](protocols/evidence/6.0/analyst-scout.json)。

## 运行 6.1 分阶段证据

正式 runner 只接受 clean、已提交且通过共享完整性校验的 `main`。先提交实现与 immutable
preselection closure、推送并确认该提交自己的 GitHub CI 全绿；之后按阶段运行：

```powershell
# 在 clean implementation commit 上 create-only 生成 closure，单独提交并推送
python scripts/build-6.1-preselection-closure.py

# 只打开 train；仅 train 通过者会继续打开 validation，最终写固定 winner freeze
python scripts/run-wide-universe-evidence.py --mode selection

# 提交并推送 protocols/evidence/6.1/winner-freeze.json，CI 通过后；仅非 null winner 可运行
python scripts/run-wide-universe-evidence.py --mode audit `
  --freeze protocols/evidence/6.1/winner-freeze.json `
  --audit-end 2026-08-21

# audit evidence（或 null winner freeze）提交后生成固定 terminal result
python scripts/run-wide-universe-evidence.py --mode finalize
python -m factor_lab.cli strategy status --release 6.1
```

`selection`、`audit`、`finalize` 的三个终端 JSON 都是固定 tracked path 且 create-only；阶段之间
必须先提交、推送并等待 CI，不得在同一未提交工作树中连续打开下一层。扩大机会集收益运行额外
要求 [6.1 runtime capsule](protocols/6.1-runtime.json) 中的 Windows CPython、distribution 文件树、
Conda artifact 和 MKL 身份完全匹配。

## 复现 6.0 evidence

完整 exact 比较会运行 `exit15` 与 `exit25` 各十个 offset；输出 JSON 文件应放在仓库外：

```powershell
python scripts/run-low-churn-evidence.py `
  --output H:\Download\factor-lab-6.0-low-churn-reproduction\result.json
```

检查 tracked 实现/evidence（可选同时重算约 450 MiB canonical 数据哈希），或从历史状态确定性
重建最新一个 sleeve 的目标：

```powershell
python -m factor_lab.cli strategy status --verify-data
python -m factor_lab.cli strategy targets --signal-date latest
```

目标构造核心是无 I/O 的纯函数，可直接嵌入后续研究或执行层：

```python
from factor_lab.strategy import (
    LowChurnStrategyConfig,
    fixed_core_score,
    generate_sleeve_target_schedule,
    select_low_churn_targets,
)

config = LowChurnStrategyConfig(retention_exit_rank=25)
scores = fixed_core_score(signal_frame, config)
targets = select_low_churn_targets(ranked_tickers, previous_sleeve_targets, config)
schedule = generate_sleeve_target_schedule(signal_frame, official_calendar, config)
```

实现固定 ticker 升序 tie-break、rank25/rank26 边界、5.2 binary64 公式兼容，以及空信号日不
重排 calendar/sleeve 的合同。

## 历史诊断

下列入口只用于复现历史研究，不代表当前正式路线：

```powershell
python -m factor_lab.cli research run --suite walk-forward --full --resume
python -m factor_lab.cli research run --suite results-first --full --resume
python -m factor_lab.cli research run --suite recovery --full --resume
python -m factor_lab.cli research run --suite next --full --resume
python -m factor_lab.cli research run --suite legacy-regression --full --resume
python -m factor_lab.cli research status
python -m factor_lab.cli report --run latest
```

6.0 不提供 `prospective`、`adaptive-shadow` 或真实券商下单命令。

## 测试与发布

```powershell
$testRoot = "H:\Download\factor-lab-test-" + [guid]::NewGuid().ToString("N")
python -m pytest tests -q --basetemp $testRoot -p no:cacheprovider
python -m compileall -q src/factor_lab
```

大方向变化递增 major 并将 minor 归零；同方向迭代递增 minor。tag 使用 `major.minor`，Python
包使用 `major.minor.0`。发布只能通过：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

脚本要求 clean `main`、对应提交自己的双平台 GitHub CI 成功，并在推送 annotated tag 后核对
本地与 GitHub 的 tag object SHA 和 peeled commit；本地 tag 不算完成发布。

wheel 只封装 `factor_lab` Python 包；版本化协议、evidence runner、配置与 canonical 数据仍属于
Git checkout。安装 wheel 后应在 checkout 内运行 CLI，或显式传入
`--root <checkout>`；它不是携带研究数据的独立应用包。
