# Factor Lab 11.0

Factor Lab 11.0 把 10.0 的单周期季度 Borda 换成双周期确认混合：每个季度末同时计算 12-1 与 6-1
相对现金动量，75% 配给两者均为正的 long-momentum top-3 等权组合，25% 保留 10.0 Borda。它仍只用
信号时点已观察数据、下一官方开盘、整手、ADV20 容量、分红与全成本会计。

这次大方向切换来自两轮完全披露的 results-first 搜索，不是 OOS。第一轮 8 个集中度/现金门候选没有
通过全部门；第二轮固定比较 25%/50%/75% 双周期混合，75% 版本在 D1/D2/D3 和 full、8bp/16bp 下均
至少领先已发布 10.0 与 static 50bp，同时通过 fill、容量和会计门。10.1 prospective 尚无任何 decision
或 outcome，因此在首周期前被替代；11.0 仍是历史诊断，未来运行需后续 prospective 小版本。

正式 exact replay：

| 区间 | 11.0 base CAGR | 11.0 stress | 10.0 base | static base |
| --- | ---: | ---: | ---: | ---: |
| D1（2015–2019） | 8.532% | 8.327% | 7.365% | 6.909% |
| D2（2020–2022） | 6.857% | 6.630% | 5.547% | 2.334% |
| D3（2023–2026） | 21.768% | 21.581% | 19.028% | 13.644% |
| Full | 12.118% | 11.916% | 10.450% | 7.783% |

Full 最大回撤约 -24.82%、Sharpe 0.865、年化换手 2.34，fill 99.88%，capacity-limited 0.03%。
协议与正式证据见 [11.0 protocol](protocols/11.0-results-first-dual-confirm-blend.json) 和
[11.0 evidence](protocols/evidence/11.0/results-first-diagnostic.json)。这些数值全部来自已暴露历史。

底层 10.0 把 9.0 的低波动风险预算降为 comparator，主线改为严格因果的季度 12-1 双动量：
每个自然季度最后一个上交所交易日，只用该时点之前第 252 与第 21 个官方 session 的六只 ETF
总回报指数；五只风险资产分别减去现金代理同期 log return，只保留正值，并按相对动量从高到低
赋 Borda `n…1` 权重。下一官方交易日开盘执行，继续计入整手、ADV20 容量、8bp/16bp、分红与逐日会计。

这条路线是在完整查看 2015–2026 市场历史和三条一次性 prototype 后做出的 results-first 选择，
不是独立 OOS。正式 exact replay 的历史目标是：D1、D2、D3 在 base/stress 下都严格跑赢匹配现金和
static CAGR；Sharpe、回撤与换手完整披露，但按用户“先跑出收益”的优先级不作为本版否决门。

隔离原型中，季度 Borda 在 2015–2019、2020–2022、2023–2026 的 base CAGR 分别为 7.365%、
5.547%、19.028%，同期 static 为 6.909%、2.334%、13.644%；全段 CAGR 10.450%，static 为 7.783%，
16bp stress 为 10.276%。代价同样明确：全段最大回撤 -25.85%、Sharpe 0.737、年化换手约 2.02，
不能据此声称稳定盈利、alpha 或投资建议。

已发布的 6.3 corrective replay 证明数值修复有效，但也给出正式 null：扩大 ADV20 机会集的两个
challenger 在 train 都是 `0/10` offset 相对 control 为正，validation 与 audit 未打开，终态为
`selection_falsified_no_candidate`。因此 7.0 按 major 版本切换研究对象，不再为同一 fixed-core
信号调整 universe、selector 或 overlay。

7.0 固定 A 股、港股、美股、黄金、五年国债和场内现金代理六类 ETF，只注册一个候选：每月末以
63/126/252 个交易日相对现金代理的正总回报比例启用各资产的预定预算，未启用部分进入现金代理；
唯一 control 是同预算、无趋势过滤的静态组合。资产、窗口、风险预算、100 万元资本、10% ADV20
容量、8bp 单边全成本、双倍成本压力和全部阶段门在打开正式收益前一次冻结，不做参数网格。

已发布的 7.1 corrective replay 证明软件排序误报已经消除，同时给出正式 null：趋势过滤 train CAGR
约 5.11%，静态预算约 6.91%；趋势只改善约 1.65 个百分点回撤，却损失约 1.80 个百分点 CAGR，
换手约为静态的 5.6 倍。因此 8.0 升格原 `static_risk_budget` control，而不再调趋势窗口或门槛。

卖方盈利预期修正仍是潜在更正交的信息源，但本机没有带原始发布时间和修订版本的可信历史 archive，
既有 `report_rc.create_time` 又已出现多年后回填。7.0 不会用不可信 vintage 凑成绩；该路线只有在
外部 archive 真正交付后才可另立协议。

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

## 10.1 已归档运行层：季度 prospective paper cycle

协议见 [protocols/10.1-quarterly-prospective-cycle.json](protocols/10.1-quarterly-prospective-cycle.json)。
它在 0 decision/0 outcome 时被 11.0 替代，仅保留作发布历史和实现参考；不要再开启新的 10.1 周期。
若复核旧实现，必须 checkout 已发布的 annotated `10.1` tag。

每个 as-of source 使用两次独立完整 provider capture。两份 manifest、calendar 和六资产必须逐值一致，
而且历史前缀必须与 retained 9.0（以后与上一正式 as-of stage）完全相同，才会原子发布一份 source。
Source manifest 同时封存双抓原始 payload、10.1 tag object/commit、协议、上一 stage hash 和窗口内时间；
同一 stage 已存在时只深验 receipt 链与完整前缀并复用，不再次请求 provider。

```powershell
# 季度末 17:10 Asia/Shanghai 后：双抓并发布 source
python -m factor_lab.cli prospective capture --as-of YYYY-MM-DD

# 同日 17:10 至下一官方交易日 09:15：封存本季 targets 与 pending shares
python -m factor_lab.cli prospective signal `
  --source-root runtime/prospective/10.1/sources `
  --stage asof-YYYYMMDD --as-of YYYY-MM-DD

# 下一季度末 17:10 后：连续账户重放成交、NAV、分红和会计，先结束旧周期
python -m factor_lab.cli prospective outcome `
  --source-root runtime/prospective/10.1/sources `
  --stage asof-NEXTYYYYMMDD --signal-date YYYY-MM-DD --as-of NEXT-YYYY-MM-DD
```

每周期只保留 `cycle=YYYYQn/decision.json` 与 `outcome.json`；source 为
`sources/stage=asof-YYYYMMDD/`。首周期从 100 万现金开始，之后严格继承上一 outcome 的现金、持仓、
应收分红和 NAV，不能每季重新投入。缺失 exact next-open 会阻断 outcome；官方份额折算只能按精确
multiplier 调整执行 share，不能改封存权重、价格、ADV 或人民币名义金额。当前 completed prospective
outcome 仍为 0，不能据此声称稳定盈利。Receipt 没有外部 attestation，不能防止有意的本地全量伪造。

## 10.0 历史方向：季度 12-1 双动量 Borda

精确合同见
[protocols/10.0-results-first-quarterly-borda.json](protocols/10.0-results-first-quarterly-borda.json)。
正式 registry 只有 `quarterly_12_1_dual_momentum_rank_budget`；没有 top-k、波动率缩放、温度、
参数网格、第二模型或 runner-up。

对季度末信号 session `t`，风险资产 `i` 的分数为：

```text
m_i(t) = log(TRI_i[t-21] / TRI_i[t-252])
       - log(TRI_cash[t-21] / TRI_cash[t-252])
```

任一六资产在两个端点缺少当时已观测的 TRI 行，整期进入现金；非有限或非正 TRI 属于畸形源并直接拒绝。
否则只保留 `m_i>0` 的
风险资产，按分数降序、固定资产顺序破同值，给予 `n,n-1,…,1` 的 Borda 分并归一；现金承接 binary64
残差。全部输入不晚于信号收盘，成交只使用下一官方开盘。

| fully exposed 分段 | Candidate CAGR | Static CAGR | Candidate Sharpe | 最大回撤 | 年化换手 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2015–2019 | 7.365% | 6.909% | 0.531 | -25.85% | 2.326 |
| 2020–2022 | 5.547% | 2.334% | 0.438 | -16.88% | 1.907 |
| 2023–2026-08 | 19.028% | 13.644% | 1.301 | -17.35% | 1.857 |
| 全段 | 10.450% | 7.783% | 0.737 | -25.85% | 2.017 |

月度 top-2 原型虽然全段 CAGR 13.31%，但 D1 输 static、最大回撤 -29.25%、换手 5.83，且 fill/capacity
失败；因果在线三专家选择器全段 CAGR 5.76%，低于 static 7.78%。两者都被记录为 fully exposed
负面边界，不作为 fallback。

正式复现命令只有一个，默认 create-only 写 evidence；它必须在 core、protocol 与 runner 的 clean
implementation commit 之后运行：

```powershell
python scripts/run-10.0-results-first.py
```

`python -m factor_lab.cli strategy status` 默认核验 10.0；`--release 9.0` 保留已发布低波动路线的
tag 与六个 JSON 证据归档核验，但不深验 retained runtime。

## 9.0 已归档方向：因果月度波动平衡

预协议 scout 与正式合同分别见
[protocols/9.0-preprotocol-scout.json](protocols/9.0-preprotocol-scout.json) 和
[protocols/9.0-causal-volatility-balanced-budget.json](protocols/9.0-causal-volatility-balanced-budget.json)。
正式 registry 只有 `causal_monthly_volatility_balanced_budget`；每个 development/audit evaluation 都
固定生成 candidate/candidate_stress、static/static_stress、cash/cash_stress 六角色及 targets、orders、
daily NAV、holdings、trades 五类 artifact。压力角色必须逐字节复用相应 base targets。

Scout 中 D1 base CAGR 4.893%、Sharpe 1.398、最大回撤 -4.25%，现金超额 CAGR 1.801pp；D2 base
CAGR 3.258%、Sharpe 0.939、最大回撤 -4.19%，现金超额 1.259pp。两段的 base/stress 均通过已冻结
绝对门，并在 Sharpe、回撤和正年份比例上不差于同成本 static；D1 允许牺牲 static CAGR，因为协议
没有 candidate-minus-static CAGR 门，但仍严格要求跑赢可投资现金。这些数字只是选择后披露。

首次未打开的 2023–2026 audit 已按顺序完成。候选 base/stress CAGR 为 6.665% / 6.614%，Sharpe
为 2.834 / 2.816，最大回撤为 -1.38% / -1.36%，3/3 个完整年度为正；base/stress 均严格跑赢匹配现金，
六角色执行、容量和会计门全部通过。与此同时 static base/stress CAGR 为 13.644% / 13.591%，所以
9.0 的通过来自风险调整收益和回撤改善，而不是绝对收益更高。terminal result 是
`historical_adaptive_beta_diagnostic_passed_fresh_evidence_required`，仍禁止 alpha、盈利或投资建议声明。

正式顺序：

```powershell
python scripts/build-9.0-preselection-closure.py
# closure 单独提交、推送且精确提交 CI 全绿后；只读取 retained 8.1 validation source
python scripts/run-multi-asset-evidence.py --mode development
# winner-freeze 单独提交、推送且 CI 全绿后；仅 non-null 才能首次打开 2023+
python scripts/run-multi-asset-evidence.py --mode audit
# null freeze 或 audit 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode finalize
```

`python -m factor_lab.cli strategy status` 默认核验 9.0；`--release 8.1` 只核验 published tag 与
protocol/closure/reclassification/freeze/result，不依赖当前 9.0 runner 或 retained runtime。

## 8.1 归档结论：政策运行指标重分类

8.1 逐字节绑定已发布的 annotated `8.0` tag（tag object
`3fcbd73f7497b074e484ce7793e2d3603bf5a177`，peeled commit
`78aba86bf4e741699afca1acd1470493785fd952`）及其
`selection_inconclusive_execution_failure` 收据。Train 不创建新的 8.1 source、binding、evaluation
或 runtime；`train-reclassification.json` 的经济 role metrics 只能来自该发布收据，同时只读深验
收据绑定的 retained 8.0 train artifacts，以提取 missing-open、capacity、负现金与杠杆 validity。
它禁止重新查询、重建、重跑 train，也禁止从 artifacts 重算经济指标：

- 收益、Sharpe、回撤、完整正年份和现金超额仍分别使用 `primary/stress/cash/cash_stress` 的原值；
- 年化换手取 `primary, stress` 最大值，成交满足率取二者最小值，容量受限比例取二者最大值；
- NAV 会计误差仍取四角色最大值，现金角色的执行诊断不得删除或隐藏。

协议见
[protocols/8.1-policy-operational-metric-reclassification.json](protocols/8.1-policy-operational-metric-reclassification.json)。
正式顺序为：

```powershell
python scripts/build-8.1-preselection-closure.py
# closure 单独提交、推送且精确提交 CI 全绿后；核验收据与其绑定的 retained artifacts，不重跑 train
python scripts/run-multi-asset-evidence.py --mode reclassify
# reclassification 单独提交、推送且 CI 全绿后；仅 pass 才能首次打开 validation
python scripts/run-multi-asset-evidence.py --mode validation
# 仅 non-null freeze 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode audit
# null freeze 或 audit 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode finalize
```

正式 reclassification 按上述聚合域通过，但它只是 post-hoc train 重分类。随后首次打开的 2020–2022
validation 正式失败：主策略 CAGR 2.3342%、相对现金 CAGR 超额 0.3356pp、Sharpe 0.2802、最大回撤
-15.9635%，三个完整年份只有 1 年为正。Base Sharpe 与 base/stress 正年份比例未过门；其余收益、
压力、回撤、换手、fill、容量和会计检查通过。Null freeze payload 为
`d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76`，terminal result payload 为
`d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4`，状态
`selection_falsified_no_candidate`。Audit 从未打开；8.1 不会调权、降门或重试，下一研究方向必须升
major 版本。

当前 `main` 使用 `python -m factor_lab.cli strategy status --release 8.1` 核验 published
reclassification/freeze/result 与 audit 缺失；`--release 8.0` 核验更早的不可变失败档案。即使历史
validation/audit 通过，含义也
只限这六只固定 ETF 的公开历史战略 beta 诊断，不能推出 alpha、未来稳定盈利或投资建议；仍需至少
252 个新交易日和 12 次新月度执行。

## 8.0 归档结论：固定战略资本预算

唯一政策 `static_risk_budget` 每月末固定目标为：A 股 30%、港股 10%、美股 10%、黄金 20%、
五年国债 30%、`511880.SH` 0%。整手、容量或开盘跳空留下的金额保留为账户现金，不主动改变预算。
`cash_only_511880` 用相同月末/下一开盘、整手、成本、分红和会计合同维持 100% 现金 ETF 目标，
只作可投资政策门槛。

Train（2015–2019）static 结果已经可见，只能 exact replay；2020–2022 validation 是 8.0 对这六只
ETF 的正式链中首个未打开阶段，只有 train 通过预注册统一绝对门才创建。Validation 通过并提交
non-null freeze 后，才允许打开 2023–2026 audit。三个阶段使用同一门：CAGR 与现金超额 CAGR 为正、Sharpe ≥ 0.30、最大回撤
不低于 -25%、完整正年份比例 ≥ 50%；16bp 压力下 Sharpe ≥ 0.25 且仍为正现金超额；年化换手 ≤ 1、
成交满足率 ≥ 99%、容量受限请求 ≤ 1%、会计误差 ≤ `1e-8` 元。

协议见 [protocols/8.0-static-capital-budget.json](protocols/8.0-static-capital-budget.json)。下列是 8.0
当时的正式顺序，只能在 checkout `8.0` tag 的归档 worktree 中重现；当前 `main` runner 已迁移到
9.0：

```powershell
python scripts/build-8.0-preselection-closure.py
# closure 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode calibration
# train-admission 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode validation
# 仅 non-null policy freeze 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode audit
# null freeze 或 audit 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode finalize
```

8.0 的 calibration 已执行且不可重跑。主策略 train CAGR 6.9088%、Sharpe 0.7264、最大回撤
-17.0246%，16bp 压力 CAGR 6.8580%，相对现金 CAGR 超额 3.8175pp；核心经济门均通过。冻结 gate
唯一失败是把 cash comparator 的 89.6578% fill 与 primary/stress 的 99.5425%/99.5149% 一起取最小值。
现金低值来自年末分红已计入应收款但未成为可用现金时，100% 目标的整手再投资被延迟到次月；不是
容量或缺失开盘价。随后 admission 写入前的 GitHub 远端复核遇到 `Empty reply`，因此
[execution-failure.json](protocols/evidence/8.0/execution-failure.json) 把 8.0 归档为
`selection_inconclusive_execution_failure`。Validation/audit 从未打开。

同 release 不会重跑。后继 8.1 只把 policy admission 的 turnover/fill/capacity 聚合域限定为
primary+stress；现金比较器收益、完整执行诊断、四角色会计有效性，以及全部资产、权重、成本、日期
和经济阈值保持不变。这个修正是在看过 8.0 train failure 后提出，明确标为 post-hoc
reclassification，不能伪装成独立 train。

即使 validation/audit 全部通过，也只表示这六只固定 ETF 的公开历史战略 beta 诊断通过；不等于
alpha、稳定未来盈利或投资建议，仍需要 closure 之后至少 252 个新交易日且至少 12 次月度执行的
前瞻证据。

## 7.1 已归档路线：7.0 固定多资产路线的纠正重放

固定资产与资本预算如下；`511880.SH` 只接收未启用预算和整手/容量残余：

| 资产 | 角色 | 固定预算 |
| --- | --- | ---: |
| `510300.SH` | A 股大盘 | 30% |
| `159920.SZ` | 港股 | 10% |
| `513100.SH` | 美股 | 10% |
| `518880.SH` | 黄金 | 20% |
| `511010.SH` | 五年国债 | 30% |
| `511880.SH` | 场内现金代理 | residual |

代表选择由 [protocols/7.0-asset-selection.json](protocols/7.0-asset-selection.json) 固定：完整枚举
2015-02-27 时仍在交易的 L/D/I 场内基金（包括后来退市者），每类要求至少 252 行 cutoff 前日线和
可复算总回报，再按 cutoff ADV20、代码升序选择。它不使用 2026 存续状态或任何候选收益。

信号只比较每只风险 ETF 与现金代理在 63、126、252 个已完成交易日上的总回报。三个窗口中严格
跑赢现金的比例乘以固定预算，得到下一次目标；没有足够历史时风险预算为零。总回报由 raw close
与当日已生效现金分红逐日重建，`fund_adj` 只作公司行动诊断；`513100.SH` 在 2022-01-13 的
官方 1:5 份额拆分按上交所公告精确调整持仓与隔夜订单，其他无法解释的动作继续 fail closed。
`pre_close` 的非经济参考价 reset 只能在冻结日期、调整因子不变且幅度小于 2% 时留痕通过。

阶段边界为：2015-03-02 至 2019-12-31 train，2020-01-02 至 2022-12-30 validation，
2023-01-03 至 2026-08-28 historical audit。Train 不过不得创建或读取 validation stage；
train 与 validation 都通过才冻结唯一 winner；audit 失败不得换模型。完整冻结合同见
[protocols/7.0-multi-asset.json](protocols/7.0-multi-asset.json)。

代码审查曾在 Git closure 生成前意外打开真实 train；候选 CAGR 约 5.11%、Sharpe 0.724、最大回撤
-15.38%，但相对静态 control 少约 1.80 个百分点 CAGR、Sharpe 低 0.0025，冻结的两项相对门失败。
Validation/audit 均未打开，完整披露见
[preclosure-train.json](protocols/evidence/7.0/preclosure-train.json)。因此下面的 selection 只能重放并
封存同一 null，不能再被描述为独立预注册检验，也不得修改 -1.5pp 门槛来“救活”候选。

正式 7.0 selection 已在关闭根和提交自身的四平台 CI 全绿后执行。Train source、binding、15 个
evaluation Parquet、metrics 与 gate 均成功落盘，完整结果哈希与披露一致，gate 仍为 `false`；但
完整性 verifier 在规范化前逐行比较 targets，把协议资产顺序与模拟器的 `signal_date, code` 排序
误判为值差异，因此在 winner freeze 前终止。独立 exact replay 证明三种角色按唯一键排序后 targets
及全部 15 个 artifact 逐值相同。这是软件执行失败，不是可发布的正式 null：validation、winner、
audit 和 terminal result 均不存在，7.0 的策略结论不成立。Tracked、自哈希的失败收据见
[execution-failure.json](protocols/evidence/7.0/execution-failure.json)；发布 tag 与 7.1 closure 将再逐字节
绑定它。修复只能进入保持全部研究语义
不变的新 7.1 runtime 与 closure，不能覆盖或续跑现有 7.0 stage。

7.1 已以自哈希 corrective amendment 冻结唯一修复：targets 完整性比较前，双方都按唯一键
`signal_date, code` 使用稳定 `mergesort` 排序；dtype、列和值继续 exact，simulation 仍由 causal
builder targets 驱动。7.1 使用全新的 `runtime/data/multi-asset-7.1`、关闭根和 evidence 路径，
逐字节绑定已发布的 annotated 7.0 tag 与 execution-failure receipt。Runner 的阶段注册表只含 train，
命令面只含 selection/finalize；任何意外 train pass、validation/audit 路径或 7.0 hardlink 复用均
fail closed。Selection 是一次性的：运行前整个 7.1 runtime 必须不存在；一旦执行中留下 runtime
却未生成 freeze，就必须归档为 execution failure，不能删除后在同一 7.1 重试。完整白名单见
[protocols/7.1-corrective-amendment-1.json](protocols/7.1-corrective-amendment-1.json)。

7.1 正式流程如下；这是归档重现说明，只能在 checkout `7.1` tag 的独立 worktree 中运行，
当前 `main` 的 runner 已迁移到 9.0。各步仍须逐步提交、推送并等待精确提交 CI 通过：

```powershell
# 仅限已 checkout 7.1 tag 的独立归档 worktree
# 实现提交推送且 CI 全绿后，冻结 corrective implementation/runtime 根
python scripts/build-7.1-preselection-closure.py

# closure 单独提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode selection

# selection 只能生成 train-failed null freeze；提交、推送且 CI 全绿后
python scripts/run-multi-asset-evidence.py --mode finalize
```

## 6.3 已归档路线：6.2 机会集实验的 corrective replay

6.3 原样执行逐字节不变的 6.2 基础协议和前置 amendment，继续比较三个逐日重建的 universe：因果
ADV20 Top500 control、ADV20 至少人民币 1 亿元、ADV20 Top1500。证券必须是沪深人民币普通股、
已上市至少 120 个官方交易日、当日非 ST；缺 bar 只有同日或尚未清除的显式整日停牌证据才可解释。
Train、validation、audit 的停复牌与 `stock_st` 分区拥有互不复用的物理私有根，不能靠同一文件
换路径伪装成隔离。

流程只有一条前进路径：截至 2022 的 train 门不过就不打开 validation；截至 2024 的 validation
只运行 train 通过者；两段都过才按预注册 q20、中位主动 CAGR、容量和换手率排序冻结至多一个
winner；2025-01-01 至 2026-08-21 的历史 audit 只运行该 winner，失败后不换 runner-up。即使
audit 通过，也只记为“需要新鲜未来样本”，不允许盈利承诺。6.3 的冻结根由以下文件组成：

- [protocols/6.2-wide-universe.json](protocols/6.2-wide-universe.json)
- [protocols/6.2-wide-universe-amendment-1.json](protocols/6.2-wide-universe-amendment-1.json)
- [protocols/6.3-corrective-amendment-1.json](protocols/6.3-corrective-amendment-1.json)
- [protocols/6.3-runtime.json](protocols/6.3-runtime.json)
- `protocols/6.3-release.json`：必须在任何 6.3 return replay 前 create-only 生成并单独提交；
- `protocols/evidence/6.3/`：只接收新的 winner freeze、historical audit 和 terminal result。

前两份 6.2 文件继续是本次 replay 的完整研究合同，不会复制成可漂移的 6.3 版本；已发布的 6.2
annotated tag、preselection closure 和
[execution-failure.json](protocols/evidence/6.2/execution-failure.json) 由 6.3 closure 逐字节绑定，
原文件不得覆盖、删除或重新解释。

6.1 实际运行完成 1,579 个 train 交易日、1,459 个信号日的因果排名后，在持久化 rankings、构造
targets、读取 next-open/复权因子或调用组合评估器之前 fail closed。Top500、Top1500、ADV≥1亿元
的有限分数覆盖中位数分别为 91.60%、88.60%、88.67%，但每天最低仍有 406、1,206、530 只
可评分股票，远超 Top25。逐日重构表明控制组 63,010 个缺分 member-day 中 59,623 个是已有
`daily_basic` 行但 `pe_ttm` 为空；供应商文档对亏损公司空 PE 的说明解释了这一主导模式，但不
足以证明每个单日 null 的经济原因。完整绑定证据见
[protocols/evidence/6.1/admission-failure.json](protocols/evidence/6.1/admission-failure.json)。6.2
逐日分别统计有行情但缺 `daily_basic`、有停牌证明且无 snapshot、空 PE、空 PB、非法非空基本面值、
有限输入的非有限算术、未分类不可评分项和预期/实际评分不一致；全部成员必须被有限可评分或已命名
不可评分并集穷尽。95%/90% 只输出诊断，任何异常硬门或 Top25 完整性门不通过都不会读取 next-open
定价或产生收益。ADV20 单位换算/聚合非有限会终止构建，非正 ADV20 在三个 arm 的共同 base 前剔除。

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

## 复现已归档的 6.3 corrective evidence

正式 runner 只接受 clean、已提交且通过共享完整性校验的 `main`。先提交实现与 immutable
preselection closure、推送并确认该提交自己的 GitHub CI 全绿；之后按阶段运行：

```powershell
# 在 clean implementation commit 上 create-only 生成 closure，单独提交并推送
python scripts/build-6.3-preselection-closure.py

# 只打开 train；仅 train 通过者会继续打开 validation，最终写固定 winner freeze
python scripts/run-wide-universe-evidence.py --mode selection

# 提交并推送 protocols/evidence/6.3/winner-freeze.json，CI 通过后；仅非 null winner 可运行
python scripts/run-wide-universe-evidence.py --mode audit `
  --freeze protocols/evidence/6.3/winner-freeze.json `
  --audit-end 2026-08-21

# audit evidence（或 null winner freeze）提交后生成固定 terminal result
python scripts/run-wide-universe-evidence.py --mode finalize
python -m factor_lab.cli strategy status --release 6.3
```

6.3 的 `selection`、`audit`、`finalize` 终端 JSON 都位于固定 tracked path 且 create-only；阶段之间
必须先提交、推送并等待 CI，不得在同一未提交工作树中连续打开下一层。正式 replay 使用全新的
`runtime/data/wide-universe-6.3/` 工作根，拒绝 6.2 的 manifest、exact run、return/trade/NAV trace、
gate、winner freeze、audit/result 或 CLI status view。canonical raw 文件可以逐字节相同，但必须在
6.3 stage 中重新枚举、运行前后重哈希并绑定到新的 manifest。

扩大机会集收益运行额外要求 [6.3 runtime capsule](protocols/6.3-runtime.json) 中的 Windows CPython、
distribution 文件树、Conda artifact 和 MKL 身份完全匹配。6.3 closure 会同时绑定已发布的 6.2
annotated tag、逐字节不变的 6.2 protocol/amendment、6.2 closure 与 execution-failure，以及 6.3
corrective amendment、runtime 和实现；6.2 的历史文件与 Git 对象原样保留。

6.2 的实际正式 selection 在 train 准入成功后，因十几亿元成交金额使用普通 binary64 不同次序
求和、再以固定 `1e-6` 元核对而产生软件假失败；没有生成 exact result、gate 或 winner freeze。
该边界已封存在 `protocols/evidence/6.2/execution-failure.json`，不能解释为策略失败。6.3 只把相关
累加改为 `math.fsum`，固定 `1e-6` 元容差和全部研究合同不变；它必须 fresh replay，不能沿用 6.2
的任何 derived stage、status、winner 或 gate。

## 复现 6.0 evidence

完整 exact 比较会运行 `exit15` 与 `exit25` 各十个 offset；输出 JSON 文件应放在仓库外：

```powershell
python scripts/run-low-churn-evidence.py `
  --output H:\Download\factor-lab-6.0-low-churn-reproduction\result.json
```

检查 tracked 实现/evidence（可选同时重算约 450 MiB canonical 数据哈希），或从历史状态确定性
重建最新一个 sleeve 的目标：

```powershell
python -m factor_lab.cli strategy status --release 6.3 --verify-data
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
