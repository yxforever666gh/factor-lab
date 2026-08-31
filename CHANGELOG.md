# Changelog

本文件记录 Factor Lab 的软件、数据和研究协议变化。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，但历史条目依据现有 Git
提交与 tag 回补，不把当时的发布说明原样视为当前事实。

> 研究证据说明：历史回测、`pass_candidate`、`ready_for_portfolio_mvp`、工程测试通过或
> 运行系统可用，都不等于投资策略已验证。除非明确说明，市场结果均属于历史诊断或
> legacy evidence，不保证未来收益，也不代表生产或实盘就绪。

## [Unreleased]

## [11.0] - 2026-09-01

### Added

- 新增 11.0 results-first 路线 `quarterly_dual_confirm_top3_borda_blend_75_25`：季度末同时计算
  12-1 与 6-1 相对现金动量；75% 分配给双周期都为正的 long-momentum top-3 等权组合，25% 保留
  10.0 全 positive-set Borda，下一官方开盘继续使用 100 万元、整手、10% ADV20、分红和 8bp/16bp
  成本会计。
- 新增 11.0 compact protocol、exact results-first runner、物理 prefix replay 与 10.0/static/cash 同口径
  对照；正式 evidence 只接受三个分段和 full 在 base/stress 下全部通过固定收益及执行门。

### Changed

- Python 包版本更新为 `11.0.0`。这是策略公式的大方向切换，不是 10.1 运行层小修；10.1 在 0 个
  prospective decision、0 个 outcome 时被历史收益更强的候选替代，不会再开启首周期。
- 11.0 选择门要求 candidate 在 D1/D2/D3/full 的 8bp 与 16bp CAGR 均至少高于 matching cash、static
  和已发布 10.0 `50bp`，且 stress CAGR 严格高于 matching 10.0 base；Sharpe 和回撤仍只披露。
- 正式 evidence payload/file SHA-256 为
  `8ceffbf9aaff605c03d7ca87c56244e47722481acaf1042cb90f4ec70b6eda4d` /
  `6c74d76285c7003ffd509c0866fc6d0b084be6770aadf32d2463293d38d83946`，绑定 implementation commit
  `adb4f5b775a391f8ad3ac154dcf93633ad5962c5`；对应四平台 CI run `33438885289` 全部通过。
- 从该 implementation commit 的 clean `git archive` 构建
  `factor_research_mvp-11.0.0-py3-none-any.whl`，SHA-256 为
  `2f2dbf623f6d7a3d8ed35f827a6f2c6c171f3c774d04ca8ee5a6697a8fef04f1`；fresh venv、`pip check`、
  detached-checkout runner import 及 wheel/Git 30 个 Python 文件逐字节核对通过。

### Research status

- 第一轮 fully exposed scratch 冻结 8 个季度集中度/现金门/持有缓冲候选，无一通过；其中
  `dual_confirm_top3_equal` 虽在每个分段提高 CAGR，但 D1 fill 与正收益年份稳定性失败。第二轮完整披露
  25%/50%/75% 三个与 10.0 Borda 的固定混合，`dual75_borda25` 按预定最差分段 edge 规则胜出。
- Scratch exact 结果中，赢家 D1/D2/D3 base CAGR 为 8.532% / 6.857% / 21.768%，10.0 为
  7.365% / 5.547% / 19.028%；full base/stress 为 12.118% / 11.916%，10.0 为
  10.450% / 10.276%。Full fill 99.88%、capacity-limited 0.03%，target prefix mismatch 为 0。

### Known limitations

- 两轮候选和三个历史分段均在选择前完全暴露，11.0 只能称
  `fully_exposed_results_first_causal_historical_diagnostic`，不是独立 OOS、alpha 或未来稳定盈利证据。
  新公式仍需另一个 prospective 小版本及 fresh future outcome 验证。

## [10.1] - 2026-09-01

### Added

- 新增 10.1 `quarterly prospective paper cycle`：稳定 source、decision、outcome 三类 create-only 状态，
  复用 10.0 的季度 Borda core 与 next-open/full-cost simulator，不引入 ledger、watchdog、数据库、
  attestation 或后台调度。
- 新增 ETF stable capture：同一 as-of 两次独立全量抓取必须 manifest/calendar/六资产逐值一致，并与
  retained 9.0 或上一正式 stage 的完整历史前缀逐值一致，才在同盘原子发布一份 as-of source。
- 新增 `prospective capture/signal/outcome` CLI。Signal 只在季度末 17:10 至下一官方交易日 09:15
  之间封存 targets、连续账户 NAV/持仓和 pending shares；outcome 在下一季度末 17:10 后按同一封单
  连续重放成交、日 NAV、分红和会计。
- Source manifest 内嵌稳定双抓 receipt，绑定 10.1 annotated tag object/peeled commit、协议、正式路径、
  两次一致抓取的原始 payload、上一 stage 和验证时间；decision/outcome 采用同目录 fsync 后 atomic
  create-only hardlink，既存文件或并发写入不能覆盖。

### Changed

- Python 包版本更新为 `10.1.0`；10.0 公式、资产、252/21 session、Borda 权重、8bp 成本、100 万元、
  整手和 ADV 容量不变。正式 prospective cycle 必须 checkout 精确的 published annotated `10.1` tag。
- 首周期从 100 万现金开始，后续周期必须继承上一 outcome 的现金、持仓、应收分红和 NAV；禁止
  fresh-cash reset、错过决策窗口后的回填、用更晚 open 替代 frozen next open，或任选有利 outcome 日。
- Exact next-open 缺失会阻断 outcome。若 next-open 发生官方份额折算，只允许按官方 multiplier 确定性
  缩放执行视图中的 share 字段；decision 内的权重、信号价、ADV、冻结人民币名义金额和原封单不变。
- Historical as-of evidence payload/file SHA-256 为
  `0d2103896410f8800cf9351cb8fb31b807df7ff06c79413b0c2ed45fbc3fed47` /
  `888313dd86c9c15bf6e915d087a784c1a8e4d48e85f3832b4e0945e77e3e27c3`，绑定 implementation commit
  `699ee3f7687d25364438faca4b0a5bbf9b69a76a`；对应四平台 CI run `33425333175` 全部通过。
- 从该 implementation commit 的 clean `git archive` 构建
  `factor_research_mvp-10.1.0-py3-none-any.whl`，SHA-256 为
  `75756f90ea930e3f4c99f6208c93e28c717d24aa3b4057967cc2d27c005ba66e`；fresh venv、`pip check`、
  detached-checkout CLI/runner import 及 wheel/Git 30 个 Python 文件逐字节核对通过。

### Research status

- Retained 全历史物理前缀 dry-run 覆盖 46 个季度 signal 和 45 个完整 outcome；target、sealed plan、
  signal-close account state 及 daily NAV/holdings/trades outcome prefix mismatch 均为 0，正式路径写入为 0。
  Synthetic 回归另覆盖 missing-open 拒绝、空封单、官方份额折算和跨季应收分红连续性。
- 发布时 prospective decision/outcome 数仍为 0；10.1 只让路线具备未来运行能力，不增加任何新盈利证据。

### Known limitations

- 每个新 source 仍需约两次完整历史 provider capture；没有增量 append、自动重试或调度器。若供应商
  历史发生任何修订，前缀 exact 门会阻断本周期，而不是接受修订后继续运行。
- Receipt 是本地 self-hash 证据，不使用 attestation 或外部透明日志；它能发现意外损坏和市场 payload
  重写，但不能用密码学阻止拥有本地写权限的人同时伪造时间和重算全部 hash。

## [10.0] - 2026-08-31

### Added

- 新增 10.0 results-first 路线 `quarterly_12_1_dual_momentum_rank_budget`：每个自然季度末只读
  `t-252` 与 `t-21` 官方 session 的六只 ETF TRI，风险资产减去现金同期 log return，所有正分资产
  按固定顺序破同值并赋 Borda `n…1` 权重；任一必要端点缺失/未观测时整组进入现金，畸形 TRI 源直接拒绝。
  下一官方开盘、100 万元、
  100 份整手、10% ADV20、分红、逐日会计及 8bp/16bp 成本保持不变。
- 新增 compact 10.0 protocol、纯因果 core、create-only results-first runner 与 CLI 默认状态；正式 runner
  直接复用 retained 9.0 audit source，不复制数据，也不引入 closure/attestation/调度平台。

### Changed

- Python 包版本更新为 `10.0.0`。9.0 的 inverse-vol 路线降为 comparator；10.0 不再允许仅凭 Sharpe
  或回撤改善掩盖收益不足，D1/D2/D3 的 base/stress CAGR 必须分别严格高于匹配现金与 static。
- 按用户 results-first 优先级，Sharpe、最大回撤与年化换手在 10.0 完整披露但不作否决门；fill 至少
  98%、capacity-limited 至多 2%、零容量违约/负现金/杠杆及会计误差不超过 `1e-8` 仍为硬执行门。
- 正式 evidence payload/file SHA-256 为
  `18c9fb75f79cf71572f65a3eade0d2af8a018e7b8aef066fa8a30dce1f721253` /
  `954be9b434d3d5c7c06ddac1f276ac032248b420956185cc372ae352685b4e89`，绑定 implementation commit
  `0462eed7eb08b110d6356d43b6c7e13d3e0fc522`；CLI retained-data 深重放通过。
- 从 clean `git archive` 构建的 `factor_research_mvp-10.0.0-py3-none-any.whl` SHA-256 为
  `bfa98db8580dd700ff151465f7762c6e8e36fa9e6eca43f6666e904fe2708a57`；全新隔离 venv 安装、
  `pip check`、版本/CLI import 与 installed-wheel + detached Git evidence status 均通过。

### Research status

- 三条一次性 prototype 均在完整查看 2015–2026 历史后选择，全部是 fully exposed causal diagnostic，
  不是独立 OOS。月度 top-2 虽有全段 CAGR 13.31%，但 D1 输 static、最大回撤 -29.25%、换手 5.83，
  且 fill/capacity 失败；在线三专家全段 CAGR 5.76%，低于 static 7.78%，二者均拒绝 runner-up。
- 入选季度 Borda 的 D1/D2/D3 base CAGR 为 7.365% / 5.547% / 19.028%，同期 static 为
  6.909% / 2.334% / 13.644%；对应 stress CAGR 7.169% / 5.385% / 18.856%。全段 CAGR
  10.450%，static 7.783%，16bp stress 10.276%；46 个信号 prefix replay 为零差异。

### Known limitations

- 季度 Borda 全段最大回撤为 -25.85%、Sharpe 0.737、年化换手约 2.02；三个历史分段都已暴露，
  且历史中现金退避从未触发。10.0 只能证明当前固定历史上的因果收益诊断，不能证明 alpha、稳定盈利
  或未来适应性；必须由发布后的新数据继续检验。

## [9.0] - 2026-08-31

### Added

- 冻结 9.0 大方向 `causal_monthly_volatility_balanced_budget`：每月末用截至信号收盘的 127 个已观测
  total-return-index 水平形成 126 个简单收益、`ddof=1` 波动率，再令五只风险 ETF 的 raw weight 等于
  8.0 固定预算除以波动率并归一；现金目标为零。任一资产样本不足或波动无效时整组回退静态预算，
  不增加 cap、目标波动、杠杆、band、参数网格、第二模型或 runner-up。
- 新增 9.0 preprotocol scout、protocol、preselection closure、retained-8.1 development exact replay、
  create-only winner freeze、首次在已提交 non-null freeze 后打开的 audit 与 terminal result 链。每阶段固定六角色、每角色五类
  artifact；stress 必须复用 base targets，target prefix、执行、会计与全部 Parquet 可 exact replay。
- CLI 默认 `strategy status` 迁移到 9.0，并映射 winner freeze、audit 与 terminal result。显式
  `--release 8.1` 改为只核验已发布 annotated tag 及 protocol/closure/reclassification/freeze/result
  精确字节，并要求 audit 缺失；它不加载当前 9.0 runner，也不依赖 retained runtime。

### Changed

- Python 包版本更新为 `9.0.0`。资产、总回报、月末/下一开盘、100 万元、整手、ADV20 容量、
  8bp/16bp 成本、分红应收与逐日会计保持不变；改变的是风险资产资本预算随因果历史波动连续调整。
- 9.0 development 直接读取并深验 published 8.1 validation source，不重新查询 provider，也不在 9.0
  runtime 复制 development source；runtime 只保存 development binding/evaluation。只有提交并推送的
  non-null freeze 与精确 CI 成功后，audit 才能首次 capture 2023+。
- 从 clean `git archive` 构建的 `factor_research_mvp-9.0.0-py3-none-any.whl` SHA-256 为
  `a20a7b11689dbc0853b317194baeef7f586f6d271514e9ac3a5e5b63c55ea350`；全新隔离 venv 的精确依赖
  安装、`pip check`、版本/CLI import 与 installed-wheel + detached Git evidence status 均通过。

### Research status

- Scout 穷尽两个一次性 prototype，且选择发生在完整查看 2015–2022 结果之后。入选波动平衡公式的
  D1 base CAGR/Sharpe/最大回撤为 4.893% / 1.398 / -4.25%，现金超额 1.801pp；D2 为
  3.258% / 0.939 / -4.19%，现金超额 1.259pp。D1/D2 的 base/stress 均通过绝对与相对稳定门；
  这些结果全部是 fully exposed development，不能称为独立 OOS。
- 入选 prototype 的平均目标约 74.42% 在 `511010.SH`，实质高度债券化。D1 相对 static 牺牲约
  2.016pp CAGR，但 Sharpe 提高约 0.672、回撤改善约 12.773pp；协议没有 static-relative CAGR 门，
  但仍要求每段严格跑赢现金。该集中度原样披露，不能在看过结果后增加 cap。
- 三专家 exponentiated-gradient prototype 平均约 82.22% 回到 strategic beta，base/stress Sharpe
  分别比 static 低约 0.0012/0.0031，三个两年 fold 仅一个现金超额为正，因此拒绝 formalization；
  不允许把它作为 runner-up。
- 2023-01-03 至 2026-08-28 的首次未打开公共历史 audit 通过：candidate base CAGR 6.665%、
  Sharpe 2.834、最大回撤 -1.38%，现金超额 5.248pp，3/3 个完整年度为正；16bp stress CAGR
  6.614%、Sharpe 2.816、最大回撤 -1.36%。但同段 static base CAGR 为 13.644%，高出候选约
  6.979pp；候选胜在 Sharpe 与回撤，不是更高收益。终态为
  `historical_adaptive_beta_diagnostic_passed_fresh_evidence_required`，不能称为 alpha 或稳定盈利。
- 9.0 closure / winner freeze / audit / result payload 分别为
  `722d93904d3bc67792f32fb7a39ab8461336fa1956513c9ea2586d9ce31e68b3`、
  `430b45eec730084a3d82e7d392bf609e533d5c7a98b5623f9d13a471171495a7`、
  `7a034510cc38aaca5ea2b2113265c2ff2b984c302f366cd68f34f8c73af98681` 和
  `3b6fbcab3dafb1086be3062109d02c1c05f408d30913dc15146ed2b7eb3aa7b2`。

### Known limitations

- 9.0 没有新信息源；它是同一固定 ETF 历史上的风险预算变换。两个 prototype、D1/D2 和选择决策均
  已暴露，任何 development 通过都不能证明 alpha、盈利或稳定未来收益。即使公共历史 audit 通过，
  仍需至少 252 个新交易日和 12 次新月度执行，且不构成投资建议。

## [8.1] - 2026-08-31

### Added

- 冻结 8.1 corrective protocol `policy_operational_metric_reclassification`，逐字节绑定已发布的 annotated
  `8.0` tag（object `3fcbd73f7497b074e484ce7793e2d3603bf5a177`，peeled commit
  `78aba86bf4e741699afca1acd1470493785fd952`）、8.0 protocol/closure 和
  `selection_inconclusive_execution_failure` 收据。协议 payload 为
  `2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5`。
- 新增 receipt-bound `reclassify` 阶段和独立的 8.1 closure、evidence、runtime namespace。经济
  role metrics 只取已发布收据；为证明四角色 missing-open、capacity、负现金和杠杆有效性，阶段会
  只读深验收据绑定的 retained 8.0 train artifacts，但禁止重新查询、重建、重跑 train 或从 artifacts
  重算经济指标，也不会创建 8.1 train runtime。只有重分类证据单独提交、推送且精确提交 CI 成功后，
  才允许首次打开 2020–2022 validation。
- CLI 默认 `strategy status` 迁移到 8.1，分别报告 train reclassification、winner freeze、historical
  audit 与 terminal result；显式 `--release 8.0` 固定核验已经发布的失败档案及其精确 tag 身份。

### Changed

- Python 包版本更新为 `8.1.0`。策略、六只 ETF、固定权重、现金比较器、月末/下一开盘执行、8bp/16bp
  成本、阶段日期、收益/风险定义与全部经济阈值均保持 8.0 不变。
- 政策运行门的年化换手、成交满足率和容量受限比例只在 `primary + stress` 上聚合；
  `cash + cash_stress` 仍完整披露并参与现金超额计算，NAV 会计误差仍要求四角色共同有效。这是看到 8.0
  train failure 后作出的 post-hoc gate-scope reclassification，不是独立 train 或新收益发现。
- `blocked_missing_open` 与 `blocked_capacity` 只作严格整数诊断，继续由原 fill/capacity ratio 门反映，
  不新增经济阈值；但 `planned_signal_notional > 10% signal-date ADV20` 的 capacity violation 仍是继承
  执行合同违约，和负现金、杠杆、会计不一致一样 fail closed。

### Research results

- Post-hoc train reclassification 按冻结 role scope 通过，payload
  `4f498ffc12deac61144c77c56ba89cb9abccc034d2d73df4f1df8a6c50184c79`；policy fill 99.5149%、
  年化换手 0.5400、容量受限比例 0，cash fill 89.6578% 继续完整披露但不进入 policy fill。该通过
  不是独立 train，只允许首次打开 validation。
- 2020–2022 validation 的 source/binding/evaluation payload 分别为
  `f5903d2b24b47662a9ba4ea3d2d127c9b5dee385d5b927140b25eda68b3ff060`、
  `7479aa06071d34544b6ce880d6a2986a09988e3905853dbab7127eaeb0e13d5b`、
  `7794ee8c81cc784d262a464c55d37f3017b1e75cbc4bb421b5e4b8eb85685981`。主策略 CAGR 2.3342%、
  cash excess CAGR 0.3356pp、Sharpe 0.2802、最大回撤 -15.9635%，完整正年份比例仅 1/3；失败项为
  base Sharpe ≥0.30、base/stress 完整正年份比例 ≥50%。其余 nominal/stress CAGR、现金超额、压力
  Sharpe、回撤、换手、fill、容量和会计门均通过。
- Create-only null freeze payload 为
  `d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76`，terminal result payload 为
  `d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4`，状态
  `selection_falsified_no_candidate`。2023–2026 audit 保持物理未创建；禁止降门、改权或 runner-up。

### Known limitations

- 8.1 没有新增信息源，也不能修复固定 ETF 的幸存/研究者选择偏差。即使后续公开历史 validation 和
  audit 均通过，结论仍只属于 fixed-instrument strategic beta diagnostic；至少 252 个新交易日与
  12 次新月度执行之前，禁止盈利、稳定未来收益或投资建议声明。
- 收据绑定的 8.0 train runtime 必须保留到 8.1 validation、audit 与 finalize 全部完成，以便每个正式
  阶段递归深验 validity；只有终态与远端 tag 均核对后才能清理。经济 role metrics 始终只取发布收据，
  retained artifacts 只提供执行/会计证明。

### Release artifacts

- 从候选提交 `b12539c61075aecf292eab1282d67a64d12e5cea` 的 `git archive` 构建
  `factor_research_mvp-8.1.0-py3-none-any.whl`；SHA-256：
  `447232fa6892f33e4bac456977a77f99219b1bd04a3a0539baf36ed996b3eb4e`。全新隔离 venv 的精确
  依赖安装、`pip check`、版本/import，以及 installed-wheel 对无 runtime detached checkout 的浅归档
  状态验证均通过。

## [8.0] - 2026-08-31

### Added

- 冻结 8.0 大方向 `strategic_static_capital_budget_beta`：在已披露 7.x train 中，静态预算 CAGR
  高趋势过滤约 1.802pp、Sharpe 高约 0.00249，但最大回撤差约 1.646pp；项目在看到该结果后把
  `static_risk_budget` 升为唯一主策略。权重固定为 30% A 股、10% 港股、10% 美股、20% 黄金、
  30% 五年国债与 0% 现金 ETF，并新增同执行合同的 `cash_only_511880` 可投资现金门槛。
- 新增独立 8.0 protocol、prevalidation closure、`multi-asset-8.0` runtime 与 evidence chain。
  Train 只作已披露 static control 的 exact calibration；通过基础/16bp 压力、现金超额、风险、
  换手、容量和会计门后才允许打开 2020–2022 validation，validation 通过并提交 freeze 后才允许
  2023–2026 audit。

### Changed

- Python 包版本更新为 `8.0.0`。8.0 不再注册趋势、波动目标、再平衡带、参数网格、第二候选或
  runner-up；现有 ETF 总回报、公司行动、月末信号/下一开盘、整手、成本、ADV20 容量和会计内核
  原样复用。
- `cash_only_511880` 每月末保持 100% 现金 ETF 目标并在下一开盘执行，仅因现金分红、整手取整或
  残余现金造成偏离时产生交易；它只是一条政策门槛，不构成 alpha comparator。

### Research result

- 8.0 formal train 已完整持久化并深度重放：主策略 CAGR 6.9088%、Sharpe 0.7264、最大回撤
  -17.0246%；16bp 压力 CAGR 6.8580%，相对可投资现金 CAGR 超额分别为 3.8175pp 与
  3.7839pp。除成交满足率外的全部预注册经济、风险、成本、容量和会计检查都通过。
- 唯一失败项是四角色最小成交满足率 89.6578% < 99%：primary/stress 各为 99.5425%/99.5149%，
  低值来自现金比较器在股息已成为应收款但尚未到账时请求整手再投资，四笔订单因可用现金不足一手
  而延迟到次月。该行为符合冻结会计/整手合同，但表明把 comparator fill 纳入 policy admission 是
  协议聚合域错配。
- Train gate 持久化为 false 后，admission 写入前的第二次 GitHub 核验遇到 `Empty reply`，因此 8.0
  以 `selection_inconclusive_execution_failure` 归档，而不是正常的 null selection。Receipt payload
  为 `751b85c6c2e52b450e9c3549f7f4504af50b634599be4c32e240ee503de9823a`；validation 与 audit 从未打开，
  同 release 禁止重跑。

### Known limitations

- Static 策略是在 control 的 2015–2019 结果已知后升格，train 不是独立证据；2020–2026 虽未被
  本项目正式打开，仍是公开历史。代表 ETF 选择存在幸存与研究者选择偏差，任何历史通过都只能
  解释为这六只固定工具的战略 beta 诊断，不能承诺未来盈利或外推到资产类别指数。
- 8.0 没有形成正常 admission/freeze/result，不能声称策略通过或已完成正式 falsification；若继续
  同一经济路线，8.1 只能公开地修正 operational role scope，且必须保持资产、权重、现金收益门、
  成本、日期与所有经济阈值不变。

### Release artifacts

- 从候选提交 `5e218f85f6a236fdd09bdd4f8fd346b77b6e5ce5` 的 `git archive` 构建
  `factor_research_mvp-8.0.0-py3-none-any.whl`；SHA-256：
  `02e220a82736be36b4dfe52dd53d5b6c7cbea8318c5dac9bc44310a6caeeb2e9`。全新隔离 venv 的
  精确依赖安装、`pip check`、版本/import，以及 installed-wheel 对 detached checkout 的深度归档状态
  验证均通过。

## [7.1] - 2026-08-31

### Added

- 新增 7.1 corrective amendment、独立 preselection closure、`multi-asset-7.1` runtime namespace
  与 `protocols/evidence/7.1/` 证据路径；它逐字节绑定已发布的 annotated `7.0` tag、7.0 协议、
  关闭根和 `selection_inconclusive_software_failure` 收据，不覆盖或续跑 7.0 derived stage。Amendment
  payload 为 `7335cdbb61cd0d7b9c3e6f6896ec576c7e403b87d83cfa3d6679965691984c86`。

### Fixed

- 7.1 唯一生产修复是在 causal targets 完整性比较前，按唯一键 `signal_date, code` 使用稳定
  `mergesort` 规范双方行序；dtype、列、全部值及后续 15 个 artifact 仍要求 exact。资产、窗口、
  信号、风险预算、总回报、组合、成本、执行、容量、阶段切分、所有 gates 与 claim contract 不变。
- Python 包版本更新为 `7.1.0`。7.1 仍只能重放已经披露且在 7.0 evaluation 中哈希一致的失败 train，
  冻结 null 后直接 finalize；validation 与 audit 必须保持物理未创建。
- 7.1 selection 是一次性执行：运行前整个 7.1 runtime 必须不存在，预存 source/evaluation、普通复制、
  hardlink、symlink 和额外顶层对象全部拒绝；若执行留下 runtime 却未生成 freeze，必须归档为新的
  execution failure，不得删除后在同一 7.1 重试。

### Research results

- 7.1 正式 corrective selection 从不存在的 `multi-asset-7.1` 根开始，全新生成 train source、binding
  和 15 个 evaluation Parquet；source/binding/evaluation payload 分别为 `58c0477745dd0afd6e8fad686af5379db00cf2736b25da71fd3a314217052130`、
  `5adcb0206bb73f1214e11d185f5f15261dd566ac980ac7aa68d7709e21342e55`、
  `6c23508a1e0e265c00be96af87aef472420e50cc0ace5eb0f8fafdc6d11ffc3c`。全部 15 个 artifact、
  33,249 行、metrics 与 gate 均通过 causal exact replay，五个结果哈希与 7.0 disclosure 完全一致。
- Train gate 仍为 `false`，只失败既有 relative CAGR / Sharpe 两门；create-only null freeze payload 为
  `451b7de8bbcba9372731b7dd7236e16a46467bdf5499eeff5e17e8e946ffabfd`，状态
  `selected_null_frozen_train_failed`。Validation、audit 与 runner-up 均未打开；terminal result 状态为
  `selection_falsified_no_candidate`，payload
  `869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9`，不允许盈利声明。

### Release artifacts

- 从候选提交 `83783c99820577b4f58083501ff7f8a7d701bd87` 的 `git archive` 构建
  `factor_research_mvp-7.1.0-py3-none-any.whl`；SHA-256：
  `c372e6f39380542c3124f0b4eb5120a942673530d0204eaaabae0796f6b5899e`。

## [7.0] - 2026-08-31

### Added

- 冻结 7.0 大方向协议 `fixed_multi_asset_causal_trend_budget`：固定 A 股、港股、美股、黄金、
  五年国债与场内现金代理六只长历史 ETF，只注册一个 63/126/252 日相对现金趋势预算候选，
  与同风险预算的静态 control 配对。未启用预算全部进入现金代理，不允许回测后替换 ETF、增加
  第二模型、网格调参或启用旧 A 股分数。
- 新增 Tushare `fund_daily` / `fund_div` / `fund_adj` 的 create-only stage capture、原始价格与
  现金分红总回报重构、下一开盘月频组合、逐日 NAV、成本、整手和信号日 ADV20 容量核算，
  以及 train → validation → winner freeze → audit 的物理阶段入口。
- 新增 create-only ETF 选择证据（payload
  `b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b`）：枚举
  2015-02-27 当时 L/D/I 全部场内基金及后来退市者，只用 cutoff 前上市状态、至少 252 行日线、
  ADV20 和可复算公司行动选出六只代表；`fund_div` 只按 cutoff 前异常 ex-date 精确查询，
  无界调用和 cutoff 后 `fund_daily` 请求均为零。

### Changed

- Python 包版本预置为 `7.0.0`。6.3 已以 annotated tag 封存为
  `selection_falsified_no_candidate`；7.0 不再改变 fixed-core universe 或权重，而把研究对象改为
  固定跨资产时间序列状态与预分配风险预算。
- 正式资本冻结为人民币 100 万元，每笔最多使用信号日 ADV20 的 10%，ETF 单边全成本 8bp、
  双倍成本压力 16bp、100 份整手、无股票印花税。这个规模由 2015-02-27 之前可见的 QDII ETF
  流动性决定，不把今天的高成交额回填到早期容量。
- 分红在 ex-date 先进入不可交易应收、pay-date 才转现金；`513100.SH` 的 2022-01-13 官方
  1:5 份额拆分以逐字节绑定的上交所公告调整既有持仓和隔夜固定订单，冻结名义金额与 ADV 容量
  不被拆分改写。参考价 reset 只留痕，不把 `pre_close` 变化伪造成经济收益或公司行动。

### Research results

- 一次只读代码审查在 preselection closure 提交前意外打开了真实 train；该越界运行未调用 formal
  runner、未修改仓库，也未打开 validation/audit。候选 train CAGR / Sharpe / 最大回撤为
  `5.1070% / 0.7239 / -15.3787%`，静态 risk-budget control 为
  `6.9088% / 0.7264 / -17.0246%`。候选虽改善回撤约 `1.646pp`，却少 `1.802pp` CAGR，
  Sharpe 也低 `0.00249`，因此冻结的 relative CAGR `>= -1.5pp` 与 relative Sharpe `>= 0`
  两门失败；双倍成本 CAGR 仍为 `4.8573%`，requested fill 为 `99.6745%`，不是软件或容量故障。
- 参数、资产、窗口和 gate 在上述 outcome 可见后均未改变；但因 Git closure 尚不存在，这不能再称为
  独立预注册 selection。create-only disclosure payload 为
  `6bd2909ddc97ec84d3535d15e8f13330a5752831aead82d8fb50afdd16ac6775`。7.0 只允许
  对同一失败 train 做完整性 replay 并冻结 null，不允许打开 validation 或借接近门槛调参。
- 关闭根提交并通过四平台 CI 后，正式 7.0 selection 在提交 `76a4735` 重新落盘 train source、binding、
  15 个 evaluation Parquet 与 gate；完整 metrics/gate 哈希均与上述披露一致，train 仍为 `false`。
  但 verifier 在正常化前逐行比较 targets，把协议资产顺序与 `simulate_targets` 的
  `signal_date, code` 规范顺序误判为数据差异，因此在 winner freeze 前终止。三种角色各 `348×8`
  行，按唯一键稳定排序后 targets 逐值 exact，全部 15 个重放 artifact 也逐值 exact；这被封存为
  `selection_inconclusive_software_failure`（execution-failure payload
  `04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9`），不能冒充正式 null 终态。

### Known limitations

- 本机没有带原始发布时间和历史修订版本的可信卖方预期 archive；Tushare `report_rc` 的历史
  `create_time` 已观察到晚 732–1831 天回填。卖方预期修正虽是更正交的信息方向，但在交付可信
  archive 前不得打开收益，也不能用权限升级冒充 vintage 证明。
- 固定 ETF 代表是在 2026 年已知其仍存续后选择，存在幸存与研究者选择偏差；2015–2026 历史也
  已被市场参与者和本项目人类观察。即使 7.0 全部历史门通过，仍只属于预注册历史诊断，首个可称
  新鲜的未来交易日不早于 2026-08-31，且不得承诺稳定盈利。
- 7.0 已冻结 runner 没有合法的 execution-failure 恢复入口；validation、winner、audit 与 terminal
  result 均未创建，7.0 不允许策略结论。唯一纠正路径是 7.1 小版本：仅规范 targets 比较顺序，
  资产、信号、预算、成本、数据与 gates 全部不变，使用新 runtime/stage/closure，且不得复用 7.0
  derived evaluation 或 status。

### Release artifacts

- 从候选提交 `c96b7e2987db06deac8218b588795ebd29c1ed5a` 的 `git archive` 构建
  `factor_research_mvp-7.0.0-py3-none-any.whl`；SHA-256：
  `72b343c41732c0128458272c310312b041b5bb33d46da70f96128cfc1494212f`。

## [6.3] - 2026-08-30

### Added

- 新增 6.3 corrective amendment 与冻结 runtime capsule，并固定新的 create-only
  `protocols/6.3-release.json`、`protocols/evidence/6.3/winner-freeze.json`、historical audit 和
  terminal result 路径。6.3 closure 逐字节绑定已发布的 annotated `6.2` tag、未改动的 6.2
  protocol/amendment、6.2 preselection closure 及其 create-only execution-failure 证据；6.2
  历史文件和结论不得覆盖或重新解释。
- 6.3 正式 train replay 已从 fresh 6.3 source ledger 封存 stage payload
  `47130f0c03268c8c13ee81e38b62d14c22ce9a45ba80b23d0d07c1a73948237b`，并生成
  create-only null winner freeze（payload
  `71a8543ea63fa949e5fee4c9a8ab792a6cc0834096778ab8cacbcef16796f55a`）。两个 challenger
  均未通过 train，因此 validation 与 historical audit 的市场结果保持物理未打开；terminal result
  状态为 `selection_falsified_no_candidate`（payload
  `5ce9e7e92a0908f2e0fb1554801b900d746cc67fd27600fbf4fc82850323cadf`）。

### Changed

- Python 包版本预置为 `6.3.0`。6.3 是同一扩大机会集方向的 corrective replay；唯一生产语义变化
  是在 `capacity_metrics` 中以 `math.fsum` 归约同一批已验证的 requested、executed、
  capacity-limited 及买卖分项名义金额，修复 6.2 的 binary64 加法次序假失败。
- 买卖守恒继续使用 `rel_tol=0`、`abs_tol=1e-6` 元；三个候选、来源语义数据准入、fixed-core 信号、
  Top10/exit25、十个 offset、成本、执行、容量、train/validation/audit 切分、选择/排序/audit 门和
  claim contract 全部保持不变，不借数值修复改变研究问题或放宽门槛。
- 6.3 禁止复用任何 6.2 derived stage、manifest、exact replay、return/trade/NAV trace、gate、winner、
  audit/result 或 CLI status view。只有 canonical raw 字节可以复用，且必须由 6.3 fresh stage 重新
  枚举、运行前后重算 SHA-256，并写入全新 manifest。
- `strategy status` 在 6.3 closure 尚未创建时会验证 corrective/runtime 精确字节，并按工作树状态
  报告 `implementation_pending_clean_commit` 或 `implementation_ready_for_preselection_closure`，不再
  退回已被当前源码变更自然打破的 6.0 implementation hash；显式 `--release 6.0` 仍可审计历史状态。

### Known limitations

- 6.3 corrective replay 已证明 6.2 的容量守恒异常只是 binary64 归约次序假失败，但同一冻结研究问题
  的扩大机会集路线在 train 被否证。相对动态 ADV20 Top500 control，`ADV20 >= 1 亿元` challenger
  的 paired relative CAGR q20/median/worst 分别为 `-2.8161%`、`-1.9542%`、`-3.4165%`，
  ADV20 Top1500 分别为 `-2.5338%`、`-1.6887%`、`-4.1251%`；两者十个 offset 的正相对收益
  计数均为 `0`。两者还分别出现最坏 `21.6998%` / `14.4238%` 的容量受限请求占比和
  `91.2416%` / `91.7009%` 的最坏请求成交率，未通过冻结的执行容量门。故不选择 winner，
  不允许打开 validation 或 audit，也不允许从该结果提出盈利声明；它只否证当前固定信号、组合、
  成本和执行合同下的两个扩大机会集 challenger，不外推为所有选股或所有市场路线均无效。

### Release artifacts

- 从候选提交 `a409f9b9859d8df0c42cf528ef29d481a60acb00` 的 `git archive` 构建
  `factor_research_mvp-6.3.0-py3-none-any.whl`；SHA-256：
  `f441dc5d5ee53b48dc4a048f4689e1c26902fda1483d564a8b6e9ed2889a217f`。

## [6.2] - 2026-08-30

### Added

- 新增 6.2 来源语义驱动的数据准入合同和逐日、逐候选诊断证据：分别记录实际行情缺
  `daily_basic`、经证明的无行情停牌、供应商当日返回的空 `pe_ttm`、独立空 `pb`、非法非空
  基本面值、有限输入的非有限算术结果、未分类不可评分项及理论/实际可评分数量不一致。通过准入的
  逐日诊断会单独落盘并绑定 stage manifest，
  每次评估还会在读取 pricing 或打开组合收益前从 rankings 与诊断文件重新执行硬门。
- 6.2 协议逐字节绑定已发布的 annotated `6.1` tag、其 peeled commit，以及 6.1 create-only
  `pre_return_data_admission_failed` 证据；完整性检查同时验证本地 tag object、历史 Git blob 和
  当前 tracked blob，避免把未跑收益的数据失败改写成策略负收益。

### Changed

- 保持三个 ADV20 机会集、fixed-core 信号、Top10/exit25、十个 offset、成本、容量、次日开盘执行、
  train/validation/audit 切分和全部收益门不变。原 `median >= 95% / q05 >= 90%` 有限分数覆盖率改为
  只读诊断；新的硬准入要求每个信号日、每个候选至少 25 个有限分数并能精确构造完整 Top25。
- `daily_basic` 源行存在但 `pe_ttm` 为空只按可证明事实归为“供应商当日 null、冻结信号不可评分”，
  不推断该证券必然亏损，也不推断抓取一定完整；空 `pb` 单独诊断。实际有 daily bar 却没有
  `daily_basic` 源行，以及非空但非数值、非有限或为零的 PE/PB、有限输入却产生非有限算术、存在
  未分类不可评分项，仍 fail closed；所有成员必须由有限可评分或已命名不可评分并集穷尽，不填补、
  不替换信号。
- ADV20 的单位换算或 20 日聚合产生非有限值会 fail closed；ADV20 非正的证券在三个候选 arm
  共同的 base universe 之前剔除，避免全停牌零成交证券进入 TopK。
- Python 包版本预置为 `6.2.0`；正式 runner、协议、runtime capsule、closure、winner/audit/result
  路径与 CLI 完整性入口迁移到 6.2；当前 main 上误用旧 6.1 closure builder 会明确要求从 annotated
  6.1 tag 运行。该版本属于同一扩大机会集方向的小迭代，不提前宣称路线通过。

### Known limitations

- 6.2 的正式 selection 已通过 train 数据准入并封存 1459 个信号日、109425 行排名，但在首个
  control/offset0 的容量摘要阶段因 binary64 加法次序触发软件假失败：1533 笔成交的 requested
  notional 按原序与先分买卖求和只差 `-0.0000030994415283203125` 元，executed notional 只差
  `-0.00000286102294921875` 元；按交易记录冻结的四位人民币精度做十进制精确求和，两项均完全
  相等。首个组合已在内存运行，但没有 exact result、gate、validation、winner freeze 或 audit
  落盘，因此 6.2 状态是 `selection_inconclusive_software_failure`，不能据此判断三条路线的收益。
  create-only 失败边界及 train manifest 绑定见
  `protocols/evidence/6.2/execution-failure.json`。同方向 6.3 只允许修复稳定求和并新增大额回归测试，
  候选、信号、组合、成本、执行、切分和所有选择门必须保持不变，且必须重新冻结实现后再跑。
- 6.2 的硬准入异常仍在成功 stage manifest 写入前终止；若正式运行再次失败，需要像 6.1 一样从
  冻结源码、源文件 ledger 和只读重构另行封存 create-only 失败证据，runner 尚不会自动发布该
  tracked failure JSON。
- `finite_score_count >= 25` 只保证冻结的 Top25 可以完整构造，不是新的百分比 coverage 门；
  median/q05 仍只作诊断。因此极端大面积供应商字段置空可能把有效横截面缩到很小但仍准入，最终
  解释必须同时报告实际 minimum/median/q05，不能只报告收益门结果。

### Release artifacts

- 从候选提交 `dd4c1d6c02ca270dbd114c362ddbf0775603bb0b` 的 `git archive` 构建
  `factor_research_mvp-6.2.0-py3-none-any.whl`；SHA-256：
  `6d787b306d14b8a37f82db251334f8941aae96b21a1e4e3bca1ef5bc1c73b427`。

## [6.1] - 2026-08-30

### Added

- Froze the 6.1 widened-opportunity-set experiment before loading any widened-universe returns. The protocol compares a strictly causal daily ADV20 Top500 control with ADV20 Top1500 and ADV20 at least RMB 100 million while keeping the fixed-core score, Top10/exit25 portfolio, ten offsets, costs and next-open execution unchanged.
- Required full-market daily ST history, an all-status stock roster, exact official-session listing age, physical 2024-12-31 selection truncation, paired candidate/control returns and explicit capacity fill/notional gates. The legacy monthly Top500 is bridge diagnostics only and cannot select a winner.
- 新增全状态证券主数据、逐交易日 `stock_st` 内容寻址同步、动态 ADV20 Top500/成交额一亿元/Top1500
  机会集构建器、稀疏 next-open 定价器、十相位低换手目标与容量/执行门，以及 train → validation →
  winner freeze → audit 的物理分阶段 runner。所有实际读取的协议、实现、检查点和数据分区均在运行
  前后复核 SHA-256；扩大机会集失败时不强选赢家，也不回退到 runner-up。
- Python 包版本预置为 `6.1.0`；正式收益运行冻结为 Windows CPython 3.10.16 及精确的 NumPy、
  pandas、PyArrow、SciPy 和日期/时区依赖版本，并绑定核心 distribution 非字节码文件树、Conda
  native artifact build/SHA-256 及 MKL/compiler/SIMD 身份。CI 同时覆盖 3.10 与 3.11、Windows
  与 Linux。

### Changed

- 保留首次 create-only preselection closure（payload `4d0e9bc7…`）作为未打开收益的废弃尝试：
  GitHub Actions run `33295646513` 在 Windows `setup-python` 阶段确认官方 toolcache 不提供
  CPython 3.10.16，测试尚未开始。后继实现把通用 CI 兼容矩阵改为可获得的 3.10/3.11，但正式收益
  runner 仍要求本机已逐字节冻结的 Conda CPython 3.10.16 capsule；新 closure 显式绑定并取代旧
  closure，不覆盖或删除旧文件。
- 同样保留第二次 closure（payload `af1fc6fc…`）：GitHub Actions run `33296042579` 已成功建立
  Windows/Linux 3.10/3.11 环境，但默认 shallow checkout 不含 closure 所绑定的祖先 commit/blob，
  因而完整性测试按设计失败。CI checkout 现固定 `fetch-depth: 0`；第三个 closure 显式取代第二个，
  前两份文件及其创建 commit 均原样保留，期间仍未运行 selection。
- 在打开任何 widened-universe 收益前，根据真实源数据红队复现修订 6.1 数据合同：只有
  `suspend_timing` 为空的整日 S 才能证明缺 bar 或建立跨日状态；未被 R/实际 daily bar 清除的历史
  整日 S 可因果解释后续缺 bar，但必须单列为 `carried_prior_explicit_full_day_S` 推断并记录来源日，
  不能冒充同日原始停牌记录。日内 S 不跨日，R 当日先移除旧证明；当日 `stock_st` 阳性证券先从
  机会集排除，缺 bar 不伪造为停牌或零收益，恢复后必须重新取得完整 20 个观测；经交易所公告验证
  的代码更名区间若新旧代码 `adj_factor` 归一化略有差异，明确采用历史 vendor code。train、
  validation、audit 分别使用精确截止且互不复用的停复牌和 strict stock_st cutoff-view artifacts；
  cutoff view 把每个分区逐字节复制到 create-only stage-private root，并拒绝 samefile、硬链接、
  符号链接或跨阶段物理复用。
  audit view 额外绑定 winner freeze。主表已知但当日 inactive 的 ST 行只计数忽略；`.BJ` 明确出域，
  任何未知 `.SH/.SZ` 代码在过滤前失败。train/validation 复跑额外绑定逐期、逐笔交易和每日账户
  净值完整轨迹，不再只比较摘要；唯一历史审计物理截止日预先固定为 2026-08-21，winner freeze
  与 audit result 均为 create-only；冻结证据额外记录 tie-break 换手率并独立重算唯一胜者/null，
  audit 状态必须与 gate 一致；create-only closure builder 与 `finalize` 模式分别生成固定
  preselection root 和 tracked terminal result。原预注册 payload
  `4d544251…` 保持逐字节不变；独立 amendment payload 为 `d51433e8…`。
- 将上市交易日年龄从“每只证券、每天重新转换 2063 日 Python tuple”改为初始化时一次向量化
  `datetime64` 搜索并缓存，真实全主表首日机会集构建由约 9.1 秒降至约 0.095 秒；上市前、上市日、
  第 119/120 个交易日和未来上市边界的语义不变。

### Known limitations

- 6.1 的正式 selection 在任何 target、next-open/复权定价、组合收益、交易、validation 或 audit
  打开前，未通过预注册的 finite-score 数据准入门。ADV≥1亿元、Top1500、Top500 control 的
  median/q05 分别为 `0.886663/0.836144`、`0.886000/0.838667`、`0.916000/0.850000`，低于
  `0.95/0.90`；但每天最低有限分数仍有 530/1206/406。逐日只读重构证明主要原因不是 raw 分区
  缺页，而是 Tushare `daily_basic.pe_ttm` 按文档对亏损公司为空，固定 control score 又必须先有
  有限 PE。该结果只能称为 `pre_return_data_admission_failed`，不能称为策略无赢家或负收益。
  绑定 execution commit、closure/runtime 和 train 输入哈希的证据见
  `protocols/evidence/6.1/admission-failure.json`。同一 widened-opportunity-set 假设的后续小版本必须
  重新冻结来源语义驱动的可评分资格合同，把结构性 PE 空值、停牌日无 snapshot 与真正抓取缺失
  分开；不得事后把 coverage 阈值调到刚好越线。

### Release artifacts

- 从候选提交 `ad82b005514d853e590828e6dd2f0c2b984a279c` 的 `git archive` 构建
  `factor_research_mvp-6.1.0-py3-none-any.whl`；SHA-256：
  `caa02c7370e914a777ac6bfb0cf2117950b435d8ed38c641e4cd783306e686e7`。

## [6.0] - 2026-08-30

### Changed

- 6.0 将研究主线从“为旧信号继续建设 prospective 账本、attestation、watchdog 和 shadow
  tournament”切换为“低换手可执行基线 + 公告时点可验证的新信息源”。保留 canonical PIT
  数据、next-open 全成本执行、停复牌/退市处理、容量检查和逐日账户核算；不再把工程门或发布
  次数当作市场进展。
- 即时正式基线保持 5.0 的 fixed-core 截面分数、Top10、等权和十个调仓相位不变，只把持仓
  保留边界由 rank15 放宽到 rank25。该改动的目标是降低换手，不作为新 alpha 声明。
- Python 包版本更新为 `6.0.0`；CI 回归为纯数据/研究/组合执行主线，不再为已删除的前瞻
  scheduler 强制修改 Windows 时区。

### Added

- 新增可从 canonical Parquet 独立重跑的 6.0 low-churn exact evidence runner、纯目标选择模块与
  版本化协议证据。train/validation 按 exact 持有期末边界切分且不越界；完整结果仍明确包含
  已被项目观察过的 2025+ audit，并非物理隔离的独立 OOS。新增
  `strategy status` 完整性检查与 `strategy targets` 绝对日历 sleeve 目标重建入口。
- 新增公告驱动的 PIT 候选研究：只使用 `financial_ann_date` / `financial_available_date` 之后
  当时可见的盈利与收入变化、现金流转换、ROIC 和杠杆字段，不把价格窗口或未来标签混入
  event component。预先定义的 3 个 standalone 与 3 个 fixed-core blend 全部未通过 train / validation
  主动收益门槛，`selected_candidate_id=null`，因此没有修改正式 score，也没有打开 2025+ audit。
- 新增对现有 canonical 价量、趋势和 PIT 质量字段的 129 定义正交搜索闭包；train / validation
  q20 为正均为 `0/129`，没有候选进入 formal exact，也未打开 2025+。该负面结果把下一条发现线
  明确转向新的卖方盈利预测修正数据，而不是继续排列组合旧字段。
- 新增 Tushare `report_rc` 卖方盈利预测修正数据线、不可变报告日分区、逐页 hash manifest 与
  纯 PIT 特征构造器。同步严格使用官方 3000 行 `limit/offset` 分页；超页、重复页、分页中断、
  跨页 identity/重复行冲突或连续 100 个满页均阻断且不发布部分数据。只有上海时区昨天及更早
  的日期可发布，避开 19–22 点更新中的空/部分响应；每个规范化 page 单独保存，resume 会重算
  page 文件与内容 hash、endpoint、字段和最终 Parquet。availability 固定为
  `report_date` 后首个官方开市日，`create_time` 只作 lineage；20/60 交易日 EPS/净利润共识修正
  和 breadth 至少要求三家配对券商，同券商同日多报告按指标中位数聚合，只输出信号年 FY0/FY1
  Q4；缺失保持 NaN，不读取价格、收益或 label。
- 在打开新收益前冻结下一阶段的两个正交工作流：分析师 FY0/FY1 共识修正、预测增长、公告相对
  共识 surprise 三个小候选族；以及保持 fixed-core/Top10/exit25/成本不变、只把 PIT Top500 扩展
  为动态 ADV≥1 亿元或流动性 Top1500 的机会集实验。二者都先用物理 2024-12-31 截止做
  train/validation，失败者不得打开 2025+。

### Removed

- 从 6.0 主线删除 5.x prospective ledger/release capsule/attestation/readiness/watchdog、5.9
  adaptive-shadow tournament 及其专用协议和测试；完整历史仍由已同步 GitHub 的 annotated
  `5.9` tag 保存。
- 删除已经被否决的 5.0 adaptive selector/overlay 研究入口。保留 results-first、recovery、
  walk-forward 等有限历史复现能力，但它们不再是默认正式路线。

### Known limitations

- rank25 是在项目已经反复观察 2017–2026 历史后选择的执行参数，不能称为独立 OOS。全历史
  exact 结果相对 rank15 为 10/10 offset CAGR 改善、中位数约 +1.045 个百分点；每个
  10-session 调仓周期的平均换手中位数由 18.216% 降至 11.717%。但 2025–2026 仅 5/10 offset
  为正、中位数约 +0.003 个百分点，说明可靠
  证据主要是降换手，而不是新增收益。
- 分析师修正路线当前为 `ingestion_implemented_research_spec_permission_and_vintage_blocked`：本机
  `report_rc` 凭据只有每天 10 次的试用权限，尚未完成 2017–2024 不截断回填，也没有冻结最终
  一维股票 score、打开收益或运行 exact validation。正式回填需要至少 8000 积分权限；旧的
  3886/5000 行无分页响应只能证明单页不可靠，不能作为完整数据。2017/2020 数据的供应商
  `create_time` 又晚了 1831/732 天；正式历史验证前还需用原始研报档案做 vintage 抽样，或只把
  首次捕获后的前瞻样本称为严格证据。

### Release artifacts

- 从候选提交 `b2901c03f0f3150aadf049da5e86ba0aa1446784` 的 `git archive` 构建
  `factor_research_mvp-6.0.0-py3-none-any.whl`；SHA-256：
  `4336a440a1e0b94dfc915c36ffb2b85816c38d45829701a960f953c69a0ff5dd`。全新隔离环境中
  `pip check`、版本、CLI/协议闭包验证均通过，wheel 内 22 个 Python 文件与候选源码逐字节一致。

## [5.9] - 2026-08-30

### Added

- 新增 5.9 独立前瞻 shadow Challenger 协议，注册 `low_turnover_20_v1` 与
  `low_volatility_252_v1`。随协议提交的 selection-freeze artifact 记录 31 个因果 trailing 公式的
  train/validation 后 finalist 定义、audit 边界及 registry composition 的 post-selection；两项
  Challenger 都明确禁止历史赢家表述。
- 前瞻 signal snapshot 新增独立 shadow adapter：在完整 ticker 历史上分别计算 trailing 20-session
  平均换手率的负值和 252 个 adjusted-close log return 的负波动率，再筛月度 Top-500；正式 5.2
  target adapter 的八列输入与摘要合同保持不变。
- 新增十 offset 并行 wealth 的配对评价器。每个 sealed 11 点 daily path 跨周期连续拼接，同日先
  更新全部可见 offset、未启动 offset 按现金 wealth=1，再取十条 wealth 均值；候选-control daily
  master active return 使用冻结 lag=10 的单侧
  Newey–West/HAC，并做 Holm–Bonferroni。major review 还要求 250 个配对周期、每 offset 25 个、
  8/10 正 offset、连续三个已闭自然月及零 missed/PIT/integrity/blocked-order。
- 新增与正式账本物理隔离的 `runtime/adaptive-shadow/1` create-only 哈希链，记录 activation、
  planning intent、plan、missed deadline、outcome 与 evaluation。每个 intent 在正式 deadline 前
  一次封存全部活跃候选计划，使部分写崩溃只能恢复原字节，不能在 deadline 后重算。
- 新增 route-neutral shadow market bundle、source contract 和独立深重放：outcome 同时绑定正式
  execution、shadow wrapper、fallback raw partitions、停复牌与退市 CAS；CLI audit、controller
  与 evaluation checkpoint 都会验证完整来源树。

### Changed

- 5.9 是不改变资本路线的小版本：正式 5.0 账本继续只执行 `fixed_core_full`，shadow 成败不得
  写入、回填或自动替换正式路线；若共同前瞻证据最终过门，仍须另发 6.0 才能切换大方向。
- shadow 与正式路线共享 signal、membership、官方日历、次日开盘、十 offset、持有窗、初始
  资本与成本模型。删除没有历史增量依据、也未形成独立组合账户的 50% softmax allocator；5.9
  只比较单个 Challenger 与正式 control，不宣称在线分配表现。
- shadow 对正式 ledger 的 audit/status/replay 全部使用不刷新 verification-cache 的只读路径；带
  rich decision/outcome 的测试逐字节证明读取前后正式树不变。公开 activation/plan/sync CLI 也不再
  接受 caller-supplied 历史时钟；release 时间来自已同步 annotated tag，其他时间取调用时 UTC。
- 若某个 signal 连 deadline 前 planning intent 都没有，该 candidate/offset fail-stop，后续周期只记
  terminated missed，且永久阻断 major review；合法 intent 的缺失 plan suffix 则可在 deadline 后
  从原封 payload 恢复，不算 missed。

### Known limitations

- 发布前 exact execution 诊断否决了原 price-anchor 与 5 日反转：10-offset net CAGR median 约
  -8.75% / -23.78%，相对约 +8.18% control 明显失败，结果 payload SHA-256 为
  `f31b9921047c314c5c7a3d753136ee7231b36fa9eabb07e9b99d1615edfd52bd`。替代的低换手与低波动
  在全成本 exact execution 中绝对 CAGR 仍为正，但相对 control 都是 0/10 offset 为正；相对 CAGR
  median 分别为 -0.93 / -1.97 个百分点，结果 payload SHA-256 为
  `127143d38edafe7b14c643783bcc9dfbaf0203fb0d46b9da7abc34e4d07cca50`。两者没有在
  train/validation/audit 三段稳定胜 control，只用于新的前瞻证伪。
- 5.9 发布时仍没有真实前瞻 shadow outcome，冻结历史桥的 provider vintage 未验证；低换手需要
  20 个有限 daily turnover 观测，低波动需要 253 个正 adjusted close。5.9 没有资本授权、不会
  自动晋级；增加或替换候选必须发布新小版本且不能回填。

## [5.8] - 2026-08-30

### Added

- 对冻结桥之后的 Tushare 日分区新增 provider-completion 合同：`daily`、`daily_basic` 与
  `adj_factor` 必须在上海时间 17:10 这一协议工程门槛之后完成至少两轮顺序独立采样，三端完整
  canonical fingerprint 跨轮稳定，`daily` 与 `daily_basic` ticker 集完全相同且被
  `adj_factor` 覆盖，才能原子发布为 `complete`。proof 同时绑定请求 ID/时间、每端行数、ticker
  与内容摘要、bundle 摘要和完整 evidence 摘要；17:10 明确不是供应商完整性 SLA。
- guarded 分区新增 durable `reconciling` 中间态、原迁移 provenance、重试历史、共享 bundle
  proof 与 revision-conflict 语义。崩溃最多留下可恢复 marker；采样/发布时钟回拨、canonical
  symlink、错误 identity/path/row count、mixed proof、6000 行接口上限或供应商稳定修订均不会
  发布成可消费的完整分区。
- watchdog 新增 `continuous` 控制模式。5.8 Windows 任务在工作日覆盖收盘后及次日准入窗口，
  周末每天六次粗粒度恢复，并在登录时补跑；注册器和 runner 都要求 `China Standard Time`，
  所有实例继续共享文件句柄锁和 readiness 的唯一 `action.argv` 合同。

### Changed

- 5.8 是 5.7 首轮 provider 预演暴露问题后的同方向可靠性纠错；5.0/5.2 冻结的
  `fixed_core_full`、prospective epoch、2026-08-31 首 signal、目标生成器、十个 sleeve、成本模型
  和评价门槛均不改变，也不回填任何市场结果。
- signal/input、月度 membership 与 i+11 execution/outcome 的 post-cutover 深层消费者现在都
  逐字节验证 provider-completion proof；execution source readiness、直接 builder 与 fallback
  全部要求并封存三端分区。持有期同步动作不再只请求 `daily`/`adj_factor`。
- 首周期静态窗口任务与长期 continuous 任务分离：前者只负责 2026-08-31 首 signal 的软/硬
  deadline，后者不再被 2026-09-01 的 `NotAfter` 永久短路，能够持续推进后续 decision、
  execution、outcome 与 evaluation。

### Fixed

- 修复 5.7 在官方日线端点仍处 15:00–17:00 更新窗口时，把首次非空响应永久 checkpoint 为完整
  数据的问题；partial universe 现在只会 waiting/reconcile，不能污染 Top-500 membership、input
  或 outcome。
- 修复缺 proof 与 present-but-null/string/list/损坏 proof 被混为同一种 legacy 状态的问题。仅
  字段完全缺失可走精确 `--resume` reconcile；存在但非法的 proof 必须 blocked 且不覆盖旧 bytes。
- 修复并发 subset writer、`--no-resume`、crash recovery 与 provider revision 可能产生混合 proof、
  丢失原 checkpoint provenance 或留下消费者永久拒绝的 `complete` 状态的问题。
- 修复 5.7 heartbeat 在首周期 `NotAfter` 之后永远退出 0、导致完整生命周期自动化实际停止的
  控制器合同断层。

### Known limitations

- 5.8 发布时正式账本仍为 0 decision、0 confirmed outcome；这些修复提高的是数据与运行证据的
  因果可靠性，没有新增盈利证据。方向是否成立仍必须由 10/60/250 个预注册前瞻 outcome 决定。
- 17:10 与两轮稳定采样只能显著降低抢读 partial publication 的风险，不能证明供应商之后绝不
  修订。若已 guarded 的完整 bundle 发生修订，系统会 blocked 而不是自动选新版本。
- Windows 任务使用当前用户的 `Interactive/Limited` 凭据；整机断电、长期未登录、断网或凭据
  失效仍会推迟推进，但周末恢复、登录 trigger、`StartWhenAvailable` 与 App heartbeat 提供冗余。

## [5.7] - 2026-08-29

### Added

- readiness 的机器动作合同覆盖完整前瞻生命周期：除 signal 日的 raw sync、exact reference、
  membership、input、admit 与 attest 外，现在还会从 sealed decision 的日历 CAS 确定性推导
  holding window，并依次开放持有期 `daily`/`adj_factor` 同步、全范围停复牌捕获、execution、
  outcome 与到期 evaluation。多个成熟周期按 `(calendar_index, decision_sha)` 稳定排序，旧周期
  不会被新 signal 或同 offset capacity wait 饿死。
- 新增完整生命周期的 controller、CLI 状态码、崩溃恢复与多周期顺序测试；自动化只需重复执行
  readiness 返回的 `action.argv`，不再手工拼接 decision、execution SHA 或日期边界。
- 新增由正式 release capsule 提供的 PowerShell 7 watchdog 与可重复的 Windows Task Scheduler
  注册器。App heartbeat 和 OS task 共享 create/open 文件句柄锁；runner 逐元素执行唯一
  `action.argv`、每个动作后重观察、最多推进 12 步，并以 create-only JSONL/alert 记录退出状态、
  argv 及输出长度/哈希而不保存 provider 正文或凭据。

### Changed

- 5.7 是 5.6 首轮 controller 演练后的同方向可靠性纠错；5.0/5.2 冻结的 `fixed_core_full`、
  prospective epoch、首个 signal、目标生成器、十个 sleeve 与预注册评价门槛均不改变。
- `data suspensions` 将 provider 暂不可用映射为退出码 2、证据冲突/非法状态映射为退出码 3，
  与统一 controller 的 waiting/blocked 语义一致。
- 首周期 watchdog 在收盘至 admission deadline 每 30 分钟运行，deadline 前最后 80 分钟加密为
  每 5 分钟；08:55 后首 decision 尚未完成 receipt 会生成 blocked alert，09:15 后不自行补发。

### Fixed

- 修复 5.6 只生成首轮 decision/attestation 动作、却要求自动化在 i+11 后手工构造
  suspensions、execution、outcome 和 evaluate 参数的合同断层。
- execution source/bundle 的就绪检查改为严格零写入并拒绝模糊匹配；部分发布可以确定性恢复，
  多个或损坏的匹配 bundle 会 fail closed，而不是任选一个结果。
- execution snapshot 的 create-only 发布改为同目录临时文件、文件耐久化、非覆盖原子发布与
  winner 逐字节验证；POSIX 额外同步父目录，Windows 的并发和进程崩溃恢复由专项测试覆盖，
  不再留下可被误判为完整结果的最终文件。
- 修复停复牌全量刷新在 Parquet 已替换、metadata 尚未提交时让 controller 永久 blocked 的问题；
  首次单边文件或可证明由旧 metadata 造成的 torn pair 会重新开放同一 `--no-resume` 动作，当前
  metadata 损坏、symlink、非法路径或不可证明的冲突仍 fail closed。

### Known limitations

- 5.7 发布时 prospective ledger 仍为 0 decision、0 outcome；完整生命周期自动化与耐久发布修复
  没有产生新的市场结果，也没有新增盈利证据。首周期仍依赖官方数据按时可用、本机持续运行与
  GitHub attestation 在 deadline 前完成。OS watchdog 不能抵抗整机断电/断网；当前任务使用
  Interactive 用户凭据，因此首周期需要保持登录和交流电供电。
- Windows 没有可移植的目录 `fsync`，因此 execution create-only publish 尚未证明突然断电后的
  目录项耐久性；它已证明非覆盖原子性、并发单一 winner 和进程崩溃后的可恢复性。
- controller 发出的 stale/首次停复牌刷新可从 torn pair 自动恢复；若操作员对一个当前有效 pair
  手工强制 `--no-resume` 并恰在 Parquet 与 metadata 两次提交之间中断，仍会 blocked 并需人工核验。

## [5.6] - 2026-08-29

### Added

- 新增 raw-only `data reference --trade-date <exact-as-of>` 捕获路径。它不允许向前一交易日
  fallback；同一官方 as-of 至少两次独立全量采样必须 canonical 一致，且 reference ticker
  集合必须覆盖该日完整 daily universe。发布的 reference checkpoint 同时绑定 daily
  partition SHA-256、ticker count、采样合同和 immutable source artifact，任一缺失、漂移或
  部分 universe 都 fail closed。
- readiness 的下一步升级为稳定机器合同：每个可运行阶段同时返回 `action.command`、结构化
  `action.arguments` 和可直接传给 CLI 的 `action.argv`。首轮因果链固定为 `data sync` →
  `data reference` → `prospective membership` → `prospective input` → `prospective admit` →
  `prospective attest`；controller 不再根据人类说明猜参数或跳步。
- 新增单一 `prospective admit --input` 准入 action，在一次 controller 动作中完成 target plan
  构造、create-only plan store 和 ledger seal。它取代首轮运行手册中可被拆开的 `plan`/`seal`
  两步，重复调用仍由内容身份与单调账本约束。
- 权威账本进入 `awaiting_receipt` 时，readiness 现在返回 `attest_decision` ready action，并逐值
  携带 snapshot、purpose、activation release tag、decision hash 和 admission deadline。deadline
  前走正常见证路径；deadline 后只允许恢复已持久化 binding，或在 24 小时窗口内 reconcile
  deadline 前的 intent，绝不新建 dispatch；没有这些先存证据才进入 terminal。
- 新增 `prospective repair-snapshots`：如果 ledger record 已耐久追加而对应 snapshot 尚未发布，
  readiness 返回这一确定性修复动作；它只从已验证 record prefix 重建精确 snapshot，遇到多余、
  错配或无效 snapshot 一律拒绝修复。

### Changed

- 5.6 是 5.5 首轮 controller 演练暴露问题后的同方向纠错版本：5.0/5.2 冻结的
  `fixed_core_full`、首个 signal、目标生成器、十个 sleeve 与预注册评价门槛均不改变。
- attestation dispatch 在本地 request lock 内先持久化确定性 intent，并优先按
  `prospective-<request-id>` 查询远端 workflow run；dispatch API 显式请求返回 run details，并把
  run id 立刻绑定到本地。已有 binding/receipt 的恢复不会再次 dispatch；缺失 binding 的 intent
  最多做 5 次、间隔 30 秒的远端 reconcile。这个有界协议不宣称跨主机 exactly-once：deadline 前
  若远端可见性异常超过宽限期，补发仍可能形成重复候选，后续会因多个匹配 run fail closed；
  deadline 后则只恢复、不补发。远端 identity 漂移或调用者提供冲突 run id 同样 fail closed。

### Fixed

- 修复 5.5 首轮 controller deadlock：零写 readiness 能指出 daily/reference 不完整，却没有提供
  可执行的 raw capture action；strict membership 又拒绝缺失的 exact reference，因此 controller
  无法在 deadline 前因果推进。5.6 把 raw observer 和 authoritative ledger observer 串成上述
  action chain，同时保持观察本身零写入。
- 修复 raw checkpoint 的 lost-update 竞争：calendar、daily namespaces 和 exact reference 分别在
  对应锁内重新加载最新 checkpoint，再原子发布合并结果；并发 writer 已完成的其他日期、数据集和
  calendar 不会被旧内存视图覆盖。相同分区/reference 的并发结果只在逐字节身份与完整验证通过时
  幂等复用，否则明确报冲突。
- exact reference 在同时持有 raw/reference checkpoint 锁时重新核对真实 as-of daily 分区；daily
  SHA 或 ticker count 在采样期间变化就不发布 reference。membership builder 也逐值比对 checkpoint
  中绑定的 daily SHA/count 与 immutable as-of daily source，禁止把旧 reference 配到新 daily。
- 修复 artifact availability 倒置：membership 和 input 的完成时间现在是各自 sidecar 与 commit
  marker 完成原子发布、目录耐久化之后的保守上界，发布者等待该上界后才返回。membership 以
  manifest-last 提交；input 以 rows → build receipt → manifest-last 提交，崩溃遗留的未提交 receipt
  只能在同一 snapshot 锁内验证并清理。真实 `build_completed_at_utc` 被 target snapshot、execution
  与 attestation 输入绑定，不再用 `inputs_available_at_utc` 冒充构建完成时间。
- 修复 ledger record 已提交、snapshot 尚未提交时永久阻塞的问题；readiness 现在只在唯一的
  `missing_snapshot` 证据形态下开放确定性修复。`seal_decision` 的默认记录时间也移到 ledger lock
  内采样，避免等待锁跨过 admission deadline 后仍携带过早时间。
- 修复 deadline 后一刀切 terminal 导致合法远端 run 无法回收的问题：deadline 前 intent/run 可以
  按上述 recovery-only 合同完成见证；无先存 dispatch 证据时 runtime 在 deadline 后拒绝网络派发。
- 修复 readiness 把 input manifest 中合法有限浮点误当成账本非法浮点的问题。input manifest 仍须
  canonical 且拒绝 NaN/Infinity；权威 ledger record/snapshot 继续执行无浮点合同。
- readiness 现在核对 reference checkpoint 的 ticker count 与 Parquet 行数/唯一 ticker 数一致；
  自动生成的 market sync argv 也显式列出 `daily`、`daily_basic`、`adj_factor`，不再依赖配置默认值。
- 修复官方日历缺失/跨度不足、行情分区本身齐全时 controller 无动作可执行的问题；readiness 现在
  返回精确 `--calendar-to` 的同步 argv。尚无法推导 candidate 时只回看最多 31 个已完成自然日，
  并把日历扩展到未来 62 日所在月末；已知冻结桥接前缀有缺口时则从首个缺失日开始。`data sync`
  遇到 provider 暂时返回空 calendar/partition 时返回结构化 `waiting` 和退出码 2，保留已完成
  checkpoint 供下次 resume，而不是把可重试状态升级成永久失败。

### Known limitations

- 5.6 发布时 prospective ledger 仍为 0 decision、0 outcome；这些控制器、并发和证据合同修复
  没有产生新的市场结果，也没有新增盈利证据。`ready` 与工程测试通过仍不等于独立 OOS 验证、
  策略收益确认或实盘就绪。

## [5.5] - 2026-08-29

### Added

- 新增严格零写入的 `prospective readiness`：在不创建 lock、cache、membership、input 或账本
  record 的前提下，把无缓存刷新的权威账本重放与下一合法 signal 的官方日历、阶段性原始分区、
  exact-as-of reference、月度 membership、input bundle 和 admission deadline 绑定。稳定状态为
  `ready`、`waiting`、`blocked`、`terminal`，自动化退出码分别为 0、2、3、4。
- 已有 membership 与 input 不再只检查外层 manifest/hash：readiness 分别调用公开 loader，从
  immutable CAS 完整重放冻结规则、60 日流动性窗口、exact-as-of reference、signal rows 和
  provenance；最终 decision admission 还必须在 active 发布 capsule 中重放 target generator，
  精确匹配 signal、entry、calendar index、offset、首轮 skipped sessions 与连续模型状态。

### Changed

- readiness 改为互斥阶段门：缺少月度成员时只开放 `membership_build`，成员已权威验证且 input
  缺失时只开放 `input_build`，input 与 active target replay 均通过后才开放
  `decision_admission`。已封存 CAS 可自证时不再因为后来变化的 membership-only live
  liquidity/reference origin 而错误错过 deadline。
- 5.5 保留 5.0/5.2 冻结的 `fixed_core_full` 研究方向；这是控制器与前瞻证据完整性的小版本迭代，
  不是再次使用同一历史样本挑选新策略。

### Fixed

- 修复后续日循环错误地把同一月 membership 的 `as_of_date` 和 effective interval 当成随每日
  signal/entry 滚动的问题。现在 membership month 仍取 entry 所在月，但 as-of 固定为月首前
  最后一个官方开市日，effective start/end 固定为月内首/末开市日；自然月末仅用于证明日历
  完整覆盖。首轮 2026-08-31 恰好正确不再掩盖第二轮必然失败的问题。
- 修复 deadline 终态优先级：候选与 deadline 一经权威账本/稳定日历确定就立即计算；后续证据
  异常、awaiting receipt/evaluation 或 same-offset capacity 不再把已错过的不可恢复 admission
  降级成普通 blocked/waiting。
- 冻结 prospective epoch 的首次 implementation canary TLog：后续纠错版本只能更新 active runtime
  的因果时间，不能重新选择首个 signal 或把本应处理的交易日列为 skipped；纠错 canary 若不早于
  已冻结首信号收盘，readiness 直接进入不可重试的 terminal。
- 修复 readiness 的稳定性门：任何 `membership_build`、`input_build` 或 `decision_admission` ready
  在返回前都必须在账本锁内重新执行零写观察，并逐值确认账本 head、deterministic snapshot 与完整
  数据报告没有变化；target replay 期间的并发 upgrade 或新增同 signal artifact 不再产生旧视图
  false-positive。
- 已封存 membership/input 中完整重放过的官方日历 CAS 成为独立 authority；它允许 admission 在
  mutable raw checkpoint 丢失或被替换后继续验证，同时会与更长的新 live 官方日历按内容哈希合并，
  不会让旧封存 horizon 永久遮住下一周期。仍需构建 membership/input 时则严格要求 live 日历集合
  与真实 builder 等价，任一损坏条目或逐日冲突都会 fail closed，不能产生 build false-ready。
- 修复新的正式环境追加 implementation upgrade 时，验证器错误要求同一解释器同时匹配所有
  已被替代实现的完整 distribution closure。decision-free transition 仍逐字节验证历史 annotated
  tag、Git blobs、closure、receipt、capsule tree、账本链和 snapshots；待追加的 5.5 capsule
  以及普通 status/audit/执行路径仍要求当前 active runtime 精确一致。
- 修正首个 2026-09 前瞻 membership 的运行手册：官方交易日历必须覆盖到整个生效月末
  2026-09-30，而不只是 i+11 的 2026-09-15；隔离的 5.4 capsule 演练已确认，覆盖月末后流程
  会在尚未发生的 `daily/2026-08-31` 分区处正确 fail closed，正式目录没有提前生成 membership
  或 decision。
- 明确停复牌快照扩展结束日期时不能使用 `--resume`；应从 2017-01-01 到新 signal date 执行
  `--no-resume` 全量抓取、审计并原子替换，避免把旧范围误当成已覆盖的新快照。
- runtime closure updater 在写 manifest 前额外要求已安装的 `factor-research-mvp` 精确等于
  `implementation_release` 对应的三段式包版本，防止 5.4 wheel 被封装成 5.5 capsule。
- 发布胶囊的原子目录发布在 Windows 遇到短暂 scanner/indexer 文件锁时采用有界退避重试；并发发布者
  已先完成目标目录时仍转入完整 capsule 验证，持续权限错误则在有限时间后保留原始异常并 fail closed。

### Known limitations

- `ready` 只表示报告中的 `next_action` 可以尝试；它不保证外部 provider 调用成功，不表示
  decision 已封存、独立 OOS 已验证或策略收益已确认。发布时账本仍没有 decision/outcome，
  5.5 因而没有新增盈利证据。
- implementation upgrade 一旦写入又 abandonment，单调账本不会回滚到旧 runtime；恢复运行
  需要发布更高的纠正版。已安装的旧控制器不能越过它不认识的中间升级记录。

## [5.4] - 2026-08-28

### Fixed

- 修正 5.2/5.3 发布 manifest 与 release runner 的精确 schema 漂移：manifest 已声明日常 suffix
  replay 与完整 audit 两项策略，但 capsule materializer 漏接纳这两个键，导致实现升级在写账本
  前正确失败。5.4 同时逐值验证两项策略，真实 tag capsule 必须能先物化并自检才可追加记录。
- `implementation_release` 与 CLI 默认 tag 同步到 5.4；首次实现绑定仍固定 generator、entrypoint、
  manifest、evaluator 与合同身份，但不再硬编码只能是已经发现运行缺陷的 5.2 tag。它要求同一
  protocol major、版本至少 5.2 且单调递增，从而允许在没有任何 decision/outcome 前直接绑定
  已发布的纠正版。5.2 与 5.3 tag 均保持不可变，且两次失败都发生在账本写入之前。

## [5.3] - 2026-08-28

### Fixed

- 修正 5.2 非 editable wheel 的默认项目根定位：CLI 不再按 `cli.py` 的固定父目录层数推断
  checkout，否则正式环境会把默认账本错误地落到 venv 的 `Lib/runtime/prospective/5.0` 并看到
  一条空平行链。5.3 会从安装文件与当前目录向上查找 `.git`、`pyproject.toml` 和冻结协议三重
  标记；找不到就明确失败，显式 `--root` 始终可用且不会先触发自动发现。
- 增加模拟项目内非 editable `site-packages` 布局的回归测试，并要求正式 wheel 的无参数
  `prospective audit/status` 实际命中 checkout 下既有账本后才允许升级实现。

## [5.2] - 2026-08-28

### Added

- 增加 `fixed_core_full` 的确定性 route→targets 生成器。它从内容寻址的单日 PIT 输入重建
  70% 防御价值 / 30% control 排名，严格保留冻结的 binary64 运算顺序；历史 2,329 个
  cohort 与 5.0 权威运行逐一一致，并单独固定两个 `0.3` 改写会改变边界证券的回归向量。
- 增加十个相互独立的 500 万元虚拟 sleeve：绝对交易日索引决定 offset，首次轮到的 sleeve
  从现金以裸 Top10 启动，此后只按该 sleeve 的 Top15 留仓缓冲调仓；未启动 sleeve 保持现金，
  历史稳健性 offset 不会伪装成已执行持仓。选择状态和逐 offset 账户状态分开封存。
- 增加 source-backed 执行与结算合同：信号日 `i`、次日开盘 `i+1`、第十个持有期后的共同
  开盘边界 `i+11`，使用固定 A 股费用/冲击模型、因果 ADV/波动率、停复牌/退市事件、公司
  行动连续复权价格和决策时点 benchmark。CLI 只能从 sealed decision 与不可变执行快照重算
  outcome，不能再公开提交手工收益标量。
- 增加从 2026-09 起生效的前向月度 Top500 builder：只使用月初前 60 个官方交易日、每只
  至少 20 个正成交额观测，按 60 日正成交额中位数降序和 ticker 打破并列选择精确 500 只；ST、
  未上市或已退市证券保留成员身份但标记为不可交易，不用事后替补改变集合。
- 增加 10 / 60 / 250 条 confirmed outcome 的预注册评估阶段：10 条只确认工程闭环，60 条
  只允许明显失败时提前否决、不得提前晋级，250 条且每 offset 至少 25 条后才运行绝对收益、
  主动收益、Sharpe、完整持有期日频回撤和跨 offset 一致性的约一年方向 gate；同时构造十个
  sleeve 合计 5,000 万元的真实日频 master portfolio，要求终值、绝对/主动 CAGR、日频
  Sharpe 和日频最大回撤全部过线。通过仍不允许宣称稳定盈利或绩效晋级。
- 增加由发布 commit 原始 Git blob 生成的 create-only release runner 胶囊。目标、输入、
  membership、执行和结算在隔离 Python 子进程中使用对应 tag 的源码重放；当前 `main` 可以
  继续演进，而不会要求旧周期偷偷改用新实现。
- 增加项目内版本专用运行环境、完整 transitive distribution lock 和逐 artifact SHA-256
  wheelhouse，Factor Lab 自身也从 clean Git archive 构建为带哈希的非 editable wheel。
  运行闭包绑定精确 CPython build 字符串、平台标签、全部已安装 distributions、数据配置、
  runtime lock 与全部包源码；干净 smoke venv 必须能完全离线重建后才可发布。

### Changed

- 前瞻 decision plan 升为 schema 2：计划只能引用内容寻址 signal snapshot；seal 在账本锁内
  恢复 genesis 或上一条 selection state 并重跑生成器。一次可同时存在多个已见证、尚未到
  结算日的周期；相同 offset 的账户状态必须连续，其他 offset 可按真实数据到达顺序结算。
- `data sync` 可用 `--calendar-to` 先保存覆盖未来持有期的官方日历，同时只下载 `--to` 以内
  的市场分区。Python、NumPy、Pandas、PyArrow、SciPy 及数据客户端改为精确版本绑定。
- 实现升级新增独立 GitHub canary；第一条 signal 只接受严格晚于 2026-08-21、且 canary
  可信 Tlog 早于该 signal 收盘的真正新数据。5.0 activation 与既有收据不重建、不回填。
- outcome 不再暴露运行时选择的 `not_executed` 路径：证据不完整就 fail-closed，证据完整则
  只能生成确定性的 complete outcome。250 条方向否决和 60 条早停会写入不可逆 evaluation
  checkpoint 并停止后续 decision；未见证且尚无 decision 的错误实现升级只能显式放弃并留痕。
- 全历史 audit 改为一次 capsule RPC 批量重放，避免每条 outcome 重启解释器造成的二次复杂度；
  胶囊收据同时绑定 annotated tag 名称、tag object、peeled commit、manifest、源码文件集和
  运行闭包，账本 audit/status 不会在缺失时隐式创建或修复证据。
- 日常 status 和写入增加一次性、可丢弃的验证前缀缓存：结构链、见证 bundle、顶层 artifact
  及递归引用的 source CAS 每次仍按当前字节复核，缓存响应仍走普通结果 validator，release
  runner 只计算缺失 suffix；完整 audit 始终绕过缓存重放全部目标、outcome 和 evaluation，
  并原子刷新当前 head 的唯一缓存文件。缓存损坏、旧 sidecar/CAS 缺失或胶囊变化都会退回
  全量重放或 fail-closed，不把派生缓存当成证据。

### Fixed

- 修正未来公司行动日复权开盘价：使用冻结桥接校准乘数乘以当日 `adj_factor`，不再把信号日
  的有效复权比例永久套到未来；拆股/送转造成 raw price 与 adjustment factor 同步变化时，
  调整价格保持连续且不会双算。
- signal、calendar、membership、raw daily/basic/adj、补充财务、停复牌和 execution source
  全部先复制到按字节 SHA-256 寻址的 create-only store，再从该副本解析。以后 `--no-resume`
  重抓、Parquet 编码变化或 checkpoint 更新不会破坏已经封存的历史周期；audit/read 不会为
  缺失证据偷偷物化替代文件。
- execution 构建现在查询官方 Tushare `stock_basic(list_status=D)`，把规范化的退市状态、
  退市日期、查询合同和获取时间写入 immutable CAS；outcome 与 sealed execution 的加载、审计
  和重放只读取并校验这份内容寻址证据，不会再次联网或用后来返回的当前状态改写已封存周期。
- checkpoint 的真实完成时间通常含微秒；进入要求整秒的纯合同前现在一律向上取整，避免
  向下截断而虚构证据提前可用，也避免真实执行快照因非 canonical timestamp 无法构建。
- signal 对应的 membership 月份改由其“下一官方交易日”确定，而不是机械沿用 signal 自身
  月份；因此 2026-08-31 的首条信号必须绑定 2026-09 集合。sealed replay 每次都从月度原始
  CAS 全量复算，成员加载不再依赖可删除的别名文件；官方 `stock_basic(D)` 还要求两次独立
  全表查询 canonical 一致，避免局部或当前幸存者集合悄悄进入历史状态。
- decision-time benchmark 冻结完整 roster：起始缺少可交易 open 的份额全周期留在现金，
  后续停牌沿用最后合法 open，退市永久归零，禁止 outcome 时删除缺失成分或给幸存者重配权重。
  策略与 benchmark 都封存 11 个 holding-session 观测，共享调仓边界以新周期交易后 NAV 为准，
  周期内暴跌不能再被仅看十日端点的回撤指标隐藏。
- 任一确定性 complete cycle 若账户 NAV 归零，评价立即终止并否决当前 major 方向；不再因该
  sleeve 无法继续生成第 6/25 个周期而让灾难性失败永久停留在 `accumulating`。公开
  `CycleOutcome.to_ledger_v2_outcome()` 也改为真正的 schema-2 rich envelope，不再返回会被
  当前账本拒绝的 legacy 13-scalar 形状。
- 同一 offset 允许在共享换仓边界短暂保留两代 open cycle，但第三代计划必须等待最老 outcome；
  execution、outcome builder 和账本 replay 又分别强制只能先关闭最老一代，不能跳过坏周期后
  持续滚动较新幸存者。10/60/250 门槛一到即公开进入 `awaiting_evaluation` 并阻断新 decision，
  方向已否决后仍可收完先前在途 outcome，但不会重新制造不可执行的 evaluation due。
- benchmark endpoint coverage 的 builder 与 public loader 统一为：起止 open 均存在且两个端点
  都不是停牌/退市才算 complete；不再依赖 builder 恰好把事件端点 open 置空来维持两条校验
  路径表面一致。
- 运行闭包对声明为 UTF-8 文本的 Python、TOML 和 JSON 先执行与 Git `text eol=lf` 一致的
  CRLF→LF 规范化，再核对发布 commit 的原始 blob。Windows `core.autocrlf` 或混合换行不再
  让工作树哈希绑定到一个 Git tag 中根本不存在的字节版本。
- 账本单元测试不再从 gitignored 的本地 `runtime/data/top500/execution.parquet` 偷读冻结交易
  日历；干净 Linux/Windows CI 现在用固定休市日重建同一 2,340 日前缀并复核其 SHA-256，避免
  本机全绿而远端因缺少私有运行数据失败。

### Known limitations

- 5.2 发布时 confirmed prospective outcome 仍为 0；历史年化、Sharpe 和回撤只决定冻结路线，
  不证明未来稳定盈利。十个 sleeve 共享同一市场路径且高度相关，正式方向判断必须等预注册
  的 250 条门槛，不能把 10 条工程闭环或 60 条未触发早停解释成策略通过。
- 当前只运行虚拟账户，不连接券商、不发送真实订单。成交价、容量、费用与事件处理是固定
  模型而非券商回报；因此它能检验可执行的前瞻研究路线，但不是实盘盈亏凭证。
- 5.2 wheelhouse、依赖和虚拟环境位于项目 `runtime/` 并会保留；基础 CPython 3.10.16
  interpreter 本体仍由发布主机提供而未 vendored。完整 build 字符串和平台已 fail-closed
  绑定，但 GitHub tag 本身不包含这些 gitignored 二进制；若本地 wheelhouse 或原解释器不可
  再取得，需要新 tag 和显式运行环境迁移，不能让旧账本静默换解释器。相同版本的已安装
  site-packages 若被原地篡改也不做每文件 RECORD 重验；逐 artifact lock 防止正常重装漂移，
  但不把本方案描述为完整软件供应链取证。
- 前向 membership 规则只从 2026-09 生效；2026-08 及更早月份继续使用已经绑定的 canonical
  membership，不将新规则反算成伪历史。实现升级后若改变 selection/accounting schema，仍需
  先封存显式状态迁移，不能把已有十 sleeve 状态重置为现金。
- 5.2 假定自动化使用唯一默认 ledger root，并在官方证据首次可用时生成一次 execution。它不设
  远端全局 registry 来阻止人为复制平行账本，也不阻止恶意操作者在首次 outcome 前反复替换
  原始 checkpoint、制造多个自洽 execution artifact 后择优提交；这属于本地操作者信任边界，
  后续若需要对抗应新增窄 execution-binding/远端唯一性版本，而不是把它误写成收益已验证。

## [5.1] - 2026-08-28

### Fixed

- 修正前瞻见证校验器对 GitHub Actions workflow-run `path` 字段的错误假设：真实
  workflow-dispatch 响应返回 canonical workflow path，而官方示例也可能包含 `@ref-name`。
  现在只接受这两种精确编码，同时仍独立锁定 dispatch run id、request title、tag、head SHA、
  run attempt、证书 RunInvocationURI、source ref 与 Tlog 时间；不会因兼容响应格式而放宽身份。
- Activation 核对远端 annotated tag 时移除强制 HTTP/1.1，并在 Git smart-HTTP 不可用时
  使用 GitHub Git database API 复核同一 tag object 与 peeled commit。API fallback 不创建提交、
  不移动 ref，也不把分支名或 contents API 响应当成发布 tag 证据。
- 修正 `gh attestation verify` 同时传入互斥的 `--cert-identity` 与 `--signer-workflow`，导致
  真实 GitHub CLI 在读取 bundle 前直接失败的问题。现在保留更严格的完整 certificate identity，
  并继续锁定 repository、source tag、source commit、GitHub-hosted runner 与精确 run attempt。
- 每次 attestation 在 dispatch/resume 前都会重验当前本地及 GitHub annotated tag object 和
  peeled commit 与 activation record 完全一致；即使 tag 名和目标 commit 未变，替换 tag object
  也会在任何网络 workflow 或账本写入前失败。

### Operations

- 5.0 零观察账本已绑定 clean 权威运行 `88009f1e5309b268`、annotated `5.0` tag 和冻结路线
  `fixed_core_full`。首次 activation-canary workflow `33132845922` 在 GitHub 成功，但 5.0
  本地校验器因上述 `path` 格式差异拒绝写入收据；5.1 必须恢复并验证这次精确 run，不能
  重建账本、重发 workflow 或把失败尝试回填成新决策。

### Known limitations

- 5.1 是证据链兼容性修复，不改变 5.0 研究协议和历史路由。可验证的 route→targets 生成器
  与十 offset 实际资本编排仍未实现，因此第一条前瞻 decision 继续阻塞。

## [5.0] - 2026-08-28

### Added

- 冻结 `protocols/5.0.json`：下一大方向改为固定防御价值核心、独立市场风险覆盖层、
  仅作挑战者的因果在线组合，以及不可回填的前瞻 hash-chain 账本。协议在首次 5.0
  历史运行前固定五类等 AUM 账户、十个 offset、配对 gate 与确定性路由，并绑定已发布
  `4.1` tag、纠正后权威运行及全部源产物哈希。
- 增加独立 `adaptive` suite：十个预注册 offset 分别建立四个全历史独立成本影子账户，
  并在共同起点新建 `fixed_core_full`、`fixed_core_overlay`、`static_prior_full`、
  `online_full`、`online_overlay` 五个等 AUM 评分账户。在线权重只读取严格成熟共同 cohort，
  市场覆盖层只读取信号日收盘可得的趋势、breadth 与波动信息；产物不生成策略排名。
- 增加 create-only 前瞻证据账本：严格 canonical JSON、逐记录 SHA-256 hash chain、
  Windows/POSIX 跨进程锁、hardlink 原子发布、activation/decision/attestation/outcome/
  correction 状态机，以及绑定 immutable `5.0` tag 的 GitHub artifact attestation workflow。
  Activation 必须显式指定一个干净、完整、非 canary 的 adaptive run，并复核 tag commit、
  双层 manifest、全部文件、协议、完整性 gate 与重算路由后，把 run/hash/route 固化进账本。

### Changed

- 默认研究入口改为 `adaptive`，research config 与 summary schema 升至 `5`，engine 升至
  `factor-lab/research/v7`；冻结 protocol 的原始 SHA-256 同时进入运行指纹、summary、
  adaptive envelope 与 manifest inputs。
- 外部目标持仓账户改为 fail-closed：任一计划调仓日缺目标、缺 promotion audit、audit 未获
  准入或目标包含非 eligible 股票时，在生成任何排名等权 fallback 会计前直接拒绝。
- Overlay 的 `return_1d`、`momentum_120` 改为 adaptive suite 强制加载字段；
  `momentum_120` 显式绑定到 `close_adj` 的保守 PIT lineage，不再遗漏在依赖审计之外。

### Fixed

- 修正市场覆盖层把 breadth 尚未完成 400 日 warm-up 误当成市场收益缺失、从而污染累计
  market level 的问题；收益覆盖与 breadth readiness 现在分别判定，覆盖层在真实样本上从
  `2018-08-22` 起可用，缺值仍保持 fail-closed。
- 修正外部自适应目标中空映射被误当成“缺少目标”并回退的问题；显式空目标现在表示全现金，
  缺失调仓日仍直接失败。组合容量审计改用全精度执行结果，不再用四位小数展示值产生误报。
- 完成运行的 checkpoint 校验现在同时核对 manifest 每行的文件大小与 SHA-256；全历史
  research summary 在账本真正激活前明确标记 `prospective_status=not_activated`。
- 在线决策的 `excluded_unmatured_cohort_count` 只统计信号日已经出现但尚未成熟的 cohort，
  不再泄露全样本未来 cohort 数；追加未来数据不会改写既有决策及其 history hash。
- Canary 与 full 现在复用同一套完整冻结协议校验；程序化 `run_research()` 与 CLI 都默认
  `adaptive`。特征、执行、停复牌和 protocol 在加载后及 manifest 发布前各复核一次，长跑中
  任一输入变化都不会发布 completed summary 或 latest checkpoint。
- 前瞻 attestation 不再用可查询但可拼接的 workflow `created_at` 代替可信时间：收据现在
  持久化并复核 request/display title、精确 run id/attempt、证书 RunInvocationURI、完整
  `verifiedTimestamps` 与最早 Tlog 时间；decision 只接受严格早于 09:15 deadline 的 Tlog。
  同一 snapshot 有多个合法 attestation 时按本次 run attempt 精确选择，resume 不能借用旧 run。

### Research results

- 首个完整开发树运行建立 40/40 个连续成本后影子账户和 50/50 个共同起点 fresh equal-AUM
  评分账户；共同起点为 `2018-09-03`，全部账户的执行输入与期间覆盖率为 1，未来 feedback/
  overlay、容量与对账违规均为 0。两组独立审计分别从原始 NAV/period 重算 200 个指标和
  2,329 条在线历史，并复核 manifest 108/108 个文件，结果与产物零差异。
- 固定核心全仓的年化收益 Q20 / median / worst 为 10.62% / 11.07% / 10.11%，Sharpe
  Q20 为 0.674，最大回撤 Q20 为 -18.30%。风险覆盖层虽将最大回撤配对 Q20 改善 2.31 个
  百分点，却损失 6.49 个百分点年化和 0.216 Sharpe，0/10 offset 改善年化；gate 失败。
- 静态分散相对固定核心的年化与 Sharpe 配对 Q20 为 -1.14 个百分点和 -0.056；在线分配
  相对静态先验又为 -0.04 个百分点和 -0.002，只有 1/10 offset 改善年化。四个冻结 gate
  全部失败，确定性路由因此收缩为 `fixed_core_full`，在线、分散与覆盖层均不部署。

### Known limitations

- 上述结果仍使用已反复观察的 2017–2026 历史，固定核心本身来自 4.1 的事后选择；十个
  offset 共享同一市场路径，不是十份独立样本。它只否决新增复杂度，不能证明稳定盈利。
- PIT feature、复权执行价和 universe 的历史 vintage 仍未完全验证，固定保持
  `investment_claim_allowed=false`。正式权威运行必须来自干净的 5.0 release commit；
  前瞻账本发布激活前为 `not_activated`，激活后也从 0 个确认观察起步且禁止历史回填。
- 5.0 activation 能证明“哪次运行决定了哪条 route”，但当前 decision targets 仍由调用方
  提供，尚无可验证的 route→targets 生成器和十 offset 实际资本编排。因此激活与远端 canary
  可作为零观察方向检查点，第一条前瞻 decision 在该缺口补齐前仍必须阻塞，不能把手工目标
  伪称为 `fixed_core_full` 产物。

## [4.1] - 2026-08-28

### Changed

- Walk-forward 比较统一改为从 `2019-08-16` 开始的 fresh-cash、空仓、5000 万等 AUM
  账户。七个静态候选、fixed comparator 与 dynamic 账户在十个 offset 上共形成 90 个
  可比评分账户；全历史影子账户只向 selector 提供当时已完成的成本后收益，不再进入
  phase 排名或绩效比较。
- 组合账户改为逐个 execution session 处理估值、停复牌和退市；复权生产口径的公司行动
  影响嵌入 `open_adj`，只有 `raw_with_actions` 测试/未来入口才逐日处理显式拆股与分红。
  每日 NAV 路径成为收益、年度/半年度指标及最大回撤的权威来源，持有期稀疏边界只用于
  周期归因。
- 生产价格合同冻结为 `adjusted_total_return`：只使用 canonical `open_adj`，来源为 AkShare
  HFQ 与 Tushare `raw × adj_factor` fallback，`lot_size=0`，并禁止同时处理非中性的显式
  拆股/分红事件。`raw_with_actions` 只保留为测试和未来数据入口。
- 研究 config 与 summary schema 升至 `4`、engine 升至 `factor-lab/research/v6`；运行指纹
  纳入精确 Python/NumPy/Pandas/PyArrow/SciPy 身份，result envelope 记录角色、offset、共同
  起点和决策 hash。
- manifest 升至 schema `2`：按 canonical JSON 计算自哈希，逐文件登记 SHA-256 与大小，
  运行前后重验输入以阻断 TOCTOU；缓存只有在 manifest、角色、共同起点及决策完全一致时
  才可复用。

### Added

- 增加 `factor-lab data suspensions --from ... --to ... --resume`：按自然年切分
  Tushare `suspend_d` 查询，并在每个窗口内用 `limit/offset=5000` 分页；输出标准化、
  去重排序的 `runtime/data/top500/suspensions.parquet` 及带查询范围、行数、S/R 统计和
  文件 SHA-256 的 `suspensions.meta.json`。续跑只有在范围覆盖、schema、统计及文件
  hash 全部验证通过后才零请求复用。
- 纳入官方停复牌快照：2017-01-03 至 2026-08-21 共 170,674 行、3,676 只证券，
  其中停牌 163,700、复牌 6,974；空白停牌时段或覆盖 09:30 的停牌阻断开盘成交，09:30
  后停牌与复牌标记不反向改写开盘状态，退市事件优先。
- 增加 schema `3` 的递归 PIT lineage 合同，覆盖 feature、execution、停复牌、builder、
  日历和 universe 依赖路径；任何生产必需字段未被 vintage 证明时均逐路径列出 blocker，
  并保持 `investment_claim_allowed=false`。
- 增加完整 daily NAV、停牌/陈旧行情诊断、benchmark 有效覆盖、账户起点及期末复利精确
  对账；完整运行要求 270/270 个 train/validation/audit 窗口通过产物完整性检查。

### Fixed

- 纠正 4.0 运行 `97840d20b4a2ff71` 的核心可比性错误：dynamic/static 账户在共同评分
  起点前已经积累财富和持仓，而 fixed 账户从 5000 万现金开始，起点 NAV 相差
  12.16%–24.78%（中位数 16.73%）。该运行的 selector future-violation=0 仍只在狭义
  cutoff 上成立，但其跨策略绩效、排名、配对差值和历史 gate 全部作废，由本版本 fresh
  equal-AUM 结果替代。
- 修正周期收益以错误的期末/边界资本为分母、稀疏持有期路径低估日内最大回撤、首日 NAV
  不等于请求初始资本，以及复利收益与期末财富不能精确对账的问题。
- 空的计划调仓信号改为 fail-closed；最后决策日之后的行情只允许退出估值，不再生成新
  信号；`holding_days` 与真实 rebalance interval 统一。
- 停牌期间不再使用脏行情标记或成交，缺失/无效 ADV 的容量为零；持仓超过 21 个交易日
  无有效价格时明确标记 stale。退市按零回收冲销且不伪造卖出，零价值尘埃仓位被清理。
- benchmark 拒绝停牌、退市、非正价格和无效端点；中途停复牌和退市即使不在调仓日也由
  逐日执行链处理。生产复权口径通过调整价格承载拆股/分红，明确拒绝再叠加非中性显式
  事件；显式事件的逐日会计只属于 `raw_with_actions` 测试/未来入口。
- 增加互斥的组合价格口径合同。生产研究只接受
  `adjusted_total_return`（canonical `open_adj`，混合 AkShare HFQ 与 Tushare
  `raw × adj_factor` fallback），使用合成总回报单位、`lot_size=0`，并禁止在复权价格上
  再处理任何非中性的拆股或现金分红字段，避免公司行动双算。显式事件会计仅保留为
  `raw_with_actions` 测试/未来入口；它必须使用非复权执行价，且当前 runner 因缺少已证明的
  raw 生产 artifact 而拒绝启用。
- 修正 `open_adj` / `close_adj` 的保守 PIT lineage 描述：实际为混合 AkShare HFQ 与
  Tushare 调整因子 fallback，而非把全部行错误描述为单一 `raw_open + adj_factor` 来源；
  两条来源的历史 vintage 仍保持 unverified、fail-closed。

### Research results

- 校正后的完整运行 `6462d5550b459fb2` 覆盖 90/90 个等 AUM 评分账户；未来输入违规、
  容量违规、账户对账错误均为 0，90 个账户均有 2,028 个完整 daily NAV 观测，benchmark
  收益覆盖率最低 98.996%。181 个 manifest 文件及 manifest 自哈希全部通过复核。
- dynamic 年化收益 Q20 / median / worst 为 8.90% / 9.25% / 6.93%，Sharpe Q20 为
  0.562、IR Q20 为 0.093、最大回撤 Q20 为 -20.60%。相对 fixed 的配对年化 / Sharpe /
  IR / 最大回撤 Q20 分别为 -1.41 个百分点 / -0.092 / -0.045 / -2.91 个百分点，且只在
  5/10 offset 年化更高；`historical_diagnostic_passed=false`，hard selector 路线被否决。
- 70% 防御价值静态候选在九个可比策略中排名第一：年化收益 Q20 / median / worst 为
  11.16% / 11.58% / 10.53%，Sharpe Q20 为 0.707、IR Q20 为 0.181、最大回撤 Q20 为
  -18.29%。它相对 fixed 的年化、Sharpe、最大回撤配对 Q20 为 +1.22 个百分点、
  +0.053、+0.03 个百分点，10/10 offset 年化改善，因此基于 4.1 已观察结果被选作下一
  方向的固定核心；这是明确的 post-selection 选择，不是预注册 gate 或独立 OOS。

### Known limitations

- 财务、复权与部分 universe 输入没有保存每次历史 revision vintage；PIT lineage 因此
  正确地阻断投资声明。月末名称可以按时点核对，但日内 ST 历史仍不可用。
- 十个 offset 是同一市场路径上的相关稳健性切片，不是十个独立样本；2017–2026 历史已被
  反复使用。4.1 只能用于否决 hard selector、冻结下一协议及建立历史基线。
- 固定核心相对 fixed 的 offset 8 最大回撤恶化 2.28 个百分点；总体配对 Q20 为正不能
  抵消这一单点尾部风险，后续协议必须继续披露并单独归因。
- 数据层虽注入 153 只退市证券及其后续 event-only sessions，本次 90 个评分账户没有持有
  到需要实际触发退市零回收的证券；该机制已实现并有测试，但本次历史结果没有实证覆盖
  这一分支，事件源 PIT vintage 也仍未验证。

## [4.0] - 2026-08-27

> **4.1 更正：**本节引用的 `97840d20b4a2ff71` 跨策略结果存在不等 AUM 起点，相关排名、
> 配对差值和 gate 已作废。selector 的收益截止规则仍无未来违规；可比绩效请以 4.1 的
> fresh equal-AUM 运行 `6462d5550b459fb2` 为准。

### Changed

- **4.0 大方向：**默认主线从 results-first 全历史冠军榜切换为 selector 内部因果的
  walk-forward 研究 framework；`causal_walk_forward_dynamic` 只是其中的实验账户，不是
  默认获胜策略。旧 `results-first`、`recovery`、`next` 与 `legacy-regression` 仍可复现，
  但只作为历史诊断。
- 默认研究 suite 改为 `walk-forward`。控制项以及防御价值、低波动、低换手三个候选均
  使用预注册固定 `+1` 方向，不再让完整历史 IC 改写较早时点的方向。
- 研究配置与运行 summary schema 升至 `3`；顶层 evidence class 与 walk-forward 产物
  统一，canary 明确只属于 `engineering_smoke`。
- Walk-forward 只读取 `end_date < signal_date` 的共同完整持有期；756 个交易日回看、
  60 个成熟持有期、距上次更新至少 63 个交易日后的首个该 offset 调仓日更新、成本后
  Sharpe、相对 control `0.10` 的选择保护，以及最多三个合格候选等权合成，均冻结为
  单一协议。
- 默认 Top-10/10 日组合必须完整运行 offset `0..9`，在共同 warmup 日期后汇总 Q20、
  median、worst 和 IQR，不允许选择最佳调仓相位。
- Results-first 曾用全部已观察历史选择方向、组合权重和冠军，现明确标记为旧的样本内
  诊断。运行 `0bfd70e808416ddc` 的 70% 防御价值混合成绩（成本后年化 9.96%、Sharpe
  0.656、IR 0.374、最大回撤 -22.49%）不再作为主线选择证据。
- 全量多组合回测继续使用批量行映射并复用相邻持有期的共同边界行情，保持成交、估值和
  基准计算语义不变。
- 完整 selector-internal-causal 历史模拟 `97840d20b4a2ff71` 覆盖十个 offset，未来选择
  违规为 0、动态实验账户周期覆盖 100%；共同区间内动态账户年化收益 Q20 / median /
  worst 为 8.88% / 9.11% / 6.85%，Sharpe Q20 为 0.561、最大回撤 Q20 为 -18.15%，
  且 10/10 offset 年化均优于 control。
- 固定等权基准的年化 Q20 / median / worst 为 9.50% / 10.22% / 8.84%。逐 offset
  配对的 dynamic − fixed 年化 / Sharpe / IR / 最大回撤 Q20 为 -1.35 个百分点 /
  -0.098 / -0.043 / -2.98 个百分点，动态仅 5/10 offset 年化更高，因此
  `historical_diagnostic_passed=false`。当前结果支持候选篮子，不支持这套轮换逻辑。
- 事后 phase 排名最强的静态防御价值候选 Q20 年化为 11.13%；它和动态账户均不标记为
  可靠性确认、可直接晋级的赢家或独立 OOS。

### Added

- 增加成本后影子候选账户、严格成熟期过滤、确定性 control guard、Top-3 等权动态信号
  组装与十相位 walk-forward 汇总；较长回看与季度更新用于降低短窗 hard switch 噪声。
- 增加同 candidate registry、同日期、同组合执行与成本模型的 `fixed_registry_equal_weight`
  基准；它等权 control 与六个预注册混合策略，并非原始因子等权。动态
  selector 的历史阈值诊断必须同时优于 control 和固定等权基准，避免把静态暴露收益误归
  因于轮换时点。
- 增加 `post_selection_causal_simulation` 证据等级：它表示模拟内部没有使用未来已实现
  收益，但候选和协议是在看过既有历史后设计的，不能声称为独立 OOS 或保证未来盈利。
- 保留 control/challenger 有向截面秩混合及 results-first 全历史排行榜，用于数值回归和
  历史问题复现，而不是当前默认研究结论。

### Known limitations

- `post_selection_causal_simulation` 只保证 selector 的历史收益 cutoff 和固定方向不读取
  当时尚未完成的收益，不代表数据、候选提出和研究协议构成全链路 pristine OOS。
- 财务数据的修订 vintage 尚未完全还原；后续修订可能污染较早决策点可见的数据版本。
- 退市、吸收合并及其 ghost position 处置仍未做完备的逐事件验证。
- 十个 offset 共享同一市场路径且不是独立样本；候选间等 AUM 可比性和历史分段资本重置
  仍有限制。2017–2026 已被反复查看，本次结果只能作为协议冻结时的历史模拟基线。

## [3.0] - 2026-08-27

### Changed

- **破坏性重构：**以本地 Parquet、Python 和单一 CLI 取代旧 WebUI、Docker、
  PostgreSQL、MinIO、Iceberg、Dagster、Hermes 与自治 Agent 主线；旧 Research OS
  保留在 `research-os-final-20260826` 归档 tag。（`c704923`、`b29d120`）
- 删除旧平台主线中的 1,388 个文件，使仓库重新聚焦于数据、研究、组合执行和测试。
  （`b29d120`）
- 研究协议改为：2017–2022 训练并冻结方向，2023–2024 验证，2025+ 单独审计；
  验证和审计均不再伪装为未观察的盲测。（`c704923`、`f3ab16e`）
- Stage A shortlist 只使用训练段；Stage B 使用成本后多头组合、动态流动性 Top-500
  universe、固定资本和明确的成交约束。（`c704923`、`f3ab16e`）
- 区分“通过研究准入”和“实际进入执行”，避免报告把 admission 与 execution
  混为一谈。（`8f57d01`）

### Added

- 增加本 Changelog、发布规范和 `scripts/publish-tag.ps1`；未来正式 tag 必须经过版本、
  clean tree、远端 `main`、GitHub CI、annotated tag 与远端 SHA 一致性检查后再发布。
- 增加可续跑的数据同步、PIT 财务补全、历史月末名称/行业、证券代码迁移与 canonical
  Parquet 审计。（`c704923`、`f3ab16e`）
- 增加主动收益循环区块 Bootstrap、固定多锚点稳健性、基准覆盖率和执行输入 PIT
  覆盖审计。（`f3ab16e`）
- 增加 `recovery` suite；当前只注册一个低相关机制 challenger
  `pit_cashflow_quality`，旧价值与 legacy 因子只用于有限复现，不再自动扩展变体。
  （`f3ab16e`）

### Fixed

- 声明 Stage A Spearman 相关计算实际需要的 SciPy 运行时依赖，修复干净 CI 环境中的
  `ModuleNotFoundError`。
- 将项目元数据与包内版本统一更新为 `3.0.0`，对应 Git tag `3.0`。
- 修复旧 builder 将已经是人民币的成交额与 ADV 再乘 1,000 的单位错误。
  （`f5062e9`）
- 修复 train/validation/audit 边界标签泄漏、Stage A/B 调仓锚点错位、容量裁剪误报
  和断点产物一致性问题。（`f5062e9`）
- 修复用验证集给 shortlist 排序、下日开盘成交读取当日收盘后 ADV/波动率、基准成分
  缺失被静默跳过，以及 20 日配置只看单一锚点的问题。（`f3ab16e`）

### Known limitations

- 当前没有 validated candidate；“0 validated”是允许且应如实保留的研究结论。
- 财务修订 vintage、退市/吸收合并处置、分段相同 AUM 重置、累计试验预算和增量组合
  评价仍需进一步加固。相关运行在问题修复前只应视为历史诊断。

## [research-os-final-20260826] - 2026-08-26

归档版本；它不在当前轻量主线中，也不是正式策略发布。

### Added

- 引入实验性 Research OS：PostgreSQL catalog、MinIO/Iceberg 数据面、Dagster 编排、
  版本化合同、sleeve 生命周期和事件式影子账本。（`50920e7`）
- 加固数据源 admission、凭据证据、主机/容器 attestation、snapshot authority 和
  readiness audit。（`7bbfe89`、`2ff3cc1`、`603cf29`、`35b5fc2`、`4238cfd`、
  `a84053a`）
- 增加绕开完整基础设施的可续跑 Research Lite 历史诊断。（`ea8aeb0`）

### Archived

- 完成 incident authority 后封存 Research OS 源码。（`a4c7ff1`）
- 此 tag 只保存工程与审计架构，不表示存在合格候选、生产就绪或实盘能力。

## [2.1] - 2026-06-24

### Added

- 整理 Small Institutionalization / Paper Portfolio 与 Market Phenomenon 两条实验性
  工作流。（`77decfe`）
- 增加市场现象生成、最小验证、深层 OOS、人工复核包、审批门、paper portfolio 周报
  和 operator-pending 状态一致性工具。（`77decfe`）

### Safety

- queue、automation、production 和 live trading 默认关闭。
- 当时的组合仍受最大回撤与人工审批阻断；现象研究只支持继续调查，不能表述为策略已验证。
- tag 提交 `06880fc` 主要刷新状态时间戳，没有产生新的市场证据。

## [2.0] - 2026-06-05

### Added

- 增加 Autonomous Strategy Lab、Harvest 控制器、机制路线、数据 blocker 和研究浪费报告。
  （`e785042`）
- 将研究动作组织为 evidence → diagnosis → hypothesis → cheap screen → controlled
  experiment → verdict，并增加 stop、request-data、manual-review 与 mechanism-switch
  决策。（`e785042`）

### Research status

- 此版本发布的是自治研究系统实验，不是 autonomous alpha；它不证明任何因子或组合有效。

## [1.3] - 2026-05-22

### Changed

- 将 Agent profile/router 从旧 provider-style/OpenClaw 语义迁移为 Hermes-native。
  （`de25dc2`）
- 增加 Hermes profile bootstrap、词汇审计、设置页及相关测试，并归档旧 Agent/provider
  文档与模板。
- 此版本是 Agent 运行与配置语义迁移，没有新的研究验证主张。

## [1.2] - 2026-05-22

### Added

- 增加 controlled autonomous research loop，以及 plan、gate、execution manifest、
  evidence、verdict 和 next-plan 产物。（`99a6da0`）
- 增加 defensive-quality 模拟修复适配器。

### Safety

- 执行保持 simulation-only、controlled-only；默认不写队列、不启用 timer、不自动晋级。
- 自动循环能够运行不代表候选或组合有效。

## [1.1] - 2026-05-16

### Changed

- 整理 de-OpenClaw 迁移、controlled-only runtime、bucket-aware 诊断、paper/simulation
  报告和风险状态门。（`f846794`、`066de92`）
- 清理大量历史运行产物与诊断文件，并将 factor lessons、watchlist、blacklist 和
  experiment ledger 保留为 legacy research memory。

### Safety

- broad daemon、自动队列和 live trading 均不启用。
- 当时仍没有满足最大回撤门槛的安全候选；paper portfolio 状态不等于通过投资验证。

## [1.0] - 2026-04-28

### Added

- 建立合成数据与 Tushare 因子研究原型。（`51c9e7e`、`b95160d`）
- 增加因子评估、简单时间切分、中性化、组合原型、factor registry、候选/淘汰区、
  SQLite 实验存储和报告。（`951601a`、`938d51e`、`23c5b1d`、`fed01be`、`dbefc78`）
- 增加本地 WebUI、LLM review、研究队列、orchestrator、daemon、opportunity engine 和
  稳健性/晋级实验。（`33fd651`、`f43018c`、`e373830`、`fafc54b`、`80c8ba2`）
- 准备 GitHub 仓库并发布中文 README。（`9ad00cf`、`8005cbf`）

### Prototype limitations

- 初始真实数据只覆盖短窗口和约 20 只股票，组合观测很少，且没有当前版本的成本、PIT、
  非重叠验证和执行约束；早期数值只能视为 smoke-test output。
- 2026-03-19 至 03-22 快速加入的大量自治控制层，主要证明工程流程可以运转，不能视为
  大量相互独立的市场实验。

## Pre-1.0 prototype history - 2026-03-18 to 2026-03-23

- `51c9e7e` 建立首个合成数据 MVP；模拟收益本身由示例因子驱动，只用于管线验证。
- `b95160d` 增加首个 Tushare 工作流；样本短、股票少并存在当前上市股票筛选偏差。
- `4a4aa29` 标记早期 “v1.0 baseline”；从首个 MVP 到该节点约两小时。
- 随后在有效的 PIT、成本和独立 OOS 基础尚未建立前，快速增加 WebUI、LLM、队列、
  daemon、planner 与 opportunity 系统；后续数月的大量工作用于收紧、迁移、诊断并最终
  移除这些实验层。

[Unreleased]: https://github.com/yxforever666gh/factor-lab/commits/main
[6.0]: https://github.com/yxforever666gh/factor-lab/tree/6.0
[5.9]: https://github.com/yxforever666gh/factor-lab/tree/5.9
[5.8]: https://github.com/yxforever666gh/factor-lab/tree/5.8
[5.7]: https://github.com/yxforever666gh/factor-lab/tree/5.7
[5.6]: https://github.com/yxforever666gh/factor-lab/tree/5.6
[5.5]: https://github.com/yxforever666gh/factor-lab/tree/5.5
[5.4]: https://github.com/yxforever666gh/factor-lab/tree/5.4
[5.3]: https://github.com/yxforever666gh/factor-lab/tree/5.3
[5.2]: https://github.com/yxforever666gh/factor-lab/tree/5.2
[5.1]: https://github.com/yxforever666gh/factor-lab/tree/5.1
[5.0]: https://github.com/yxforever666gh/factor-lab/tree/5.0
[4.1]: https://github.com/yxforever666gh/factor-lab/tree/4.1
[4.0]: https://github.com/yxforever666gh/factor-lab/tree/4.0
[3.0]: https://github.com/yxforever666gh/factor-lab/tree/3.0
[research-os-final-20260826]: https://github.com/yxforever666gh/factor-lab/tree/research-os-final-20260826
[2.1]: https://github.com/yxforever666gh/factor-lab/tree/2.1
[2.0]: https://github.com/yxforever666gh/factor-lab/tree/2.0
[1.3]: https://github.com/yxforever666gh/factor-lab/tree/1.3
[1.2]: https://github.com/yxforever666gh/factor-lab/tree/1.2
[1.1]: https://github.com/yxforever666gh/factor-lab/tree/1.1
[1.0]: https://github.com/yxforever666gh/factor-lab/tree/1.0
