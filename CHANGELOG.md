# Changelog

本文件记录 Factor Lab 的软件、数据和研究协议变化。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，但历史条目依据现有 Git
提交与 tag 回补，不把当时的发布说明原样视为当前事实。

> 研究证据说明：历史回测、`pass_candidate`、`ready_for_portfolio_mvp`、工程测试通过或
> 运行系统可用，都不等于投资策略已验证。除非明确说明，市场结果均属于历史诊断或
> legacy evidence，不保证未来收益，也不代表生产或实盘就绪。

## [Unreleased]

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
