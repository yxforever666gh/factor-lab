# Changelog

本文件记录 Factor Lab 的软件、数据和研究协议变化。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，但历史条目依据现有 Git
提交与 tag 回补，不把当时的发布说明原样视为当前事实。

> 研究证据说明：历史回测、`pass_candidate`、`ready_for_portfolio_mvp`、工程测试通过或
> 运行系统可用，都不等于投资策略已验证。除非明确说明，市场结果均属于历史诊断或
> legacy evidence，不保证未来收益，也不代表生产或实盘就绪。

## [Unreleased]

## [4.0] - 2026-08-27

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
[4.0]: https://github.com/yxforever666gh/factor-lab/tree/4.0
[3.0]: https://github.com/yxforever666gh/factor-lab/tree/3.0
[research-os-final-20260826]: https://github.com/yxforever666gh/factor-lab/tree/research-os-final-20260826
[2.1]: https://github.com/yxforever666gh/factor-lab/tree/2.1
[2.0]: https://github.com/yxforever666gh/factor-lab/tree/2.0
[1.3]: https://github.com/yxforever666gh/factor-lab/tree/1.3
[1.2]: https://github.com/yxforever666gh/factor-lab/tree/1.2
[1.1]: https://github.com/yxforever666gh/factor-lab/tree/1.1
[1.0]: https://github.com/yxforever666gh/factor-lab/tree/1.0
