# Factor Lab Research OS 架构与运行手册

本文描述当前 `factor_lab.research_os` 主干的事实来源、研究合同、统计治理、组合生命周期、影子账本、WebUI、CLI 和本地部署方式。这里的“完成”指工程能力可测试、可审计、可重演，不代表策略已经盈利，也不代表恢复机制已经通过真实未来市场验证。

## 1. 目标与边界

Research OS 的目标是持续执行以下闭环：

```text
发现 → 预注册 → 证伪/验证 → 影子观察 → 激活
                                  │
                                  ▼
                      降权 → 休眠 → 复活观察
```

系统只覆盖 A 股历史研究、影子组合和自动恢复演练，明确不包含：

- 券商连接、实盘下单、真实资金切换或订单路由 API；
- 保证市场中始终存在可盈利因子；
- 通过不断增加变体、降低门槛或重看 holdout 来制造漂亮回测；
- 让 LLM 直接决定统计结论、晋级或资金暴露。

截至当前版本，全部历史数据都已经被观察过，只能视为训练、诊断或 pseudo-OOS。当前旧扩样本快照带有 `st_history_unverified` 阻断项：历史 ST 数据为空或未经可信验证，因此旧结果只可作为交易引擎数值回归，不可作为投资证据或候选晋级依据。

## 2. 总体数据流

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Tushare / AkShare / LocalFile SourceAdapter                         │
└──────────────────────────────┬───────────────────────────────────────┘
                               ▼
              Bronze 原始响应 + 请求/版本/哈希 lineage
                               ▼
       Silver 双时态标准化 + point-in-time 截取 + 多源逐字段对账
                               ▼
             fail-closed 数据质量门（accepted/disputed/quarantined）
                               ▼
               Gold Iceberg snapshot + 不可变 named tag
                               │
             ┌─────────────────┴──────────────────┐
             ▼                                    ▼
  ExperimentSpec + DSL                    事件式影子组合
             ▼                                    │
 nested walk-forward + trial ledger               │
             ▼                                    │
         Sleeve / Champion ← 健康与状态机 ────────┘
             │
             ▼
       PostgreSQL 权威 catalog → WebUI read-model / Dagster
```

存储职责并不混用：

- **PostgreSQL 16** 保存实验身份、权威结果、累计试验账本、生命周期事件、恢复案例、运行状态、legacy evidence 和影子账户投影。
- **Alembic** 管理 `ros_*` schema。应用启动不应靠隐式建表替代正式迁移。
- **MinIO** 保存内容寻址的 Bronze 原始 Parquet、lineage metadata、Silver Parquet/对账审计及快照 manifest；**PyIceberg** 使用 PostgreSQL SQL catalog，在同一 MinIO warehouse 中发布 Gold 表、snapshot 与 tag。
- **Parquet** 是当前本机数据交换格式；**Polars、DuckDB** 已纳入 Research OS 依赖和 `doctor` 检查，供单机查询服务使用，不替代 PostgreSQL 中的运行事实。当前 CLI 评估入口仍接收 Pandas frame。
- `FACTOR_LAB_LAKE_ROOT` 只是 DuckDB/Polars 与处理中间步骤使用的 worker 本地缓存。CLI 与 Dagster 都会按 SHA-256 immutable key 将 Bronze/Silver 数据及 manifest 归档到 MinIO，并在上传后重新读取校验；同一 key 出现不同字节会 fail-closed。Gold 权威发布还必须通过 Iceberg commit 和 immutable tag，单独存在的本地缓存或 JSON manifest 不等于 Gold 表发布。

Dagster 的应用动作通过 `ResearchOSServices` 注入。仓库内提供任务、调度、传感器、服务协议，以及覆盖 15 个编排操作的 `factor_lab.research_os.application_services:create_services`。默认工厂还要求 `FACTOR_LAB_ORCHESTRATION_CONFIG` 指向 schema 为 `research-os/application-services/v1` 的显式 JSON；可从[安全配置模板](../configs/research_os_orchestration.example.json)开始填写。配置或必需输入缺失时任务会明确失败或合法跳过，不会制造成功证据。也可以替换成其他实现同一协议的工厂。

每个 Dagster 应用操作先在 `ros_runs` 以输入 fingerprint 原子认领，再把终态、摘要和结构化输出更新回同一记录。下游依赖和幂等重试只读取 PostgreSQL catalog；本地 `*.result.json` 不再是运行事实或依赖依据。本地 JSON/Parquet 仍可作为不可变数据产物，但其权威身份必须由 catalog run、snapshot 或 Iceberg commit 引用。WebUI 的运行列表直接投影这些 `ros_runs` 摘要。

## 3. 数据完整性与不可变快照

### 3.1 SourceAdapter 与字段合同

`TushareSourceAdapter`、`AkShareSourceAdapter` 和 `LocalFileSourceAdapter` 共享以下边界：

- 真实连接探测与健康状态；
- 每个数据集的 key、字段类型、单位、复权方式和发布时间合同；
- 数据源优先级、endpoint 映射、速率限制和供应商版本；
- 请求参数、响应 lineage、摄取时间和内容哈希。

适配器不会在一个源失败时静默切换到另一个源。并行来源进入对账层，由确定性政策产生 `accepted`、`disputed` 或 `quarantined` 结果。

正式 orchestration 配置通过 `profile_name` 把每个 source config 绑定到 WebUI 保存的同名、已启用 profile；Tushare profile 提供内存中的 Token，local profile 可覆盖受限根目录，AkShare profile提供启停和审计身份。profile 缺失、禁用或类型不符都会 fail-closed。未声明 `profile_name` 的 inline canary/test spec 保持自包含，不会被 worker 进程中不相关的 profile 状态改写。Token 不进入 Bronze lineage，lineage 只记录 profile 名称和类型。

AkShare 的原始中文列会原样保留在 Bronze；只有配置中 `response_field_mapping` 明示的列才会额外生成合同字段别名，常量证券标识也必须通过 `constant_fields` 明示。当前示例只用 AkShare 对单只证券的 OHLC 做小范围交叉核对，刻意不比较成交量和成交额，因为供应商单位不同且未声明换算。连接探测同样固定为单证券、两个交易日，不调用全市场 spot 接口。

正式 Gold 所需九类数据集在[orchestration 示例](../configs/research_os_orchestration.example.json)中都有 SourceAdapter 合同。`trade_status` 按交易日读取；`stock_basic`、`historical_st`、PIT 行业与公司行动读取 `data/reference/pit` 下的不可伪造历史文件，且每行必须携带 `available_at`。文件或根目录不存在、历史 ST 为空、L/P/D 证据不足时会在 Bronze/Gold 边界阻断。WebUI 的 local-file 测试只读取所填根目录内的一个相对文件，不能用 `..` 或绝对测试路径逃逸该根。

### 3.2 双时态与 PIT

Silver 规范化至少保存：

- `event_time`：经济事件或行情发生时间；
- `available_at`：当时最早可被研究者看到的时间；
- `ingested_at`：系统实际接收该版本的时间；
- `vendor_revision`：供应商版本；
- 来源、优先级、单位、复权方法和 lineage。

规范化要求 `event_time <= available_at <= ingested_at`。研究截面同时应用 decision cutoff 和 system cutoff，因此供应商日后回填不会改写旧快照，也不会进入过去的决策。

### 3.3 Fail-closed 质量门

以下情况会阻止 Gold 发布或候选晋级：

- 必需数据集或开市日为空；
- 历史 ST 不可用、降级、空表或缺少区间列；
- 必需列缺失、key 为空或重复；
- 中文名称出现乱码或替换字符；
- 分区日期不完整、checkpoint/path/size 不一致；
- 文件哈希或 snapshot manifest 校验失败；
- 多源值、单位或复权方法无法在容差内对齐；
- `available_at` 晚于决策 cutoff，或时间顺序非法。

Gold manifest 只有在质量报告为 `pass` 时才能创建。Silver/Gold 必须引用父快照，避免失去 lineage。

### 3.4 Research-ready Gold panel

`factor_lab.research_os.gold_panel` 把 catalog 中全部经验证 Silver parquet 组装为唯一的 `ticker/trade_date` 宽面板。正式配置固定从 2017-01-01 开始，并要求 `daily`、`daily_basic`、`adj_factor`、交易状态、交易日历、L/P/D 股票基础、历史 ST、PIT 行业和公司行动九类数据齐备；缺任一数据集、开市日分区、Bronze 祖先或 manifest 文件都会阻断发布。

面板提供非未来依赖的 `open_adj/close_adj`、20 日 ADV、20 日波动率、交易/涨跌停状态、公司行动、PIT 行业和规模；月度 Universe 只使用上月末之前 60 个交易日的流动性，要求上市 180 日并严格排除当时 ST，输出 `eligible`、`universe_member` 和等权基准。`LabelSpec` 的次日开盘至第六日开盘收益带有独立 `label_available_at` 和 `label_is_research_only=true`，只能进入研究评估，不能进入 shadow 执行。

Gold manifest 包含 panel、月度成员、审计、build spec、当前 DQ 报告以及完整 Silver 父集合；每个 Silver 父又必须能在 PostgreSQL catalog 中闭包到已接受 Bronze。供应商回填按 Gold freeze 时点选择最新 `ingested_at`，同时间冲突版本会冻结；新版本生成新的 Gold snapshot，不修改旧 tag。Iceberg 主分支对全历史 panel 使用原子 overwrite，防止同一 partition/revision 追加重复行，而所有旧 snapshot/tag 保留时间旅行能力。

### 3.5 内容寻址

快照 ID 由父快照、配置、代码提交、tracked dirty patch、未跟踪且未被 Git 忽略的文件哈希、依赖锁、数据文件哈希和质量信息共同决定。发布后：

- 相同内容重试是幂等的；
- 同一 ID 对应不同内容会被拒绝；
- Gold Iceberg tag 已指向其他 snapshot 时会被拒绝；
- Git 忽略的 secrets 与运行产物不会进入 dirty patch，也不会被意外公开。

## 4. 唯一研究合同与评估器

所有公开合同使用 `research-os/v1`，核心对象包括：

- `DataSnapshotRef`
- `UniverseSpec`
- `LabelSpec`
- `FeatureSpec`
- `FactorSpec`
- `SleeveSpec`
- `PortfolioPolicy`
- `ValidationProtocol`
- `ExperimentSpec`
- `RecoveryCase`

Pydantic 合同是冻结且 `extra="forbid"` 的：拼错参数不能被静默忽略。实验 fingerprint 固定包含：

```text
snapshot + universe + label + features + factor/sleeve
+ portfolio + validation + evaluator + environment + preregistration
```

相同 fingerprint 只能登记为一个实验，catalog 对每个实验最多保留一个 authoritative result；唯一约束负责并发冲突检测。`RunCoordinator` 另行保存每次操作的输入 fingerprint 和状态，用于审计而不是替代实验唯一约束。新实验的 evaluator allow-list 当前只有 `research_os.long_only.v2`，并要求 `environment.evaluator_build` 完全一致。旧评估器结果只能导入 `legacy_evidence`，不能注册成新候选。

研究合同、cycle 与唯一 evaluator 共同硬校验以下核心执行口径：

- 2017 至今、月度 PIT 流动性前 500；
- 5000 万元资金、每 5 个交易日调仓；
- 目标 50 只、允许范围 50–100 只、单股不超过 2%；
- ADV 参与率不超过 5%，不允许负权重；
- 合格 500 只等权组合为主基准；
- 约束买入留现金，约束卖出继续持仓。

`PortfolioPolicy` 还声明行业/规模主动偏离各不超过 5 个百分点、beta 0.9–1.1 和 Ledoit-Wolf 协方差。`CanonicalLongOnlyEvaluator` 会在每个真实调仓信号日，以当时可见的 exposure、历史收益和双时态基准调用 `optimize_stock_weights`，并把不等权目标交给同一个成交/会计引擎。优化审计随 period 和权威结果保存；缺可信 PIT 行业、少于 60 期历史、非双时态基准、求解失败或任一约束失败时，等权诊断仍可运行，但 `promotion_eligible=false`，cycle 会把 blocker 传入确定性晋级门。

## 5. DSL、沙箱和统计治理

### 5.1 因子 DSL

类型化 DAG 支持 field、constant、lag、rolling、rank、winsorize、标准化、中性化、二元运算和条件组合。编译器根据字段角色、可用时间和决策点检查依赖，拒绝未来字段、forward label 和不满足 lag 的盘后数据。

DSL 无法表达的机制可以作为 Docker 沙箱插件运行。沙箱只接收一个只读 Arrow 输入，禁止网络、禁止仓库挂载、根文件系统只读，并限制 CPU、内存、进程数、运行时间和输出体积。插件输出仍必须通过 host 端 schema/PIT 检查后才能进入研究快照。

LLM 提案只生成结构化假设、DSL 或诊断建议。确定性代码负责数据门、统计检验、晋级决定和生命周期迁移。

### 5.2 Nested walk-forward

默认验证协议为 expanding nested walk-forward：

- 初始训练：2017–2020；
- outer test：2021、2022、2023、2024、2025；
- 2026 已观察部分：只作诊断；
- 5 日标签使用 6 个交易日 purge 和 5 个交易日 embargo；
- 拼接 outer-OOS 后才计算最终晋级指标。

### 5.3 Trial ledger 与统计预算

每项实验先登记经济机制、预期方向/状态、否证条件、允许变体、停止规则和统计预算。成功、失败、缺数据、反向测试、手工测试和拒绝记录全部进入 lifetime ledger。

默认预算为每月最多 3 个确认性 Challenger、每个 Family 每月最多 1 个、失败后最多 2 个预注册诊断分支。Family 级使用依赖修正的 online alpha，Family 内使用 Holm；结果同时提供时间块 bootstrap 和 Deflated Sharpe。失败不会自动放宽门槛。

默认晋级条件全部满足才通过：

- 拼接 outer-OOS 成本后超额年化严格为正；
- 净 Sharpe ≥ 0.8，主动 Information Ratio ≥ 0.5；
- 最大回撤绝对值 ≤ 25%；
- 至少 60% 半年窗口超额为正；
- 至少 3 个 outer 年度为正；
- 容量违规为 0；
- 数据质量无阻断项且 lifetime 统计预算通过。

## 6. Sleeve、Champion 与状态机

Sleeve 是机制级研究和生产观察单位。当前 Champion builder 接收已经带 `cluster_id` 的合格 Sleeve，再按簇均衡构建静态 Champion；相关性聚类和代表选择必须作为可审计的上游输入，builder 本身不会猜测 cluster。动态 Challenger 的约束是：

- 至少 75% 静态核心；
- 状态自适应倾斜最多 25%；
- 单 Sleeve 上限 35%；
- 相对上月的单 Sleeve 变化最多 5 个百分点；
- 若历史拼接 outer-OOS 和至少 60 个新影子交易日不能同时优于静态 Champion，则保留静态 Champion。

市场状态首版使用强收缩 ridge，只使用当时可见的趋势、宽度、波动、横截面离散度、流动性、风格主动收益、Sleeve 相关性和拥挤度，不使用 HMM 或深度模型。

生命周期为：

```text
proposed → preregistered → canary → walk_forward → shadow
→ active → reduced → dormant → probation → active
                                      └→ retired
data failure → frozen_data
```

自动迁移规则：

- 周检出现 `13 周 IR<0`、`26 周 IC 方向错误`、`成本>基线 1.5 倍`、`主动回撤超过训练期 95% 阈值` 等告警；连续两次周检至少命中两项，`active → reduced`，权重减半。
- `reduced` 满四周且 26 周 IR<0，进入 `dormant`；无告警且 IR 恢复时可以回到 `active`。
- 数据质量失败立即进入 `frozen_data`，有效权重归零并转为 100% 现金；数据重新验证后先进入 `dormant`，不会直接恢复风险。
- `dormant` 至少积累 60 个全新交易日，20/60 日主动收益均为正、IC 方向恢复且无告警，才进入 `probation`。
- `probation` 每次最多恢复 5 个百分点；四次干净周检且 13/26 周 IR 非负后才能回到 `active`，反转则重新休眠。

组合降险时，单 Sleeve 释放的权重转入 PIT 等权基准；没有健康 Sleeve 时为 50% 基准 + 50% 现金；数据故障时为 100% 现金。

恢复 SLA 使用 5/20/60 个新交易日：5 日内登记漂移，20 日内形成诊断和最多 3 个 Challenger，Challenger 至少经历 60 个新交易日影子观察后，才有资格被称为“研究恢复”。

周检 JSON 只提供带 snapshot ID 的原始测量，收到后立即作为 append-only `health_measurement_recorded` 事件持久化。连续告警次数、`reduced/probation` 周数和当前状态从上一次 catalog monitor event 重建；配置中同名计数会被忽略。`dormant → probation` 的 60 日资格以及恢复 SLA 的交易日年龄，只计算关联影子账户中已持久化的交易日事件；外部 `new_sessions_since_dormant`、`trading_sessions` 或 `as_of` 值不能授予资格。旧 JSON 只允许在 catalog 尚无事件时迁移 sleeve 的身份、初始状态和权重，运行时计数与休眠日期不会被继承。

## 7. 事件式影子账本

影子引擎不读取 `forward_return_5d` 或任何 forward/label 列：

1. 决策日收盘后登记 target、snapshot ID 和 model version；
2. 后续交易日按当时 `open_adj`、停牌、退市、涨跌停和 ADV 状态成交；
3. 卖出先于买入，账户不隐式借款；
4. 记录交易费、日期化印花税、冲击、分红送转、阻塞订单、fill 和收盘 mark-to-market；
5. 更新现金、持仓、账户 NAV 和基准 NAV。

PostgreSQL 使用以下权威结构：

- `ros_shadow_accounts`：账户头部与最新投影；
- `ros_shadow_events`：按 sequence 和 `previous_hash` 串联的不可变事件；
- `ros_shadow_positions`：由事件事务更新的持仓 read-model。

一次 shadow step 会把全部领域事件、持仓投影和账户投影在同一事务中原子提交，并校验 expected previous hash。部分 fill 不会在投影更新前单独落库。每次提交后验证哈希链以及 `cash + positions = NAV`。

这些订单和 fill 仅是研究账本事件，不会发送给任何券商。

## 8. Dagster 编排

所有时间使用 `Asia/Shanghai`，run key 包含 cycle 和 partition key，避免重复分区产生多份权威结果。

| 周期 | 默认时间 | 任务 |
| --- | --- | --- |
| 日 | 工作日 18:30 | Source sync → reconciliation → quality gate → Gold Iceberg snapshot → shadow NAV。 |
| 周 | 周五 20:00 | Sleeve health → drift detection → lifecycle transition → recovery SLA。 |
| 月 | 每月 1 日 20:00 | Confirmatory budget gate → limited discovery → weight re-estimation → Challenger generation。 |
| 季 | 1/4/7/10 月 1 日 10:00 | Validation protocol audit → lifetime research budget audit；不自动修改门槛。 |

交易分区 sensor 默认关闭，以免与日调度重复；恢复 SLA sensor 默认开启。Dagster 只负责边缘编排，业务动作必须通过 `ResearchOSServices`，且 allow-list 中没有 broker/live-order 操作。

## 9. WebUI read-model

五项主导航为：

| 页面 | 路径 | 权威内容 |
| --- | --- | --- |
| 总览 | `/` | Champion、风险/降险状态、Gold 数据健康、恢复 SLA、最近运行。 |
| 研究 | `/research` | 预注册实验、Family、试验预算和否证信息。 |
| 组合 | `/portfolios` | 已发布 Champion、Sleeve 状态/权重、影子 NAV 与基准 NAV。 |
| 运行 | `/runs` | 数据、快照、研究、监控和 shadow run。 |
| 设置 | `/data-sources` | 数据源能力、连接状态与模型设置入口。 |

活跃页面是严格的 PostgreSQL 只读边界：

- 不初始化或修改 schema；
- 不扫描 `artifacts/*/task_state.json` 拼状态；
- 不把 SQLite 当作生产 fallback；
- PostgreSQL 不可达时快速显示 unavailable，而不是展示旧结果冒充当前状态；
- 首页 read-model 使用短时缓存，查询 snapshots、experiments、runs、lifecycle events 和 recovery cases 的受限列表及轻量 summary。

SQLite read-model 仅能在测试代码显式 `allow_sqlite=True` 时启用。

## 10. CLI

入口由 `factor-lab` 提供：

```text
factor-lab data sync
factor-lab snapshot publish
factor-lab research cycle
factor-lab monitor tick
factor-lab shadow step
factor-lab legacy import
factor-lab doctor
```

常用签名：

```powershell
factor-lab data sync --spec source.json [--lake-root PATH]
factor-lab snapshot publish --spec snapshot.json
factor-lab research cycle --experiment experiment.json --data gold.parquet `
  [--fields fields.json] [--negative-controls controls.json] `
  [--sleeve-signal COLUMN] [--bootstrap-resamples 2000] [--seed 0]
factor-lab monitor tick --input monitor.json
factor-lab shadow step --input shadow-step.json
factor-lab legacy import --root artifacts [--seal | --no-seal]
factor-lab doctor [--no-network]
```

除 `doctor` 外，操作命令通过 `RunCoordinator` 建立带输入 fingerprint 的审计 run 记录；实验与快照自身的幂等性由内容 ID 和 catalog 唯一约束保证。`--database-url` 是全局参数，必须放在子命令之前，例如：

```powershell
factor-lab --database-url "postgresql+psycopg://..." doctor
```

## 11. 本地 Compose、迁移与诊断

安装依赖：

```powershell
python -m pip install -e ".[research-os,dev]"
Copy-Item infra/research_os/.env.example infra/research_os/.env
```

编辑 `.env`，至少替换 PostgreSQL 和 MinIO 密码。默认 factory 已随仓库提供，但必须把 `FACTOR_LAB_ORCHESTRATION_CONFIG` 指向根据[安全配置模板](../configs/research_os_orchestration.example.json)填写、且容器内可见的配置；示例中的缺失输入不会被当成成功证据。若只想查看基础设施 UI，可以清空 `FACTOR_LAB_ORCHESTRATION_FACTORY`，不要误以为调度已经完成业务配置。

启动：

```powershell
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml up --build -d
```

Compose 服务包括：

- `postgres`
- `minio` / `minio-init`
- `catalog-migrate`
- `dagster-webserver`
- `dagster-daemon`

`catalog-migrate` 等待 PostgreSQL healthcheck 后执行 Alembic `upgrade head`。手工迁移/核对时，先在宿主设置与 Compose 一致的 URL：

```powershell
$env:RESEARCH_OS_DATABASE_URL = "postgresql+psycopg://factor_lab:your-password@127.0.0.1:5433/factor_lab"
$env:FACTOR_LAB_DATABASE_URL = $env:RESEARCH_OS_DATABASE_URL

python -m alembic -c infra/research_os/alembic.ini upgrade head
python -m alembic -c infra/research_os/alembic.ini current
python -m alembic -c infra/research_os/alembic.ini check
factor-lab doctor
```

`doctor` 检查 Pydantic、SQLAlchemy、psycopg、Dagster、PyIceberg、Polars、DuckDB、PostgreSQL、MinIO 和本地路径。`--no-network` 只跳过网络探测，不会把缺失依赖标记为可用。

查看和停止：

```powershell
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml ps
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml logs dagster-daemon
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml down
```

普通 `down` 保留 named volumes。本文不提供删除 volumes 的快捷命令，因为它会删除权威本地 catalog 和对象存储。

## 12. 测试与验收解释

### 12.1 架构冻结与前瞻证据权限

`ros_evidence_epochs` 只允许一个 Research OS 架构冻结记录。冻结事实包含版本、数据库时间以及 code/config/dependency/dirty-patch 哈希；首次真正前瞻交易日初始为 pending。它只能通过一个已登记、accepted Gold snapshot 内的内容寻址交易日历单向激活，而且必须是冻结日后的第一个日历 session，不能按工作日猜测。命令入口为 `factor-lab research freeze-epoch`，可先冻结、后在可信日历就绪时补充 `--calendar-snapshot-id` 与 `--first-forward-session`。

历史月度确认统一写入 `historical-observed-pseudo-oos-v1`，EvidenceClass 固定为 `pseudo_oos`；请求中的 `registered_at`、`evidence_class`、`holdout_id` 会被拒绝，登记时间来自数据库时钟。没有已激活 epoch 时，catalog 不授予 `pristine_forward`。

动态 Challenger 不读取四个本地 return 文件。历史 Challenger/Champion 必须绑定两个 catalog authoritative completed experiment result 及其 result hash；前瞻比较必须绑定两个独立 shadow account 的完整事件链、至少 60 个严格对齐且全部在 epoch 首日之后的 `account_projected` session。adaptive approval 同时固化 experiment/result、account chain tips、session range、epoch/window 和 adaptive-score 哈希；任一来源继续变化后，旧 approval 自动失效并保持静态 Champion。默认 `monthly.weights.input_mode=authoritative_pg` 不读取状态/收益文件，也不生成自适应分数；仅显式 test/legacy_import 可用文件输入。

```powershell
# Compose 语义检查，不要求容器已启动
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml config --quiet

# Research OS
python -m pytest tests -k research_os -q

# WebUI PostgreSQL read-model 与路由
python -m pytest tests/test_webui_research_os_read_model.py tests/test_webui_routes.py -q

# 全量回归
python -m pytest -q
```

迁移测试应同时覆盖 PostgreSQL 方言离线 SQL、SQLite 测试 fallback 和连接真实 PostgreSQL 后的 `upgrade head`/`alembic check`。Docker daemon 未启动时，只能完成 Compose config 与离线测试，不能报告 live stack smoke 成功。

`.github/workflows/research-os-ci.yml` 固定在 Windows/Linux 两个平台执行完整测试与 `compileall`，并在独立 Linux job 中启动真实 Compose 栈，核对 Alembic head/check、PostgreSQL、MinIO、Dagster definition 和 daemon 存活状态。CI 使用隔离的测试凭据，不连接任何正式数据源。

工程验收允许“没有候选通过”：只要系统正确冻结坏数据、记录失败、遵守统计预算、降险并停止无效搜索，就符合架构目标。市场层面的恢复能力只有在正式版本冻结后的下一完整交易日起，累计至少 60 个从未用于设计的新交易日，并通过影子比较后才能开始讨论；即便如此，也不能宣称未来盈利。

## 13. Legacy evidence

旧 SQLite、`task_state.json`、结果 JSON、历史 ST 文件和扩样本产物通过 `factor-lab legacy import` 一次性登记：

- 记录 `source_uri`、内容 hash、trust label、reasons 和 imported time；
- 相同内容重复导入幂等；
- 旧 SQLite 默认尝试移除写位；兼容读取仍必须使用只读/immutable 方式，不能依赖 Windows `chmod` 作为唯一保护；
- legacy evidence 不生成新 `ExperimentSpec`，不进入当前实验或 Champion 状态；WebUI 最多展示其审计计数；
- `st_history_unverified` 和旧执行口径明确保留为阻断/回归标签，不会因导入而升级可信等级。

因此，旧 SQLite/JSON 是审计材料，不是 Research OS 的第二事实源。

## 相关文件

- [Research OS 合同](../src/factor_lab/research_os/contracts.py)
- [权威 catalog](../src/factor_lab/research_os/catalog.py)
- [Dagster definitions](../src/factor_lab/research_os/dagster_defs.py)
- [Dagster 应用服务](../src/factor_lab/research_os/application_services.py)
- [编排安全配置模板](../configs/research_os_orchestration.example.json)
- [影子账本](../src/factor_lab/research_os/shadow.py)
- [WebUI read-model](../src/factor_lab/webui/services/research_os_read_model.py)
- [本地基础设施手册](../infra/research_os/README.md)
- [运行产物策略](artifact-policy.md)
