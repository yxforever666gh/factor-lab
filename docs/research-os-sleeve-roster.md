# Research OS 初始 Sleeve 名册与相关聚类

`configs/research_os_initial_sleeves.json` 预注册四条机制路线：价值质量、低风险防御、中期趋势、反转/流动性。每条路线都包含可编译的 `factor-dsl/v1`、PIT 字段与 `available_at` 绑定、机制、否证条件、静态机制 `cluster_id`、多头约束和 35% 权重上限。

这份名册不是候选白名单，也不是晋级记录。其状态固定为 `registered_research_only`，`promotion_policy` 固定为 `authoritative_experiment_only`。只有统一试验账本中的权威实验通过全部统计、数据审计、容量和生命周期门槛后，另一个确定性流程才能把 Sleeve 纳入 Champion；生成名册或聚类结果本身没有任何晋级效果。

`factor_lab.research_os.sleeve_registry` 提供两类内容寻址对象：

- `SleeveRosterManifest`：对排序后的完整 Sleeve 合同计算身份，因此输入顺序不改变 `roster_id`，任何 DSL、PIT 字段、机制或约束变化都会改变身份。
- `SleeveClusterManifest`：只接受带来源 ID、来源内容哈希、提取序列哈希和显式 `as_of` 的主动收益；四条 Sleeve 必须在同一个截止时点至少有 60 个共同观测。缺源、哈希不符、重复日期、未来可用时间、样本不足或未定义相关性都会阻断。

聚类算法固定为绝对 Pearson 相关阈值图的连通分量。每簇代表按“预声明优先级 → 可用覆盖率 → Sleeve ID”选择，不读取 Sharpe、验证期收益或其他表现指标。聚类只用于减少同类暴露和重复试验，不提供投资证据。
