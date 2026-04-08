# Session: 2026-04-08 17:34:06 UTC

- **Session Key**: agent:main:main
- **Session ID**: baf522f3-b193-4de9-af9d-e8ce2cd07b2e
- **Source**: webchat

## Conversation Summary

assistant: 根因已经更具体了：`rolling_recent_45d/90d/120d` 这条不是 RSS，而是配置里的某些 factor 表达式直接引用了数据集里不存在的字段 `momentum_20`。我先把这批“确定性坏配置”筛出来，不让它们继续进队列拖 CPU。
assistant: 我找到更像根因的方向了：不是 `momentum_20` 真的不存在，而是 workflow 里某一步很可能把完整数据框裁窄了，导致 `apply_factor()` 评估时只看到少数列，最后把 `momentum_20` 当成未知字段。我直接查 `workflow.py` 里 factor 计算调用链。
assistant: 我再做最后一个定点确认：直接看 `rolling_recent_45d` 载入后的 `dataset.frame` 到底有没有 `momentum_20`。如果有，那就是 factor 解析/替换链的问题；如果没有，就是数据裁切链的问题。
assistant: 根因抓到了：`rolling_recent_45d` 这条不是表达式映射问题，是真实切出来的 `dataset.frame` 为空，而且空到连基础列都没有，所以后面任何因子表达式都会报 `Unknown field ...`。

也就是说当前这条近期验证其实是“无数据任务”，应该在前置阶段就被判死，不该进入重 workflow 和自动重试。
assistant: 我继续往前一步：这类 `rolling_recent_*` 任务如果切出来的数据集本身是空的，就应该在 workflow 一开始直接被判成 deterministic，不该再消耗 worker、重试、拖 CPU。顺手我也查一下 feature store 覆盖范围，看看是不是日期窗本来就超出了缓存。
assistant: 还有一个更关键的发现：我刚查了 feature store，`tushare_top_60` 和 `tushare_top_40` 现在是空的，而我为了降负载刚把轻量验证默认 universe 缩到 `60/40`，结果等于把任务引导到了“空数据宇宙”上。  
这就是最近 `rolling_recent_45d` 反而开始报 deterministic 的根因之一。  
我现在把轻量 universe 直接改到 `20`，并补一个“空数据集前置失败”的明确判定。
assistant: 我把测试里的默认预期也顺手校一下，然后再把 live 的 `rolling_recent_*` 重生成一遍。
assistant: 我现在重建这三份 recent config 并重启 daemon，让它吃到 `universe=20` 的轻量口径。
assistant: [[reply_to_current]] 查到了，而且我已经顺手继续修了一轮。

这次根因分成两层：

- 之前那条 `generated candidate revalidation` 坏链，我已经切掉了
- 现在冒出来的下一层问题，是 `rolling_recent_45d/90d/120d` 这条近期验证链

我查到的关键事实：

- 这条近期验证之前不只是 RSS 问题，还混了一个更底层的问题：
  - 切出来的 dataset 为空
  - 然后 workflow 继续往下跑，最后在 `apply_factor()` 里报
  - `Unknown field in expression: momentum_20`
- 我进一步查了 feature store，发现：
  - `tushare_top_60` 和 `tushare_top_40` 现在是空的
  - 而我前面为了减负载，把轻量验证默认缩到了 `60/40`
  - 这等于把任务引导到了“空数据宇宙”上

我已经做掉的修复：

- `src/factor_lab/research_expansion.py`
  - `rolling_recent_45d/90d/120d` 现在统一改成 light 配置
  - `universe_limit` 从之前那套重口径改成了 `20`
  - `rolling window/step` 改成 `31/10`
  - `validation_mode = light_recent_window`
- `src/factor_lab/workflow.py`
  - 加了前置判定：如果 `dataset.frame` 为空，直接抛明确错误
  - 不再让它空着往下跑到 `apply_factor()` 才炸
- `src/factor_lab/research_queue.py`
  - 把 `dataset slice empty:` 加进 deterministic failure 分类
  - 这样这类任务以后会被快速隔离，不会反复重试拖 CPU

我还做了 live 动作：

- 重新生成了
  - `artifacts/generated_configs/rolling_recent_45d.json`
  - `artifacts/generated_configs/rolling_recent_90d.json`
  - `artifacts/generated_configs/rolling_recent_120d.json`
- 现在这三份已经确认是：
  - `universe_limit = 20`
  - `window_size = 31`
  - `step_size = 10`
  - `validation_mode = light_recent_window`
- 然后我把 daemon 重新拉起来了

现在可以明确说：

- 这次“查”不只是查出问题
- 我已经把当前暴露出的第二层执行问题也处理掉了
- 下一步就看这版 `recent_45d｜light｜universe=20` 能不能稳定跑完，不再掉回空数据 / deterministic / RSS 链路

如果你要最短结论：

- `查到了`
- `新问题是 recent validation 走到了空数据宇宙`
- `我已经把它改成 light + universe=20 + 空数据前置失败`
- `daemon 也已经按新逻辑重启运行`
user: System: [2026-04-09 00:53:28 GMT+8] Exec failed (nimble-c, signal SIGKILL) :: :143: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns w…
System: [2026-04-09 00:56:28 GMT+8] Exec completed (nova-mis, code 0) :: ecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries befor…

Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK.
When reading HEARTBEAT.md, use workspace file /home/admin/.openclaw/workspace/HEARTBEAT.md (exact case). Do not read docs/heartbeat.md.
Current time: Thursday, April 9th, 2026 - 00:59 (Asia/Shanghai) / 2026-04-08 16:59 UTC
assistant: [[reply_to_current]] 有一条需要注意：我刚把 `rolling_recent_45d/90d/120d` 这批近期验证配置重新生成为 `light` 口径（`universe_limit=20`、`window_size=31`、`step=10`），并且已经重启 daemon；当前主链还在继续跑。期间有一个诊断 probe 被 `SIGKILL`，但不影响这轮修复结论。接下来最关键的是盯 `rolling_recent_45d｜light` 能不能稳定收尾；如果它还挂，问题就会继续落在 deterministic/worker lifecycle 这条线上。
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Thu 2026-04-09 01:24 GMT+8] 如何解决？
assistant: [[reply_to_current]] 现在这个问题的解法，核心不是“继续堆优化”，而是把这几条错误链彻底截断：

1. `坏链一：generated candidate revalidation`
- 问题：它会反复派生 `45d/90d` 重验证，之前会并发把内存顶爆，后面又会拖 CPU 和队列
- 解法：
  - 默认关闭这条自动 revalidation
  - 如果以后要开，只允许 `45d light -> 过线再 90d`
  - 串行执行，不再并发

2. `坏链二：recent validation 用到了空 universe`
- 问题：我刚查到 `tushare_top_60` / `tushare_top_40` feature store 其实是空的，导致 `rolling_recent_45d` 切出来的数据集为空，后面才在 `apply_factor()` 里报 `Unknown field in expression: momentum_20`
- 解法：
  - 轻量验证默认 universe 改成 `20`
  - 不再走空的 `40/60`
  - 并且在 workflow 刚加载完数据时就检查 `dataset.frame.empty`
  - 如果为空，直接判 deterministic fail，不进入重跑/重试链

3. `坏链三：maintenance failure 误开 circuit`
- 问题：像 `stale_running_task_cleaned` 这种维修性失败，不应该和真实研究失败混在一起
- 解法：
  - 从 `consecutive_failures` 统计里排除
  - 不再因为这种维修型失败把 orchestrator 打进 `circuit_open`

4. `坏链四：recent 45d 本身太重`
- 问题：就算不空，也可能太重
- 解法：
  - 已经把 `rolling_recent_45d/90d/120d` 全部切成轻量配置：
    - `universe_limit=20`
    - `window_size=31`
    - `step=10`
    - `validation_mode=light_recent_window`

所以如果你问“到底怎么解决”，答案就是这四件事：

- `切掉 generated revalidation 坏循环`
- `避免 recent validation 落到空 universe`
- `把空数据前置判死，不再拖到 worker 后面炸`
- `把 recent validation 轻量化`

而且不是只说方案，我已经在做：

- generated revalidation 默认已关
- `rolling_recent_45d/90d/120d` 已重生成到 `universe=20`
- workflow 已加空数据前置失败
- daemon 已经按新配置重启

现在剩下的就是验证结果：

- 如果这版 `rolling_recent_45d｜light｜universe=20` 能稳定跑完，说明方向对了
- 如果它还挂，我下一步就不再调参数，而是直接修 `feature_store / data slicing` 那一层

一句话版本：

- `解决方法就是：先让任务“别跑错、别跑空、别跑重”，再谈效率`
- `我现在已经把这条路往这个方向改了`
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Thu 2026-04-09 01:30 GMT+8] 给我计划书。
assistant: [[reply_to_current]] 可以，给你一份直接执行的计划书，目标就是把“CPU 掉下去、任务链不稳、研究效率不高”这件事系统修完。

**修复目标**
- 把当前系统从“会跑一阵、然后掉速/掉 CPU/掉任务”修到“连续稳定跑”
- 让 `CPU 提升` 和 `有效研究吞吐提升` 同时发生，而不是只做表面优化
- 优先保证这条主链稳定：
  - `候选生成 -> 轻量验证 -> 深验证 -> 写回 outcome -> 继续扩展`

**当前问题诊断**
- `P0 问题 1`：generated candidate 的自动 revalidation 会形成坏循环
  - 典型表现：`45d / 90d` 双 workflow 重复派生
  - 后果：并发高、RSS 顶爆、quarantine、CPU 掉下去
- `P0 问题 2`：recent validation 会落到空数据宇宙
  - 典型表现：`rolling_recent_45d` 最后报 `Unknown field in expression: momentum_20`
  - 根因不是表达式本身，而是切出来的 dataset 为空
- `P0 问题 3`：maintenance failure 被误算成真实失败
  - 典型表现：`stale_running_task_cleaned` / `repaired_after_outputs_written`
  - 后果：误触发 `circuit_open`
- `P1 问题 4`：validation 路径过重、分级不够
  - 典型表现：本该做轻筛的任务直接走重验证
  - 后果：CPU 波动大，但有效产出不稳定
- `P1 问题 5`：队列生命周期和 worker 生命周期还会互相干扰
  - 典型表现：`code -15`、retry 链过长、任务被中途打断

**总体策略**
- 第一原则：先让任务“别跑错、别跑空、别跑重”
- 第二原则：先稳定执行链，再追 CPU 利用率
- 第三原则：所有提升都必须用量化指标验证，不凭感觉

**执行计划**

**阶段一：止血与隔离**
- 目标：先切掉最直接导致 CPU 掉下去的坏链
- 工作项：
  - 关闭 generated candidate 的默认自动 revalidation
  - generated revalidation 改成 `45d light -> 过线再 90d`
  - generated candidate validation 串行执行
  - maintenance failure 从 `consecutive_failures` 中排除
  - 清理当前遗留的坏 retry / pending 脏任务
- 产出：
  - 队列里不再反复出现同一 generated candidate 的坏重试链
  - `circuit_open` 不再由维护性失败触发
- 验收：
  - `generated candidate` 不再一上来触发 `45d/90d` 双重压测
  - `circuit_open` 触发次数明显下降

**阶段二：空数据与配置口径修复**
- 目标：避免 recent validation 落到空数据宇宙
- 工作项：
  - recent light validation 默认 `universe_limit=20`
  - `rolling_recent_45d/90d/120d` 统一 light 配置
  - workflow 增加 `dataset empty` 前置判定
  - `dataset slice empty` 归类为 deterministic failure
  - 不再允许空数据任务进入重试链
- 产出：
  - `rolling_recent_*` 不再用 `60/40` 空 feature store
  - 空数据任务能被快速判死
- 验收：
  - `Unknown field in expression: momentum_20` 这类由空数据引发的错误消失
  - `dataset slice empty` 若发生，会被直接 deterministic quarantine，不拖累吞吐

**阶段三：validation 漏斗化**
- 目标：把“验证”做成真漏斗，而不是所有候选都直接打满
- 工作项：
  - 建立 4 层验证漏斗：
    - `cheap screen`
    - `45d light`
    - `45d full / 90d light`
    - `90d+ / 120d+`
  - 每层明确晋级条件：
    - 是否高信息
    - 是否去重后还有增量
    - 是否非 fragile
    - 是否值得继续花算力
  - 每层明确淘汰条件：
    - 空数据
    - 低价值重复
    - 执行风险高
    - family 当前不值得扩展
- 产出：
  - 新 candidate 不再直接进入重型 workflow
- 验收：
  - 重型验证任务数下降
  - generated outcome 的高信息占比上升

**阶段四：吞吐与 CPU 稳定化**
- 目标：让 CPU 高位和研究产出同步
- 工作项：
  - 区分轻任务并发和重任务并发
  - daemon / worker / retry 三套并发上限分开管
  - recent validation 保持轻量并发
  - baseline / rolling back 保持有限并发
  - generated candidate validation 固定串行或低并发
- 产出：
  - CPU 不再只是一时冲高，而是更稳定
- 验收：
  - CPU 能连续维持在更高区间
  - 同时 `processed_tasks_total`、`outcome_count` 也同步增加

**阶段五：观测与报警完善**
- 目标：以后 CPU 掉下去时，一眼知道为什么
- 工作项：
  - UI 增加：
    - `generated candidate active`
    - `recent quarantine`
    - `rss exceeded count`
    - `dataset empty count`
    - `circuit_open status`
    - `heavy task slots`
  - 项目进步观察继续细化：
    - 24h / 7d 总分
    - 因子发现 / 研究增益 / 组合边际
    - 低价值重复 / 高信息失败 / useful success 分布
- 产出：
  - 你不需要再靠“CPU 好像低了”来猜状态
- 验收：
  - 页面能直接解释“为什么没跑满”

**检查计划**

**A｜每次代码修改后的即时检查**
- 跑回归测试
- 重启 daemon
- 检查队列里 `pending/running` 是否符合新策略
- 检查 heartbeat 有没有立刻出现同类错误

**B｜30 分钟观察**
- 看 CPU 是否能从低位重新拉起
- 看有没有再次出现：
  - `workflow_worker_rss_exceeded`
  - `dataset slice empty`
  - `Unknown field in expression`
  - `circuit_open`
- 目标：
  - 至少不再重复旧错误链

**C｜6 小时观察**
- 核心指标：
  - `CPU 区间`
  - `processed_tasks_total`
  - `generated_candidate_task_count_recent`
  - `generated_candidate_outcome_count`
  - `quarantine count`
  - `deterministic_task_error count`
  - `project progress 24h total`
- 目标：
  - 系统连续跑，不再频繁掉队列

**D｜24 小时观察**
- 核心指标：
  - 新候选注入数
  - 新候选 outcome 数
  - 高信息占比
  - 低价值重复占比
  - CPU 平均 / 峰值
  - recent validation 成功率
  - guardrail 触发次数
- 目标：
  - CPU 提升和有效研究推进同时成立

**量化验收标准**
- `最低通过`
  - 不再频繁出现旧的 generated revalidation 坏循环
  - `circuit_open` 不再成为常态
  - recent validation 不再因为空数据直接炸链
- `中等通过`
  - CPU 平均显著高于当前故障态
  - recent validation 能稳定连续跑完
  - generated outcome 持续写回
- `理想通过`
  - CPU 高位稳定
  - generated candidate 持续进入验证
  - 项目进步总分 24h / 7d 可见抬升
  - 研究吞吐和组合边际都同步改善

**风险提示**
- 只提并发，不修空数据/坏任务，会再次把系统打回异常
- 只修 UI，不修执行链，没有意义
- 只看 CPU，不看 outcome，会误判“跑满=跑好”

**执行顺序**
1. 继续完成 `recent validation` 空数据/坏配置问题的彻底收口
2. 再做 validation 漏斗
3. 再做并发/吞吐调优
4. 最后做 UI 补强

**一句话版本**
- 这份计划书的核心就是：
  - `先让任务别跑错`
  - `再让任务别跑空`
  - `再让任务别跑重`
  - `最后再把 CPU 稳定拉高`

如果你要，我下一步就按这份计划书继续执行，不再重新讨论。
