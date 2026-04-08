# Session: 2026-04-04 13:18:03 UTC

- **Session Key**: agent:main:main
- **Session ID**: e3faa093-be90-4134-990f-41ef593b0569
- **Source**: webchat

## Conversation Summary

user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Sat 2026-04-04 09:14 GMT+8] 行
assistant: [[reply_to_current]] 收到，我继续。  
下一步直接进入 **Planner runner 智能升级实现**，不再停留在计划层。
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Sat 2026-04-04 09:15 GMT+8] 把剩下的板块给完成一下
assistant: 我直接把剩下最关键的板块收尾：先把 **Planner runner / Failure runner 智能升级** 做掉，让它们不再只是最小规则版。
assistant: 我已经把上下文看完了。现在最该补的是：让 Planner runner 真正利用这些现成信号，而不是只做几条硬编码判断。
assistant: 我把 Planner / Failure runner 的规则都加细了。现在跑一次 end-to-end，看看新的响应是不是更像真正的决策器。
assistant: 现在在跑升级后的 runner 验证，这一轮能看出来它是不是已经从“最小可用”变成“更像决策器”。
assistant: [[reply_to_current]] 这块我已经往前推进了一截，剩下的关键板块基本也补得差不多了。

## 我刚完成的
### Planner runner 智能升级
它现在不再只是简单地回一个 recover/validate，而是已经会结合：

- `research_flow_state`
- `queue_budget`
- `stable_candidates`
- `latest_graveyard`
- `open_questions`
- `candidate_pool_suppressed`

来产出更细的：

- `priority_families`
- `recommended_actions`
- `task_mix`
- `suppress_families`

这次跑出来的结果已经明显更像“研究决策器”了，比如：

- `priority_families`
  - `graveyard_diagnosis`
  - `recent_window_validation`
  - `stable_candidate_validation`

- `recommended_actions`
  - 优先做 `graveyard_diagnosis`
  - 优先验证 `mom_20`
  - 优先解决当前 open question

---

### Failure runner 智能升级
它现在也不只是看到 warning 就给一个模糊提示了，而是已经能更明确地产出：

- `failure_patterns`
- `should_probe`
- `should_reroute`

这次已经跑出了更细的结构，比如：

- `diag-warning-cluster`
- `graveyard-pressure`

以及更明确的 reroute：

- `broad_exploration->graveyard_diagnosis`
- `broad_exploration->stable_candidate_validation`

这已经不是“有个壳子”，而是真的开始做研究路线切换了。

---

# 现在还剩多少工作？
如果按你说的“把剩下的板块完成一下”来算，

我现在的判断是：

## 主体工程
**基本已经完成了，大概 85%-90% 左右。**

---

## 剩下还没完全做完的
### 1. proposal / branch planner 更深接入
现在 Agent 已经影响：
- candidate pool
- strategy plan
- budget
- approved tasks

但还可以更深入影响：
- proposal ranking
- branch planner 选路

这一块属于**增强项**，不是主骨架缺失。

---

### 2. 更强的 stop / reroute 执行策略
现在：
- stop / reroute 已经能进入 memory
- reroute 已经能前移到 candidate pool
- strategy 也会吃它

但还可以继续细化到：
- branch 生命周期更直接变更
- 不同 family 的更细力度 stop/deprioritize/reroute

这也是**增强项**。

---

### 3. 效果验证
严格来说，还差最后一块：
- 验证 Agent 介入后是不是真的让研究更聚焦、更少空转、更快收敛

这是**验收项**，不是基础设施项。

---

# 所以一句话总结
> **核心板块现在已经基本做完了。**
> 剩下的不是“系统还没搭好”，而是：
> - 更深的执行增强
> - 最后的效果验证

---

# 如果你要我继续
我建议接下来只做两件高价值收尾：

1. **proposal / branch planner 深接入**
2. **效果验证报告**

这样就可以从“功能已完成”走到“证明它真的有用”。

如果你点头，我下一步就直接做这两个收尾项。
user: System: [2026-04-04 09:17:46 GMT+8] Exec completed (mild-dun, code 0) :: "symptom": "mom_120,mom_60,mom_60_skip_5", "likely_cause": "墓地候选持续出现，说明存在结构性失败模式尚未吃透。", "recommended_action": "diagnose", "confidence_score": 0.67 } ], "failure_should_probe": [ "…

Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Sat 2026-04-04 09:20 GMT+8] 收尾
assistant: [[reply_to_current]] 好，收尾版给你。

# 这轮工作收尾结论

## 已经完成的核心内容
### 1. 性能主线
- 找到了真正瓶颈：`factor_eval`
- 证伪了 naive 多线程并行
- 做了算法级优化
- 典型 workflow 从 **58.8s** 压到 **27.3s**

### 2. 调度主线
- 找到了外层 heavy task 串行问题
- orchestrator 已改成一轮可并发处理多个任务
- 不再是“claim 一个、等一个”

### 3. Agent 决策主线
已经打通完整闭环：

- brief 生成
- response schema
- 最小 runner
- planner pipeline 自动触发
- 回流 strategy plan
- 回流 analyst signals
- 进入 budget
- 影响 approved tasks
- 写入 `research_memory.agent_control`
- suppress / stop / reroute 前移到 candidate pool

### 4. runner 智能升级
现在已经不是最初的粗糙占位符了，已经能更像决策器地输出：

- `priority_families`
- `recommended_actions`
- `failure_patterns`
- `should_probe`
- `should_reroute`

---

# 当前完成度
如果按你这一轮的目标算：

> **大约 85%-90% 已完成。**

---

# 剩余的尾巴
真正还没完全做掉的，只剩两类：

## 尾巴 1：更深接入 proposal / branch planner
现在 Agent 已经影响很多层了，  
但还没有完全深入到：

- proposal ranking
- branch planner 选路本体

这属于增强收尾，不是骨架缺失。

## 尾巴 2：效果验证
还缺最后一层验收：

- Agent 介入后有没有更少空转
- 有没有更聚焦
- 有没有更快收敛

这是验证问题，不是建设问题。

---

# 所以现在的状态
一句话：

> **系统已经从“靠规则硬跑的研究流水线”，进化成了“本地计算引擎 + Agent 决策层”的可运行原型。**

而且不是空壳，是已经能：
- 调节 task mix
- 控制 recover / validate 节奏
- 做 suppress / reroute
- 把决策沉淀进 memory

---

# 建议的最终收尾动作
我建议接下来只做两个轻量收尾：

1. **把 Agent 决策再接深一层到 proposal / branch planner**
2. **做一次效果验证报告**

做完这两个，这条线就可以算阶段性完成了。

---

# 如果你现在想停在这里
也完全可以。

因为当前这套已经不是“概念方案”，而是：
- 能跑
- 能控
- 能提速
- 能积累决策状态

已经到了可以进入下一阶段观察和迭代的程度。
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Sat 2026-04-04 20:47 GMT+8] 为什么研究进程现在中断了
assistant: 我先查一下 daemon、队列和最近 heartbeat，看看是“真停了”还是卡在某个环节。
assistant: [[reply_to_current]] 不是“进程没了”，而是**被守门器主动停住了**。

## 现在的真实状态
research daemon 还活着：

- `factor-lab-research-daemon.service`
- 状态：**active (running)**

但它当前进入了：

- `state = guardrail`
- `guardrail = circuit_open`

也就是：

> **研究进程没有死，是被故障保护机制卡住了。**

---

## 为什么会被卡住
根因非常明确：

### 最近连续很多个 `generated_batch` 任务都因为内存超限失败
错误基本都是：

- `research task worker rss exceeded limit: 205xMB >= 2048MB`

我查到最近一串都是这个模式：

- 2054MB >= 2048MB
- 2049MB >= 2048MB
- 2052MB >= 2048MB
- 2053MB >= 2048MB
- …

所以系统判断成：

> **连续失败太多（现在看到是 19 次），打开 circuit breaker，暂停继续跑。**

heartbeat 里已经连续在报：

- `paused due to consecutive failures=19`

---

## 还有一个附带问题
heartbeat 里还出现了一条 daemon 自身报错：

- `write_status() got multiple values for keyword argument 'rss_mb'`

这个不是主因，**主因还是 generated_batch 连续爆 RSS 触发 circuit_open**。  
但这个 bug 会让 daemon 状态写入不够干净，也得修。

---

## 一句话总结
### 为什么研究进程现在中断了？
因为：

1. `generated_batch` 连续很多次内存超限
2. 触发了系统的 **circuit_open** 保护
3. 守门器为了防止继续炸，主动暂停了研究执行

所以它不是自然停机，而是：

> **被 guardrail 暂停了。**

---

## 现在最该做的
按优先级是：

### 1. 先解除“坏任务反复重试”
现在还有一个：
- `generated_batch` pending retry

这个会继续卡系统。

### 2. 修 generated_batch 的 RSS 超限问题
要么：
- 降低这类任务体积
- 改映射策略
- 或者别让它不断自动 retry

### 3. 顺手修 daemon 的 `rss_mb` 重复传参 bug
这是次级问题，但应该一起收掉。

---

## 最短结论
> **研究没跑，是因为最近一串 generated_batch 任务反复爆 2GB RSS 限制，触发了 circuit breaker。**

如果你要，我下一步就直接去把：
- 这个 pending retry 干掉 / 隔离
- 修掉 `circuit_open`
- 让研究流恢复运行
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Sat 2026-04-04 20:55 GMT+8] 给我修复清单
assistant: [[reply_to_current]] 可以，给你一个直接可执行的修复清单。

# 研究进程中断修复清单

## 当前故障结论
### 直接原因
- 一串 `generated_batch` 任务连续 **RSS 超过 2048MB**
- 触发 `circuit_open`
- orchestrator 被 guardrail 暂停

### 次级问题
- daemon 还有一个状态写入 bug：
  - `write_status() got multiple values for keyword argument 'rss_mb'`

---

# 修复清单

## P0｜先恢复研究流
### 1. 处理当前卡住的 pending retry
- [ ] 找出当前 pending 的 `generated_batch` retry 任务
- [ ] 不让它继续作为“坏任务”堵住队列
- [ ] 该隔离就隔离、该 quarantine 就 quarantine

### 2. 清掉连续失败态 / 恢复 circuit
- [ ] 查 `consecutive_failures` 的状态来源
- [ ] 让系统从 `circuit_open` 恢复
- [ ] 确保恢复后不会立刻又被同一个坏任务打回去

---

## P0｜修 generated_batch 连续爆内存
### 3. 查为什么这些 generated_batch 都稳定卡在 2GB 附近
- [ ] 看这批 generated_batch 的共同特征
- [ ] 看是不是 batch 太大
- [ ] 看是不是 candidate 组合太重
- [ ] 看是不是某条生成路径特别容易膨胀

### 4. 给 generated_batch 加更强的前置瘦身/降级
- [ ] 在进入 heavy path 之前再做一次体积判断
- [ ] 高风险 generated_batch 直接降级成更轻任务
- [ ] 避免进入“必炸但还反复重试”的路径

### 5. 限制 generated_batch 的自动重试
- [ ] 对 `rss exceeded limit` 这种失败视为高概率确定性失败
- [ ] 不要继续无脑 retry
- [ ] 直接 quarantine / downgrade / reroute

---

## P1｜修 daemon 自身 bug
### 6. 修 `write_status(... rss_mb duplicated ...)`
- [ ] 定位 `run_research_daemon.py` 里重复传 `rss_mb` 的地方
- [ ] 保证 guardrail / failed / idle / running 路径都不会重复传参
- [ ] 避免 daemon 状态文档异常

---

## P1｜增强 guardrail 恢复机制
### 7. 不让同类坏任务反复把系统拖死
- [ ] 同类 generated_batch 连续 RSS 超限时，自动进入 family/blocklist
- [ ] 把“坏路线”从活跃队列里提前踢出去
- [ ] 保证健康任务还能继续跑

### 8. circuit_open 不应长期僵死
- [ ] 增加恢复策略
- [ ] 如果失败族群已隔离，允许健康任务继续运行
- [ ] 不要因为一个坏家族把全研究流完全冻住

---

## P2｜后续稳定性收尾
### 9. 给 generated_batch 做资源画像
- [ ] 记录哪类 generated_batch 最容易爆 RSS
- [ ] 记录常见 source / target family / branch pattern
- [ ] 为后面 Agent 的 stop / reroute 提供依据

### 10. 把这次事故转成 Agent 可读失败模式
- [ ] 把 “generated_batch RSS 爆炸” 纳入 failure pattern
- [ ] 让 Agent 后续自动减少这类路线的预算
- [ ] 把它接入 reroute / suppress 逻辑

---

# 推荐执行顺序
## 第一波：先恢复运行
- [ ] 隔离 pending retry
- [ ] 清掉 circuit_open 的根因
- [ ] 恢复研究流

## 第二波：防止再次发生
- [ ] generated_batch 失败分类改严
- [ ] RSS 超限不要自动 retry
- [ ] 强化 quarantine / downgrade

## 第三波：系统化
- [ ] 修 daemon bug
- [ ] 增强 guardrail 恢复
- [ ] 把事故经验写进 Agent failure logic

---

# 最小目标
先做到这三个：

- [ ] 当前卡住的 generated_batch 不再堵住系统
- [ ] circuit_open 解除
- [ ] 同类 RSS 超限任务不再自动重试

做到这三条，研究流就能先恢复。

如果你要，我下一步就直接按这个清单开始修，不先讲了。
