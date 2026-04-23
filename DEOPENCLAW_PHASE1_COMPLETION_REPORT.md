# De-OpenClaw Phase 1 完成报告

## 状态：✅ 完成

**生成时间：** 2026-04-24 04:33 UTC  
**验证者：** Hermes Agent (Task 5)

---

## 执行摘要

Phase 1 成功完成了 Factor Lab 从 OpenClaw 依赖到通用 LLM provider 的灰度迁移基础设施建设。系统现在具备：
- 通用 provider 抽象层，支持任意 OpenAI 兼容端点
- 观察模式（observation-only）灰度切换能力
- 独立的 daemon 通知机制，不再依赖 OpenClaw CLI
- 完整的回滚能力，单环境变量即可切换

**关键成果：** 系统已具备切换到非 OpenClaw provider 的能力，但当前仍运行在 legacy OpenClaw 模式以保持稳定性。

---

## 已完成任务总结

### Task 1: 规范化 Provider 词汇表 ✅
**提交：** `acf341b` - feat: normalize provider vocabulary with legacy OpenClaw aliases

**完成内容：**
- 建立通用 provider 语义：`real_llm`, `heuristic`, `mock`
- 引入 legacy 别名映射：`openclaw_gateway` → `legacy_openclaw_gateway`
- Provider 分类系统：primary (real_llm) / legacy (openclaw_*) / local (heuristic/mock)
- 向后兼容：旧 provider 名称仍可使用
- 新增 16 个测试用例验证规范化逻辑

**影响文件：**
- `src/factor_lab/llm_provider_router.py` (规范化逻辑)
- `tests/test_llm_provider_router.py` (新增 427 行测试)
- `src/factor_lab/llm_schema_validation.py` (验证白名单)
- `src/factor_lab/agent_responses.py` (验证白名单)

**验证结果：** 16/16 测试通过

---

### Task 2: 提升通用决策配置为一等公民 ✅
**提交：** `f57e995` - feat: promote generic decision config to first-class default

**完成内容：**
- 创建 `.env.factor-lab-decision-layer.example` 展示通用配置优先
- 扩展 `check_factor_lab_llm_provider.py` 支持通用 provider 检测
- 新增 3 个健康检查测试
- 文档化 Option A (通用 OpenAI 兼容) 和 Option B (legacy OpenClaw)

**影响文件：**
- `.env.factor-lab-decision-layer.example` (新建)
- `scripts/check_factor_lab_llm_provider.py` (增强)
- `tests/test_check_factor_lab_llm_provider.py` (新建 69 行)

**验证结果：** 3/3 测试通过

---

### Task 3: 添加仅观察模式灰度开关 ✅
**提交：** `f82f76e` - feat: add observation-only gray switch with explicit diagnostics

**完成内容：**
- 实现 `FACTOR_LAB_OBSERVATION_DECISION_PROVIDER` 独立配置
- 实现 `FACTOR_LAB_LIVE_DECISION_PROVIDER` 独立配置
- 支持 live/observation 路径使用不同 provider
- 增强诊断输出，明确显示 live vs observation provider
- 新增 4 个决策层测试验证灰度切换逻辑

**影响文件：**
- `src/factor_lab/research_planner_pipeline.py` (241 行修改)
- `tests/test_research_planner_pipeline_decision_layer.py` (新建 440 行)

**关键能力：**
```python
# 灰度配置示例
FACTOR_LAB_DECISION_PROVIDER=openclaw_gateway          # 默认
FACTOR_LAB_LIVE_DECISION_PROVIDER=openclaw_gateway     # 生产决策
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=real_llm      # 观察验证
```

**验证结果：** 4/4 测试通过

---

### Task 4: 解耦 Daemon 唤醒通知与 OpenClaw CLI ✅
**提交：** `f2d6dcc` - feat: decouple daemon wake notifications from OpenClaw CLI

**完成内容：**
- 实现可插拔通知后端：`noop`, `stdout`, `file`, `legacy_openclaw_event`
- 默认使用 `file` 后端，写入 `artifacts/runtime_notifications.jsonl`
- 移除对 `openclaw system event` 命令的硬依赖
- 新增 6 个 daemon 通知测试

**影响文件：**
- `scripts/run_research_daemon.py` (233 行修改)
- `tests/test_run_research_daemon_notifier.py` (新建 95 行)

**环境变量：**
- `FACTOR_LAB_RUNTIME_NOTIFIER` (默认: `file`)
- `FACTOR_LAB_RUNTIME_NOTIFY_FILE` (默认: `artifacts/runtime_notifications.jsonl`)

**验证结果：** 6/6 测试通过

---

## 当前系统状态

### Provider 配置
```json
{
  "configured_provider": "openclaw_gateway",
  "normalized_provider": "legacy_openclaw_gateway",
  "provider_class": "legacy",
  "real_provider_configured": false,
  "openclaw_gateway_configured": true,
  "effective_source": "legacy_openclaw_gateway"
}
```

### 健康状态
- ✅ Gateway probe: 成功 (200, 129-146ms 延迟)
- ✅ Planner agent: `factor-lab-planner`
- ✅ Failure agent: `factor-lab-failure`
- ✅ 无降级到启发式模式

### 测试覆盖
```
✅ test_llm_provider_router.py              16 passed
✅ test_check_factor_lab_llm_provider.py     3 passed
✅ test_research_planner_pipeline_decision_layer.py  4 passed
✅ test_research_attribution.py              1 passed
✅ test_decision_impact_report.py            1 passed
✅ test_run_research_daemon_notifier.py      6 passed
✅ test_research_daemon_recycle.py          10 passed
---------------------------------------------------
总计: 41 passed
```

---

## 回滚程序

### 单步回滚（推荐）
如果切换到 `real_llm` 后出现问题，只需修改以下环境变量：

```bash
# 回滚到 legacy OpenClaw
FACTOR_LAB_DECISION_PROVIDER=openclaw_gateway
FACTOR_LAB_LIVE_DECISION_PROVIDER=openclaw_gateway
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=openclaw_gateway
```

或回滚到本地启发式：
```bash
FACTOR_LAB_DECISION_PROVIDER=heuristic
```

### 验证回滚
```bash
PYTHONPATH=src python scripts/check_factor_lab_llm_provider.py
```

检查输出中的 `effective_source` 字段确认回滚成功。

### 回滚不需要
- ❌ 修改代码
- ❌ 重新部署
- ❌ 数据库迁移
- ❌ 重启多个服务

---

## 下一阶段准入标准（Go/No-Go）

### ✅ Go 条件：可以切换到 live provider

满足以下条件时，可以将 `FACTOR_LAB_LIVE_DECISION_PROVIDER` 切换到 `real_llm`：

1. **观察模式稳定性**
   - `FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=real_llm` 运行 5-10 个周期
   - 无 schema invalid 输出
   - 无 parse 错误
   - 无超时

2. **Planner 管道健康**
   - `scripts/run_agent_briefs.py` 使用 real_llm 成功运行
   - 结构化输出完整（包含 mode, task_mix, priority_families）
   - 无空响应或空 action 列表

3. **Attribution 和报告生成**
   - `artifacts/research_attribution.json` 正确记录 provider source
   - `artifacts/factor_quality_observation_report.md` 正常生成
   - Decision impact 报告无异常

4. **Daemon 稳定性**
   - Daemon 在无 OpenClaw CLI 的环境下稳定运行
   - 通知机制正常工作（file 或其他后端）
   - 无 wake-event 依赖导致的崩溃

5. **配置就绪**
   - `FACTOR_LAB_LLM_BASE_URL` 已配置
   - `FACTOR_LAB_LLM_API_KEY` 已配置
   - `FACTOR_LAB_LLM_MODEL` 已配置
   - 健康检查显示 `real_provider_configured: true`

### ❌ No-Go 条件：不应切换

以下情况下不应切换到 live provider：

1. **配置不完整**
   - 缺少 API key 或 base URL
   - 健康检查显示 `real_provider_configured: false`

2. **观察模式不稳定**
   - Schema validation 频繁失败
   - Parse 错误率 > 10%
   - 超时率 > 20%
   - 连续 3 次空响应

3. **结构化输出质量差**
   - Planner 输出缺少必需字段
   - Failure analyst 输出不完整
   - 决策逻辑明显退化（与 heuristic baseline 对比）

4. **系统依赖未解决**
   - Daemon 仍依赖 `openclaw` CLI 命令
   - 回滚机制未验证
   - 监控和告警未就位

---

## 灰度切换建议路径

### 阶段 1：观察模式验证（当前可执行）
```bash
# 仅切换观察路径，不影响生产决策
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=real_llm
FACTOR_LAB_LIVE_DECISION_PROVIDER=openclaw_gateway  # 保持不变
```

**验证周期：** 5-10 个 planner 循环  
**监控指标：** schema_valid, parse_success, response_completeness

### 阶段 2：Live 切换（满足 Go 条件后）
```bash
# 切换生产决策路径
FACTOR_LAB_LIVE_DECISION_PROVIDER=real_llm
FACTOR_LAB_DECISION_PROVIDER=real_llm
```

**验证周期：** 24-48 小时  
**监控指标：** task_completion_rate, failure_rate, decision_quality

### 阶段 3：完全迁移（稳定后）
```bash
# 移除所有 OpenClaw 配置
unset FACTOR_LAB_OPENCLAW_GATEWAY_URL
unset FACTOR_LAB_OPENCLAW_PLANNER_AGENT
unset FACTOR_LAB_OPENCLAW_FAILURE_AGENT
```

---

## 风险评估

### 低风险 ✅
- Provider 抽象层已充分测试
- 回滚机制简单可靠
- 向后兼容性完整
- 测试覆盖率高

### 中风险 ⚠️
- Real LLM 结构化输出质量未知（需观察模式验证）
- 新 provider 的延迟和稳定性特征未知
- 成本模型可能不同（需监控 token 使用）

### 已缓解风险 ✅
- ~~Daemon 依赖 OpenClaw CLI~~ → 已解耦
- ~~无法独立配置 observation provider~~ → 已支持
- ~~回滚需要多步操作~~ → 单环境变量回滚
- ~~缺少 provider 规范化~~ → 已建立

---

## 技术债务和后续工作

### 可选优化（不阻塞切换）
1. **Shadow mode 实现**
   - 并行运行 legacy 和 real_llm，对比输出
   - 生成 diff 报告用于质量评估

2. **Provider 性能监控**
   - 记录每个 provider 的延迟、成功率、token 使用
   - 自动化性能回归检测

3. **Legacy 代码清理**
   - 在稳定运行 30 天后，考虑移除 legacy_openclaw_* 代码
   - 归档历史文档到 `docs/legacy-openclaw/`

4. **路径抽象**
   - 实现 `src/factor_lab/paths.py` 统一路径管理
   - 支持 `FACTOR_LAB_ROOT` 等环境变量
   - 为未来目录迁移做准备

---

## 验证命令记录

### 健康检查输出
```bash
$ PYTHONPATH=src python scripts/check_factor_lab_llm_provider.py
{
  "generated_at_utc": "2026-04-23T20:35:08.618662+00:00",
  "configured_provider": "openclaw_gateway",
  "normalized_provider": "legacy_openclaw_gateway",
  "provider_class": "legacy",
  "configured_model": "codex-for-me/gpt-5.4",
  "real_provider_configured": false,
  "openclaw_gateway_configured": true,
  "openclaw_agent_configured": true,
  "gateway_url": "http://127.0.0.1:18789/v1/chat/completions",
  "timeout_seconds": 90.0,
  "probe": {
    "attempted": true,
    "ok": true,
    "status_code": 200,
    "latency_ms": 129,
    "error": null
  },
  "recommended_effective_source": "legacy_openclaw_gateway",
  "effective_source": "legacy_openclaw_gateway",
  "degraded_to_heuristic": false,
  "planner_agent": "factor-lab-planner",
  "failure_agent": "factor-lab-failure"
}
```

### 测试套件结果
```bash
$ PYTHONPATH=src pytest -q tests/test_llm_provider_router.py
................                                                         [100%]
16 passed in 0.06s

$ PYTHONPATH=src pytest -q tests/test_check_factor_lab_llm_provider.py
...                                                                      [100%]
3 passed in 0.22s

$ PYTHONPATH=src pytest -q tests/test_research_planner_pipeline_decision_layer.py
....                                                                     [100%]
4 passed in 2.50s

$ PYTHONPATH=src pytest -q tests/test_research_attribution.py
.                                                                        [100%]
1 passed in 0.05s

$ PYTHONPATH=src pytest -q tests/test_decision_impact_report.py
.                                                                        [100%]
1 passed in 0.06s

$ PYTHONPATH=src pytest -q tests/test_run_research_daemon_notifier.py
......                                                                   [100%]
6 passed in 1.57s

$ PYTHONPATH=src pytest -q tests/test_research_daemon_recycle.py
..........                                                               [100%]
10 passed, 1 warning in 1.78s
```

---

## 结论

Phase 1 成功建立了从 OpenClaw 到通用 LLM provider 的迁移基础设施。系统现在：

1. **具备切换能力** - 可以通过环境变量切换到任意 OpenAI 兼容 provider
2. **保持稳定性** - 当前仍运行在验证过的 legacy 路径
3. **支持灰度验证** - 可以先在观察模式验证新 provider
4. **简化回滚** - 单环境变量即可回滚
5. **运行时独立** - 不再依赖 OpenClaw CLI

**下一步操作建议：**
配置 `real_llm` provider 的 API credentials，然后按照灰度切换路径进行观察模式验证。

**操作决策权：** 现在是运营决策，而非代码猜测练习。
