# Factor Lab 自主策略研究系统实现方案计划书

> **For Hermes:** 本计划用于把当前 Factor Lab 从“自动化因子工厂”升级为“自主研究经理 + 受控实验执行器”。实施时优先使用 TDD；每一阶段必须先 dry-run、再 targeted tests、再受控执行。禁止在本计划内启用 timer、恢复 broad daemon、写入 live trading、自动晋级候选、修改模型/provider 配置或放松风险阈值。

**方案名称:** Autonomous Strategy Lab / 自主策略研究实验室
**目标:** 让系统具备自主分析、自主判断、自主停止、自主请求新数据/新机制的能力，而不是只会继续生成因子和继续回测。
**当前判断:** 现有系统已有很多自动化组件，但缺少统一研究决策层。当前证据显示 `industry_relative_value` 等路线受回撤和数据不足阻塞，继续同路线批量回测的信息增益很低。
**实施原则:** 计划 → 测试/诊断 → dry-run → 受控执行 → 报告 → 知识沉淀。

---

## 1. 背景与问题定义

### 1.1 当前系统已经具备的能力

Factor Lab 当前已有：

- 因子候选生成；
- workflow/backtest 执行；
- Harvest Agent v1-v5 相关组件；
- Hermes correction state；
- controlled restart dry-run；
- runtime takeover audit；
- small institutional risk-reduction executor；
- research quality / knowledge artifacts。

这些说明工程链路并不缺少“能跑”的能力。

### 1.2 当前主要失败模式

从最新 artifact-backed dry-run 看，系统处于：

```text
repair_status = blocked_no_drawdown_safe_candidate
drawdown_limit = -0.35
best_available_max_drawdown = -0.475431
candidate_count = 0
latest decision = request_data
```

主要 blocker：

```text
drawdown_blocker_no_safe_candidate
best_drawdown_worse_than_limit
repeated_insufficient_data
recent_no_ok_rows
candidate_pool_fragile_or_rejected_heavy
no_claimable_controlled_workflow
broad_daemon_should_remain_paused
```

### 1.3 根因判断

根因不是“自动化不够多”，而是“自动化目标不完整”：

旧目标：

```text
生成更多因子表达式 -> 跑更多回测 -> 找 pass
```

新目标应该是：

```text
判断研究路线是否值得继续 -> 选择机制/数据/组合修复方向 -> 只执行有信息增益的受控实验
```

---

## 2. 总体设计

### 2.1 新系统定位

Autonomous Strategy Lab 是一个**研究决策层**，不是替代现有 backtest runner 的新回测器。

它位于：

```text
Factor Lab evidence/artifacts/database
        ↓
Autonomous Strategy Lab 决策层
        ↓
受控实验计划 / data request / stop route / manual review
        ↓
现有 workflow / Harvest controller / controlled executor
```

### 2.2 核心循环

```text
collect_evidence()
  -> diagnose_failure_modes()
  -> score_routes()
  -> choose_decision()
  -> materialize_next_plan()
  -> execute_controlled_if_allowed()
  -> write_verdict()
  -> update_knowledge()
```

### 2.3 决策类型

系统每轮必须输出一个明确 decision：

```text
continue_route_with_constraints
repair_portfolio_construction
switch_mechanism_route
request_data
stop_route
manual_review
```

### 2.4 安全默认值

所有运行默认：

```json
{
  "mode": "dry_run",
  "queue_write_allowed": false,
  "automation_allowed": false,
  "live_trading_enabled": false,
  "timer_enable_allowed": false,
  "systemd_change_allowed": false,
  "auto_promotion_allowed": false
}
```

---

## 3. 系统架构

### 3.1 模块划分

建议新增模块：

```text
src/factor_lab/autonomous_strategy_lab.py
src/factor_lab/autonomous_strategy_evidence.py
src/factor_lab/autonomous_strategy_diagnosis.py
src/factor_lab/autonomous_strategy_routes.py
src/factor_lab/autonomous_strategy_decision.py
src/factor_lab/autonomous_strategy_planner.py
src/factor_lab/autonomous_strategy_execution_adapter.py
src/factor_lab/autonomous_strategy_report.py
```

脚本入口：

```text
scripts/run_autonomous_strategy_lab.py
scripts/inspect_autonomous_strategy_lab.py
scripts/run_autonomous_strategy_lab_controlled.py
```

配置：

```text
configs/autonomous_strategy_lab.json
configs/autonomous_strategy_routes.json
```

测试：

```text
tests/test_autonomous_strategy_evidence.py
tests/test_autonomous_strategy_diagnosis.py
tests/test_autonomous_strategy_routes.py
tests/test_autonomous_strategy_decision.py
tests/test_autonomous_strategy_planner.py
tests/test_autonomous_strategy_execution_adapter.py
tests/test_autonomous_strategy_report.py
```

Artifacts：

```text
artifacts/autonomous_strategy_lab/runs/<run_id>/evidence.json
artifacts/autonomous_strategy_lab/runs/<run_id>/diagnosis.json
artifacts/autonomous_strategy_lab/runs/<run_id>/route_scores.json
artifacts/autonomous_strategy_lab/runs/<run_id>/decision.json
artifacts/autonomous_strategy_lab/runs/<run_id>/next_plan.json
artifacts/autonomous_strategy_lab/runs/<run_id>/summary.md
artifacts/autonomous_strategy_lab/latest_decision.json
artifacts/autonomous_strategy_lab/latest_decision.md
```

---

## 4. 数据与证据输入

### 4.1 SQLite 输入

读取：

```text
artifacts/factor_lab.db
```

重点表：

```text
factor_candidates
factor_evaluations
research_tasks
```

核心指标：

```text
candidate status distribution
rejection reason distribution
pass rate
final score
robustness
coverage
split failures
max drawdown
turnover
research task pending/running/finished 状态
```

### 4.2 Artifact 输入

读取：

```text
artifacts/small_institutional_simulation/risk_reduction_repair.json
artifacts/small_institutional_simulation/risk_reduction_results.json
artifacts/harvest_agent/v3_status.json
artifacts/harvest_agent/latest_controller_run.json
artifacts/controlled_restart_dry_run.json
artifacts/runtime_takeover_audit.json
knowledge/mechanism_lessons.md
knowledge/data_blockers.json
knowledge/research_waste.md
```

### 4.3 Evidence schema

```json
{
  "schema_version": 1,
  "run_id": "strategy_lab_YYYYMMDDTHHMMSSZ",
  "db_summary": {},
  "risk_summary": {},
  "harvest_summary": {},
  "runtime_summary": {},
  "knowledge_summary": {},
  "data_blockers": [],
  "observed_failure_modes": []
}
```

---

## 5. 诊断系统设计

### 5.1 Failure Mode 分类

第一版支持：

```text
drawdown_blocker_no_safe_candidate
best_drawdown_worse_than_limit
repeated_insufficient_data
recent_no_ok_rows
candidate_pool_fragile_or_rejected_heavy
semantic_repeat_limit_reached
coverage_too_low
neutralization_breaks_signal
too_many_split_failures
cost_sensitivity
portfolio_construction_failure
no_claimable_controlled_workflow
broad_daemon_should_remain_paused
```

### 5.2 Diagnosis 输出

```json
{
  "severity": "blocker",
  "reason_codes": [
    "drawdown_blocker_no_safe_candidate",
    "repeated_insufficient_data"
  ],
  "summary": "Current route is blocked by portfolio drawdown; more same-route backtests are low value.",
  "blocked_actions": [
    "same_route_full_backtest_batch",
    "broad_daemon_restore"
  ],
  "allowed_actions": [
    "write_blocker_report",
    "draft_new_mechanism_or_data_request",
    "run_cheap_screen_only_after_review"
  ]
}
```

---

## 6. 机制路线系统设计

### 6.1 路线注册表

新增：

```text
configs/autonomous_strategy_routes.json
```

示例：

```json
{
  "routes": [
    {
      "route_id": "quality_value_recovery",
      "family": "value_quality",
      "hypothesis": "低估值且盈利质量改善的股票存在估值修复机会。",
      "required_fields": [
        "earnings_yield",
        "roe",
        "profit_growth",
        "cashflow_quality",
        "leverage"
      ],
      "cheap_screens": [
        "field_coverage",
        "semantic_novelty",
        "risk_feasibility"
      ],
      "known_failure_modes": [
        "value_trap",
        "drawdown_high",
        "coverage_low"
      ],
      "max_initial_backtests": 5
    }
  ]
}
```

### 6.2 路线评分维度

每条路线评分：

```text
signal_evidence_score
portfolio_viability_score
information_gain_score
data_sufficiency_score
semantic_novelty_score
cost_robustness_score
```

最终：

```text
route_score = weighted_sum(scores) - blockers_penalty
```

### 6.3 路线选择原则

优先级：

1. 如果 drawdown blocker 持续存在，同路线继续回测大幅扣分；
2. 如果 data insufficiency 重复出现，优先 request_data；
3. 如果语义重复严重，优先 switch_mechanism_route；
4. 如果 risk/data/novelty 都可接受，才允许 continue_route_with_constraints；
5. 如果没有正信息增益路线，输出 stop_route/manual_review。

---

## 7. Cheap Screen 设计

### 7.1 目标

在完整回测前，用低成本判断实验是否值得跑。

### 7.2 第一批 screen

```text
field_coverage_screen
semantic_novelty_screen
historical_failure_overlap_screen
risk_feasibility_screen
portfolio_capacity_proxy_screen
cost_sensitivity_prior_screen
```

### 7.3 Cheap Screen 输出

```json
{
  "screen_status": "blocked",
  "passed": false,
  "reason_codes": [
    "missing_required_fields",
    "semantic_repeat"
  ],
  "estimated_information_gain": 0.12,
  "recommended_action": "request_data"
}
```

---

## 8. 计划生成器设计

### 8.1 Next Plan schema

```json
{
  "schema_version": 1,
  "decision": "request_data",
  "selected_route_id": "quality_value_recovery",
  "objective": "Acquire or validate missing data before same-route backtesting.",
  "max_backtests_before_review": 0,
  "requires_human_review": true,
  "allowed_next_actions": [
    "write_blocker_report",
    "draft_new_mechanism_or_data_request"
  ],
  "blocked_next_actions": [
    "same_route_full_backtest_batch",
    "broad_daemon_restore",
    "queue_write"
  ]
}
```

### 8.2 Plan 类型

支持：

```text
data_request_plan
mechanism_switch_plan
portfolio_repair_plan
cheap_screen_plan
controlled_backtest_plan
stop_route_plan
manual_review_plan
```

---

## 9. 执行适配器设计

### 9.1 默认 dry-run

```bash
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
```

只写 artifact，不执行 backtest，不写队列。

### 9.2 受控执行

```bash
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab_controlled.py \
  --allow-controlled-execution \
  --max-backtests 5
```

必须满足：

```text
latest decision 允许 controlled_backtest
cheap screens passed
max_backtests <= policy cap
queue_write_allowed=false unless explicit write flag and review artifact exists
```

### 9.3 禁止项

受控执行适配器不得：

```text
start systemd daemon
enable timer
write broad research queue
auto-promote candidates
relax drawdown limit
call live trading/order APIs
modify Hermes/Factor Lab model provider settings
```

---

## 10. Agent/profile/provider 设置迁移原则

### 10.1 总原则

本系统接入 Hermes 时，**不再新增 Factor Lab 内部 agent/provider/model/profile 设置**。

旧式设置包括：

```text
agent_id
agent_role
legacy_agent_id
llm_fallback_order
model
provider
base_url
api_key
profile
```

这些字段要么删除，要么替换成 **Hermes temporary worker specs**：

```text
configs/autonomous_strategy_workers.json
```

运行方式必须优先使用 **Hermes CLI one-shot worker**，避免把 agent 接错到 Factor Lab 内部旧角色系统或 `delegate_task` 同步子任务：

```bash
hermes chat -Q --source factor-lab-worker \
  --skills factor-lab \
  --toolsets file,terminal,skills,session_search \
  --query '<self-contained worker prompt>'
```

新规则：

```text
Hermes worker = hermes chat 一次性工作会话 / prompt / artifact contract
Factor Lab = 确定性 gate、queue 权限、执行权限、风控权限
模型/provider/profile = 继承当前 Hermes 配置，不由 Factor Lab 固定或 pin
不得传 --model / --provider / --resume / --continue / --yolo
```

### 10.2 替换后的 worker 设置

`configs/autonomous_strategy_workers.json` 只允许表达：

```text
worker_key
purpose
toolsets
skills
input_artifacts
output_artifact_namespace
forbidden_actions
verification_after
```

明确禁止表达：

```text
model/provider/base_url/api_key/profile/legacy_agent_id/llm_fallback_order
```

这样做的目的：

```text
避免 Factor Lab 再维护一套伪 agent/provider 配置；
避免模型 pinning；
避免旧 OpenClaw/HermesNative agent 残留；
让 Hermes 当前主配置负责模型与 provider，Factor Lab 只保存 worker 契约；
通过 hermes chat 命令启动新的一次性 worker 会话，避免误接旧 agent/profile 层。
```

### 10.3 Hermes CLI worker 调用规范

worker 调用必须由一个后续 launcher/adapter 生成完整命令，不允许手写拼接半截 prompt。

推荐命令模板：

```bash
hermes chat -Q --source factor-lab-worker \
  --skills factor-lab \
  --toolsets file,terminal,skills,session_search \
  --query "$(cat artifacts/autonomous_strategy_lab/workers/<run_id>/<worker_key>_prompt.txt)"
```

要求：

```text
1. 每个 worker 使用全新 one-shot Hermes 会话，不使用 --resume / --continue。
2. 不传 --model / --provider；继承当前 Hermes 配置。
3. 不传 --yolo；不绕过安全确认。
4. worker prompt 必须自包含：任务、输入 artifact 路径、输出 artifact 路径、禁止动作、必须验证项。
5. worker 输出只写自己的 artifact namespace，不写 queue/systemd/timer/daemon/live trading。
6. Factor Lab deterministic gate 读取 worker artifact 后再决定是否进入下一步。
```

### 10.4 关键步骤后的验证设计

每一个实现阶段后必须立刻运行对应验证，不允许“先继续做后面再统一测”。

最小验证序列：

```text
1. 修改 worker config 后：
   PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_worker_config.py -q

2. 修改 diagnosis / route / decision 后：
   PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_lab.py -q

3. 修改 dry-run 脚本后：
   PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
   PYTHONPATH=src .venv/bin/python -m py_compile scripts/run_autonomous_strategy_lab.py

4. 修改 controlled execution adapter 后：
   先 dry-run，再 targeted tests，再显式 --allow-controlled-execution；不得跳过 dry-run。

5. 修改 Harvest/worker 集成后：
   先 fake worker tests，再生成 hermes chat 命令预览，再真实 Hermes CLI one-shot worker dry-run，最后校验 final decision artifact。
```

每个 worker spec 自带 `verification_after`，表示该 worker 输出后必须执行的检查。任何 worker 输出如果缺 schema、包含 forbidden action、要求 queue/systemd/timer/provider/model 变更，都必须被 deterministic gate 拒绝。

---

## 11. 报告与知识沉淀

### 11.1 每轮报告

写：

```text
artifacts/autonomous_strategy_lab/runs/<run_id>/summary.md
artifacts/autonomous_strategy_lab/latest_decision.md
```

报告包括：

```text
当前证据
失败模式
路线评分
最终 decision
为什么不跑 / 为什么跑
下一步计划
安全限制
```

### 11.2 知识更新

dry-run 默认不更新 knowledge。受控执行完成后，允许写：

```text
knowledge/autonomous_strategy_lessons.md
knowledge/data_requests.json
knowledge/stopped_routes.json
```

但必须先通过：

```text
--write-knowledge
```

并且不得写 Hermes memory，除非形成稳定、跨轮有用的结论。

---

## 12. 实施阶段

## Phase 0：当前原型固化

**目标:** 把已经写好的 dry-run prototype 固化为可验证基线。

当前已有：

```text
scripts/run_autonomous_strategy_lab.py
docs/plans/2026-06-01-autonomous-strategy-lab-redesign-plan.md
artifacts/autonomous_strategy_lab/latest_decision.json
artifacts/autonomous_strategy_lab/latest_decision.md
```

验收命令：

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
PYTHONPATH=src .venv/bin/python -m py_compile scripts/run_autonomous_strategy_lab.py
```

预期：

```text
decision=request_data
queue_write_allowed=false
automation_allowed=false
```

---

## Phase 1：模块化 Evidence / Diagnosis / Decision

**目标:** 从脚本中抽取正式决策模块，建立测试。

当前实现采用一个聚合模块承载第一版最小闭环，后续可继续拆分：

```text
src/factor_lab/autonomous_strategy_lab.py
scripts/run_autonomous_strategy_lab.py
tests/test_autonomous_strategy_lab.py
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_lab.py -q
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
```

当前验证：

```text
5 passed；dry-run 输出 decision=request_data，且 queue_write_allowed=false / automation_allowed=false。
```

---

## Phase 1.5：Hermes CLI worker request / command preview / response gate

**目标:** 在真实 Hermes worker 执行前，先完成 worker request、prompt、命令预览和 response gate，避免 agent 接错到旧 Factor Lab agent/profile/provider 层。

新增：

```text
configs/autonomous_strategy_workers.json
src/factor_lab/autonomous_strategy_worker_requests.py
src/factor_lab/autonomous_strategy_worker_launcher.py
src/factor_lab/autonomous_strategy_worker_responses.py
scripts/prepare_autonomous_strategy_workers.py
```

测试：

```text
tests/test_autonomous_strategy_worker_config.py
tests/test_autonomous_strategy_worker_requests.py
tests/test_autonomous_strategy_worker_launcher.py
tests/test_autonomous_strategy_worker_responses.py
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_autonomous_strategy_worker_config.py \
  tests/test_autonomous_strategy_worker_requests.py \
  tests/test_autonomous_strategy_worker_launcher.py \
  tests/test_autonomous_strategy_worker_responses.py \
  tests/test_autonomous_strategy_lab.py -q

PYTHONPATH=src .venv/bin/python scripts/prepare_autonomous_strategy_workers.py --run-id worker_preview_test
```

必须验证：

```text
runtime_binding=hermes_cli_one_shot
命令预览使用 hermes chat -Q --source factor-lab-worker
不包含 --model / --provider / --resume / --continue / --yolo
prompt 自包含输入 artifact、输出 artifact、forbidden actions、verification_after
response gate 拒绝 forbidden actions 和 model/provider/profile 字段
preview_only，executed=false
```

当前验证：

```text
17 passed；worker_preview_final_check 已生成 4 个 hermes chat 命令预览；
4 个 Hermes CLI one-shot worker 均已真实 dry-run；
worker_verdict.json/md 已写出，consensus_decision=request_data，controlled_execution_allowed=false，queue_write_allowed=false。
```

---

## Phase 2：机制路线注册表与评分

**目标:** 用 Hermes mechanism worker 的 route proposals 生成结构化 route registry，并在字段缺失/blocked 时保持 request_data，不进入 full backtest。

新增：

```text
configs/autonomous_strategy_routes.json
configs/autonomous_strategy_routes.md
src/factor_lab/autonomous_strategy_routes.py
scripts/write_autonomous_strategy_routes.py
tests/test_autonomous_strategy_routes.py
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_routes.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_routes.py --run-id worker_preview_final_check
```

必须验证：

```text
缺字段路线会被 blocked_missing_fields
schema blocked fields 会被 blocked_fields 标记
同路线 drawdown/data blocker 后不会生成可执行 backtest
queue_write_allowed=false
controlled_execution_allowed=false
```

当前验证：

```text
3 passed；生成 3 条路线，3 条均 blocked_missing_fields，cheap_screen_candidate_count=0，controlled_execution_allowed=false。
```

---

## Phase 2.5：Data / Mechanism Request Report

**目标:** 当 worker verdict 与 route registry 都指向 request_data 时，生成正式数据/机制请求报告，而不是进入 cheap screen 或 controlled execution。

新增：

```text
src/factor_lab/autonomous_strategy_data_request_report.py
scripts/write_autonomous_strategy_data_request_report.py
tests/test_autonomous_strategy_data_request_report.py
artifacts/autonomous_strategy_lab/data_request_report.json
artifacts/autonomous_strategy_lab/data_request_report.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_data_request_report.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_data_request_report.py --run-id worker_preview_final_check
```

必须验证：

```text
decision=request_data
field requests 聚合 missing_field 与 blocked_field_provider_support
controlled_execution_allowed=false
queue_write_allowed=false
blocked_actions 包含 same_route_full_backtest_batch / queue_write / timer_enable / broad_daemon_restore / auto_promotion
```

当前验证：

```text
2 passed；data_request_report 已生成；field_request_count=12；blocked_route_count=3；controlled_execution_allowed=false。
```

---

## Phase 2.6：Field Availability Resolver / Data Request Intake

**目标:** 将 data_request_report 中的字段请求按当前 feature schema 分类，区分 already_available、alias_available、derivable_from_available_history、blocked_provider_support_required、external_data_required 等状态；只有字段问题解决后才允许重跑 route registry。

新增：

```text
src/factor_lab/autonomous_strategy_field_resolver.py
scripts/write_autonomous_strategy_field_resolution.py
tests/test_autonomous_strategy_field_resolver.py
artifacts/autonomous_strategy_lab/field_resolution_report.json
artifacts/autonomous_strategy_lab/field_resolution_report.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_field_resolver.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_field_resolution.py --run-id worker_preview_final_check
```

必须验证：

```text
blocked fields 不得被当成 available
derivable history fields 必须声明 source_field
external data fields 必须保持 request_data
ready_for_route_registry_rerun=false 直到 blocking statuses 解决
controlled_execution_allowed=false
queue_write_allowed=false
```

当前验证：

```text
2 passed；field_count=12；external_data_required=9；blocked_provider_support_required=1；derivable_from_available_history=2；ready_for_route_registry_rerun=false。
```

---

## Phase 2.7：Derivable Field Spec / Historical Valuation Preview Unlock

**目标:** 对 `derivable_from_available_history` 字段生成派生规格，并仅在 preview/规划层将这些 spec-only 字段加入 route registry 可用字段集合；不实际物化数据、不写队列、不允许 controlled execution。

新增：

```text
src/factor_lab/autonomous_strategy_field_derivations.py
scripts/write_autonomous_strategy_field_derivations.py
tests/test_autonomous_strategy_field_derivations.py
artifacts/autonomous_strategy_lab/field_derivation_specs.json
artifacts/autonomous_strategy_lab/field_derivation_specs.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_field_derivations.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_field_derivations.py
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_routes.py --run-id worker_preview_final_check
```

当前验证：

```text
2 passed；derived_field_count=2；pb_history_756d 和 pe_ttm_history_756d 均为 spec_only_not_materialized；重跑 route registry 后 historical_relative_valuation_repair 变成 cheap_screen_candidate；仍然 controlled_execution_allowed=false / queue_write_allowed=false。
```

---

## Phase 3：Cheap Screen Planner

**目标:** 对 route registry 中的 `cheap_screen_candidate` 生成 preview-only cheap screen plan；不执行 cheap screen、不写 queue、不允许 controlled execution。

新增：

```text
src/factor_lab/autonomous_strategy_cheap_screen_planner.py
scripts/write_autonomous_strategy_cheap_screen_plan.py
tests/test_autonomous_strategy_cheap_screen_planner.py
artifacts/autonomous_strategy_lab/cheap_screen_plan.json
artifacts/autonomous_strategy_lab/cheap_screen_plan.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_cheap_screen_planner.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_cheap_screen_plan.py --run-id worker_preview_final_check
```

必须验证：

```text
只包含 cheap_screen_candidate 路线
mode=preview_only
execution_status=not_executed
max_backtests_before_review=0
controlled_execution_allowed=false
queue_write_allowed=false
```

当前验证：

```text
2 passed；cheap_screen_plan 已生成；task_count=1；任务为 historical_relative_valuation_repair；controlled_execution_allowed=false；queue_write_allowed=false。
```

---

## Phase 3.5：Historical Valuation Coverage Preflight

**目标:** 在任何 information screen / risk screen / controlled backtest 前，先用本地 Tushare 覆盖缓存验证 `pb_history_756d` 与 `pe_ttm_history_756d` 是否达到 route 要求的 60% ticker 历史覆盖率。

新增：

```text
src/factor_lab/autonomous_strategy_coverage_preflight.py
scripts/write_autonomous_strategy_coverage_preflight.py
tests/test_autonomous_strategy_coverage_preflight.py
artifacts/autonomous_strategy_lab/historical_valuation_coverage_preflight.json
artifacts/autonomous_strategy_lab/historical_valuation_coverage_preflight.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_coverage_preflight.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_coverage_preflight.py --run-id worker_preview_final_check
```

必须验证：

```text
mode=preflight_only
coverage pass/fail 只基于本地缓存和派生 spec
不写 queue
不跑 backtest
不允许 controlled execution / automation
覆盖不足时 next_allowed_actions=request_data_or_extend_cache
```

当前验证：

```text
初次验证时旧 cache 覆盖率为 50/97=0.515464，低于 0.60；Phase 3.6 扩展 cache 后重跑，source_path=artifacts/tushare_cache/tushare_2016-09-09_2023-12-31_97.csv；ticker_count=96；coverage_overall_status=pass；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：coverage blocker 已解除，可以进入实际 information_screen / risk_screen；仍不能直接进入 Phase 4 controlled backtest，因为 cheap_screen_plan 仍是 preview-only，尚未产生 metric-bearing screen 通过结论。

---

## Phase 3.6：Historical Valuation Cache Extension Automation

**目标:** 把 `request_data_or_extend_cache` 从人工提示变成受控、可审计、默认 dry-run 的数据处理自动化步骤；优先复用 covering cache，只有显式允许时才访问 Tushare。

### 是否需要 agent 判断

默认不需要 agent 判断。该阶段应主要是确定性数据工程：

```text
cache 扫描
字段/日期/股票覆盖率计算
缺口定位
生成 fetch plan
可选执行 Tushare 拉取
写 cache
重跑 coverage preflight
```

Agent 只适合参与少数 policy 决策，例如：

```text
是否接受降低 coverage 门槛
是否缩小 universe
是否切换到另一条机制路线
数据源失败时选择人工策略
```

因此 Phase 3.6 的设计原则是：

```text
机械判断用代码
策略取舍由人工/agent 生成建议，但不得自动放行
默认 dry-run
显式 --allow-fetch 才允许外部 API 请求
下载后仍不写 queue、不跑 backtest、不启 timer、不恢复 daemon
```

新增：

```text
src/factor_lab/autonomous_strategy_cache_extension.py
scripts/extend_autonomous_strategy_history_cache.py
tests/test_autonomous_strategy_cache_extension.py
artifacts/autonomous_strategy_lab/cache_extension_plan.json
artifacts/autonomous_strategy_lab/cache_extension_plan.md
```

验收：

```bash
# 默认只写计划，不访问外部 API
PYTHONPATH=src .venv/bin/python scripts/extend_autonomous_strategy_history_cache.py \
  --run-id worker_preview_final_check

# 显式允许时才实际拉取 Tushare
PYTHONPATH=src .venv/bin/python scripts/extend_autonomous_strategy_history_cache.py \
  --run-id worker_preview_final_check \
  --allow-fetch

# 拉取后必须重跑 coverage preflight
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_coverage_preflight.py \
  --run-id worker_preview_final_check
```

必须验证：

```text
dry-run 不发外部请求
优先使用 covering cache
fetch plan 固定 ticker universe / start_date / end_date / cache path
缺 TUSHARE_TOKEN 时给出 request_data_or_configure_token，不崩溃
allow-fetch 成功后只写 cache，不写 queue、不跑 backtest
coverage pass 后才允许进入 information_screen / risk_screen
```

目标状态：

```text
pb_history_756d eligible_ticker_ratio >= 0.60
pe_ttm_history_756d eligible_ticker_ratio >= 0.60
overall_status=pass
```

当前验证：

```text
TUSHARE_TOKEN 已从 .env 加载；--allow-fetch 已实际执行并完成；生成 artifacts/tushare_cache/tushare_2016-09-09_2023-12-31_97.csv；重跑 coverage preflight 后 source_path 切换到新 cache；ticker_count=96；coverage_overall_status=pass；随后重跑 dry-run cache extension 得到 action=no_fetch_needed；queue_write_allowed=false；controlled_execution_allowed=false。
```

结论：Phase 3.6 数据自动化已完成并解除 coverage blocker。当前下一层 blocker 已变为 cheap_screen_plan_disallows_controlled_execution / missing_allow_controlled_execution_flag，需要进入实际 information_screen / risk_screen，或人工决定是否把 cheap-screen planner 从 preview-only 提升为 metric-bearing screen。

---

## Phase 3.7：Metric-bearing Historical Valuation Cheap Screen

**目标:** 把 preview-only cheap screen plan 变成实际 metric-bearing screen；在进入 controlled backtest 前验证历史相对估值 cheapness 是否有信息含量，以及风险代理是否可接受。

新增：

```text
src/factor_lab/autonomous_strategy_cheap_screen_runner.py
scripts/run_autonomous_strategy_cheap_screen.py
tests/test_autonomous_strategy_cheap_screen_runner.py
artifacts/autonomous_strategy_lab/cheap_screen_result.json
artifacts/autonomous_strategy_lab/cheap_screen_result.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_cheap_screen_runner.py -q
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_cheap_screen.py --run-id worker_preview_final_check
```

必须验证：

```text
coverage_overall_status 必须先 pass
计算 pb_history_percentile / pe_ttm_history_percentile
生成 historical_valuation_cheapness 和 valuation_bucket
information_screen 检查 cheap vs expensive forward_return_5d spread / rank_ic
risk_screen 检查 daily cheap-expensive spread drawdown proxy
screen result 不写 queue、不启 timer、不恢复 daemon
只有 information + risk 都 pass 时，才允许 one controlled backtest
```

当前验证：

```text
4 passed；实际 cheap screen 已运行；usable_row_count=55116；usable_ticker_count=81；information_screen_status=pass；cheap_expensive_spread=0.004760050393093048；rank_ic=0.06507747735493304；risk_screen_status=fail；drawdown_proxy=-1.2372644422703525；overall_status=manual_review；recommended_next_step=manual_review_risk；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：历史相对估值 cheapness 有弱正信息，但风险代理严重失败，不能进入 controlled backtest。下一步应做风险层面的人工复核/修复：检查 spread drawdown 发生区间、行业/日期集中度、是否需要更保守 bucket、行业中性或过滤 value trap。

---

## Phase 3.8：Cheap Screen Risk Diagnostic / Repair Probe

**目标:** 解释 Phase 3.7 的 risk_screen 为什么失败，并验证简单修复候选是否足以把路线推进到 controlled backtest。

新增：

```text
src/factor_lab/autonomous_strategy_risk_diagnostic.py
scripts/run_autonomous_strategy_risk_diagnostic.py
tests/test_autonomous_strategy_risk_diagnostic.py
artifacts/autonomous_strategy_lab/cheap_screen_risk_diagnostic.json
artifacts/autonomous_strategy_lab/cheap_screen_risk_diagnostic.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_risk_diagnostic.py -q
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_risk_diagnostic.py --run-id worker_preview_final_check
```

必须验证：

```text
定位 original_drawdown peak/bottom/recovery
列出 worst_dates
列出负贡献行业
评估简单修复候选，如 exclude_negative_industries / moderate_cheap_only
修复候选仍不允许直接 controlled execution，必须人工复核
```

当前验证：

```text
3 passed；risk diagnostic 已运行；original max_drawdown=-1.2372644422703525；peak_date=2021-03-19；bottom_date=2021-10-26；recovery_date=2022-02-28；negative_industry_count=8；best_repair_candidate=exclude_negative_industries；best_repair_mean_daily_spread=0.0037165101540693303；best_repair_max_drawdown=-1.4915107599555961；best_repair_risk_pass=false；overall_status=fail；recommended_next_step=stop_route_or_design_risk_filter。
```

结论：简单排除负贡献行业不仅没有修复风险，drawdown 还更差；当前历史相对估值路线应停止或重新设计风险过滤机制，不能进入 controlled backtest。

---

## Phase 3.9：Route Verdict / Bounded Next Probe Decision

**目标:** 把 Phase 3.5-3.8 的结果压缩成一个 route-level verdict，避免在风险失败后无边界地继续试错。

新增：

```text
src/factor_lab/autonomous_strategy_route_verdict.py
scripts/write_autonomous_strategy_route_verdict.py
tests/test_autonomous_strategy_route_verdict.py
artifacts/autonomous_strategy_lab/route_verdict.json
artifacts/autonomous_strategy_lab/route_verdict.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_route_verdict.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_route_verdict.py --run-id worker_preview_final_check
```

必须验证：

```text
coverage 未 pass -> request_data
information screen fail -> stop_route
information + risk pass -> allow_one_controlled_backtest
information pass / risk fail / simple repair fail / weak positive signal -> 只允许 1 次 risk_filter_probe
queue_write_allowed=false
controlled_execution_allowed=false，除非 information+risk 都 pass
```

当前验证：

```text
5 passed；route verdict 已生成；verdict=design_risk_filter_one_probe；reason_codes=[information_screen_passed, risk_screen_failed, simple_repair_failed, weak_positive_signal_allows_one_more_risk_filter_probe]；max_next_risk_filter_probes=1；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：不直接 stop，也不放行 controlled backtest；只允许再做一次有边界的 value-trap/risk-filter probe。如果失败，下一步应正式 stop route。

---

## Phase 3.10：Value Trap / Risk Filter One-Probe

**目标:** 执行 Phase 3.9 允许的唯一一次有边界风险过滤 probe；若不能修复 drawdown，则正式 stop 当前 historical_relative_valuation_repair 路线。

新增：

```text
src/factor_lab/autonomous_strategy_risk_filter_probe.py
scripts/run_autonomous_strategy_risk_filter_probe.py
tests/test_autonomous_strategy_risk_filter_probe.py
artifacts/autonomous_strategy_lab/value_trap_risk_filter_probe.json
artifacts/autonomous_strategy_lab/value_trap_risk_filter_probe.md
```

候选过滤：

```text
baseline_cheap_vs_expensive
moderate_cheap_only_0.6_to_0.8
exclude_top_30pct_daily_volatility_20
exclude_bottom_30pct_daily_turnover
quality_overlay_roe_top70_debt_bottom70
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_risk_filter_probe.py -q
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_risk_filter_probe.py --run-id worker_preview_final_check
```

必须验证：

```text
只能在 route_verdict=design_risk_filter_one_probe 时运行
candidate_count 有限，不允许继续扩散
每个候选输出 mean_daily_spread / max_drawdown / rank_ic / usable_row_count
risk pass 需要 mean_daily_spread>0 且 max_drawdown>=-0.35
失败后 recommended_next_step=stop_route
仍不得 controlled execution / queue write / timer / daemon / promotion
```

当前验证：

```text
3 passed；实际 risk filter probe 已运行；candidate_count=5；best_candidate=baseline_cheap_vs_expensive；best_candidate_mean_daily_spread=0.0023264342179552472；best_candidate_max_drawdown=-1.2372644422703525；best_candidate_risk_pass=false；overall_status=fail；recommended_next_step=stop_route；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：唯一允许的风险过滤 probe 已失败；当前路线应正式 stop，不应继续增加过滤器或进入 controlled backtest。

---

## Phase 3.11：Stop Route State / Mechanism Handoff

**目标:** 正式关闭 `historical_relative_valuation_repair`，把路线状态从 cheap_screen_candidate 改为 stopped，并把下一机制切换到 `quality_cashflow_distress_filter`。

修改/新增：

```text
configs/autonomous_strategy_routes.json
artifacts/autonomous_strategy_lab/stop_route_state.json
artifacts/autonomous_strategy_lab/stop_route_state.md
artifacts/autonomous_strategy_lab/status_report.json
artifacts/autonomous_strategy_lab/status_report.md
```

当前验证：

```text
historical_relative_valuation_repair route_status=stopped
historical_relative_valuation_repair recommended_next_step=handoff_to_quality_cashflow_distress_filter
quality_cashflow_distress_filter route_status=next_mechanism_candidate
quality_cashflow_distress_filter recommended_next_step=resolve_missing_pit_cashflow_leverage_fields
status_report decision=stop_route
controlled_execution_allowed=false
queue_write_allowed=false
timer_enable_allowed=false
```

结论：当前路线已正式收口；后续不再给 historical_relative_valuation_repair 增加过滤器，进入下一机制路线。

---

## 接下来的阶段：Quality Cashflow Distress Filter 路线

### Phase 4A：Next Mechanism Field Resolution

**目标:** 确认 `quality_cashflow_distress_filter` 缺失字段里哪些可由现有 Tushare/PIT 财务缓存派生，哪些必须 request_data。

新增：

```text
src/factor_lab/autonomous_strategy_distress_field_resolution.py
scripts/write_autonomous_strategy_distress_field_resolution.py
tests/test_autonomous_strategy_distress_field_resolution.py
artifacts/autonomous_strategy_lab/quality_cashflow_distress_field_resolution.json
artifacts/autonomous_strategy_lab/quality_cashflow_distress_field_resolution.md
```

字段：

```text
operating_cashflow_ttm
net_profit_ttm
interest_coverage
debt_to_asset
roe
```

验收：

```text
字段状态分为 available / pit_available / proxy_available_requires_review / missing_external_or_derivation_required
不得把非 PIT 安全字段当可用
仍不写 queue、不跑 backtest
```

当前验证：

```text
3 passed；实际字段解析已运行；decision=resolve_missing_pit_cashflow_leverage_fields；ready_for_distress_screen=false；unresolved_field_count=3；debt_to_asset=pit_available；roe=available；operating_cashflow_ttm=proxy_available_requires_review；net_profit_ttm=proxy_available_requires_review；interest_coverage=missing_external_or_derivation_required；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：下一机制尚不能进入 distress cheap screen；需要 Phase 4B 做 PIT safety/data preflight，决定是否使用 proxy、派生 TTM，或 request_data。


### Phase 4B：Distress Data Extension / PIT Safety Preflight

**目标:** 如果 TTM 现金流、利润、利息覆盖率可以从 PIT 财务表派生，则生成受控数据扩展计划；否则输出 request_data。

新增：

```text
src/factor_lab/autonomous_strategy_distress_pit_preflight.py
scripts/write_autonomous_strategy_distress_pit_preflight.py
tests/test_autonomous_strategy_distress_pit_preflight.py
artifacts/autonomous_strategy_lab/quality_cashflow_distress_pit_preflight.json
artifacts/autonomous_strategy_lab/quality_cashflow_distress_pit_preflight.md
```

验收：

```text
announcement/report date 对齐可解释
缓存覆盖率达标
没有未来函数
```

当前验证：

```text
2 passed；decision=use_proxy_distress_screen_without_interest_coverage；ready_for_proxy_distress_screen=true；ticker_count=77；row_count=50431；controlled_execution_allowed=false；queue_write_allowed=false。
```

### Phase 4C：Distress Cheap Screen

**目标:** 在 cheap/value universe 中测试 distress filter 是否能降低 drawdown proxy，同时保持正 spread。

新增：

```text
src/factor_lab/autonomous_strategy_distress_cheap_screen.py
scripts/run_autonomous_strategy_distress_cheap_screen.py
tests/test_autonomous_strategy_distress_cheap_screen.py
artifacts/autonomous_strategy_lab/quality_cashflow_distress_cheap_screen.json
artifacts/autonomous_strategy_lab/quality_cashflow_distress_cheap_screen.md
```

候选 screen：

```text
baseline
exclude_high_debt_to_asset_top30
exclude_weak_cashflow_to_profit_bottom30
exclude_low_roe_bottom30
combined_debt_cashflow_roe_proxy_filter
```

验收：

```text
mean_daily_spread > 0
max_drawdown_proxy >= -0.35
usable rows 不过度缩水
```

当前验证：

```text
2 passed；overall_status=fail；recommended_next_step=stop_route；best_candidate=baseline；best_mean_daily_spread=0.0023264342179552472；best_max_drawdown=-1.2372644422703525；best_risk_pass=false；controlled_execution_allowed=false；queue_write_allowed=false。
```

### Phase 4D：Route Verdict

**目标:** 如果 distress screen 仍失败，则 stop route；如果通过，才进入 one controlled backtest。

新增：

```text
src/factor_lab/autonomous_strategy_distress_route_verdict.py
scripts/write_autonomous_strategy_distress_route_verdict.py
tests/test_autonomous_strategy_distress_route_verdict.py
artifacts/autonomous_strategy_lab/quality_cashflow_distress_route_verdict.json
artifacts/autonomous_strategy_lab/quality_cashflow_distress_route_verdict.md
```

当前验证：

```text
3 passed；verdict=stop_route；reason_codes=[distress_screen_failed, bounded_proxy_distress_filters_did_not_repair_drawdown]；quality_cashflow_distress_filter route_status=stopped；recommended_next_step=request_new_mechanism_or_external_distress_data；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：第四阶段已完成。`quality_cashflow_distress_filter` 也未能通过 proxy distress screen；当前应请求新机制或外部 distress 数据（尤其 interest_coverage / true TTM cashflow / true TTM net profit），不能进入 controlled backtest。

---

## Phase 5A：New Mechanism / External Data Request

**目标:** 在两条路线均 stop 后，生成给机制研究员/数据处理阶段的约束化请求，防止系统重复提出纯 valuation cheapness 或无 PIT 安全的数据方案。

新增：

```text
src/factor_lab/autonomous_strategy_new_mechanism_request.py
scripts/write_autonomous_strategy_new_mechanism_request.py
tests/test_autonomous_strategy_new_mechanism_request.py
artifacts/autonomous_strategy_lab/new_mechanism_request.json
artifacts/autonomous_strategy_lab/new_mechanism_request.md
```

当前验证：

```text
2 passed；decision=request_new_mechanism_or_external_distress_data；stopped_route_count=2；candidate_next_mechanism_families=[earnings_revision_valuation_repair, balance_sheet_improvement_recovery, cashflow_acceleration_quality_value, industry_cycle_inflection_with_value_anchor]；controlled_execution_allowed=false；queue_write_allowed=false；status_report decision=request_new_mechanism_or_external_distress_data。
```

约束：

```text
不要重复 pure valuation cheapness
不要依赖行业排除作为唯一 drawdown 修复
cheap/risk screen 未通过前不得 controlled backtest
proxy 字段不得冒充 true TTM 字段
```

外部数据请求：

```text
interest_coverage 或 finance_cost + EBIT/EBITDA
true operating_cashflow_ttm + PIT announcement/report-date alignment
true net_profit_ttm + PIT announcement/report-date alignment
earnings forecast/revision fields if switching to earnings_revision_valuation_repair
```

结论：当前 Autonomous Strategy Lab 已完成两条路线的安全探索并收口；下一步应进入新机制研究或补外部 distress/revision 数据。

---

## Phase 5B：Mechanism Researcher Request Pack

**目标:** 把 Phase 5A 的路线失败证据、反重复约束、候选机制族和外部数据需求打包成机制研究员 preview worker 请求；仍不写 queue、不跑 backtest。

新增：

```text
src/factor_lab/autonomous_strategy_mechanism_request_pack.py
scripts/write_autonomous_strategy_mechanism_request_pack.py
tests/test_autonomous_strategy_mechanism_request_pack.py
artifacts/autonomous_strategy_lab/mechanism_researcher_request.json
artifacts/autonomous_strategy_lab/mechanism_researcher_request.md
```

当前验证：

```text
2 passed；decision=send_to_mechanism_researcher；worker_task_count=1；candidate_next_mechanism_families=[earnings_revision_valuation_repair, balance_sheet_improvement_recovery, cashflow_acceleration_quality_value, industry_cycle_inflection_with_value_anchor]；controlled_execution_allowed=false；queue_write_allowed=false；status_report decision=send_to_mechanism_researcher。
```

worker 约束：

```text
只返回区别于 static valuation cheapness 的机制
每个候选必须包含 PIT-safe 字段或 request_data path
每个候选必须定义 cheap-screen falsification test
不得复用 stopped route，除非有新 catalyst 或新数据
```

结论：请求包已准备好；下一步可运行机制研究员 preview worker 生成新路线，然后重新进入 route registry / field resolution。

---

## Phase 5C：Mechanism Researcher Preview Response

**目标:** 基于 Phase 5B 请求包生成下一轮候选机制路线；仍为 preview，不写 queue、不跑 backtest。

新增：

```text
src/factor_lab/autonomous_strategy_mechanism_preview.py
scripts/run_autonomous_strategy_mechanism_preview.py
tests/test_autonomous_strategy_mechanism_preview.py
artifacts/autonomous_strategy_lab/workers/worker_preview_next_mechanism/factor_lab_mechanism_researcher_response.json
artifacts/autonomous_strategy_lab/workers/worker_preview_next_mechanism/factor_lab_mechanism_researcher_response.md
```

当前验证：

```text
1 passed；decision_recommendation=switch_mechanism_route；candidate_count=3；candidate routes=[earnings_revision_valuation_repair_v2, balance_sheet_improvement_recovery_v1, industry_cycle_inflection_value_anchor_v1]；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：下一轮候选机制已生成。下一步应进入 Phase 5D route registry v2，把这三个候选转成 registry 并做字段解析/数据请求。

---

## Phase 5D：Route Registry v2

**目标:** 将机制研究员 preview response 的 3 条候选路线转成新的 route registry，并按可执行性排序；仍不允许 controlled execution / queue write。

新增：

```text
src/factor_lab/autonomous_strategy_routes_v2.py
scripts/write_autonomous_strategy_routes_v2.py
tests/test_autonomous_strategy_routes_v2.py
configs/autonomous_strategy_routes_v2.json
configs/autonomous_strategy_routes_v2.md
```

当前验证：

```text
1 passed；route_count=3；decision_recommendation=run_field_resolution_for_top_candidate；top_route_id=industry_cycle_inflection_value_anchor_v1；controlled_execution_allowed=false；queue_write_allowed=false。
```

排序结果：

```text
1. industry_cycle_inflection_value_anchor_v1 -> field_resolution_candidate / derivable_from_available_market_history
2. balance_sheet_improvement_recovery_v1 -> proxy_review_candidate / proxy_available_requires_review
3. earnings_revision_valuation_repair_v2 -> request_data_candidate / request_data
```

结论：v2 registry 已生成；下一步应对 top route `industry_cycle_inflection_value_anchor_v1` 做字段解析，尤其确认 `industry_return_60d` 是否可由行业日收益派生。

---

## Phase 5E：Top Route Field Resolution

**目标:** 对 v2 top route `industry_cycle_inflection_value_anchor_v1` 做字段解析，确认其是否可进入派生规格阶段。

新增：

```text
src/factor_lab/autonomous_strategy_industry_cycle_field_resolution.py
scripts/write_autonomous_strategy_industry_cycle_field_resolution.py
tests/test_autonomous_strategy_industry_cycle_field_resolution.py
artifacts/autonomous_strategy_lab/industry_cycle_field_resolution.json
artifacts/autonomous_strategy_lab/industry_cycle_field_resolution.md
```

当前验证：

```text
2 passed；decision=prepare_industry_cycle_derivation_specs；route_id=industry_cycle_inflection_value_anchor_v1；ready_for_derivation_specs=true；industry=available；industry_return_60d=derivable；industry_relative_pb=available；industry_relative_earnings_yield=available；date/ticker/forward_return_5d=available；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：top route 字段解析通过；下一步应进入 Phase 5F，生成并物化/预览 `industry_return_60d` 派生特征。

---

## Phase 5F：Industry Cycle Feature Derivation

**目标:** 实际派生 `industry_return_60d`，用于后续 industry-cycle cheap screen；仍不写 queue、不跑 backtest。

新增：

```text
src/factor_lab/autonomous_strategy_industry_cycle_features.py
scripts/write_autonomous_strategy_industry_cycle_features.py
tests/test_autonomous_strategy_industry_cycle_features.py
artifacts/autonomous_strategy_lab/industry_cycle_feature_frame.csv
artifacts/autonomous_strategy_lab/industry_cycle_feature_derivation.json
artifacts/autonomous_strategy_lab/industry_cycle_feature_derivation.md
```

当前验证：

```text
2 passed；industry_return_60d 已派生；row_count=123719；ticker_count=96；coverage_ratio=0.976689；ready_for_industry_cycle_screen=true；controlled_execution_allowed=false；queue_write_allowed=false。
```

结论：行业周期特征已可用；下一步应进入 Phase 5G，运行 `industry_cycle_inflection_value_anchor_v1` 的 cheap/risk screen。

---

## Phase 4：受控执行适配器

**目标:** 只有当 decision/cheap screen/coverage preflight 允许时，才准备极小规模受控 backtest；默认不执行 backtest，只写决策 artifact。

新增：

```text
src/factor_lab/autonomous_strategy_execution_adapter.py
scripts/run_autonomous_strategy_lab_controlled.py
tests/test_autonomous_strategy_execution_adapter.py
artifacts/autonomous_strategy_lab/controlled_execution_decision.json
artifacts/autonomous_strategy_lab/controlled_execution_decision.md
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_execution_adapter.py -q
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab_controlled.py --run-id worker_preview_final_check
```

安全验收：

```text
无 --allow-controlled-execution 时不得跑 backtest
max_backtests 超 policy cap 时失败
coverage_preflight=blocked 时不得跑 backtest
cheap_screen_plan controlled_execution_allowed=false 时不得跑 backtest
任何情况下不得 enable timer/systemd/live trading/auto promotion
```

当前验证：

```text
5 passed；默认 dry-run adapter 已生成 controlled_execution_decision；execution_status=blocked；reason_codes=[missing_allow_controlled_execution_flag, cheap_screen_plan_disallows_controlled_execution, coverage_preflight_blocked]；controlled_execution_started=false；max_backtests_allowed=0；queue_write_allowed=false；timer_enable_allowed=false。
```

结论：Phase 4 安全适配器已完成，但当前数据覆盖和 cheap-screen policy 未放行，所以不会执行受控 backtest。

---

## Phase 5：与 Harvest Controller 低耦合集成

**目标:** Harvest controller 可读取 Autonomous Strategy Lab 的 latest decision，但不能绕过原有安全门。

修改：

```text
src/factor_lab/harvest_autonomous_research_controller.py
scripts/run_harvest_autonomous_research_controller.py
```

新增/扩展测试：

```text
tests/test_harvest_autonomous_research_controller.py
tests/test_run_harvest_autonomous_research_controller.py
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_run_harvest_autonomous_research_controller.py -q
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 1 \
  --max-backtests 10 \
  --use-autonomous-strategy-lab-decision
```

必须验证：

```text
latest decision=request_data -> controller 不执行 same-route backtest
latest decision=manual_review -> controller 停止并写 stop_state
latest decision=continue_route_with_constraints -> 仍受 max_backtests 控制
```

当前验证：

```text
11 passed；真实脚本使用当前 autonomous_strategy_lab controlled_execution_decision 后停止；cycles_run=0；executed_backtest_count=0；stop_reason=autonomous_strategy_lab_request_data；started_systemd_daemon=false；scheduled_timer_enabled=false。
```

结论：Phase 5 已完成。Harvest controller 现在可以显式通过 --use-autonomous-strategy-lab-decision 读取 Autonomous Strategy Lab 决策；当前由于 Phase 3.5 coverage preflight blocked，会在 controller 层停止，不会执行 same-route backtest。

---

## Phase 6：报告、WebUI、知识沉淀

**目标:** 让人能看懂系统为什么跑/不跑。

新增/修改：

```text
src/factor_lab/autonomous_strategy_lab_report.py
scripts/write_autonomous_strategy_lab_report.py
src/factor_lab/webui_app.py
```

新增 API：

```text
/autonomous-strategy-lab/status
```

新增 artifacts：

```text
artifacts/autonomous_strategy_lab/status_report.json
artifacts/autonomous_strategy_lab/status_report.md
```

展示：

```text
latest decision/status
route statuses
coverage field summary
execution reason codes
blocked actions
allowed actions
artifact paths
```

测试：

```text
tests/test_autonomous_strategy_lab_report.py
```

验收：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_lab_report.py -q
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_strategy_lab_report.py
PYTHONPATH=src .venv/bin/python - <<'PY'
from fastapi.testclient import TestClient
from factor_lab import webui_app
client = TestClient(webui_app.app)
r = client.get('/autonomous-strategy-lab/status')
print(r.status_code)
print(r.json().get('status'), r.json().get('decision'))
PY
```

当前验证：

```text
2 passed；status_report 已生成；status=blocked；decision=request_data；coverage_overall_status=blocked；execution_status=blocked；controlled_execution_started=false；WebUI API 返回 200 / blocked request_data。
```

结论：Phase 6 已完成；当前报告清楚显示系统没有跑的原因是 coverage preflight 与 controlled execution gate 阻断。

---

## 13. 第一轮实际运行方案

### 12.1 只允许 dry-run

第一轮只跑：

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
```

当前已跑通，结果：

```text
decision=request_data
selected_route_id=new_mechanism_or_data_request
max_backtests_before_review=0
```

### 12.2 第一轮不允许做的事

```text
不跑 same-route full backtest
不写 research queue
不恢复 broad daemon
不启 timer
不自动 promotion
不放松 drawdown limit
```

### 12.3 第一轮产物

```text
artifacts/autonomous_strategy_lab/latest_decision.json
artifacts/autonomous_strategy_lab/latest_decision.md
```

---

## 14. 验收标准

### 13.1 工程验收

```text
py_compile pass
targeted pytest pass
script dry-run exit 0
artifacts schema valid
```

### 13.2 决策验收

当前项目状态下，系统应输出：

```text
decision=request_data 或 manual_review
```

而不是：

```text
continue_route_with_constraints
```

因为当前证据已经表明：

```text
同一路线继续跑低信息增益，且回撤约束未满足。
```

### 13.3 安全验收

所有 artifacts 必须包含：

```json
{
  "queue_write_allowed": false,
  "automation_allowed": false,
  "live_trading_enabled": false,
  "timer_enable_allowed": false,
  "systemd_change_allowed": false,
  "auto_promotion_allowed": false
}
```

---

## 15. 风险与规避

### 风险 1：新系统又变成另一个复杂 Agent

规避：

```text
第一版必须 deterministic，不调用 LLM 生成因子代码。
```

### 风险 2：它只会输出“不跑”

规避：

```text
不跑必须伴随具体 next_plan：request_data、switch_route、cheap_screen、manual_review。
```

### 风险 3：绕过现有安全门

规避：

```text
Autonomous Strategy Lab 只产生 decision artifact，执行必须经过 controlled adapter。
```

### 风险 4：信息增益评分主观

规避：

```text
第一版用显式规则和 reason_codes；后续再考虑学习型权重。
```

### 风险 5：继续在弱数据上自嗨

规避：

```text
缺字段或 repeated insufficient_data 时，优先 request_data，而不是虚构实验。
```

---

## 16. 建议立即执行的下一步

按优先级：

1. 将当前 prototype 拆成正式模块；
2. 加测试锁住当前 `request_data` 判断；
3. 建立 `configs/autonomous_strategy_routes.json`；
4. 实现 cheap screen；
5. 只在 cheap screen 通过后允许最多 5 个受控 backtest；
6. 再考虑接入 Harvest controller。

推荐第一批命令：

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
PYTHONPATH=src .venv/bin/python -m py_compile scripts/run_autonomous_strategy_lab.py
```

然后进入 Phase 1 TDD。

---

## 17. 方案结论

新系统不应该追求“更自动地跑更多因子”，而应该追求：

```text
更自动地判断哪些研究不值得跑。
```

当前阶段，正确的自主判断是：

```text
停止同路线批量回测；
输出 blocker；
请求新机制或新数据；
只允许 cheap screen；
通过后才允许极小规模 controlled backtest。
```

这才是 Factor Lab 下一阶段应该实现的自动化能力。
