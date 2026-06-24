# Phenomenon-First Autonomous Research Agent Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. Keep all first-pass runs artifact-only/dry-run. Do not enable timers, daemons, queue writes, live trading, auto-promotion, model/provider pinning, or controlled backtests without explicit human approval.

**Goal:** 把 Factor Lab 自主化 agent 从“策略/因子生成器”改造成“可验证市场现象发现与最小实验验证器”。

**Architecture:** 在现有 Autonomous Strategy Lab controller/dispatcher 前增加 phenomenon-first 研究层：先生成市场现象、做新颖性与可观测性审查、生成最小实验计划、运行条件分布诊断、写 phenomenon verdict；只有 phenomenon 通过最小验证，才允许进入 strategy route / cheap screen / controlled experiment。现有安全边界不放松：data/cache/artifact/diagnostic/plumbing 可自动推进，controlled backtest、queue、timer、daemon、promotion、live trading 继续阻断。

**Tech Stack:** Python 3, JSON/Markdown artifacts, pytest, existing Factor Lab ASL modules under `src/factor_lab/`, scripts under `scripts/`, artifacts under `artifacts/autonomous_strategy_lab/`, knowledge under `knowledge/`.

---

## 0. Design Position / 设计立场

当前失败不应继续解释为“agent 产出策略不够多”。新的目标函数是：

```text
发现可交易市场现象 -> 写可证伪假设 -> 设计最小实验 -> 验证条件分布变化 -> 写机制 verdict -> 决定是否进入策略/route
```

不是：

```text
生成更多策略/因子 -> 回测 -> 找 pass
```

核心原则：

1. **Agent 是研究员，不是指标厨师。**
2. **产物是 phenomenon artifact，不是 strategy artifact。**
3. **先验证市场现象，再考虑交易规则。**
4. **回测数量不是 KPI；高质量、可证伪、可沉淀的现象发现率才是 KPI。**
5. **每个现象必须说明参与者、约束、错误定价窗口、可观测变量、失效条件、最小实验。**
6. **禁止传统指标作为核心逻辑。指标只能作为市场机制代理变量。**

---

## 1. Target Research Loop

新循环：

```text
controller_state
  -> choose_mechanism_source
  -> generate_market_phenomena
  -> score_and_filter_phenomena
  -> novelty_review_against_memory
  -> data_feasibility_check
  -> minimal_experiment_plan
  -> run_minimal_experiment
  -> phenomenon_verdict
  -> update_phenomenon_memory
  -> choose_next_action
```

只有当 `phenomenon_verdict.verdict` 是以下状态之一时，才允许进入 route/strategy 层：

```text
supported_ready_for_route_design
partial_support_requires_regime_filter
partial_support_requires_portfolio_repair
```

以下状态禁止进入策略/回测：

```text
rejected_no_mechanism
rejected_not_observable
rejected_duplicate
rejected_no_mispricing_window
rejected_failed_minimal_experiment
blocked_missing_data
blocked_human_review_required
```

---

## 2. New Artifact Contract

### 2.1 `phenomenon_ideas.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/phenomenon_ideas.json
artifacts/autonomous_strategy_lab/phenomenon_ideas.md
```

Required JSON shape:

```json
{
  "schema_version": 1,
  "run_id": "phenomenon_YYYYMMDDTHHMMSSZ",
  "market": "cn_equity_daily",
  "generated_at_utc": "...",
  "mode": "artifact_only",
  "source_context": {
    "latest_controller_state": "proxy_workstream_completed_failed_alpha",
    "latest_failed_routes": ["quality_profit_proxy_value_repair_v1", "industry_cycle_inflection_value_anchor_v1"]
  },
  "mechanism_sources": ["institutional_constraints", "delayed_repricing", "liquidity_gap", "risk_transfer", "crowding_reflexivity"],
  "ideas": [
    {
      "phenomenon_id": "quality_repair_delayed_repricing_v1",
      "phenomenon_name": "盈利质量修复后的延迟重估",
      "participants": ["低频基本面资金", "卖方覆盖不足股票", "行业轮动资金"],
      "constraint_source": ["信息处理延迟", "财报可信度折价", "行业风险偏好约束"],
      "mispricing_mechanism": "...",
      "why_not_arbitraged_away": "...",
      "observable_variables": ["profit_yoy", "roe", "debt_to_asset", "operating_cashflow_to_profit", "pb", "industry_return_60d"],
      "prediction_target": "future_60d_relative_return",
      "holding_horizon": "60d/120d",
      "market_regime": ["industry_beta_non_negative", "post_report_window"],
      "failure_conditions": ["cashflow_quality_not_confirmed", "valuation_already_repriced", "industry_regime_negative"],
      "minimal_experiment": "Compare future return/downside-risk distribution for quality repair + low valuation vs matched controls.",
      "traditional_indicator_difference": "Not PB/ROE screening; PB/ROE are proxies for delayed repricing after quality repair.",
      "scores": {
        "mechanism_strength": 8,
        "observability": 7,
        "tradability": 6,
        "novelty": 6,
        "crowding_risk": 5,
        "overfit_risk": 4,
        "cost_sensitivity": 5,
        "total_score": 19.0
      },
      "hard_gate_status": "candidate"
    }
  ],
  "controlled_execution_allowed": false,
  "queue_write_allowed": false,
  "timer_enable_allowed": false
}
```

Hard gates:

- no participants -> reject
- no constraint source -> reject
- no mispricing mechanism -> reject
- no `why_not_arbitraged_away` -> reject
- no observable variables -> reject
- no minimal experiment -> reject
- core logic is RSI/MACD/Bollinger/KDJ/MA-cross/grid/martingale/turtle -> reject

### 2.2 `phenomenon_novelty_review.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/phenomenon_novelty_review.json
artifacts/autonomous_strategy_lab/phenomenon_novelty_review.md
```

Purpose: compare new ideas against `knowledge/phenomenon_memory.json`, existing route configs, and recent failed artifacts.

JSON fields:

```json
{
  "schema_version": 1,
  "reviewed_ideas": [
    {
      "phenomenon_id": "quality_repair_delayed_repricing_v1",
      "similar_existing_ideas": [],
      "mechanism_similarity_max": 0.42,
      "novelty_score": 0.74,
      "duplicate_status": "not_duplicate",
      "decision": "keep_for_data_feasibility"
    }
  ],
  "rejected_duplicates": [],
  "controlled_execution_allowed": false,
  "queue_write_allowed": false
}
```

### 2.3 `phenomenon_data_feasibility.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/phenomenon_data_feasibility.json
artifacts/autonomous_strategy_lab/phenomenon_data_feasibility.md
```

Purpose: determine whether current caches/data can observe each phenomenon without future leakage.

Checks:

1. required fields exist or are derivable;
2. PIT alignment requirements;
3. ticker/date coverage;
4. horizon availability;
5. leakage risk;
6. minimum usable rows/tickers;
7. data blocker write-back if missing.

### 2.4 `minimal_experiment_plan.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/minimal_experiment_plan.json
artifacts/autonomous_strategy_lab/minimal_experiment_plan.md
```

Purpose: turn supported phenomenon into one or more condition-distribution experiments, not trading rules.

Example shape:

```json
{
  "schema_version": 1,
  "phenomenon_id": "quality_repair_delayed_repricing_v1",
  "experiment_type": "conditional_distribution_test",
  "condition_variables": ["profit_yoy_change", "debt_to_asset_change", "pb_industry_percentile"],
  "target_variables": ["future_60d_return", "future_60d_downside_risk", "future_60d_max_drawdown"],
  "comparison_groups": ["quality_repair_low_valuation", "low_quality_low_valuation", "quality_repair_high_valuation"],
  "success_criteria": {
    "return_spread_positive": true,
    "drawdown_below_limit": true,
    "regime_stability_required": true,
    "minimum_usable_tickers": 50,
    "minimum_daily_observations": 250
  },
  "forbidden_outputs": ["buy_rule", "sell_rule", "position_size", "queue_write"],
  "controlled_execution_allowed": false,
  "queue_write_allowed": false
}
```

### 2.5 `minimal_experiment_result.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/minimal_experiment_result.json
artifacts/autonomous_strategy_lab/minimal_experiment_result.md
```

Metrics:

- group counts;
- ticker counts;
- coverage;
- future return distribution by group;
- downside risk by group;
- max drawdown proxy by group;
- regime split stability;
- beta/industry exposure warnings;
- cost/turnover sensitivity proxy if available;
- pass/fail against success criteria.

### 2.6 `phenomenon_verdict.{json,md}`

Path:

```text
artifacts/autonomous_strategy_lab/phenomenon_verdict.json
artifacts/autonomous_strategy_lab/phenomenon_verdict.md
```

Purpose: convert experiment result into research decision.

Allowed verdicts:

```text
supported_ready_for_route_design
partial_support_requires_regime_filter
partial_support_requires_portfolio_repair
blocked_missing_data
rejected_failed_minimal_experiment
rejected_duplicate
rejected_no_mechanism
manual_review_required
```

---

## 3. Files to Create / Modify

Create:

```text
src/factor_lab/autonomous_strategy_phenomenon.py
src/factor_lab/autonomous_strategy_phenomenon_memory.py
src/factor_lab/autonomous_strategy_phenomenon_novelty.py
src/factor_lab/autonomous_strategy_phenomenon_data_feasibility.py
src/factor_lab/autonomous_strategy_minimal_experiment_plan.py
src/factor_lab/autonomous_strategy_minimal_experiment_runner.py
src/factor_lab/autonomous_strategy_phenomenon_verdict.py

scripts/write_autonomous_strategy_phenomenon_ideas.py
scripts/write_autonomous_strategy_phenomenon_novelty_review.py
scripts/write_autonomous_strategy_phenomenon_data_feasibility.py
scripts/write_autonomous_strategy_minimal_experiment_plan.py
scripts/run_autonomous_strategy_minimal_experiment.py
scripts/write_autonomous_strategy_phenomenon_verdict.py

knowledge/phenomenon_memory.json
knowledge/phenomenon_lessons.md

tests/test_autonomous_strategy_phenomenon.py
tests/test_autonomous_strategy_phenomenon_memory.py
tests/test_autonomous_strategy_phenomenon_novelty.py
tests/test_autonomous_strategy_phenomenon_data_feasibility.py
tests/test_autonomous_strategy_minimal_experiment_plan.py
tests/test_autonomous_strategy_minimal_experiment_runner.py
tests/test_autonomous_strategy_phenomenon_verdict.py
```

Modify:

```text
src/factor_lab/autonomous_strategy_controller.py
src/factor_lab/autonomous_strategy_dispatcher.py
tests/test_autonomous_strategy_controller.py
tests/test_autonomous_strategy_dispatcher.py
knowledge/data_blockers.json
```

Do **not** modify:

```text
Hermes provider/model settings
queue/timer/daemon runtime configuration
live trading settings
risk thresholds
```

---

## 4. Implementation Tasks

### Task 1: Add phenomenon schema and hard gates

**Objective:** Create the core dataclasses/functions for market phenomenon ideas and reject low-quality indicator/strategy outputs.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_phenomenon.py`
- Test: `tests/test_autonomous_strategy_phenomenon.py`

**Implementation notes:**

Functions:

```python
FORBIDDEN_CORE_LOGIC_TERMS = [
    "RSI", "MACD", "布林", "Bollinger", "KDJ", "均线金叉", "均线死叉",
    "MA cross", "grid", "网格", "martingale", "马丁格尔", "turtle", "海龟",
]

def score_phenomenon(idea: dict) -> dict: ...
def validate_phenomenon_hard_gates(idea: dict) -> dict: ...
def build_phenomenon_ideas_report(...): ...
def render_phenomenon_ideas_markdown(report: dict) -> str: ...
```

Tests:

1. valid idea passes hard gates;
2. no participants rejects;
3. no `why_not_arbitraged_away` rejects;
4. RSI/MACD as core logic rejects;
5. total score follows configured formula.

Run:

```bash
pytest tests/test_autonomous_strategy_phenomenon.py -q
```

Expected:

```text
5 passed
```

---

### Task 2: Write phenomenon idea generator artifact

**Objective:** Produce deterministic starter phenomenon ideas from current failed ASL state without LLM/provider dependency.

**Files:**

- Create: `scripts/write_autonomous_strategy_phenomenon_ideas.py`
- Modify: `src/factor_lab/autonomous_strategy_phenomenon.py`
- Test: `tests/test_autonomous_strategy_phenomenon.py`

**Starter mechanism sources:**

```text
institutional_constraints
delayed_repricing
liquidity_gap
risk_transfer
crowding_reflexivity
information_delay
portfolio_rebalancing
coverage_neglect
```

**Starter CN equity ideas:**

1. `quality_repair_delayed_repricing_v1`
2. `value_trap_escape_after_balance_sheet_repair_v1`
3. `industry_cycle_recovery_confirmation_v1`
4. `coverage_neglect_post_report_drift_v1`
5. `liquidity_discount_reversal_after_volume_recovery_v1`

Script behavior:

```bash
python scripts/write_autonomous_strategy_phenomenon_ideas.py
```

Writes:

```text
artifacts/autonomous_strategy_lab/phenomenon_ideas.json
artifacts/autonomous_strategy_lab/phenomenon_ideas.md
```

Expected artifact safety:

```json
{
  "controlled_execution_allowed": false,
  "queue_write_allowed": false,
  "timer_enable_allowed": false
}
```

---

### Task 3: Add phenomenon memory

**Objective:** Persist idea history and failure lessons so the agent does not repeat equivalent mechanisms.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_phenomenon_memory.py`
- Create: `knowledge/phenomenon_memory.json`
- Create: `knowledge/phenomenon_lessons.md`
- Test: `tests/test_autonomous_strategy_phenomenon_memory.py`

Memory JSON shape:

```json
{
  "schema_version": 1,
  "ideas": [],
  "updated_at_utc": null
}
```

Functions:

```python
def load_phenomenon_memory(path: str | Path) -> dict: ...
def upsert_phenomenon_memory(memory: dict, verdict: dict) -> dict: ...
def write_phenomenon_memory(memory: dict, path: str | Path) -> None: ...
```

Tests:

1. missing memory returns empty schema;
2. upsert adds new phenomenon;
3. upsert updates existing status/failure reasons;
4. write/read roundtrip preserves schema.

---

### Task 4: Add novelty checker

**Objective:** Reject duplicate or low-information ideas by comparing mechanism, participants, variables, and failed routes.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_phenomenon_novelty.py`
- Create: `scripts/write_autonomous_strategy_phenomenon_novelty_review.py`
- Test: `tests/test_autonomous_strategy_phenomenon_novelty.py`

Similarity heuristic v1:

```text
mechanism token Jaccard
+ participants overlap
+ observable variable overlap
+ same prediction target
```

Decisions:

```text
keep_for_data_feasibility
reject_duplicate
manual_review_low_novelty
```

Thresholds:

```text
similarity >= 0.70 -> reject_duplicate
0.55 <= similarity < 0.70 -> manual_review_low_novelty
similarity < 0.55 -> keep_for_data_feasibility
```

Script reads:

```text
artifacts/autonomous_strategy_lab/phenomenon_ideas.json
knowledge/phenomenon_memory.json
configs/autonomous_strategy_routes*.json
artifacts/autonomous_strategy_lab/*verdict*.json
```

Writes:

```text
artifacts/autonomous_strategy_lab/phenomenon_novelty_review.json
artifacts/autonomous_strategy_lab/phenomenon_novelty_review.md
```

---

### Task 5: Add data feasibility checker

**Objective:** Determine which phenomena can be tested with current caches without data leakage.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_phenomenon_data_feasibility.py`
- Create: `scripts/write_autonomous_strategy_phenomenon_data_feasibility.py`
- Test: `tests/test_autonomous_strategy_phenomenon_data_feasibility.py`

Checks:

1. required fields present;
2. required fields derivable;
3. PIT alignment required for financial fields;
4. minimum coverage threshold;
5. minimum ticker/date overlap;
6. target horizon exists;
7. no future fields in condition variables;
8. missing fields append/update `knowledge/data_blockers.json`.

Decisions:

```text
ready_for_minimal_experiment_plan
blocked_missing_data
blocked_low_coverage
blocked_leakage_risk
manual_review_required
```

---

### Task 6: Add minimal experiment planner

**Objective:** Convert feasible phenomena into condition-distribution experiment plans, not buy/sell strategies.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_minimal_experiment_plan.py`
- Create: `scripts/write_autonomous_strategy_minimal_experiment_plan.py`
- Test: `tests/test_autonomous_strategy_minimal_experiment_plan.py`

Rules:

- No `buy`, `sell`, `position`, `rebalance`, `portfolio_weight` fields.
- Only condition variables and target variables.
- Must include comparison groups.
- Must include success criteria and falsification criteria.
- Must include `controlled_execution_allowed=false`, `queue_write_allowed=false`.

Supported experiment type v1:

```text
conditional_distribution_test
```

---

### Task 7: Add minimal experiment runner

**Objective:** Run a bounded statistical diagnostic over existing feature frames/caches and write result artifacts.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_minimal_experiment_runner.py`
- Create: `scripts/run_autonomous_strategy_minimal_experiment.py`
- Test: `tests/test_autonomous_strategy_minimal_experiment_runner.py`

Runner constraints:

- no queue writes;
- no controlled backtest;
- no timer/daemon;
- no strategy order generation;
- input is an experiment plan artifact;
- output is conditional distribution summary only.

Metrics:

```text
usable_row_count
usable_ticker_count
group_count
future_return_mean_by_group
future_return_median_by_group
future_downside_risk_by_group
future_max_drawdown_proxy_by_group
spread_between_target_and_control
rank_ic_optional
industry_beta_warning
coverage_warnings
pass_fail_against_success_criteria
```

---

### Task 8: Add phenomenon verdict writer

**Objective:** Convert minimal experiment results into a research verdict and recommended next action.

**Files:**

- Create: `src/factor_lab/autonomous_strategy_phenomenon_verdict.py`
- Create: `scripts/write_autonomous_strategy_phenomenon_verdict.py`
- Test: `tests/test_autonomous_strategy_phenomenon_verdict.py`

Verdict logic v1:

```text
if missing_data -> blocked_missing_data
elif experiment failed all return/risk criteria -> rejected_failed_minimal_experiment
elif return positive but drawdown failed -> partial_support_requires_portfolio_repair
elif return positive only in one regime -> partial_support_requires_regime_filter
elif return/risk/regime pass -> supported_ready_for_route_design
else -> manual_review_required
```

Must write `do_not_do` guidance, e.g.:

```text
do_not_generate_more_pb_pe_variants
```

---

### Task 9: Integrate phenomenon states into controller

**Objective:** Make controller prefer phenomenon-first next steps after a failed route/workstream.

**Files:**

- Modify: `src/factor_lab/autonomous_strategy_controller.py`
- Modify: `tests/test_autonomous_strategy_controller.py`

New artifact priority should place phenomenon verdict/state above old route retry logic after failed alpha:

```python
ARTIFACT_PRIORITY = [
    "phenomenon_verdict.json",
    "minimal_experiment_result.json",
    "minimal_experiment_plan.json",
    "phenomenon_data_feasibility.json",
    "phenomenon_novelty_review.json",
    "phenomenon_ideas.json",
    ...existing artifacts...
]
```

New states:

```text
need_phenomenon_ideas
phenomenon_ideas_ready
phenomenon_novelty_review_ready
phenomenon_data_feasibility_ready
minimal_experiment_plan_ready
minimal_experiment_completed
phenomenon_supported_route_design_allowed
phenomenon_rejected_request_new_phenomenon
phenomenon_blocked_missing_data
```

Expected current transition from today’s state:

```text
proxy_workstream_completed_failed_alpha
  -> recommended_next_step = write_phenomenon_ideas
```

not:

```text
write_new_mechanism_request_v2
```

unless phenomenon layer rejects or requests new mechanism source.

---

### Task 10: Integrate safe actions into dispatcher

**Objective:** Allow dispatcher to autonomously advance safe phenomenon pipeline steps.

**Files:**

- Modify: `src/factor_lab/autonomous_strategy_dispatcher.py`
- Modify: `tests/test_autonomous_strategy_dispatcher.py`

Add safe actions:

```python
"write_phenomenon_ideas": ["python", "scripts/write_autonomous_strategy_phenomenon_ideas.py"],
"write_phenomenon_novelty_review": ["python", "scripts/write_autonomous_strategy_phenomenon_novelty_review.py"],
"write_phenomenon_data_feasibility": ["python", "scripts/write_autonomous_strategy_phenomenon_data_feasibility.py"],
"write_minimal_experiment_plan": ["python", "scripts/write_autonomous_strategy_minimal_experiment_plan.py"],
"run_minimal_experiment": ["python", "scripts/run_autonomous_strategy_minimal_experiment.py"],
"write_phenomenon_verdict": ["python", "scripts/write_autonomous_strategy_phenomenon_verdict.py"],
```

Keep blocked words:

```text
backtest
queue
timer
daemon
live_trading
auto_promotion
```

Add tests:

1. registered phenomenon action executes in safe mocked subprocess path;
2. unsafe controller state still blocks;
3. action names containing `backtest` still block;
4. generated dispatch report keeps all execution gates false.

---

### Task 11: Add bounded dispatch loop artifact

**Objective:** Stop requiring manual “next step” for each safe phenomenon pipeline transition.

**Files:**

- Create: `scripts/run_autonomous_strategy_dispatch_loop.py`
- Modify: `src/factor_lab/autonomous_strategy_dispatcher.py`
- Test: `tests/test_autonomous_strategy_dispatcher.py`

Loop behavior:

```text
for step in range(max_steps):
  run controller
  if recommended_next_step is registered safe action:
    dispatch_once
    continue
  else:
    stop and write reason
```

Defaults:

```text
--max-steps 6
--max-seconds-per-step 300
--artifact-dir artifacts/autonomous_strategy_lab
```

Stop reasons:

```text
completed_no_next_safe_action
blocked_no_registered_safe_action
blocked_unsafe_controller_state
blocked_human_required
failed_command
max_steps_reached
```

Writes:

```text
artifacts/autonomous_strategy_lab/dispatch_loop.json
artifacts/autonomous_strategy_lab/dispatch_loop.md
```

---

### Task 12: Add docs and operational policy

**Objective:** Document the new agent contract so future work does not regress into strategy generation.

**Files:**

- Create: `docs/ops/phenomenon-first-autonomous-research-agent.md`
- Modify: `docs/ops/harvest-agent.md` if it currently implies strategy-count objectives.
- Modify: `knowledge/harvest_agent.md` if it currently emphasizes strategy/factor production.

Must state:

```text
Daily KPI is not strategies/backtests produced.
Primary KPI is verified phenomenon backlog quality and falsification velocity.
```

---

## 5. Verification Plan

Targeted tests after each task:

```bash
pytest tests/test_autonomous_strategy_phenomenon.py -q
pytest tests/test_autonomous_strategy_phenomenon_memory.py -q
pytest tests/test_autonomous_strategy_phenomenon_novelty.py -q
pytest tests/test_autonomous_strategy_phenomenon_data_feasibility.py -q
pytest tests/test_autonomous_strategy_minimal_experiment_plan.py -q
pytest tests/test_autonomous_strategy_minimal_experiment_runner.py -q
pytest tests/test_autonomous_strategy_phenomenon_verdict.py -q
pytest tests/test_autonomous_strategy_controller.py tests/test_autonomous_strategy_dispatcher.py -q
```

Subset verification:

```bash
pytest tests/test_autonomous_strategy_*.py -q
```

Artifact smoke test:

```bash
python scripts/run_autonomous_strategy_controller_once.py
python scripts/run_autonomous_strategy_dispatch_loop.py --max-steps 6
python scripts/run_autonomous_strategy_controller_once.py
```

Expected end state after smoke test:

```text
controller current_state is one of:
- phenomenon_supported_route_design_allowed
- phenomenon_rejected_request_new_phenomenon
- phenomenon_blocked_missing_data
- partial_support_requires_regime_filter
- partial_support_requires_portfolio_repair

controlled_execution_allowed: false
queue_write_allowed: false
timer_enable_allowed: false
```

Full safety check:

```bash
python scripts/verify_de_hermes_native_runtime.py
python scripts/verify_de_openclaw_runtime.py
```

Expected: no provider/model/profile regression; no legacy Agent/OpenClaw wiring restored.

---

## 6. Acceptance Criteria

Implementation is complete only when:

1. `phenomenon_ideas.{json,md}` exists and contains valid phenomenon artifacts, not strategy rules.
2. Traditional indicator core logic is rejected by hard gates.
3. Novelty review rejects duplicated/equivalent mechanisms.
4. Data feasibility distinguishes missing source fields, low coverage, PIT alignment, and leakage risk.
5. Minimal experiment plan contains condition-distribution tests, not buy/sell rules.
6. Minimal experiment result summarizes distribution/risk/regime evidence.
7. Phenomenon verdict writes clear next action and `do_not_do` guidance.
8. Controller prioritizes phenomenon-first workflow after failed alpha routes.
9. Dispatcher can automatically advance safe phenomenon steps without human step-by-step prompting.
10. All artifacts keep:

```text
controlled_execution_allowed=false
queue_write_allowed=false
timer_enable_allowed=false
```

11. Tests pass for new modules and existing ASL controller/dispatcher subset.
12. Docs state that the agent optimizes for market phenomenon discovery, not strategy/backtest count.

---

## 7. Non-Goals

This phase does **not**:

- restore broad daemon;
- enable timer;
- write to research queue;
- run controlled backtests;
- auto-promote any candidate;
- change live trading settings;
- change model/provider/profile configuration;
- relax drawdown or risk thresholds;
- optimize for daily strategy count;
- generate buy/sell rules before minimal experiment verdict.

---

## 8. Suggested Execution Order

Recommended order:

```text
Task 1 -> Task 2 -> Task 3 -> Task 4 -> Task 5 -> Task 6 -> Task 7 -> Task 8 -> Task 9 -> Task 10 -> Task 11 -> Task 12
```

Do not start Task 9 controller integration until Tasks 1-8 have artifact-producing scripts and tests.

Do not start Task 11 dispatch loop until Task 10 proves one-step safe dispatch.

Do not generate route/strategy code until a `phenomenon_verdict.json` explicitly allows route design.

---

## 9. Reporting Template After Execution

Every execution report should use this shape:

```text
Phase/status:
Completed:
Remaining:
Engineering outcome:
Research/alpha outcome:
Verification commands and real results:
Latest controller state:
Latest artifacts:
Next recommended step:
human_required:
Blocked actions still blocked:
```

Separate engineering success from research success. A passing test suite does not mean alpha success; it only means the phenomenon-first research machinery works.
