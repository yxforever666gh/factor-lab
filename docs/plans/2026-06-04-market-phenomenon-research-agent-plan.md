# Market Phenomenon Research Agent Implementation Plan

> **For Hermes:** This is a new standalone plan. Do not treat it as a continuation of the previous Autonomous Strategy Lab plan. Do not name the system “Autonomous Strategy”, do not optimize for strategy count, and do not generate trading strategies as the first-class artifact. Use subagent-driven-development skill only after this plan is explicitly approved for execution.

**Goal:** Build an autonomous market-phenomenon research agent that discovers non-trivial market phenomena/mechanisms/factors, validates them, runs controlled research/backtest steps when evidence allows, learns from drawdowns/results, and iteratively improves the research direction. It must not optimize for low-level technical-analysis recipes or strategy count.

**Architecture:** Create a separate research layer under Factor Lab focused on phenomenon discovery and autonomous research iteration, not automatic live trading. The system produces structured phenomenon artifacts, novelty reviews, data-feasibility checks, minimal statistical experiment plans, phenomenon verdicts, controlled research/backtest handoffs, result diagnostics, and iteration proposals. Existing backtest/route machinery may be used downstream after research gates pass; live trading, auto-promotion, and production execution remain blocked without explicit approval.

**Tech Stack:** Python 3, JSON/Markdown artifacts, pytest, existing Factor Lab data/cache utilities where available, docs under `docs/`, knowledge files under `knowledge/`, reports under `artifacts/market_phenomena/`.

---

## 0. Why This Is a New Plan

The previous plan framed the work around the existing `autonomous_strategy_*` namespace. That is the wrong framing for the revised objective.

This plan intentionally avoids:

```text
autonomous_strategy
strategy generator
route generator
automatic strategy
daily strategy production
backtest factory
```

The new system name is:

```text
Market Phenomenon Research Agent
```

The primary object is:

```text
market phenomenon
```

not:

```text
strategy
factor route
trading rule
```

The agent’s job is to answer:

```text
What verifiable market phenomenon may exist here?
Who causes it?
Why can it persist temporarily?
How can we observe it?
What minimal experiment can falsify it?
```

not:

```text
What strategy should we trade?
```

---

## 1. Product Definition

### 1.1 Agent Role

The agent is a **researcher**.

It should behave like a junior quant researcher that is forced to write a research memo before proposing a strategy.

It must not behave like:

- an indicator chef;
- a factor expression generator;
- a technical-analysis prompt machine;
- an automatic strategy/backtest loop;
- a daily strategy production bot.

### 1.2 First-Class Artifact

The first-class artifact is a market phenomenon:

```text
A repeatable, observable market behavior caused by identifiable participants, constraints, information frictions, liquidity effects, institutional rules, or risk-transfer needs, which may create a temporary and testable distributional anomaly.
```

### 1.3 Non-Goal

This plan does **not** build an automatic strategy system.

It does not:

- optimize for number of strategies produced;
- produce low-level technical-analysis recipes as the research target;
- treat Bollinger/MA/RSI/MACD-style ideas as acceptable core mechanisms;
- change live-trading behavior;
- auto-promote candidates to production;
- replace human approval around production/live execution.

It **does** aim to automate the research loop:

- discover market phenomena/mechanisms/factors;
- validate them with data;
- run controlled research/backtest steps after evidence gates pass;
- diagnose drawdown/failed results;
- iterate hypotheses, conditions, and factor definitions based on evidence.

---

## 2. Research Workflow

The workflow is:

```text
Mechanism source
  -> Market phenomenon idea
  -> Hard quality gates
  -> Novelty review
  -> Data feasibility check
  -> Minimal verification experiment plan
  -> Minimal verification result
  -> Phenomenon verdict
  -> Phenomenon memory update
  -> Optional downstream research handoff
```

The downstream handoff is **not** automatic strategy execution. It is a research handoff artifact saying whether a phenomenon deserves future route/strategy design.

---

## 3. Directory and Naming Rules

Use a new namespace.

Create artifacts under:

```text
artifacts/market_phenomena/
```

Create source files under:

```text
src/factor_lab/market_phenomena_*.py
```

Create scripts under:

```text
scripts/market_phenomena_*.py
```

Create tests under:

```text
tests/test_market_phenomena_*.py
```

Create knowledge files under:

```text
knowledge/market_phenomena_memory.json
knowledge/market_phenomena_lessons.md
knowledge/market_phenomena_data_requests.json
```

Avoid names containing:

```text
autonomous_strategy
auto_strategy
strategy_generator
route_generator
```

unless referencing old artifacts for historical context only.

---

## 4. Artifact Contracts

### 4.1 `phenomenon_candidates.json`

Path:

```text
artifacts/market_phenomena/phenomenon_candidates.json
artifacts/market_phenomena/phenomenon_candidates.md
```

Purpose: store candidate market phenomena, not strategies.

Required shape:

```json
{
  "schema_version": 1,
  "run_id": "phenomena_YYYYMMDDTHHMMSSZ",
  "market": "cn_equity_daily",
  "mode": "research_artifact_only",
  "generated_at_utc": "...",
  "phenomena": [
    {
      "phenomenon_id": "quality_repair_delayed_repricing_v1",
      "title": "盈利质量修复后的延迟重估",
      "mechanism_source": "information_delay",
      "participants": ["低频基本面资金", "覆盖不足股票投资者", "行业轮动资金"],
      "participant_constraints": ["信息处理慢", "财报可信度折价", "行业风险偏好约束"],
      "behavioral_story": "...",
      "temporary_mispricing_reason": "...",
      "why_not_immediately_arbitraged": "...",
      "observable_variables": ["profit_yoy", "roe", "debt_to_asset", "operating_cashflow_to_profit", "pb", "industry_return_60d"],
      "prediction_target": "future_60d_relative_return_distribution",
      "expected_horizon": "60d/120d",
      "market_states_where_stronger": ["行业风险偏好修复", "财报后信息扩散期"],
      "failure_conditions": ["现金流未确认", "估值已提前修复", "行业趋势转负"],
      "minimal_verification_question": "当盈利质量改善且估值仍被压制时，未来收益和下行风险分布是否优于对照组？",
      "indicator_translation": {
        "pb": "估值压制代理，不是策略本身",
        "roe": "盈利质量代理，不是买入规则"
      },
      "scores": {
        "mechanism_strength": 8,
        "observability": 7,
        "testability": 7,
        "tradability_potential": 6,
        "novelty": 6,
        "crowding_risk": 5,
        "overfit_risk": 4,
        "cost_sensitivity": 5,
        "total_score": 19.0
      },
      "hard_gate_decision": "candidate"
    }
  ],
  "strategy_generation_allowed": false,
  "backtest_allowed": false,
  "queue_write_allowed": false,
  "timer_enable_allowed": false
}
```

### 4.2 `phenomenon_quality_review.json`

Path:

```text
artifacts/market_phenomena/phenomenon_quality_review.json
artifacts/market_phenomena/phenomenon_quality_review.md
```

Purpose: reject ideas that are just strategies, indicators, or untestable narratives.

Hard reject if:

```text
missing participants
missing constraints
missing temporary mispricing reason
missing why-not-arbitraged explanation
missing observable variables
missing minimal verification question
core logic is RSI/MACD/Bollinger/KDJ/MA cross/grid/martingale/turtle
contains direct buy/sell rule as primary output
requires unavailable future data
```

### 4.3 `phenomenon_novelty_review.json`

Path:

```text
artifacts/market_phenomena/phenomenon_novelty_review.json
artifacts/market_phenomena/phenomenon_novelty_review.md
```

Purpose: compare candidates against existing phenomenon memory and failed research lessons.

Decisions:

```text
keep
reject_duplicate
revise_too_similar
manual_review
```

Similarity dimensions:

- mechanism source;
- participant set;
- constraint set;
- observable variables;
- prediction target;
- expected horizon;
- previous failed phenomena.

### 4.4 `phenomenon_data_feasibility.json`

Path:

```text
artifacts/market_phenomena/phenomenon_data_feasibility.json
artifacts/market_phenomena/phenomenon_data_feasibility.md
```

Purpose: determine whether the phenomenon can be observed with current data.

Checks:

```text
field existence
field derivability
PIT alignment for financial fields
coverage by ticker/date
future target horizon availability
sample size
leakage risk
whether missing data should be requested
```

Decisions:

```text
ready_for_minimal_verification
blocked_missing_data
blocked_low_coverage
blocked_leakage_risk
manual_review
```

### 4.5 `minimal_verification_plan.json`

Path:

```text
artifacts/market_phenomena/minimal_verification_plan.json
artifacts/market_phenomena/minimal_verification_plan.md
```

Purpose: define a statistical verification question, not a trading strategy.

Allowed experiment types:

```text
conditional_distribution_test
cross_sectional_group_comparison
regime_split_distribution_test
lead_lag_distribution_test
```

Forbidden fields:

```text
buy_rule
sell_rule
position_size
rebalance_rule
portfolio_weight
order_generation
queue_write
```

### 4.6 `minimal_verification_result.json`

Path:

```text
artifacts/market_phenomena/minimal_verification_result.json
artifacts/market_phenomena/minimal_verification_result.md
```

Purpose: show whether the phenomenon changed a target distribution.

Metrics:

```text
usable_rows
usable_tickers
coverage
condition_group_count
control_group_count
future_return_mean_by_group
future_return_median_by_group
future_downside_risk_by_group
future_max_drawdown_proxy_by_group
spread_vs_control
regime_stability
industry_or_beta_warning
result_status
```

### 4.7 `phenomenon_verdict.json`

Path:

```text
artifacts/market_phenomena/phenomenon_verdict.json
artifacts/market_phenomena/phenomenon_verdict.md
```

Purpose: decide what the research result means.

Verdicts:

```text
supported_for_further_research
partially_supported_needs_regime_condition
partially_supported_needs_risk_repair
blocked_missing_data
rejected_not_observable
rejected_duplicate
rejected_failed_verification
rejected_indicator_disguised_as_mechanism
manual_review_required
```

It must include:

```text
what_was_learned
what_failed
do_not_repeat
next_research_question
strategy_design_allowed: false by default
```

---

## 5. Module Plan

### 5.1 Create `market_phenomena_schema.py`

Path:

```text
src/factor_lab/market_phenomena_schema.py
```

Responsibilities:

- define required fields;
- validate phenomenon candidates;
- score candidates;
- reject strategy-like ideas;
- render basic summaries.

Tests:

```text
tests/test_market_phenomena_schema.py
```

### 5.2 Create `market_phenomena_generator.py`

Path:

```text
src/factor_lab/market_phenomena_generator.py
```

Responsibilities:

- generate deterministic seed phenomena from mechanism sources;
- use current market/domain context;
- avoid LLM/provider dependency in v1;
- never generate buy/sell rules.

Script:

```text
scripts/market_phenomena_write_candidates.py
```

### 5.3 Create `market_phenomena_quality.py`

Path:

```text
src/factor_lab/market_phenomena_quality.py
```

Responsibilities:

- hard gate review;
- indicator-disguise detection;
- strategy-output detection;
- missing-mechanism detection.

Script:

```text
scripts/market_phenomena_write_quality_review.py
```

### 5.4 Create `market_phenomena_memory.py`

Path:

```text
src/factor_lab/market_phenomena_memory.py
```

Responsibilities:

- load memory;
- upsert verdicts;
- track duplicate mechanisms;
- track failure lessons;
- write memory and lessons.

Files:

```text
knowledge/market_phenomena_memory.json
knowledge/market_phenomena_lessons.md
```

### 5.5 Create `market_phenomena_novelty.py`

Path:

```text
src/factor_lab/market_phenomena_novelty.py
```

Responsibilities:

- compare candidate to memory;
- compute simple token/field overlap;
- reject duplicates;
- flag low novelty.

Script:

```text
scripts/market_phenomena_write_novelty_review.py
```

### 5.6 Create `market_phenomena_data.py`

Path:

```text
src/factor_lab/market_phenomena_data.py
```

Responsibilities:

- inspect available fields/caches;
- distinguish missing source data vs low coverage vs derivable features;
- check PIT requirements;
- update data requests.

Script:

```text
scripts/market_phenomena_write_data_feasibility.py
```

### 5.7 Create `market_phenomena_experiment_plan.py`

Path:

```text
src/factor_lab/market_phenomena_experiment_plan.py
```

Responsibilities:

- convert a feasible phenomenon into minimal verification plan;
- ensure no trading rules;
- define condition variables and target distributions.

Script:

```text
scripts/market_phenomena_write_minimal_verification_plan.py
```

### 5.8 Create `market_phenomena_experiment_runner.py`

Path:

```text
src/factor_lab/market_phenomena_experiment_runner.py
```

Responsibilities:

- run bounded distribution checks;
- compute group metrics;
- produce verification results;
- never create strategy orders or queue writes.

Script:

```text
scripts/market_phenomena_run_minimal_verification.py
```

### 5.9 Create `market_phenomena_verdict.py`

Path:

```text
src/factor_lab/market_phenomena_verdict.py
```

Responsibilities:

- convert result to verdict;
- record what was learned;
- prevent repeat failure;
- decide whether future research handoff is allowed.

Script:

```text
scripts/market_phenomena_write_verdict.py
```

### 5.10 Create `market_phenomena_research_loop.py`

Path:

```text
src/factor_lab/market_phenomena_research_loop.py
```

Responsibilities:

- run bounded artifact-only pipeline;
- stop at missing data/manual review;
- write loop report;
- remain separate from strategy/backtest systems.

Script:

```text
scripts/market_phenomena_run_research_loop.py
```

---

## 6. Implementation Tasks

### Task 1: Add schema and validation

**Objective:** Create the phenomenon schema and hard validation gates.

**Files:**

- Create: `src/factor_lab/market_phenomena_schema.py`
- Create: `tests/test_market_phenomena_schema.py`

**Tests to write:**

1. complete phenomenon passes;
2. missing participants rejects;
3. missing temporary mispricing reason rejects;
4. missing why-not-arbitraged rejects;
5. direct buy/sell rule rejects;
6. RSI/MACD/Bollinger as core logic rejects;
7. score calculation is deterministic.

**Command:**

```bash
pytest tests/test_market_phenomena_schema.py -q
```

**Expected:**

```text
7 passed
```

---

### Task 2: Add deterministic candidate writer

**Objective:** Write initial phenomenon candidates into the new artifact directory.

**Files:**

- Create: `src/factor_lab/market_phenomena_generator.py`
- Create: `scripts/market_phenomena_write_candidates.py`
- Create: `tests/test_market_phenomena_generator.py`

**Starter candidates:**

```text
quality_repair_delayed_repricing_v1
value_trap_escape_after_balance_sheet_repair_v1
industry_cycle_confirmation_lag_v1
coverage_neglect_post_report_drift_v1
liquidity_discount_reversal_after_volume_recovery_v1
```

**Command:**

```bash
python scripts/market_phenomena_write_candidates.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/phenomenon_candidates.json
artifacts/market_phenomena/phenomenon_candidates.md
```

---

### Task 3: Add quality review

**Objective:** Filter out low-quality or strategy-like candidates.

**Files:**

- Create: `src/factor_lab/market_phenomena_quality.py`
- Create: `scripts/market_phenomena_write_quality_review.py`
- Create: `tests/test_market_phenomena_quality.py`

**Command:**

```bash
pytest tests/test_market_phenomena_quality.py -q
python scripts/market_phenomena_write_quality_review.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/phenomenon_quality_review.json
artifacts/market_phenomena/phenomenon_quality_review.md
```

---

### Task 4: Add memory and novelty review

**Objective:** Prevent repeated mechanisms and record historical learning.

**Files:**

- Create: `src/factor_lab/market_phenomena_memory.py`
- Create: `src/factor_lab/market_phenomena_novelty.py`
- Create: `scripts/market_phenomena_write_novelty_review.py`
- Create: `tests/test_market_phenomena_memory.py`
- Create: `tests/test_market_phenomena_novelty.py`
- Create: `knowledge/market_phenomena_memory.json`
- Create: `knowledge/market_phenomena_lessons.md`

**Command:**

```bash
pytest tests/test_market_phenomena_memory.py tests/test_market_phenomena_novelty.py -q
python scripts/market_phenomena_write_novelty_review.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/phenomenon_novelty_review.json
artifacts/market_phenomena/phenomenon_novelty_review.md
```

---

### Task 5: Add data feasibility review

**Objective:** Check whether a phenomenon can be observed with current data before any experiment.

**Files:**

- Create: `src/factor_lab/market_phenomena_data.py`
- Create: `scripts/market_phenomena_write_data_feasibility.py`
- Create: `tests/test_market_phenomena_data.py`
- Create: `knowledge/market_phenomena_data_requests.json`

**Command:**

```bash
pytest tests/test_market_phenomena_data.py -q
python scripts/market_phenomena_write_data_feasibility.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/phenomenon_data_feasibility.json
artifacts/market_phenomena/phenomenon_data_feasibility.md
```

---

### Task 6: Add minimal verification planner

**Objective:** Convert feasible phenomena into condition-distribution verification plans.

**Files:**

- Create: `src/factor_lab/market_phenomena_experiment_plan.py`
- Create: `scripts/market_phenomena_write_minimal_verification_plan.py`
- Create: `tests/test_market_phenomena_experiment_plan.py`

**Command:**

```bash
pytest tests/test_market_phenomena_experiment_plan.py -q
python scripts/market_phenomena_write_minimal_verification_plan.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/minimal_verification_plan.json
artifacts/market_phenomena/minimal_verification_plan.md
```

---

### Task 7: Add minimal verification runner

**Objective:** Run non-trading statistical checks over existing data.

**Files:**

- Create: `src/factor_lab/market_phenomena_experiment_runner.py`
- Create: `scripts/market_phenomena_run_minimal_verification.py`
- Create: `tests/test_market_phenomena_experiment_runner.py`

**Command:**

```bash
pytest tests/test_market_phenomena_experiment_runner.py -q
python scripts/market_phenomena_run_minimal_verification.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/minimal_verification_result.json
artifacts/market_phenomena/minimal_verification_result.md
```

---

### Task 8: Add phenomenon verdict writer

**Objective:** Convert minimal verification results into explicit research conclusions.

**Files:**

- Create: `src/factor_lab/market_phenomena_verdict.py`
- Create: `scripts/market_phenomena_write_verdict.py`
- Create: `tests/test_market_phenomena_verdict.py`

**Command:**

```bash
pytest tests/test_market_phenomena_verdict.py -q
python scripts/market_phenomena_write_verdict.py
```

**Expected outputs:**

```text
artifacts/market_phenomena/phenomenon_verdict.json
artifacts/market_phenomena/phenomenon_verdict.md
```

---

### Task 9: Add research loop, not automatic strategy loop

**Objective:** Run the artifact-only phenomenon pipeline end-to-end without touching strategy/backtest systems.

**Files:**

- Create: `src/factor_lab/market_phenomena_research_loop.py`
- Create: `scripts/market_phenomena_run_research_loop.py`
- Create: `tests/test_market_phenomena_research_loop.py`

**Loop steps:**

```text
write_candidates
write_quality_review
write_novelty_review
write_data_feasibility
write_minimal_verification_plan
run_minimal_verification
write_verdict
update_memory
```

**Stop conditions:**

```text
no_candidate_passes_quality
all_candidates_duplicate
missing_data
low_coverage
manual_review_required
verification_failed
loop_complete
```

**Command:**

```bash
pytest tests/test_market_phenomena_research_loop.py -q
python scripts/market_phenomena_run_research_loop.py --max-phenomena 5
```

**Expected outputs:**

```text
artifacts/market_phenomena/research_loop.json
artifacts/market_phenomena/research_loop.md
```

---

### Task 10: Add agent boundary and research policy

**Objective:** Define the boundary contract for the research agent before adding any more manually-authored research tasks. The assistant defines rules, tools, artifact schemas, budgets, and safety gates; the agent chooses mechanisms, variables, tests, backtests, diagnostics, and mutations.

**Files:**

- Create: `src/factor_lab/market_phenomena_agent_policy.py`
- Create: `scripts/market_phenomena_write_agent_policy.py`
- Create: `tests/test_market_phenomena_agent_policy.py`

**Policy must say the agent may choose:**

```text
mechanism hypotheses
observable variables
validation methods
controlled research/backtest designs
split/regime tests
factor mutations
failure/drawdown diagnoses
next-generation hypotheses
```

**Policy must forbid:**

```text
live trading
production queue writes
timer/daemon restore
auto-promotion
simple TA recipes as core ideas: Bollinger, MA cross, RSI, MACD, KDJ, grid, martingale
manual assistant-authored task lists masquerading as autonomy
```

**Expected outputs:**

```text
artifacts/market_phenomena/agent_policy.json
artifacts/market_phenomena/agent_policy.md
```

---

### Task 11: Add Hermes research worker contract

**Objective:** Create the prompt/contract artifact that a Hermes worker uses to autonomously generate the next research plan. This makes the agent the researcher; the existing code only verifies boundaries and executes allowed tools.

**Files:**

- Create: `src/factor_lab/market_phenomena_worker_contract.py`
- Create: `scripts/market_phenomena_write_worker_contract.py`
- Create: `tests/test_market_phenomena_worker_contract.py`

**Worker contract inputs:**

```text
agent_policy.json
research_handoff.json
phenomenon_verdict.json
minimal_verification_result.json
market_phenomena_lessons.md
available data/catalog summary
```

**Worker contract output requirements:**

```text
agent must propose its own research/backtest plan
agent must explain mechanism and participant logic
agent must identify required variables and data feasibility
agent must include drawdown/failure diagnostics
agent must include mutation logic
agent must not use low-level TA as core mechanism
agent must keep production/live gates closed
```

**Expected outputs:**

```text
artifacts/market_phenomena/worker_contract.json
artifacts/market_phenomena/worker_contract.md
```

---

### Task 12: Add agent-generated iteration plan validator

**Objective:** Validate an iteration plan generated by a Hermes research worker. The code must not manually define the research tasks; it only checks that the agent-generated plan obeys boundary rules and is executable.

**Files:**

- Create: `src/factor_lab/market_phenomena_agent_iteration_plan.py`
- Create: `scripts/market_phenomena_validate_agent_iteration_plan.py`
- Create: `tests/test_market_phenomena_agent_iteration_plan.py`

**Validation checks:**

```text
has mechanism hypothesis
has observable variables
has controlled research/backtest design
has drawdown diagnostics
has mutation logic
has stop conditions
forbids live trading / production queue / daemon / timer / auto-promotion
rejects low-level TA core logic
rejects assistant-authored static task lists unless attached to an agent rationale
```

**Expected outputs:**

```text
artifacts/market_phenomena/agent_iteration_plan_review.json
artifacts/market_phenomena/agent_iteration_plan_review.md
```

---

### Task 13: Add controlled research/backtest executor

**Objective:** Execute validated agent-generated research/backtest plans. This is research execution, not live trading or production queue execution.

**Files:**

- Create: `src/factor_lab/market_phenomena_controlled_backtest.py`
- Create: `scripts/market_phenomena_run_controlled_backtest.py`
- Create: `tests/test_market_phenomena_controlled_backtest.py`

**Metrics:**

```text
return/spread
max_drawdown
downside risk
turnover/cost proxy
industry split stability
size split stability
regime split stability
horizon stability
failure reason codes
```

**Expected outputs:**

```text
artifacts/market_phenomena/controlled_backtest_result.json
artifacts/market_phenomena/controlled_backtest_result.md
```

---

### Task 14: Add result critic / drawdown diagnosis

**Objective:** Let the system critique controlled research/backtest results and produce evidence-backed failure/drawdown diagnosis for the agent to use in the next iteration.

**Files:**

- Create: `src/factor_lab/market_phenomena_result_critic.py`
- Create: `scripts/market_phenomena_write_result_critique.py`
- Create: `tests/test_market_phenomena_result_critic.py`

**Diagnosis dimensions:**

```text
industry concentration
size exposure
liquidity/turnover exposure
regime instability
horizon instability
drawdown source
cost sensitivity
overfit risk
data coverage weakness
```

**Expected outputs:**

```text
artifacts/market_phenomena/result_critique.json
artifacts/market_phenomena/result_critique.md
```

---

### Task 15: Add agent mutation request

**Objective:** Convert result critique into an agent-facing mutation request. The agent, not the assistant, proposes the next-generation mechanism/factor/test changes.

**Files:**

- Create: `src/factor_lab/market_phenomena_mutation_request.py`
- Create: `scripts/market_phenomena_write_mutation_request.py`
- Create: `tests/test_market_phenomena_mutation_request.py`

**Expected outputs:**

```text
artifacts/market_phenomena/mutation_request.json
artifacts/market_phenomena/mutation_request.md
```

---

### Task 16: Add autonomous research loop controller

**Objective:** Wire the worker contract, agent-generated iteration plan validation, controlled research/backtest execution, result critique, and mutation request into a bounded autonomous research loop. The loop should stop on budget exhaustion, duplicated ideas, unsupported mechanisms, missing data, or production/live boundary.

**Files:**

- Create: `src/factor_lab/market_phenomena_autonomous_research_loop.py`
- Create: `scripts/market_phenomena_run_autonomous_research_loop.py`
- Create: `tests/test_market_phenomena_autonomous_research_loop.py`

**Loop:**

```text
research_handoff
  -> worker_contract
  -> Hermes worker generates agent_iteration_plan
  -> validate_agent_iteration_plan
  -> controlled_backtest
  -> result_critique
  -> mutation_request
  -> next worker iteration
```

**Expected outputs:**

```text
artifacts/market_phenomena/autonomous_research_loop.json
artifacts/market_phenomena/autonomous_research_loop.md
```

---

### Task 17: Add documentation and handoff policy

**Objective:** Document the new research-agent contract, the distinction between autonomous research/backtest and live trading, and the human approval boundary for production execution.

**Files:**

- Create: `docs/ops/market-phenomenon-research-agent.md`
- Create: `docs/research/market-phenomena-methodology.md`

Must include:

```text
This is not an automatic live-trading system.
This is an autonomous research/backtest/iteration system.
The agent chooses research tasks; the assistant/code defines boundaries and verifies artifacts.
This does not optimize for strategy count.
This avoids low-level TA recipes such as Bollinger/MA/RSI/MACD as core ideas.
Production/live execution and auto-promotion remain separately approved.
```

---

## 7. Safety Rules

Every script in the discovery/minimal-verification layer must write these flags into its top-level JSON output:

```json
{
  "strategy_generation_allowed": false,
  "backtest_allowed": false,
  "controlled_research_backtest_allowed": false,
  "queue_write_allowed": false,
  "timer_enable_allowed": false,
  "daemon_restore_allowed": false,
  "auto_promotion_allowed": false,
  "live_trading_allowed": false
}
```

After a phenomenon verdict is `supported_for_further_research`, a later handoff artifact may explicitly open the **controlled research/backtest** gate while keeping production gates closed:

```json
{
  "phenomenon_supported_for_further_research": true,
  "controlled_research_backtest_allowed": true,
  "strategy_generation_allowed": true,
  "queue_write_allowed": false,
  "timer_enable_allowed": false,
  "daemon_restore_allowed": false,
  "auto_promotion_allowed": false,
  "live_trading_allowed": false
}
```

This keeps phenomenon research separate from production execution, but does not artificially block autonomous research/backtest iteration.

---

## 8. Verification Plan

Run targeted tests:

```bash
pytest tests/test_market_phenomena_schema.py -q
pytest tests/test_market_phenomena_generator.py -q
pytest tests/test_market_phenomena_quality.py -q
pytest tests/test_market_phenomena_memory.py tests/test_market_phenomena_novelty.py -q
pytest tests/test_market_phenomena_data.py -q
pytest tests/test_market_phenomena_experiment_plan.py -q
pytest tests/test_market_phenomena_experiment_runner.py -q
pytest tests/test_market_phenomena_verdict.py -q
pytest tests/test_market_phenomena_research_loop.py -q
pytest tests/test_market_phenomena_research_handoff.py -q
pytest tests/test_market_phenomena_agent_policy.py -q
pytest tests/test_market_phenomena_worker_contract.py -q
pytest tests/test_market_phenomena_agent_iteration_plan.py -q
pytest tests/test_market_phenomena_controlled_backtest.py -q
pytest tests/test_market_phenomena_result_critic.py -q
pytest tests/test_market_phenomena_mutation_request.py -q
pytest tests/test_market_phenomena_autonomous_research_loop.py -q
```

Run full new subset:

```bash
pytest tests/test_market_phenomena_*.py -q
```

Run artifact smoke test:

```bash
python scripts/market_phenomena_run_research_loop.py --max-phenomena 5
```

Expected result:

```text
artifacts/market_phenomena/research_loop.json exists
artifacts/market_phenomena/phenomenon_verdict.json exists or loop stops with explicit blocker
no production queue writes
no timer changes
no daemon changes
no live trading
discovery/minimal-verification artifacts remain separated from controlled research/backtest handoff artifacts
```

---

## 9. Acceptance Criteria

This plan is complete when:

1. A new `market_phenomena_*` namespace exists.
2. Artifacts are written under `artifacts/market_phenomena/`, not under automatic-strategy artifacts.
3. Candidate outputs are market phenomena, not strategies.
4. Quality review rejects strategy-like and indicator-like ideas.
5. Novelty review prevents repeated mechanisms.
6. Data feasibility distinguishes missing data, low coverage, leakage risk, and ready status.
7. Minimal verification plan contains statistical questions, not buy/sell rules.
8. Minimal verification result reports distribution evidence.
9. Verdict records what was learned, what failed, and what not to repeat.
10. Research loop can run end-to-end and produce the next research/backtest handoff decision.
11. Discovery/minimal-verification scripts do not enable production queue, timer, daemon, auto-promotion, or live trading; later handoff scripts may enable controlled research/backtest only after evidence gates pass.
12. Documentation explicitly says this is **not** an automatic live-trading system, but it is an autonomous research/backtest/iteration system.

---

## 10. Reporting Template

Use this after implementation:

```text
Plan/status:
Completed:
Remaining:
Engineering outcome:
Research outcome:
Artifacts produced:
Verification commands and real results:
Safety gates:
Next recommended step:
Human approval required:
```

Important distinction:

```text
Engineering success = the research-agent framework, contracts, validators, and executors work.
Research success = a phenomenon survives evidence gates and produces useful iteration signals.
Controlled backtest success = research/backtest variants improve return/drawdown under the research gates.
Production/live success = out of scope and requires separate approval.
```

---

## 11. Explicit Non-Goals

This plan must not be reinterpreted as automatic live trading or production deployment.

Do not implement:

```text
live trading
production queue writes
timer/daemon restore for production
auto-promotion to production
provider/model setting changes
risk-threshold relaxation without evidence and review
strategy-count dashboards as the optimization target
assistant-authored manual research task lists masquerading as agent autonomy
low-level TA recipes as core research mechanisms
```

Controlled research/backtest execution is in scope after agent-policy and evidence gates pass; production/live execution remains out of scope.
