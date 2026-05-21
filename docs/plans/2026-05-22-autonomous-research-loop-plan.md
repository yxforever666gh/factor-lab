# Factor Lab Autonomous Research Loop Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. Follow plan → tests/diagnostics → verification → plan-doc update. Keep all execution simulated/backtest-only; do not enable live trading or broad daemon restoration.

**Goal:** Build a controlled autonomous research loop where Factor Lab can write its own research plan, gate it, execute a small bounded experiment batch, produce evidence/verdict, generate the next plan, and repeat without the user manually asking for the next step.

**Architecture:** Add a first-class `autonomous_research_loop` layer around the existing controlled-only runtime. The loop is a state machine: state snapshot → cycle plan → deterministic gate → admitted controlled execution → evidence ledger → verdict → next-cycle plan. The first production target is the defensive quality / low-risk enhancement mainline because current small-institutionalization is blocked by max drawdown, not by missing code execution.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, JSON/Markdown artifacts under `artifacts/autonomous_research_loop/`, configs under `configs/`, pytest tests under `tests/`, existing controlled feeder/daemon admission components.

---

## Non-negotiable boundaries

1. No live trading. `live_trading_enabled` remains false.
2. No broad daemon restoration. Existing audit recommendation `pause_broad_daemon` must remain valid.
3. First loop is limited to `defensive_quality_risk_layer` and simulated backtest / diagnostics only.
4. Each cycle can admit at most 1-3 experiments until evidence proves the loop is safe.
5. The loop must be resumable and auditable from files, not dependent on chat history.
6. Any escalation beyond the safe envelope requires manual approval:
   - broad daemon restart
   - higher daily budget
   - new external data/API pulls
   - paper portfolio promotion
   - live/paper-live rule changes
   - deletion or destructive cleanup

---

## Target artifact layout

Create one directory per cycle:

```text
artifacts/autonomous_research_loop/
  state.json
  latest_cycle.json
  cycle_0001/
    state_snapshot.json
    cycle_plan.json
    cycle_plan.md
    gate_decision.json
    gate_decision.md
    execution_manifest.json
    evidence_ledger.json
    evidence_ledger.md
    verdict.json
    verdict.md
    next_plan.json
    next_plan.md
  cycle_0002/
    ...
```

Knowledge summaries should be copied or summarized into:

```text
knowledge/autonomous_research_loop.md
knowledge/small_institutionalization.md
```

---

## Phase 0: Baseline audit and dry-run contract

### Task 0.1: Add a loop policy config

**Objective:** Define the safe envelope for autonomous cycles in a config file.

**Files:**
- Create: `configs/autonomous_research_loop.json`
- Test: `tests/test_autonomous_research_loop_config.py`

**Required config shape:**

```json
{
  "schema_version": 1,
  "enabled": true,
  "mode": "dry_run_first",
  "primary_mainline": "defensive_quality_risk_layer",
  "allowed_mainlines": ["defensive_quality_risk_layer"],
  "live_trading_enabled": false,
  "broad_daemon_restore_allowed": false,
  "max_experiments_per_cycle": 3,
  "default_experiments_per_cycle": 2,
  "cycle_budget": {
    "max_cycles_per_day": 4,
    "cooldown_minutes": 180
  },
  "risk_targets": {
    "max_drawdown_limit": -0.35,
    "min_holding_count": 50,
    "max_holding_count": 100,
    "max_single_position_weight": 0.02,
    "min_sharpe_preference": 0.8
  },
  "manual_approval_required_for": [
    "broad_daemon_restore",
    "increase_budget",
    "new_external_data_source",
    "paper_portfolio_promotion",
    "live_trading"
  ]
}
```

**Tests:**
- Config loader returns defaults when the file is missing.
- Config rejects `live_trading_enabled=true`.
- Config rejects `broad_daemon_restore_allowed=true` unless explicit override is passed by a test-only parameter.
- Config caps `max_experiments_per_cycle` to 3 for the first release.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_loop_config.py
pytest tests/test_autonomous_research_loop_config.py -q
```

---

### Task 0.2: Add baseline state snapshot builder

**Objective:** Build the read-only snapshot that every cycle plan must use.

**Files:**
- Create: `src/factor_lab/autonomous_research_loop_state.py`
- Test: `tests/test_autonomous_research_loop_state.py`

**Snapshot must include:**
- current timestamp
- small institutionalization status from `knowledge/small_institutionalization.md` if present
- simulation policy from `configs/small_institutional_simulation_policy.json`
- autonomous loop config
- runtime audit summary from existing artifacts if available
- controlled restart dry-run summary if available
- recent research quality summary if available
- current blocker: `drawdown_risk_too_high` / `blocked_no_drawdown_safe_candidate`
- last cycle id and last verdict if any

**Tests:**
- Missing optional artifacts do not crash the snapshot builder.
- Snapshot preserves the current drawdown blocker when parsed from knowledge.
- Snapshot marks runtime mode as `controlled_only_required` when broad daemon pause is recommended.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_loop_state.py
pytest tests/test_autonomous_research_loop_state.py -q
```

---

## Phase 1: Plan-writing agent output

### Task 1.1: Define cycle plan schema

**Objective:** Make research plans machine-checkable rather than free-form notes.

**Files:**
- Create: `src/factor_lab/autonomous_research_cycle_plan.py`
- Test: `tests/test_autonomous_research_cycle_plan.py`

**Cycle plan schema:**

```json
{
  "schema_version": 1,
  "cycle_id": "cycle_0001",
  "mainline": "defensive_quality_risk_layer",
  "research_question": "Can defensive quality / low-risk filters reduce max drawdown without destroying return?",
  "hypothesis": "Lower-volatility, non-distressed, not-overvalued stocks plus market-state risk reduction should reduce drawdown for the long-only small institutional portfolio.",
  "mechanism_id": "defensive_quality_risk_layer",
  "why_now": ["current blocker is drawdown_risk_too_high"],
  "experiments": [
    {
      "experiment_id": "dq_low_vol_quality_filter_v1",
      "type": "simulated_portfolio_repair",
      "objective": "Test whether low-volatility plus non-negative quality filters reduce max drawdown.",
      "required_fields": ["roe", "pb", "pe_ttm", "earnings_yield", "return_1d", "total_mv", "turnover"],
      "expected_information_gain": "Separate single-name risk filtering effect from market-state drawdown effect.",
      "falsification_criteria": ["max_drawdown remains below -0.45", "return collapses by more than 50%"],
      "max_runtime_minutes": 20
    }
  ],
  "budget": {"max_experiments": 2},
  "stop_conditions": ["no drawdown improvement", "missing required fields", "duplicate equivalent experiment"],
  "success_criteria": ["max_drawdown >= -0.35", "holding count remains 50-100", "single position cap passes"],
  "manual_approval_required": false
}
```

**Tests:**
- Valid defensive quality plan passes validation.
- Plan with unsupported mainline is rejected.
- Plan with more than configured max experiments is rejected.
- Plan missing hypothesis or falsification criteria is rejected.
- Plan requesting live trading is rejected.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_cycle_plan.py
pytest tests/test_autonomous_research_cycle_plan.py -q
```

---

### Task 1.2: Add deterministic first-cycle plan writer

**Objective:** Generate the first cycle plan without relying on an LLM, so the loop can be tested deterministically.

**Files:**
- Create: `src/factor_lab/autonomous_research_planner.py`
- Create: `scripts/write_autonomous_research_cycle_plan.py`
- Test: `tests/test_autonomous_research_planner.py`

**Behavior:**
- If there is no previous cycle, write `cycle_0001`.
- Select `defensive_quality_risk_layer`.
- Generate 2 experiments:
  1. low-volatility + quality floor + valuation sanity filter
  2. market-state risk reduction overlay
- Write both JSON and Markdown plan artifacts.
- Do not enqueue or run anything.

**Tests:**
- First-cycle plan contains the defensive quality mainline.
- Plan references the current drawdown blocker.
- Plan writes `manual_approval_required=false` for dry-run only.
- Script prints paths to generated artifacts.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_planner.py scripts/write_autonomous_research_cycle_plan.py
pytest tests/test_autonomous_research_planner.py -q
python3 scripts/write_autonomous_research_cycle_plan.py --dry-run
```

---

## Phase 2: Gate review

### Task 2.1: Implement autonomous loop gate

**Objective:** Stop bad plans before they reach execution.

**Files:**
- Create: `src/factor_lab/autonomous_research_gate.py`
- Create: `scripts/check_autonomous_research_cycle_gate.py`
- Test: `tests/test_autonomous_research_gate.py`

**Gate checks:**
- mainline is allowed
- required fields are available or explicitly marked derived
- experiment count within budget
- no live trading
- no broad daemon restore
- no duplicate equivalent experiment from recent cycles
- uses simulated/backtest-only execution type
- has falsification criteria
- has expected information gain
- respects current blocker priority

**Gate decision enum:**
- `allow_dry_run`
- `allow_controlled_execution`
- `manual_review`
- `block`

**Tests:**
- Defensive quality dry-run plan returns `allow_dry_run`.
- Same plan with `--allow-controlled-execution` returns `allow_controlled_execution`.
- Unsupported field returns `block` with `missing_required_fields`.
- Duplicate experiment returns `block` with `duplicate_equivalent_experiment`.
- Any live trading request returns `block`.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_gate.py scripts/check_autonomous_research_cycle_gate.py
pytest tests/test_autonomous_research_gate.py -q
python3 scripts/check_autonomous_research_cycle_gate.py artifacts/autonomous_research_loop/cycle_0001/cycle_plan.json
```

---

## Phase 3: Experiment adapter for defensive quality

### Task 3.1: Define experiment manifest schema

**Objective:** Convert gated plan experiments into executable but still bounded manifests.

**Files:**
- Create: `src/factor_lab/autonomous_research_execution_manifest.py`
- Test: `tests/test_autonomous_research_execution_manifest.py`

**Manifest fields:**
- cycle_id
- experiment_id
- source_plan_path
- execution_mode: `dry_run` or `controlled_local`
- script or module to call
- input artifacts
- output directory
- expected output files
- timeout
- admission metadata

**Tests:**
- Manifest is generated from an allowed plan.
- Manifest refuses blocked gate decisions.
- Manifest output paths stay under `artifacts/autonomous_research_loop/<cycle_id>/runs/`.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_execution_manifest.py
pytest tests/test_autonomous_research_execution_manifest.py -q
```

---

### Task 3.2: Build defensive quality repair experiment generator

**Objective:** Generate concrete simulated portfolio repair configs/scripts for the first two defensive quality experiments.

**Files:**
- Create: `src/factor_lab/defensive_quality_experiments.py`
- Create: `scripts/generate_defensive_quality_experiments.py`
- Test: `tests/test_defensive_quality_experiments.py`

**First two experiment families:**

1. `dq_low_vol_quality_filter_v1`
   - Use existing dataset from `small_institutional_simulation_policy.dataset_path`.
   - Require fields: `roe`, `pb`, `pe_ttm`, `earnings_yield`, `return_1d`, `total_mv`, `turnover`.
   - Derive rolling volatility and recent drawdown from daily return history if available; otherwise mark derived-field limitation.
   - Filter rules for MVP:
     - exclude bottom ROE tail
     - exclude valuation extreme expensive tail
     - exclude highest realized volatility tail
     - keep holding target 50/75/100 variants.

2. `dq_market_state_de_risk_v1`
   - Use benchmark or broad-universe return proxy if already available.
   - Test reduced equity exposure in downtrend regimes.
   - Must be simulated-only and produce an explicit assumption note if benchmark history is incomplete.

**Tests:**
- Generates exactly the configured experiments.
- Does not generate experiments requiring unavailable fields without a blocker.
- Manifest references only simulated/backtest execution.
- Output config records mechanism_id and hypothesis.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/defensive_quality_experiments.py scripts/generate_defensive_quality_experiments.py
pytest tests/test_defensive_quality_experiments.py -q
python3 scripts/generate_defensive_quality_experiments.py --dry-run
```

---

## Phase 4: Controlled execution runner

### Task 4.1: Add dry-run executor

**Objective:** Prove the loop can go through planning and admission without executing expensive backtests.

**Files:**
- Create: `src/factor_lab/autonomous_research_executor.py`
- Create: `scripts/run_autonomous_research_cycle.py`
- Test: `tests/test_autonomous_research_executor.py`

**Dry-run behavior:**
- Load plan.
- Load gate decision.
- Generate execution manifest.
- Validate inputs exist.
- Do not run experiments.
- Write `execution_manifest.json` with `would_run` entries.

**Tests:**
- Dry-run writes manifest and no run outputs.
- Blocked gate refuses execution.
- Missing dataset produces `manual_review` or `block`, not crash.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_executor.py scripts/run_autonomous_research_cycle.py
pytest tests/test_autonomous_research_executor.py -q
python3 scripts/run_autonomous_research_cycle.py --dry-run
```

---

### Task 4.2: Add controlled local executor

**Objective:** Allow one cycle to run 1-2 bounded experiments locally, without broad daemon.

**Files:**
- Modify: `src/factor_lab/autonomous_research_executor.py`
- Modify: `scripts/run_autonomous_research_cycle.py`
- Test: `tests/test_autonomous_research_executor_controlled.py`

**Controlled execution behavior:**
- Requires gate decision `allow_controlled_execution`.
- Runs at most `max_experiments_per_cycle`.
- Writes per-experiment run status.
- Captures stdout/stderr and runtime.
- Enforces timeout.
- On failure, writes failure reason into manifest; does not retry infinitely.

**Tests:**
- Controlled executor runs a fake experiment script and captures outputs.
- Timeout marks experiment failed with `timeout`.
- More than 3 experiments are not executed.
- No systemd daemon is started.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_executor.py scripts/run_autonomous_research_cycle.py
pytest tests/test_autonomous_research_executor_controlled.py -q
python3 scripts/run_autonomous_research_cycle.py --allow-controlled-execution --max-experiments 1
```

---

## Phase 5: Evidence ledger and verdict

### Task 5.1: Add evidence ledger writer

**Objective:** Convert raw run outputs into structured research evidence.

**Files:**
- Create: `src/factor_lab/autonomous_research_evidence.py`
- Create: `scripts/write_autonomous_research_evidence.py`
- Test: `tests/test_autonomous_research_evidence.py`

**Ledger must include:**
- experiment status
- metrics available
- max drawdown before/after if available
- return / Sharpe if available
- holding count, cap, turnover if available
- field limitations
- failure class
- information gain class:
  - `positive_progress`
  - `negative_but_informative`
  - `blocked_missing_data`
  - `duplicate_or_low_information`
  - `execution_failure`

**Tests:**
- Parses successful simulated repair output.
- Handles missing metrics without crashing.
- Classifies drawdown improvement but still below threshold as `positive_progress`.
- Classifies repeated no-improvement as `negative_but_informative` or `duplicate_or_low_information` depending on evidence.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_evidence.py scripts/write_autonomous_research_evidence.py
pytest tests/test_autonomous_research_evidence.py -q
```

---

### Task 5.2: Add verdict writer

**Objective:** Decide whether the loop should continue, stop, modify, or request manual approval.

**Files:**
- Create: `src/factor_lab/autonomous_research_verdict.py`
- Create: `scripts/write_autonomous_research_verdict.py`
- Test: `tests/test_autonomous_research_verdict.py`

**Verdict enum:**
- `continue_same_mainline`
- `modify_experiment_design`
- `blocked_needs_data_or_manual_review`
- `promote_to_paper_review_manual_approval`
- `stop_no_information_gain`

**Decision rules for MVP:**
- If max drawdown reaches `>= -0.35` and constraints pass: request manual review for paper promotion.
- If drawdown improves by at least 5 percentage points but remains below target: continue same mainline.
- If drawdown does not improve and returns collapse: modify experiment design.
- If required fields are missing: block for data/manual review.
- If two consecutive cycles produce no information gain: stop or request manual review.

**Tests:**
- Current blocker with improvement continues same mainline.
- Passing drawdown target triggers manual promotion review, not automatic paper/live promotion.
- Missing data blocks.
- Consecutive no-gain cycles stop.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_verdict.py scripts/write_autonomous_research_verdict.py
pytest tests/test_autonomous_research_verdict.py -q
```

---

## Phase 6: Next-plan generation

### Task 6.1: Generate next-cycle plan from verdict

**Objective:** Let the system write the next plan automatically from evidence.

**Files:**
- Modify: `src/factor_lab/autonomous_research_planner.py`
- Create: `scripts/write_next_autonomous_research_plan.py`
- Test: `tests/test_autonomous_research_next_plan.py`

**Behavior:**
- If verdict is `continue_same_mainline`, generate next defensive quality experiment focused on the strongest evidence.
- If verdict is `modify_experiment_design`, narrow or alter the experiment, not expand blindly.
- If verdict is `blocked_needs_data_or_manual_review`, write a plan marked `manual_review` and do not execute.
- If verdict is `stop_no_information_gain`, write a stop report and no executable plan.

**Tests:**
- Positive market-state evidence generates next market-state refinement plan.
- Positive low-vol evidence generates stricter/softer threshold comparison plan.
- No-gain verdict does not generate more random factor variants.
- Next plan increments cycle id.

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_planner.py scripts/write_next_autonomous_research_plan.py
pytest tests/test_autonomous_research_next_plan.py -q
```

---

## Phase 7: One-command cycle orchestration

### Task 7.1: Add orchestrator script

**Objective:** Provide one command that performs the full loop for exactly one cycle.

**Files:**
- Create: `scripts/run_autonomous_research_loop_once.py`
- Test: `tests/test_run_autonomous_research_loop_once.py`

**Command modes:**

```bash
python3 scripts/run_autonomous_research_loop_once.py --dry-run
python3 scripts/run_autonomous_research_loop_once.py --allow-controlled-execution --max-experiments 1
```

**Pipeline:**
1. Build state snapshot.
2. Write or load cycle plan.
3. Gate plan.
4. Execute dry-run or controlled local run.
5. Write evidence ledger.
6. Write verdict.
7. Write next plan.
8. Update `artifacts/autonomous_research_loop/latest_cycle.json`.
9. Update `knowledge/autonomous_research_loop.md`.

**Tests:**
- Dry-run completes all artifacts except run outputs.
- Controlled mode executes fake experiment under test fixture.
- Blocked gate exits cleanly with non-zero only for actual execution requests.
- Latest cycle pointer is updated.

**Verification:**

```bash
python3 -m py_compile scripts/run_autonomous_research_loop_once.py
pytest tests/test_run_autonomous_research_loop_once.py -q
python3 scripts/run_autonomous_research_loop_once.py --dry-run
```

---

## Phase 8: Safe scheduling, only after manual review

### Task 8.1: Render preview-only systemd units or cron wrapper

**Objective:** Prepare unattended operation, but do not enable it automatically.

**Files:**
- Create: `scripts/render_autonomous_research_loop_timer.py`
- Create: `docs/ops/autonomous-research-loop.md`
- Test: `tests/test_autonomous_research_loop_timer_render.py`

**Timer policy:**
- Default interval: every 6 hours.
- Default mode: dry-run or at most one controlled experiment per cycle.
- Must not start research daemon.
- Must not enable itself; render only.

**Tests:**
- Rendered unit uses `run_autonomous_research_loop_once.py`.
- Rendered unit includes `--allow-controlled-execution --max-experiments 1` only if explicitly requested.
- Rendered unit does not call broad daemon scripts.

**Verification:**

```bash
python3 -m py_compile scripts/render_autonomous_research_loop_timer.py
pytest tests/test_autonomous_research_loop_timer_render.py -q
python3 scripts/render_autonomous_research_loop_timer.py --preview
```

---

## Phase 9: WebUI / reporting integration

### Task 9.1: Add research loop status report

**Objective:** Make it easy to inspect what the autonomous loop did and why.

**Files:**
- Create: `src/factor_lab/autonomous_research_loop_report.py`
- Create: `scripts/write_autonomous_research_loop_report.py`
- Test: `tests/test_autonomous_research_loop_report.py`

**Report fields:**
- latest cycle id
- latest mainline
- latest plan summary
- gate decision
- executed experiments
- current best drawdown
- improvement vs previous best
- verdict
- next action
- manual approval required flag

**Verification:**

```bash
python3 -m py_compile src/factor_lab/autonomous_research_loop_report.py scripts/write_autonomous_research_loop_report.py
pytest tests/test_autonomous_research_loop_report.py -q
python3 scripts/write_autonomous_research_loop_report.py
```

---

## Acceptance criteria for v1

The feature is accepted only when all of the following pass:

```bash
python3 -m py_compile \
  src/factor_lab/autonomous_research_loop_config.py \
  src/factor_lab/autonomous_research_loop_state.py \
  src/factor_lab/autonomous_research_cycle_plan.py \
  src/factor_lab/autonomous_research_planner.py \
  src/factor_lab/autonomous_research_gate.py \
  src/factor_lab/autonomous_research_execution_manifest.py \
  src/factor_lab/defensive_quality_experiments.py \
  src/factor_lab/autonomous_research_executor.py \
  src/factor_lab/autonomous_research_evidence.py \
  src/factor_lab/autonomous_research_verdict.py \
  src/factor_lab/autonomous_research_loop_report.py \
  scripts/write_autonomous_research_cycle_plan.py \
  scripts/check_autonomous_research_cycle_gate.py \
  scripts/generate_defensive_quality_experiments.py \
  scripts/run_autonomous_research_cycle.py \
  scripts/write_autonomous_research_evidence.py \
  scripts/write_autonomous_research_verdict.py \
  scripts/write_next_autonomous_research_plan.py \
  scripts/run_autonomous_research_loop_once.py \
  scripts/write_autonomous_research_loop_report.py

pytest tests/test_autonomous_research_loop_config.py \
       tests/test_autonomous_research_loop_state.py \
       tests/test_autonomous_research_cycle_plan.py \
       tests/test_autonomous_research_planner.py \
       tests/test_autonomous_research_gate.py \
       tests/test_autonomous_research_execution_manifest.py \
       tests/test_defensive_quality_experiments.py \
       tests/test_autonomous_research_executor.py \
       tests/test_autonomous_research_executor_controlled.py \
       tests/test_autonomous_research_evidence.py \
       tests/test_autonomous_research_verdict.py \
       tests/test_autonomous_research_next_plan.py \
       tests/test_run_autonomous_research_loop_once.py \
       tests/test_autonomous_research_loop_report.py -q

python3 scripts/run_autonomous_research_loop_once.py --dry-run
python3 scripts/write_autonomous_research_loop_report.py
python3 scripts/dry_run_controlled_restart.py
python3 scripts/audit_runtime_takeover.py
```

Expected final safety state:
- `live_trading_enabled=false`
- broad daemon remains paused / not restored
- dry-run cycle produces plan, gate, manifest, evidence, verdict, next plan
- controlled execution mode can run at most one defensive quality experiment when explicitly requested
- all artifacts are written under `artifacts/autonomous_research_loop/`
- `knowledge/autonomous_research_loop.md` summarizes the latest cycle

---

## Recommended rollout order

1. Implement Phases 0-2 first: config, state snapshot, plan, gate. This gives a safe dry-run planning brain.
2. Run dry-run for one full cycle and inspect artifacts manually.
3. Implement Phases 3-5: defensive quality experiment adapter, controlled executor, evidence, verdict.
4. Run one controlled experiment only.
5. Implement Phase 6: next-plan generation.
6. Run two sequential cycles manually.
7. Implement Phase 7 one-command orchestration.
8. Only after stable manual cycles, implement Phase 8 timer rendering.
9. Do not enable the timer until the user explicitly approves.

---

## What this plan intentionally does not do

- It does not restore the old broad daemon.
- It does not search arbitrary factor expressions.
- It does not implement live trading.
- It does not promote to paper portfolio automatically.
- It does not add new paid/external data pulls.
- It does not solve every alpha problem in v1.

The v1 goal is narrower and more important: make the system autonomously run a safe, auditable, evidence-driven research loop around the current drawdown blocker.

---

## Implementation result — 2026-05-21/22

All v1 phases in this plan were implemented and verified under the simulation-only / controlled-only boundary.

### Implemented files

Config:
- `configs/autonomous_research_loop.json`

Core modules:
- `src/factor_lab/autonomous_research_loop_config.py`
- `src/factor_lab/autonomous_research_loop_state.py`
- `src/factor_lab/autonomous_research_cycle_plan.py`
- `src/factor_lab/autonomous_research_planner.py`
- `src/factor_lab/autonomous_research_gate.py`
- `src/factor_lab/autonomous_research_execution_manifest.py`
- `src/factor_lab/defensive_quality_experiments.py`
- `src/factor_lab/autonomous_research_executor.py`
- `src/factor_lab/autonomous_research_evidence.py`
- `src/factor_lab/autonomous_research_verdict.py`
- `src/factor_lab/autonomous_research_loop_report.py`

Scripts:
- `scripts/write_autonomous_research_cycle_plan.py`
- `scripts/check_autonomous_research_cycle_gate.py`
- `scripts/generate_defensive_quality_experiments.py`
- `scripts/run_autonomous_research_cycle.py`
- `scripts/write_autonomous_research_evidence.py`
- `scripts/write_autonomous_research_verdict.py`
- `scripts/write_next_autonomous_research_plan.py`
- `scripts/run_autonomous_research_loop_once.py`
- `scripts/write_autonomous_research_loop_report.py`
- `scripts/render_autonomous_research_loop_timer.py`

Ops / knowledge:
- `docs/ops/autonomous-research-loop.md`
- `knowledge/autonomous_research_loop.md`

Tests:
- `tests/test_autonomous_research_loop_config.py`
- `tests/test_autonomous_research_loop_state.py`
- `tests/test_autonomous_research_cycle_plan.py`
- `tests/test_autonomous_research_planner.py`
- `tests/test_autonomous_research_gate.py`
- `tests/test_autonomous_research_execution_manifest.py`
- `tests/test_defensive_quality_experiments.py`
- `tests/test_autonomous_research_executor.py`
- `tests/test_autonomous_research_executor_controlled.py`
- `tests/test_autonomous_research_evidence.py`
- `tests/test_autonomous_research_verdict.py`
- `tests/test_autonomous_research_next_plan.py`
- `tests/test_run_autonomous_research_loop_once.py`
- `tests/test_autonomous_research_loop_report.py`
- `tests/test_autonomous_research_loop_timer_render.py`

### Verification

TDD was followed: initial targeted test run failed with `ModuleNotFoundError` for the autonomous loop modules/scripts; modules/scripts were then implemented and the targeted suite passed.

Final targeted test result:

```text
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_autonomous_research_loop_config.py \
  tests/test_autonomous_research_loop_state.py \
  tests/test_autonomous_research_cycle_plan.py \
  tests/test_autonomous_research_planner.py \
  tests/test_autonomous_research_gate.py \
  tests/test_autonomous_research_execution_manifest.py \
  tests/test_defensive_quality_experiments.py \
  tests/test_autonomous_research_executor.py \
  tests/test_autonomous_research_executor_controlled.py \
  tests/test_autonomous_research_evidence.py \
  tests/test_autonomous_research_verdict.py \
  tests/test_autonomous_research_next_plan.py \
  tests/test_run_autonomous_research_loop_once.py \
  tests/test_autonomous_research_loop_report.py \
  tests/test_autonomous_research_loop_timer_render.py -q

49 passed
```

Compile and script smoke verification:

```text
PYTHONPATH=src .venv/bin/python -m py_compile ... autonomous loop modules/scripts
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_research_loop_once.py --dry-run
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_research_loop_once.py --allow-controlled-execution --max-experiments 1
PYTHONPATH=src .venv/bin/python scripts/write_autonomous_research_loop_report.py
PYTHONPATH=src .venv/bin/python scripts/render_autonomous_research_loop_timer.py --preview
```

Observed one-cycle controlled result:

```json
{
  "latest_cycle_id": "cycle_0001",
  "gate_decision": "allow_controlled_execution",
  "executed_experiments": 1,
  "current_best_drawdown": -0.458106,
  "verdict": "modify_experiment_design",
  "next_action": "narrow_or_modify_experiment_design",
  "manual_approval_required": false
}
```

Interpretation: the loop is now autonomous and auditable for one safe cycle. It correctly generated a defensive-quality plan, gated it, ran at most one controlled simulated experiment when explicitly allowed, wrote evidence/verdict/next-plan/report artifacts, and did not claim the strategy is institutionally ready because drawdown remains too high.

### Safety verification

```text
scripts/dry_run_controlled_restart.py:
  pending_count=4
  would_run_count=0
  blocked_count=0

scripts/audit_runtime_takeover.py:
  recommendations=['pause_broad_daemon', 'allow_controlled_only_daemon']

check_daemon.py:
  service active
  pending=4
  running=0
```

No live trading path, broker path, order generation, broad daemon restoration, destructive cleanup, or timer enablement was introduced. Timer rendering is preview-only.
