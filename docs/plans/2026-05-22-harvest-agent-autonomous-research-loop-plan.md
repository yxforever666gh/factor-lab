# Harvest Agent Autonomous Research Loop Formal Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. The implementation must remain Hermes-native, simulation/backtest-only, controlled-admission-first, and auditable from repo artifacts rather than chat history.

**Goal:** Build a Harvest Agent research institution inside Factor Lab: agents can write research plans, execute bounded experiments, evaluate evidence with fixed standards, update knowledge, and decide whether to continue, modify, promote, block, or stop until genuinely robust alpha candidates are found.

**Architecture:** Harvest Agent is not a single factor generator. It is a controlled research-loop orchestrator around existing Factor Lab governance: state snapshot → hypothesis/plan → reviewer gate → budget/admission → controlled execution → evidence ledger → verdict → knowledge update → next-cycle plan. The loop must use Hermes profiles/sessions/toolsets/artifacts and must not resurrect old OpenClaw/legacy broad daemon behavior.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, configs under `configs/`, artifacts under `artifacts/harvest_agent/`, knowledge under `knowledge/`, pytest tests under `tests/`, existing controlled feeder/admission/runtime takeover/ledger/quality-summary components.

---

## 1. Executive decision

The correct design is not “let agents run forever until a backtest looks profitable.” That would automate overfitting.

The correct design is “let a governed research committee of Hermes-native agents spend limited research budget on mechanism-driven hypotheses, require independent evidence, and stop or redirect when information gain is low.”

Harvest Agent should be evaluated as an autonomous research process, not as a one-shot alpha miner.

The v1 success condition is therefore:

- It can produce a complete research plan without user prompting.
- It can reject bad or duplicate plans before execution.
- It can run a very small number of controlled experiments.
- It can evaluate backtest evidence using fixed standards.
- It can update local knowledge.
- It can generate the next plan or stop.
- It cannot silently expand into broad search, live trading, or uncontrolled daemon behavior.

---

## 2. Non-negotiable safety boundaries

1. No live trading.
2. No broker/order path.
3. No automatic paper promotion without manual approval.
4. No broad daemon restoration.
5. No generated/recent/rolling legacy broad search paths.
6. No production `--force-new` default.
7. No external data/API expansion without manual approval.
8. No deletion/destructive cleanup.
9. No unlimited loops.
10. No “best Sharpe wins” promotion.
11. No model/provider pinning in Factor Lab agent specs; Hermes profiles follow the current Hermes main model/provider.
12. No old Agent/OpenClaw terminology in target architecture.

Manual approval is required for:

- increasing daily/cycle budget;
- enabling timers/cron/systemd schedules;
- adding new paid or rate-limited data sources;
- promoting a candidate to paper portfolio review;
- enabling long-running daemon behavior;
- changing live/paper-live execution rules;
- changing hard evidence thresholds.

---

## 3. Harvest Agent role model

Harvest Agent is the top-level research portfolio manager. It delegates bounded work to role-specific Hermes-native profiles.

### 3.1 Harvest Agent / Research Portfolio Manager

Responsibilities:

- read current state, knowledge, blockers, and available budget;
- choose one mainline per cycle;
- allocate research budget across mechanism validation, robustness, data quality, portfolio construction, and exploration;
- write the cycle charter;
- ensure no role bypasses governance;
- produce final cycle verdict.

Allowed actions:

- read artifacts and knowledge;
- write plans, verdicts, reports;
- call deterministic scripts;
- request controlled execution only after gate approval.

Forbidden actions:

- direct broad daemon restart;
- live/paper promotion;
- external data expansion;
- unbounded factor search.

### 3.2 Researcher

Responsibilities:

- propose economic mechanisms;
- define hypothesis, required fields, expected direction, expected horizon, falsification criteria;
- avoid blind arithmetic recombination;
- identify missing data and mechanism assumptions.

Required output:

- structured research proposal JSON;
- human-readable rationale;
- expected information gain.

### 3.3 Engineer

Responsibilities:

- convert accepted proposals into configs/scripts;
- keep configs deterministic and schema-valid;
- ensure experiment output paths are isolated under the current cycle;
- add or update tests before code changes.

### 3.4 Backtester

Responsibilities:

- run only admitted experiments;
- capture stdout/stderr/runtime/output paths;
- never rerun duplicates as “new evidence”;
- respect timeout and max-experiment caps.

### 3.5 Diagnostician

Responsibilities:

- classify outcome and failure reason;
- distinguish mechanism failure from data failure, direction error, bucket-shape issue, universe/horizon mismatch, cost sensitivity, and portfolio-construction failure;
- propose the smallest informative next experiment.

### 3.6 Reviewer

Responsibilities:

- reject overfit, duplicate, weak, or ungrounded plans;
- verify OOS and cost evidence;
- enforce manual review boundaries;
- block promotion if evidence is not independent.

### 3.7 Knowledge Steward

Responsibilities:

- update `knowledge/harvest_agent.md`, `knowledge/mechanism_lessons.md`, `knowledge/data_blockers.json`, and `knowledge/research_waste.md`;
- keep Hermes long-term memory compact and only for durable conclusions;
- never dump raw run data into Hermes memory.

---

## 4. Research loop state machine

Each Harvest cycle must follow this exact state machine:

1. `state_snapshot`
   - Read controlled runtime state, latest research quality summary, route policy, knowledge, and prior verdicts.

2. `cycle_charter`
   - Harvest Agent chooses one mainline and budget.

3. `research_proposals`
   - Researcher writes 1-3 mechanism-driven proposals.

4. `review_gate`
   - Reviewer and deterministic gate decide: allow, cheap_screen_only, manual_review, block.

5. `execution_manifest`
   - Engineer converts allowed proposals to executable controlled manifests.

6. `controlled_execution`
   - Backtester runs at most the admitted budget.

7. `evidence_ledger`
   - Diagnostician extracts metrics and failure classes.

8. `verdict`
   - Harvest Agent decides promote, continue, modify, hold, demote, block, stop, or manual_review.

9. `knowledge_update`
   - Knowledge Steward updates durable local knowledge.

10. `next_cycle_plan`
   - If allowed, generate the next plan; otherwise write stop/manual-review report.

No later state may run if an earlier gate blocks.

---

## 5. Target artifact layout

Create a separate Harvest namespace so autonomous research is auditable independently from older loop artifacts.

```text
artifacts/harvest_agent/
  state.json
  latest_cycle.json
  cycle_0001/
    state_snapshot.json
    cycle_charter.json
    cycle_charter.md
    proposals.json
    proposals.md
    reviewer_decision.json
    reviewer_decision.md
    gate_decision.json
    gate_decision.md
    execution_manifest.json
    runs/
      <experiment_id>/
        stdout.txt
        stderr.txt
        status.json
        results.json
        factor_evaluations.json
        portfolio_results.json
    evidence_ledger.json
    evidence_ledger.md
    verdict.json
    verdict.md
    knowledge_update.json
    knowledge_update.md
    next_cycle_plan.json
    next_cycle_plan.md
```

Knowledge files:

```text
knowledge/harvest_agent.md
knowledge/mechanism_lessons.md
knowledge/data_blockers.json
knowledge/research_waste.md
knowledge/factor_watchlist.json
knowledge/factor_blacklist.json
```

---

## 6. Core schemas

### 6.1 Cycle charter

```json
{
  "schema_version": 1,
  "cycle_id": "cycle_0001",
  "created_at_utc": "2026-05-22T00:00:00Z",
  "mainline": "bucket_aware_oos_followup",
  "research_budget": {
    "max_experiments": 2,
    "max_runtime_minutes": 60,
    "budget_bucket": "robustness_validation"
  },
  "current_blockers": ["drawdown_risk_too_high", "duplicate_equivalent_evidence_exists"],
  "research_question": "Can promoted bucket-aware routes survive stricter cost and tail-risk tests?",
  "success_definition": [
    "positive cost-adjusted return",
    "bucket-aware OOS stable",
    "drawdown within configured limit",
    "no duplicate-equivalent evidence",
    "mechanism rationale remains valid"
  ],
  "manual_approval_required": false
}
```

### 6.2 Research proposal

```json
{
  "proposal_id": "value_quality_cost_sensitivity_v1",
  "mechanism_id": "value_quality_no_distress",
  "hypothesis": "Quality-filtered value works in the upper-middle bucket but is sensitive to turnover and transaction costs.",
  "required_fields": ["earnings_yield", "roe", "pb", "turnover", "return_1d"],
  "derived_fields": ["bucket_pair_spread", "cost_adjusted_return"],
  "experiment_type": "controlled_backtest",
  "portfolio_construction": {
    "mode": "bucket_pair",
    "long_quantile": 3,
    "short_quantile": 0
  },
  "validation_protocol": "bucket_aware_oos_cost_sensitivity",
  "expected_information_gain": "Tests whether prior bucket-aware promotion survives realistic costs.",
  "falsification_criteria": [
    "net spread turns negative under baseline costs",
    "OOS pass fails in more than one validation split",
    "signal disappears after industry/size controls"
  ],
  "duplicate_rationale": "Uses stricter cost protocol and is not equivalent to the prior raw bucket-aware run."
}
```

### 6.3 Gate decision

```json
{
  "decision": "allow_controlled_execution",
  "reasons": [],
  "allowed_experiments": ["value_quality_cost_sensitivity_v1"],
  "blocked_experiments": [],
  "manual_review_required": false,
  "budget_after_decision": {
    "remaining_cycle_experiments": 1,
    "remaining_daily_experiments": 2
  }
}
```

### 6.4 Evidence ledger row

```json
{
  "experiment_id": "value_quality_cost_sensitivity_v1",
  "status": "finished",
  "mechanism_id": "value_quality_no_distress",
  "metrics": {
    "rank_ic_mean": 0.03,
    "rank_ic_ir": 0.21,
    "bucket_pair_spread_net": 0.0041,
    "sharpe_net": 0.85,
    "max_drawdown": -0.29,
    "turnover": 0.18,
    "coverage": 0.92
  },
  "evidence_quality": {
    "oos_status": "pass",
    "cost_status": "pass",
    "duplicate_status": "independent_followup",
    "data_quality_status": "pass"
  },
  "failure_class": null,
  "information_gain": "positive_progress"
}
```

### 6.5 Verdict

```json
{
  "cycle_id": "cycle_0001",
  "decision": "continue_same_mainline",
  "promoted_candidates": [],
  "held_candidates": ["value_quality_no_distress"],
  "blocked_candidates": [],
  "reasoning": [
    "Cost-adjusted evidence improved but promotion still needs another independent OOS window.",
    "No duplicate-equivalent pollution detected."
  ],
  "next_action": "run stricter tail-risk validation",
  "manual_approval_required": false
}
```

---

## 7. Fixed evaluation standard

Harvest Agent must never judge by one metric alone. It must score every experiment across hard gates and soft evidence.

### 7.1 Hard promotion gates

A candidate cannot be promoted unless all applicable gates pass:

1. Data gate
   - required fields available or explicitly derived;
   - coverage above configured threshold;
   - no known future-data leakage;
   - no unsupported P0 fields treated as available.

2. Duplicate gate
   - semantic/equivalence fingerprint is not a disguised repeat;
   - output_dir or timestamp changes do not count as new evidence.

3. OOS gate
   - bucket-aware OOS policy passes where applicable;
   - raw split mismatch is not treated as final if bucket-aware policy is the intended construction;
   - at least one independent validation beyond the discovery run.

4. Cost gate
   - net return/spread remains acceptable after baseline transaction costs;
   - cost sensitivity does not collapse immediately.

5. Portfolio construction gate
   - chosen construction matches diagnosed bucket shape;
   - top-bottom failure is not ignored if strategy uses top-bottom;
   - middle-hump signals require bucket-pair validation.

6. Risk gate
   - max drawdown within configured limit;
   - concentration and holding-count constraints pass;
   - no single exposure dominates.

7. Mechanism gate
   - hypothesis remains economically interpretable;
   - result is not purely an artifact of industry/size/liquidity exposure unless that exposure is intentional and controlled.

### 7.2 Soft scorecard

Use a 0-5 score for each:

- mechanism credibility;
- data quality;
- expected information gain;
- OOS stability;
- cost robustness;
- risk improvement;
- portfolio-construction fit;
- novelty versus existing evidence;
- implementation reliability;
- knowledge value even if failed.

Suggested verdict mapping:

- `promote_to_manual_review`: all hard gates pass and soft score average >= 4.
- `continue_same_mainline`: hard gates mostly pass, evidence positive but incomplete.
- `modify_experiment_design`: signal exists but construction/horizon/cost/risk is wrong.
- `hold`: not enough independent evidence, or route recently tested.
- `demote`: repeated weak or unstable evidence.
- `block`: data unavailable, duplicate, leakage, or unsafe request.
- `stop_no_information_gain`: two consecutive cycles add no useful information.

### 7.3 Failure classes

Every failed or partial experiment must receive one of these classes:

- `coverage_too_low`
- `missing_required_fields`
- `unsupported_feature_requested`
- `duplicate_equivalent_experiment`
- `future_data_or_timing_risk`
- `neutralization_breaks_signal`
- `too_many_split_failures`
- `bucket_shape_middle_hump`
- `direction_error`
- `portfolio_construction_mismatch`
- `cost_sensitivity_failure`
- `drawdown_too_deep`
- `negative_return_after_cost`
- `horizon_mismatch`
- `universe_mismatch`
- `mechanism_failed`
- `execution_failure`
- `manual_review_required`

---

## 8. Initial mainlines

Harvest should not start with arbitrary factor expression search. Start with controlled, high-information mainlines.

### Mainline A: Promoted bucket-aware route follow-ups

Use when route policy already shows bucket-aware OOS stability.

Purpose:

- test cost sensitivity;
- test stricter bucket tails;
- test independent windows;
- test drawdown behavior;
- avoid duplicate-equivalent reruns.

Candidate routes:

- `industry_relative_value`
- `value_quality_no_distress`
- `value_momentum_confirmation`

### Mainline B: Defensive quality / risk layer

Use when small-institutionalization is blocked by max drawdown.

Purpose:

- reduce drawdown without destroying return;
- test holding count 50-100;
- enforce position caps;
- simulate only.

### Mainline C: Mechanism-driven data gap analysis

Use when promising hypotheses need fields that are not currently available.

Purpose:

- classify blocked data;
- prioritize data enrichment;
- avoid wasting backtests on impossible factors.

This mainline must not fetch new data automatically.

### Mainline D: Reverse/direction sanity diagnostics

Use when IC and spread disagree.

Purpose:

- test sign convention;
- test original vs inverted;
- test top-bottom vs bottom-top;
- test bucket shape;
- avoid false rejection.

---

## 9. Implementation plan

### Phase 0: Audit current implementation and avoid duplication

**Objective:** Determine what already exists under `autonomous_research_loop` and what must be added specifically for Harvest Agent.

**Files:**
- Read: `docs/plans/2026-05-22-autonomous-research-loop-plan.md`
- Read: `docs/ops/autonomous-research-loop.md`
- Read: existing `src/factor_lab/autonomous_research_*` modules
- Create: `artifacts/harvest_agent/implementation_audit.md`

**Steps:**

1. Inspect existing autonomous loop modules and tests.
2. Identify reusable modules: config, state, gate, executor, evidence, verdict, report.
3. Identify missing Harvest-specific pieces: roles, proposal schema, reviewer decision, fixed evaluation standard, knowledge steward, multi-mainline policy.
4. Write an audit file listing reuse vs new work.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_research_loop_config.py tests/test_autonomous_research_gate.py tests/test_autonomous_research_verdict.py -q
```

---

### Phase 1: Add Harvest policy config

**Objective:** Define the safe envelope and evaluation thresholds for Harvest cycles.

**Files:**
- Create: `configs/harvest_agent_policy.json`
- Create: `src/factor_lab/harvest_agent_policy.py`
- Test: `tests/test_harvest_agent_policy.py`

**Config requirements:**

```json
{
  "schema_version": 1,
  "enabled": true,
  "mode": "dry_run_first",
  "max_experiments_per_cycle": 2,
  "max_cycles_per_day": 4,
  "cooldown_minutes": 180,
  "allowed_mainlines": [
    "bucket_aware_oos_followup",
    "defensive_quality_risk_layer",
    "mechanism_data_gap_analysis",
    "direction_sanity_diagnostics"
  ],
  "manual_approval_required_for": [
    "enable_timer",
    "increase_budget",
    "external_data_source",
    "paper_portfolio_promotion",
    "broad_daemon_restore",
    "live_trading"
  ],
  "hard_gates": {
    "require_oos": true,
    "require_cost_adjustment": true,
    "require_duplicate_check": true,
    "require_mechanism_id": true,
    "require_falsification_criteria": true
  },
  "promotion_thresholds": {
    "min_rank_ic_mean": 0.02,
    "min_rank_ic_ir": 0.15,
    "min_sharpe_net": 0.5,
    "max_drawdown_floor": -0.35,
    "min_coverage": 0.8
  }
}
```

**Tests:**

- missing config returns safe defaults;
- live trading cannot be enabled;
- broad daemon restore cannot be enabled;
- max experiments is capped in v1;
- unsupported mainline is rejected;
- promotion thresholds are loaded and exposed.

---

### Phase 2: Add Harvest proposal and reviewer schemas

**Objective:** Make agent output machine-checkable.

**Files:**
- Create: `src/factor_lab/harvest_research_proposal.py`
- Create: `src/factor_lab/harvest_reviewer_decision.py`
- Test: `tests/test_harvest_research_proposal.py`
- Test: `tests/test_harvest_reviewer_decision.py`

**Requirements:**

- proposal must contain mechanism_id, hypothesis, required fields, expected information gain, falsification criteria, duplicate rationale;
- reviewer decision must contain allow/block/manual_review, reasons, required changes, and overfit risk;
- proposal requesting live trading is invalid;
- proposal missing falsification criteria is invalid;
- reviewer can downgrade allow to cheap_screen_only or manual_review.

---

### Phase 3: Add Harvest state snapshot

**Objective:** Give each cycle a grounded view of current project state.

**Files:**
- Create: `src/factor_lab/harvest_state.py`
- Create: `scripts/write_harvest_state_snapshot.py`
- Test: `tests/test_harvest_state.py`

**Snapshot sources:**

- `artifacts/research_quality_summary.json`
- `artifacts/controlled_route_policy.json`
- `artifacts/controlled_run_ledger_summary.json`
- `knowledge/mechanism_lessons.md`
- `knowledge/data_blockers.json`
- `knowledge/research_waste.md`
- `knowledge/autonomous_research_loop.md`
- latest `artifacts/autonomous_research_loop/latest_cycle.json`
- latest `artifacts/harvest_agent/latest_cycle.json`

**Tests:**

- missing optional files do not crash;
- old-path pollution in audit creates a blocker;
- promoted bucket-aware routes are included;
- data blockers are included;
- latest verdict is included.

---

### Phase 4: Add Harvest planner

**Objective:** Generate one bounded cycle charter and 1-2 proposals from state.

**Files:**
- Create: `src/factor_lab/harvest_planner.py`
- Create: `scripts/write_harvest_cycle_plan.py`
- Test: `tests/test_harvest_planner.py`

**Planning rules:**

- choose only one mainline per cycle;
- prefer promoted bucket-aware follow-ups when available;
- prefer defensive quality if drawdown blocker dominates;
- prefer data-gap analysis if required fields are unavailable;
- never generate arbitrary expression variants without mechanism_id;
- never exceed policy budget.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python scripts/write_harvest_cycle_plan.py --dry-run
```

---

### Phase 5: Add deterministic gate and budget allocator

**Objective:** Stop bad plans before execution.

**Files:**
- Create: `src/factor_lab/harvest_gate.py`
- Create: `src/factor_lab/harvest_budget.py`
- Create: `scripts/check_harvest_gate.py`
- Test: `tests/test_harvest_gate.py`
- Test: `tests/test_harvest_budget.py`

**Gate checks:**

- allowed mainline;
- supported required fields;
- no blocked feature used as available;
- no duplicate/equivalent experiment;
- within cycle and daily budget;
- no legacy broad path;
- no live/paper promotion;
- has falsification criteria;
- has expected information gain;
- has mechanism_id;
- reviewer decision is not block/manual_review.

**Decisions:**

- `allow_dry_run`
- `allow_controlled_execution`
- `cheap_screen_only`
- `manual_review`
- `block`

---

### Phase 6: Add execution manifest and controlled runner

**Objective:** Run only admitted experiments in bounded mode.

**Files:**
- Create: `src/factor_lab/harvest_execution_manifest.py`
- Create: `src/factor_lab/harvest_executor.py`
- Create: `scripts/run_harvest_cycle.py`
- Test: `tests/test_harvest_execution_manifest.py`
- Test: `tests/test_harvest_executor.py`

**Execution rules:**

- dry-run first by default;
- controlled execution requires explicit CLI flag;
- at most `max_experiments_per_cycle`;
- output path under `artifacts/harvest_agent/<cycle_id>/runs/`;
- capture stdout/stderr/status;
- enforce timeout;
- no daemon start;
- no timer enablement.

---

### Phase 7: Add evidence scorer

**Objective:** Convert raw outputs into fixed-standard evidence.

**Files:**
- Create: `src/factor_lab/harvest_evidence.py`
- Create: `src/factor_lab/harvest_scorecard.py`
- Create: `scripts/write_harvest_evidence.py`
- Test: `tests/test_harvest_evidence.py`
- Test: `tests/test_harvest_scorecard.py`

**Required output:**

- metrics extracted from run artifacts;
- hard-gate pass/fail;
- soft scorecard;
- failure class;
- information gain class;
- promotion eligibility;
- manual-review requirement.

**Information gain classes:**

- `positive_progress`
- `negative_but_informative`
- `blocked_missing_data`
- `duplicate_or_low_information`
- `execution_failure`
- `overfit_suspected`

---

### Phase 8: Add verdict and next-plan generator

**Objective:** Let Harvest decide what to do next without infinite search.

**Files:**
- Create: `src/factor_lab/harvest_verdict.py`
- Create: `src/factor_lab/harvest_next_plan.py`
- Create: `scripts/write_harvest_verdict.py`
- Create: `scripts/write_next_harvest_plan.py`
- Test: `tests/test_harvest_verdict.py`
- Test: `tests/test_harvest_next_plan.py`

**Verdict decisions:**

- `promote_to_manual_review`
- `continue_same_mainline`
- `modify_experiment_design`
- `hold_route`
- `demote_route`
- `block_route`
- `stop_no_information_gain`
- `manual_review_required`

**Stop rules:**

- two consecutive no-information-gain cycles;
- repeated duplicate-equivalent attempts;
- missing required data with no approved data expansion;
- overfit suspected;
- old-path pollution detected;
- budget exhausted.

---

### Phase 9: Add knowledge steward

**Objective:** Persist useful lessons locally without polluting Hermes memory.

**Files:**
- Create: `src/factor_lab/harvest_knowledge.py`
- Create: `scripts/update_harvest_knowledge.py`
- Test: `tests/test_harvest_knowledge.py`

**Writes:**

- `knowledge/harvest_agent.md`
- `knowledge/mechanism_lessons.md`
- `knowledge/data_blockers.json`
- `knowledge/research_waste.md`
- optional updates to factor watchlist/blacklist.

**Rules:**

- detailed run records stay in local artifacts;
- Hermes memory only gets compact durable conclusions after explicit review;
- raw JSON/result dumps never go into Hermes memory;
- stale conclusions can be superseded with timestamped local notes.

---

### Phase 10: Add one-command orchestrator

**Objective:** Run one complete Harvest cycle safely.

**Files:**
- Create: `scripts/run_harvest_agent_once.py`
- Test: `tests/test_run_harvest_agent_once.py`

**Commands:**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_agent_once.py --dry-run
PYTHONPATH=src .venv/bin/python scripts/run_harvest_agent_once.py --allow-controlled-execution --max-experiments 1
```

**Pipeline:**

1. write state snapshot;
2. write cycle charter and proposals;
3. write reviewer decision;
4. run deterministic gate;
5. write execution manifest;
6. execute dry-run or controlled run;
7. write evidence ledger;
8. write verdict;
9. update knowledge;
10. write next plan;
11. update latest-cycle pointer.

---

### Phase 11: Add reporting and WebUI status

**Objective:** Make the loop inspectable.

**Files:**
- Create: `src/factor_lab/harvest_report.py`
- Create: `scripts/write_harvest_report.py`
- Modify: WebUI route module if existing architecture supports it
- Test: `tests/test_harvest_report.py`
- Test: targeted WebUI route test if route is added

**Report fields:**

- latest cycle id;
- selected mainline;
- proposals;
- gate decision;
- executed experiments;
- evidence summary;
- verdict;
- next action;
- manual approval required;
- budget status;
- safety status.

---

### Phase 12: Preview-only scheduling

**Objective:** Prepare unattended operation without enabling it.

**Files:**
- Create: `scripts/render_harvest_agent_timer.py`
- Create: `docs/ops/harvest-agent.md`
- Test: `tests/test_harvest_agent_timer_render.py`

**Rules:**

- render only;
- default every 6 hours;
- default dry-run;
- controlled mode max one experiment;
- does not enable timer;
- does not start broad daemon.

---

## 10. Acceptance criteria

v1 is accepted only when all checks pass.

### 10.1 Compile

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_agent_policy.py \
  src/factor_lab/harvest_research_proposal.py \
  src/factor_lab/harvest_reviewer_decision.py \
  src/factor_lab/harvest_state.py \
  src/factor_lab/harvest_planner.py \
  src/factor_lab/harvest_gate.py \
  src/factor_lab/harvest_budget.py \
  src/factor_lab/harvest_execution_manifest.py \
  src/factor_lab/harvest_executor.py \
  src/factor_lab/harvest_evidence.py \
  src/factor_lab/harvest_scorecard.py \
  src/factor_lab/harvest_verdict.py \
  src/factor_lab/harvest_next_plan.py \
  src/factor_lab/harvest_knowledge.py \
  src/factor_lab/harvest_report.py \
  scripts/write_harvest_state_snapshot.py \
  scripts/write_harvest_cycle_plan.py \
  scripts/check_harvest_gate.py \
  scripts/run_harvest_cycle.py \
  scripts/write_harvest_evidence.py \
  scripts/write_harvest_verdict.py \
  scripts/write_next_harvest_plan.py \
  scripts/update_harvest_knowledge.py \
  scripts/run_harvest_agent_once.py \
  scripts/write_harvest_report.py \
  scripts/render_harvest_agent_timer.py
```

### 10.2 Targeted tests

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_agent_policy.py \
  tests/test_harvest_research_proposal.py \
  tests/test_harvest_reviewer_decision.py \
  tests/test_harvest_state.py \
  tests/test_harvest_planner.py \
  tests/test_harvest_gate.py \
  tests/test_harvest_budget.py \
  tests/test_harvest_execution_manifest.py \
  tests/test_harvest_executor.py \
  tests/test_harvest_evidence.py \
  tests/test_harvest_scorecard.py \
  tests/test_harvest_verdict.py \
  tests/test_harvest_next_plan.py \
  tests/test_harvest_knowledge.py \
  tests/test_harvest_report.py \
  tests/test_run_harvest_agent_once.py \
  tests/test_harvest_agent_timer_render.py -q
```

### 10.3 Smoke checks

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_agent_once.py --dry-run
PYTHONPATH=src .venv/bin/python scripts/write_harvest_report.py
PYTHONPATH=src .venv/bin/python scripts/render_harvest_agent_timer.py --preview
PYTHONPATH=src .venv/bin/python scripts/dry_run_controlled_restart.py
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_takeover.py
systemctl --user is-active factor-lab-research-daemon.service || true
```

Expected final safety state:

- dry-run cycle completes;
- controlled run is possible only with explicit flag;
- no broad daemon restored;
- no live trading path added;
- no timer enabled;
- artifacts are under `artifacts/harvest_agent/`;
- local knowledge is updated;
- verdict is conservative when evidence is incomplete.

---

## 11. Rollout sequence

1. Implement Phase 0 audit first.
2. Implement Phases 1-5: policy, proposal, reviewer, state, planner, gate.
3. Run dry-run only and inspect artifacts manually.
4. Implement Phases 6-8: manifest, executor, evidence, verdict.
5. Run one controlled experiment only.
6. Implement Phase 9 knowledge steward.
7. Run two manual cycles and verify that the second cycle uses the first cycle’s evidence.
8. Implement Phase 10 one-command orchestrator.
9. Implement Phase 11 report/WebUI.
10. Implement Phase 12 preview-only timer rendering.
11. Do not enable scheduling until explicitly approved.

---

## 12. What this plan intentionally does not do

- It does not promise to find a profitable factor.
- It does not treat a lucky backtest as alpha.
- It does not run unlimited experiments.
- It does not restore the old broad daemon.
- It does not add live trading.
- It does not add new data pulls.
- It does not bypass admission or duplicate control.
- It does not replace human approval for promotion decisions.

The purpose of v1 is to make autonomous research safe, auditable, evidence-driven, and hard to fool.

---

## 13. Immediate next step

Start with Phase 0 audit and Phase 1 policy config. Since the repo already contains an implemented `autonomous_research_loop`, the first engineering decision is reuse versus extension:

- Reuse existing autonomous loop modules for dry-run orchestration and evidence/verdict plumbing.
- Add Harvest-specific schemas and policy around research roles, proposal quality, reviewer decisions, scorecards, and knowledge stewardship.
- Avoid duplicating working code unless Harvest needs stricter semantics.

Recommended first command after approval:

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_research_loop_config.py tests/test_autonomous_research_gate.py tests/test_autonomous_research_verdict.py -q
```

---

## 14. Implementation status — 2026-05-22

Status: **implemented through Phase 12 and verification complete**.

Implemented Harvest Agent v1 components:

- Phase 0 audit: `artifacts/harvest_agent/implementation_audit.md`
- Phase 1 policy: `configs/harvest_agent_policy.json`, `src/factor_lab/harvest_agent_policy.py`
- Phase 2 schemas: `src/factor_lab/harvest_research_proposal.py`, `src/factor_lab/harvest_reviewer_decision.py`
- Phase 3 state: `src/factor_lab/harvest_state.py`, `scripts/write_harvest_state_snapshot.py`
- Phase 4 planner: `src/factor_lab/harvest_planner.py`, `scripts/write_harvest_cycle_plan.py`
- Phase 5 gate/budget: `src/factor_lab/harvest_gate.py`, `src/factor_lab/harvest_budget.py`, `scripts/check_harvest_gate.py`
- Phase 6 manifest/executor: `src/factor_lab/harvest_execution_manifest.py`, `src/factor_lab/harvest_executor.py`, `scripts/run_harvest_cycle.py`
- Phase 7 evidence/scorecard: `src/factor_lab/harvest_evidence.py`, `src/factor_lab/harvest_scorecard.py`, `scripts/write_harvest_evidence.py`
- Phase 8 verdict/next plan: `src/factor_lab/harvest_verdict.py`, `src/factor_lab/harvest_next_plan.py`, `scripts/write_harvest_verdict.py`, `scripts/write_next_harvest_plan.py`
- Phase 9 knowledge steward: `src/factor_lab/harvest_knowledge.py`, `scripts/update_harvest_knowledge.py`
- Phase 10 one-command orchestrator: `scripts/run_harvest_agent_once.py`
- Phase 11 report/status: `src/factor_lab/harvest_report.py`, `scripts/write_harvest_report.py`, WebUI status route `/harvest-agent/status`
- Phase 12 preview-only scheduling: `scripts/render_harvest_agent_timer.py`, `docs/ops/harvest-agent.md`

Verification completed:

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_agent_policy.py \
  src/factor_lab/harvest_research_proposal.py \
  src/factor_lab/harvest_reviewer_decision.py \
  src/factor_lab/harvest_state.py \
  src/factor_lab/harvest_planner.py \
  src/factor_lab/harvest_gate.py \
  src/factor_lab/harvest_budget.py \
  src/factor_lab/harvest_execution_manifest.py \
  src/factor_lab/harvest_executor.py \
  src/factor_lab/harvest_evidence.py \
  src/factor_lab/harvest_scorecard.py \
  src/factor_lab/harvest_verdict.py \
  src/factor_lab/harvest_next_plan.py \
  src/factor_lab/harvest_knowledge.py \
  src/factor_lab/harvest_report.py \
  scripts/write_harvest_state_snapshot.py \
  scripts/write_harvest_cycle_plan.py \
  scripts/check_harvest_gate.py \
  scripts/run_harvest_cycle.py \
  scripts/write_harvest_evidence.py \
  scripts/write_harvest_verdict.py \
  scripts/write_next_harvest_plan.py \
  scripts/update_harvest_knowledge.py \
  scripts/run_harvest_agent_once.py \
  scripts/write_harvest_report.py \
  scripts/render_harvest_agent_timer.py

PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_agent_policy.py \
  tests/test_harvest_research_proposal.py \
  tests/test_harvest_reviewer_decision.py \
  tests/test_harvest_state.py \
  tests/test_harvest_planner.py \
  tests/test_harvest_gate.py \
  tests/test_harvest_budget.py \
  tests/test_harvest_execution_manifest.py \
  tests/test_harvest_executor.py \
  tests/test_harvest_evidence.py \
  tests/test_harvest_scorecard.py \
  tests/test_harvest_verdict.py \
  tests/test_harvest_next_plan.py \
  tests/test_harvest_knowledge.py \
  tests/test_harvest_report.py \
  tests/test_run_harvest_agent_once.py \
  tests/test_harvest_agent_timer_render.py -q
```

Result: `59 passed, 1 warning`. The warning is an existing pandas `FutureWarning` from `defensive_quality_experiments.py`.

Smoke checks completed:

- `scripts/run_harvest_agent_once.py --dry-run` completed a full dry-run cycle with `executed_count=0`, `started_systemd_daemon=false`, and `scheduled_timer_enabled=false`.
- `scripts/write_harvest_report.py` wrote the latest Harvest report.
- `scripts/render_harvest_agent_timer.py --preview` rendered service/timer preview artifacts only; it did not install or enable them.
- `systemctl --user is-active factor-lab-harvest-agent.timer factor-lab-harvest-agent.service` returned `inactive` / `inactive`.
- `scripts/dry_run_controlled_restart.py` reported `would_run_count=0`.
- `scripts/audit_runtime_takeover.py` continued to recommend `['pause_broad_daemon', 'allow_controlled_only_daemon']`.
- Full `pytest tests -q` was attempted again after the Harvest targeted verification. Collection is currently blocked by pre-existing missing optional dependencies in the environment (`scipy`, `yaml`/PyYAML, and `httpx` for FastAPI TestClient), not by Harvest targeted tests. The collection failure shows 21 import-time errors, all from those dependency gaps.

Safety state after implementation:

- No live trading path added.
- No broker/order path added.
- No broad daemon restoration added.
- No Harvest timer/service enabled.
- Controlled execution still requires explicit CLI approval.
- Harvest artifacts are isolated under `artifacts/harvest_agent/`.
- Knowledge updates stay local under `knowledge/`.
