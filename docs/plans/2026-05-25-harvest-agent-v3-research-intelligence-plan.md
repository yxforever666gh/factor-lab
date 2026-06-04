# Harvest Agent v3 Research Intelligence Implementation Plan

> **For Hermes:** Use test-driven-development for each code change. Keep v3 Hermes-native, local-artifact driven, simulation/backtest-only, bounded, auditable, and rollback-safe. Do not enable timers, daemons, live trading, broker/order paths, automatic promotion, or broad external data fetching in this phase.

**Goal:** Upgrade Harvest Agent from v2's safe bounded self-correction loop into a stronger research-intelligence loop that can explain why a route failed, decide whether to repair/switch/stop/request data, and generate more information-rich next-cycle experiments instead of repeatedly tuning the same narrow parameter box.

**Architecture:** Add deterministic research-intelligence modules around the existing v2 loop: failure attribution → route memory/state policy → experiment branch selector → portfolio/risk construction planner → data blocker detector → richer next-cycle plan. v3 should still use bounded registries and explicit artifacts, but it should choose between `repair_same_route`, `switch_route`, `portfolio_construction_branch`, `risk_reduction_branch`, `data_request`, and `stop_route` instead of always continuing the same mainline.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, configs under `configs/`, artifacts under `artifacts/harvest_agent/`, knowledge outputs under `knowledge/`, pytest tests under `tests/`.

---

## Implementation status — 2026-05-25

Implemented and verified in this workspace.

- Added v3 modules:
  - `src/factor_lab/harvest_failure_attribution.py`
  - `src/factor_lab/harvest_route_state.py`
  - `src/factor_lab/harvest_research_decision.py`
  - `src/factor_lab/harvest_portfolio_branch_planner.py`
  - `src/factor_lab/harvest_data_request.py`
  - `src/factor_lab/harvest_v3_next_plan.py`
  - `src/factor_lab/harvest_research_lessons.py`
- Added scripts:
  - `scripts/inspect_harvest_v3_status.py`
  - `scripts/write_harvest_research_lessons.py`
- Upgraded:
  - `src/factor_lab/harvest_self_correction_planner.py`
  - `src/factor_lab/harvest_evolution_loop.py`
- Added tests:
  - `tests/test_harvest_failure_attribution.py`
  - `tests/test_harvest_route_state.py`
  - `tests/test_harvest_research_decision.py`
  - `tests/test_harvest_portfolio_branch_planner.py`
  - `tests/test_harvest_data_request.py`
  - `tests/test_harvest_v3_next_plan.py`
  - `tests/test_harvest_research_lessons.py`
  - `tests/test_inspect_harvest_v3_status.py`

Verification completed:

```text
Targeted v3 tests: 28 passed
py_compile: pass
Dry-run cycle: cycle_0049, executed_backtest_count=0
Controlled smoke: cycle_0050, executed_backtest_count=81
Harvest timer/service: inactive/inactive
```

Latest controlled research result from `cycle_0050`:

```text
oos_class: fail
research_decision: cost_robustness_branch
best row: industry_relative_book_yield, 2021-2022, cost_bps=0, Sharpe=0.760898, max_drawdown=-0.379894
failure attribution:
  - drawdown_concentrated_by_window
  - drawdown_concentrated_by_signal
  - zero_cost_only_best
  - cost_sensitivity
  - possible_portfolio_construction_issue
next branch experiments:
  - restrict_costs to [30, 60]
  - turnover >= 0.4 quantile
  - prefer_cost_robust
```

Interpretation: v3 engineering is implemented. It still has not found a promotable alpha; it correctly identified that the latest best result is zero-cost-only / cost-sensitive and chose a cost robustness branch rather than generic same-mainline repair.

---

## Why v3 is needed

v2 completed the engineering loop, but `cycle_0048` showed the current autonomous research behavior is still too shallow:

```text
best_sharpe: 0.668238 < 0.7
worst_drawdown: -0.550991 < -0.35
oos_class: fail
failure_classes:
  - drawdown_too_high
  - weak_risk_adjusted_return
  - zero_cost_best_only
  - not_promotion_ready
```

v2 correctly rejected the candidate, but the next-cycle plan was still generic:

```text
next_action: generate correction plan from latest diagnostics
```

That is not enough. v3 must make the agent more research-aware:

1. If drawdown is the blocker, identify whether the problem is window-specific, cost-specific, signal-specific, holding-count-specific, or portfolio-construction-specific.
2. If the same mechanism repeatedly fails, stop or switch route instead of continuing the same mainline forever.
3. If available fields cannot test the suspected mechanism, emit a data request/blocker instead of wasting more backtests.
4. If IC/return signal exists but portfolio monetization is bad, branch into bucket/risk/portfolio construction experiments.
5. If best result only works at zero cost, prioritize cost-aware experiments or reject the route.

---

## Safety boundaries

1. No live trading.
2. No broker/order path.
3. No automatic paper promotion.
4. No timer/service enablement.
5. No broad daemon restoration.
6. No automatic external data/API expansion.
7. No arbitrary LLM-generated factor code without deterministic validation and tests.
8. Controlled execution still requires explicit CLI flag.
9. Promotion remains manual-review only.
10. Route switching must use bounded registries.
11. Data expansion is plan-only in v3 unless separately approved.
12. v3 may write artifacts/knowledge files, but must not delete historical artifacts.

---

## V3 acceptance criteria

1. A v3 cycle writes `failure_attribution.json/.md` with failure breakdown by at least signal, window, cost, holding count, drawdown, and Sharpe buckets where available.
2. A v3 cycle writes `route_state.json/.md` describing recent semantic-family outcomes, consecutive failures, repeated failure classes, and route status: `active`, `watch`, `hold`, `demote`, or `stop`.
3. A v3 cycle writes `research_decision.json/.md` with one of:
   - `repair_same_route`
   - `switch_route`
   - `portfolio_construction_branch`
   - `risk_reduction_branch`
   - `cost_robustness_branch`
   - `data_request`
   - `stop_route`
4. `next_cycle_plan.json` becomes actionable: it must include selected branch, rationale, experiments to run, expected information gain, stop conditions, and success criteria.
5. The self-correction planner can generate different action sets for different branches instead of always generic same-mainline repair.
6. If a route repeatedly fails with the same failure classes, v3 must avoid repeating semantically equivalent experiments and either switch route, request data, or stop route.
7. If OOS failure is mostly drawdown/risk but returns are positive, v3 must prioritize risk/portfolio construction experiments before adding more raw signal variants.
8. If a mechanism requires unavailable fields, v3 must produce `data_request.json/.md` and block that branch from controlled execution.
9. Dry-run smoke completes with `started_systemd_daemon=false` and `scheduled_timer_enabled=false`.
10. Targeted Harvest v3 tests pass.

---

## New artifact contract

Each v3 cycle should write the existing v2 artifacts plus:

```text
failure_attribution.json
failure_attribution.md
route_state.json
route_state.md
research_decision.json
research_decision.md
portfolio_branch_plan.json       # only when relevant
portfolio_branch_plan.md
data_request.json                # only when relevant
data_request.md
v3_next_cycle_plan.json
v3_next_cycle_plan.md
```

The old `next_cycle_plan.json` can remain for compatibility, but it should point to `v3_next_cycle_plan.json` once v3 is active.

---

## Task 1: Add failure attribution module

**Objective:** Convert raw result rows and OOS validation into actionable failure attribution.

**Files:**
- Create: `src/factor_lab/harvest_failure_attribution.py`
- Test: `tests/test_harvest_failure_attribution.py`

**Behavior:**
- Accept a result payload from `run_plan_backtest()` / `build_small_institutional_backtest_matrix`.
- Group OK rows by:
  - `signal_column`
  - `label` / window
  - `cost_bps`
  - `holding_count`
- Compute per-group:
  - best/worst/mean Sharpe
  - best/worst/mean drawdown
  - best/worst/mean total return
  - profitable count
  - cost-positive count
- Identify primary blockers:
  - `drawdown_concentrated_by_window`
  - `drawdown_concentrated_by_signal`
  - `cost_sensitivity`
  - `weak_all_windows`
  - `zero_cost_only_best`
  - `holding_count_instability`
  - `possible_portfolio_construction_issue`
- Return a compact JSON-safe dict.

**Minimum API:**

```python
def attribute_harvest_failure(result_payload: dict, oos_validation: dict | None = None) -> dict:
    ...
```

**Test cases:**
1. Empty/no OK rows returns `insufficient_data` attribution.
2. One window has much worse drawdown and gets flagged as window-concentrated risk.
3. Best row is `cost_bps=0`, while positive-cost rows are weaker, so `zero_cost_only_best` is flagged.
4. All signals have Sharpe below threshold, so `weak_all_windows` is flagged.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_failure_attribution.py -q
```

---

## Task 2: Add route state and stop/switch policy

**Objective:** Track recent route outcomes and prevent infinite same-mainline retries.

**Files:**
- Create: `src/factor_lab/harvest_route_state.py`
- Test: `tests/test_harvest_route_state.py`

**Behavior:**
- Scan prior `artifacts/harvest_agent/cycle_*/` directories.
- Read available:
  - `mechanism_route.json`
  - `oos_validation.json`
  - `diagnosis.json`
  - `semantic_signature.json`
  - `research_decision.json` if present
- Summarize per mechanism route:
  - attempts
  - pass/near_miss/fail count
  - consecutive failures
  - repeated failure classes
  - repeated semantic family hashes
  - latest best Sharpe / drawdown if available
- Classify route status:
  - `active`: insufficient evidence or improving
  - `watch`: failures exist but not enough to demote
  - `hold`: repeated failures but route may be repairable
  - `demote`: repeated same blockers with weak improvement
  - `stop`: semantic repetition and repeated fail beyond threshold

**Suggested policy defaults:**

```python
max_consecutive_failures_before_hold = 2
max_consecutive_failures_before_demote = 3
max_semantic_repeats_before_stop = 3
near_miss_keeps_route_active = True
```

**Minimum API:**

```python
def build_route_state(root: str | Path = ".", current_route: str | None = None) -> dict:
    ...
```

**Test cases:**
1. No history returns empty/active state.
2. Three consecutive fails with same route returns `demote` or `stop` depending semantic repetition.
3. A near-miss keeps the route from being stopped.
4. Different route histories are separated correctly.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_route_state.py -q
```

---

## Task 3: Add bounded research decision engine

**Objective:** Decide the next research branch from diagnosis, attribution, route state, OOS validation, and available fields.

**Files:**
- Create: `src/factor_lab/harvest_research_decision.py`
- Test: `tests/test_harvest_research_decision.py`

**Decision values:**

```text
repair_same_route
switch_route
portfolio_construction_branch
risk_reduction_branch
cost_robustness_branch
data_request
stop_route
```

**Decision rules:**

1. If no OK rows or missing required fields: `data_request` or `stop_route`.
2. If route state is `stop`: `stop_route`.
3. If route state is `demote` and another bounded route is available: `switch_route`.
4. If return is positive but drawdown is the main blocker: `risk_reduction_branch`.
5. If IC/return looks useful but top/bottom monetization is weak or bucket history says middle-hump: `portfolio_construction_branch`.
6. If best row is zero-cost-only: `cost_robustness_branch`.
7. Otherwise: `repair_same_route`.

**Minimum API:**

```python
def decide_next_research_branch(
    *,
    diagnosis: dict,
    oos_validation: dict,
    failure_attribution: dict,
    route_state: dict,
    mechanism_route: dict,
    available_fields: set[str] | None = None,
) -> dict:
    ...
```

**Output shape:**

```json
{
  "schema_version": 1,
  "decision": "risk_reduction_branch",
  "rationale": ["drawdown_below_threshold", "positive_return_exists"],
  "blocked": false,
  "expected_information_gain": "test whether risk controls preserve return while reducing drawdown",
  "manual_approval_required": false
}
```

**Test cases:**
1. Stop-state route returns `stop_route`.
2. Repeated route failure with alternative route returns `switch_route`.
3. Drawdown blocker with positive return returns `risk_reduction_branch`.
4. Zero-cost-only best returns `cost_robustness_branch`.
5. Missing required fields returns `data_request` and `blocked=true`.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_research_decision.py -q
```

---

## Task 4: Add portfolio/risk branch planner

**Objective:** Generate bounded portfolio/risk construction experiments when factor signal exists but risk-adjusted performance fails.

**Files:**
- Create: `src/factor_lab/harvest_portfolio_branch_planner.py`
- Test: `tests/test_harvest_portfolio_branch_planner.py`

**Behavior:**
- For `risk_reduction_branch`, generate actions such as:
  - stricter `volatility_20` filter bands: `<= 0.4`, `<= 0.5`
  - larger holding counts: `[75, 100]`
  - exclude top turnover instability if turnover exists
  - require positive-cost settings `[30, 60]`
- For `portfolio_construction_branch`, generate bounded portfolio construction configs such as:
  - `bucket_pair` with `long_quantile=3`, `short_quantile=0`
  - `bucket_pair` with `long_quantile=3`, `short_quantile=1`
  - keep the route's allowed signals only
- For `cost_robustness_branch`, generate actions such as:
  - remove zero-cost option
  - prefer lower turnover filter
  - compare 30/60 bps only

**Do not:**
- Change default workflow behavior globally.
- Enable broad daemon execution.
- Generate arbitrary factor code.

**Minimum API:**

```python
def build_portfolio_branch_plan(
    *,
    decision: dict,
    current_plan: dict,
    failure_attribution: dict,
    mechanism_route: dict,
) -> dict:
    ...
```

**Test cases:**
1. Risk branch includes stricter volatility filter and positive cost settings.
2. Portfolio branch emits bucket-pair construction configs.
3. Cost branch excludes `cost_bps=0`.
4. Route allowed signals are respected.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_portfolio_branch_planner.py -q
```

---

## Task 5: Add data blocker and data request detector

**Objective:** Make the agent explicitly stop and request data when the mechanism cannot be tested with current fields.

**Files:**
- Create: `src/factor_lab/harvest_data_request.py`
- Test: `tests/test_harvest_data_request.py`

**Behavior:**
- Compare mechanism route required fields and optional desired fields against available feature schema.
- Use `factor_lab.feature_schema.TUSHARE_FEATURE_COLUMNS` and blocked-field conventions if available.
- For routes such as value-trap/no-distress, identify missing useful fields like:
  - cashflow quality
  - leverage/debt ratio
  - revenue/profit growth
  - dividend yield
  - historical valuation percentile
- Return `blocked=true` only for required missing fields; return `recommended_data` for optional improvement fields.

**Minimum API:**

```python
def build_harvest_data_request(mechanism_route: dict, available_fields: set[str] | None = None) -> dict:
    ...
```

**Output shape:**

```json
{
  "schema_version": 1,
  "blocked": false,
  "missing_required_fields": [],
  "recommended_data": ["cashflow_quality", "leverage", "earnings_growth"],
  "rationale": ["value trap exclusion cannot be strongly tested with current fields"]
}
```

**Test cases:**
1. Existing required fields produce `blocked=false`.
2. Missing required field produces `blocked=true`.
3. Value-quality route recommends cashflow/leverage/growth fields if absent.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_data_request.py -q
```

---

## Task 6: Upgrade self-correction planner for branch-specific actions

**Objective:** Replace generic same-mainline repair with branch-specific bounded plans.

**Files:**
- Modify: `src/factor_lab/harvest_self_correction_planner.py`
- Test: `tests/test_harvest_self_correction_planner.py`

**Behavior:**
- Add optional parameters:

```python
research_decision: dict | None = None
failure_attribution: dict | None = None
route_state: dict | None = None
portfolio_branch_plan: dict | None = None
```

- If decision is `risk_reduction_branch`, include stricter risk actions.
- If decision is `portfolio_construction_branch`, include explicit portfolio construction metadata.
- If decision is `cost_robustness_branch`, exclude zero-cost experiments.
- If decision is `switch_route`, select another bounded route from `harvest_mechanism_routes`.
- If decision is `data_request` or `stop_route`, produce a non-executable plan with `plan_status="blocked"` or `plan_status="stopped"`.

**Backward compatibility:**
- Existing v2 tests should still pass if no v3 decision is provided.

**Test cases:**
1. Old v2 call path still returns schema_version 2 correction plan.
2. Risk branch adds stricter risk controls.
3. Cost branch excludes zero-cost.
4. Stop route returns non-executable stopped plan.
5. Switch route changes mechanism route while respecting registry.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_self_correction_planner.py tests/test_harvest_research_decision.py tests/test_harvest_portfolio_branch_planner.py -q
```

---

## Task 7: Integrate v3 artifacts into evolution loop

**Objective:** Make each Harvest cycle write v3 intelligence artifacts and use them to build next-cycle plans.

**Files:**
- Modify: `src/factor_lab/harvest_evolution_loop.py`
- Test: `tests/test_harvest_evolution_loop.py`

**Integration order inside `run_harvest_evolution_once`:**

1. Build or load v2 correction plan as today.
2. Execute or dry-run as today.
3. Analyze result as today.
4. Validate OOS as today.
5. New: call `attribute_harvest_failure(result, oos_validation)`.
6. New: call `build_route_state(root, current_route=mechanism_id)`.
7. New: call `build_harvest_data_request(mechanism_route)`.
8. New: call `decide_next_research_branch(...)`.
9. New: call `build_portfolio_branch_plan(...)` when branch requires it.
10. New: build `v3_next_cycle_plan` with concrete branch, experiments, rationale, and stop conditions.
11. Write all v3 artifacts.
12. Keep `started_systemd_daemon=false` and `scheduled_timer_enabled=false`.

**Important:**
- If research decision is `data_request` or `stop_route`, do not run controlled execution for that branch in the same cycle.
- Existing v2 artifacts must still be written.
- Existing CLI should remain compatible.

**Test cases:**
1. Dry-run v3 cycle writes v3 artifacts.
2. Controlled failed result writes failure attribution and research decision.
3. Stop-route decision marks manual execution as blocked.
4. Next-cycle plan includes branch-specific experiments.
5. Safety fields remain false.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_evolution_loop.py -q
```

---

## Task 8: Add v3 next-cycle plan builder

**Objective:** Produce a human-readable and machine-readable plan that is more specific than `generate correction plan from latest diagnostics`.

**Files:**
- Create: `src/factor_lab/harvest_v3_next_plan.py`
- Test: `tests/test_harvest_v3_next_plan.py`

**Behavior:**
- Input:
  - current cycle id
  - diagnosis
  - OOS validation
  - failure attribution
  - route state
  - research decision
  - portfolio branch plan
  - data request
- Output:

```json
{
  "schema_version": 1,
  "cycle_id": "cycle_0049",
  "based_on_cycle": "cycle_0048",
  "plan_status": "planned",
  "branch": "risk_reduction_branch",
  "rationale": [...],
  "experiments": [...],
  "expected_information_gain": "...",
  "stop_conditions": [...],
  "success_criteria": {...},
  "manual_approval_required": false
}
```

**Test cases:**
1. Risk branch plan includes risk experiments.
2. Data request branch plan has `plan_status=blocked` and lists missing/recommended fields.
3. Stop route branch plan has `plan_status=stopped`.
4. All outputs are JSON serializable.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_v3_next_plan.py -q
```

---

## Task 9: Add a v3 status/inspection script

**Objective:** Give a quick CLI summary of whether v3 is improving research quality or stuck.

**Files:**
- Create: `scripts/inspect_harvest_v3_status.py`
- Test: `tests/test_inspect_harvest_v3_status.py`

**Behavior:**
- Read latest N cycles from `artifacts/harvest_agent/`.
- Print/write summary including:
  - latest cycle id
  - latest mechanism route
  - OOS class
  - research decision
  - route state
  - repeated blockers
  - whether next action is executable or blocked
- Write:

```text
artifacts/harvest_agent/v3_status.json
artifacts/harvest_agent/v3_status.md
```

**CLI:**

```bash
PYTHONPATH=src .venv/bin/python scripts/inspect_harvest_v3_status.py --latest 5
```

**Test cases:**
1. No cycles returns graceful empty summary.
2. Cycles with v3 artifacts produce expected summary.
3. Missing v3 artifacts falls back to v2 artifacts.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_inspect_harvest_v3_status.py -q
```

---

## Task 10: Knowledge output for repeated blockers

**Objective:** Persist stable repeated lessons locally without dumping every run into Hermes memory.

**Files:**
- Create: `src/factor_lab/harvest_research_lessons.py`
- Create: `scripts/write_harvest_research_lessons.py`
- Test: `tests/test_harvest_research_lessons.py`

**Behavior:**
- Read recent v3 cycle artifacts.
- Summarize stable lessons into:

```text
knowledge/harvest_research_lessons.md
knowledge/harvest_route_state.json
knowledge/harvest_data_requests.json
```

**Example lessons:**

```text
- industry_relative_value has repeated drawdown/Sharpe failure; prioritize risk construction or demote after N attempts.
- value-trap exclusion cannot be fully tested without cashflow/leverage/growth fields.
- zero-cost-only best rows should not be treated as promotion evidence.
```

**Do not:**
- Write Hermes persistent memory automatically.
- Store raw result rows in memory.

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_research_lessons.py -q
PYTHONPATH=src .venv/bin/python scripts/write_harvest_research_lessons.py
```

---

## Task 11: V3 dry-run and controlled smoke

**Objective:** Verify v3 behavior without enabling timers/daemons or pretending blocked plans are research success.

**Commands:**

```bash
cd /home/admin/factor-lab

PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_failure_attribution.py \
  src/factor_lab/harvest_route_state.py \
  src/factor_lab/harvest_research_decision.py \
  src/factor_lab/harvest_portfolio_branch_planner.py \
  src/factor_lab/harvest_data_request.py \
  src/factor_lab/harvest_v3_next_plan.py \
  src/factor_lab/harvest_research_lessons.py \
  src/factor_lab/harvest_self_correction_planner.py \
  src/factor_lab/harvest_evolution_loop.py \
  scripts/inspect_harvest_v3_status.py \
  scripts/write_harvest_research_lessons.py \
  scripts/run_harvest_evolution_loop.py

PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_failure_attribution.py \
  tests/test_harvest_route_state.py \
  tests/test_harvest_research_decision.py \
  tests/test_harvest_portfolio_branch_planner.py \
  tests/test_harvest_data_request.py \
  tests/test_harvest_v3_next_plan.py \
  tests/test_harvest_research_lessons.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_evolution_loop.py -q

PYTHONPATH=src .venv/bin/python scripts/run_harvest_evolution_loop.py --cycles 1
PYTHONPATH=src .venv/bin/python scripts/inspect_harvest_v3_status.py --latest 5
systemctl --user is-active factor-lab-harvest-agent.timer factor-lab-harvest-agent.service || true
```

**Expected dry-run result:**
- v3 artifacts are written.
- `executed_backtest_count=0`.
- `started_systemd_daemon=false`.
- `scheduled_timer_enabled=false`.
- timer/service remain `inactive`.

**Controlled smoke only after dry-run passes:**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_evolution_loop.py --cycles 1 --allow-controlled-execution
PYTHONPATH=src .venv/bin/python scripts/inspect_harvest_v3_status.py --latest 5
systemctl --user is-active factor-lab-harvest-agent.timer factor-lab-harvest-agent.service || true
```

**Expected controlled result:**
- Controlled execution may still fail research thresholds.
- The important v3 success criterion is not immediate alpha discovery; it is a better branch decision and non-repetitive next-cycle plan.

---

## Reporting template after implementation

When v3 is implemented, report in three separate layers:

### Engineering status

```text
v3 modules implemented: yes/no
v3 artifacts written: yes/no
targeted tests: N passed
py_compile: pass/fail
```

### Safety status

```text
harvest timer: inactive
harvest service: inactive
live/broker path: none
controlled execution requires flag: yes
```

### Research status

```text
latest cycle: cycle_xxxx
route: ...
oos_class: pass/near_miss/fail/insufficient_data
research_decision: ...
next branch: ...
blocked/data request: yes/no
promotion_ready: yes/no/manual review only
```

Do not say “system found alpha” unless OOS validation and manual-review promotion criteria are actually met.

---

## Out of scope for this v3 plan

These are intentionally excluded and require separate plans/approval:

1. Enabling a timer or daemon.
2. Live trading / broker integration.
3. Automatic paper portfolio promotion.
4. External data fetching from new APIs.
5. Large-scale feature-store rebuild.
6. Arbitrary LLM-written factor code execution.
7. Full WebUI cycle browser for v3 artifacts.
8. Production cron/event-chain automation.

---

## Implementation order summary

1. `harvest_failure_attribution.py`
2. `harvest_route_state.py`
3. `harvest_research_decision.py`
4. `harvest_portfolio_branch_planner.py`
5. `harvest_data_request.py`
6. Upgrade `harvest_self_correction_planner.py`
7. Integrate v3 artifacts in `harvest_evolution_loop.py`
8. `harvest_v3_next_plan.py`
9. `scripts/inspect_harvest_v3_status.py`
10. `harvest_research_lessons.py` + `scripts/write_harvest_research_lessons.py`
11. Run py_compile, targeted tests, dry-run smoke, controlled smoke.

---

## Key design principle

v3 should not optimize for “more cycles.” It should optimize for **higher information gain per cycle**.

A good v3 failure is acceptable if it says:

```text
This route failed for repeated drawdown reasons.
The likely issue is portfolio construction, not raw signal absence.
Next branch is risk_reduction_branch with specific experiments.
If that branch fails twice, demote this route and request data for value-trap exclusion.
```

A bad v3 failure is:

```text
oos_class=fail
next_action=generate correction plan from latest diagnostics
```

The whole point of v3 is to eliminate that generic loop.
