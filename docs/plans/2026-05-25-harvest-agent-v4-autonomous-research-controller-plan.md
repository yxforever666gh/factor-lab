# Harvest Agent v4 Autonomous Research Controller Implementation Plan

> **For Hermes:** Use `subagent-driven-development` or strict TDD to implement this plan task-by-task. Keep the implementation Hermes-native, local-artifact driven, simulation/backtest-only, bounded, auditable, and rollback-safe. Do **not** enable timers, daemons, live trading, broker/order paths, automatic promotion, or broad external data fetching in this phase.

**Goal:** Upgrade Harvest Agent from v3 “诊断 + 决策 + 写下一轮计划” to v4 “按预算、安全闸门和 stop policy 自动连续执行研究分支”，让系统能从 `v3_next_cycle_plan.json` 自动 materialize、执行、评估并迭代后续 cycles，直到 pass / stop / data_request / manual_review / budget exhausted。

**Architecture:** Add a deterministic `harvest_autonomous_research_controller` above the existing v3 loop. The controller reads the latest completed Harvest cycle, validates its v3 next-cycle plan, materializes it into an executable correction plan, runs bounded real backtests only when explicitly allowed, evaluates OOS/route state, writes a controller ledger, and repeats under strict budgets and stop conditions. v4 does **not** let the agent rewrite its own code; meta self-improvement remains a later v5 plan-only layer.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, artifacts under `artifacts/harvest_agent/`, controller artifacts under `artifacts/harvest_agent/controller_runs/`, pytest tests under `tests/`, docs under `docs/plans/`.

---

## Implementation status — 2026-05-25

Implemented and verified in this workspace.

Added v4 modules:

```text
src/factor_lab/harvest_controller_policy.py
src/factor_lab/harvest_v3_plan_loader.py
src/factor_lab/harvest_v3_plan_materializer.py
src/factor_lab/harvest_controller_budget.py
src/factor_lab/harvest_controller_ledger.py
src/factor_lab/harvest_real_execution_guard.py
src/factor_lab/harvest_cycle_runner.py
src/factor_lab/harvest_autonomous_research_controller.py
```

Added scripts:

```text
scripts/run_harvest_autonomous_research_controller.py
scripts/inspect_harvest_controller_status.py
```

Added tests:

```text
tests/test_harvest_controller_policy.py
tests/test_harvest_v3_plan_loader.py
tests/test_harvest_v3_plan_materializer.py
tests/test_harvest_controller_budget.py
tests/test_harvest_controller_ledger.py
tests/test_harvest_real_execution_guard.py
tests/test_harvest_cycle_runner.py
tests/test_harvest_autonomous_research_controller.py
tests/test_run_harvest_autonomous_research_controller.py
tests/test_inspect_harvest_controller_status.py
```

Verification completed:

```text
Targeted + v3 regression slice: 33 passed
py_compile: pass
Dry-run controller smoke: controller_20260525T013322Z, cycles_run=1, executed_backtest_count=0, stop_reason=dry_run_complete
Controlled multi-cycle smoke: controller_20260525T013539Z, cycles_run=2, executed_backtest_count=72, stop_reason=backtest_budget_exceeded
Controlled branch sequence: portfolio_construction_branch -> cost_robustness_branch
Latest controlled cycle: cycle_0057, status=ok, best cost_bps=30, Sharpe=0.315312, max_drawdown=-0.576326
Controller artifacts verified: controller_summary, controller_ledger, latest_decision, budget_state, stop_state
Harvest timer/service: inactive/inactive
Research daemon service: active before/after verification; v4 did not start it
```

Implementation refinement after controlled smoke: dry-run controller cycles no longer advance `latest_cycle.json`; dry-run writes proposal artifacts and a `dry_run_complete` stop event, while controlled execution remains the only mode that advances the latest metric-bearing cycle. `_next_cycle_id` also avoids collisions with existing dry-run cycle directories.

Interpretation: v4 engineering is implemented. It can read a v3 next-cycle plan, materialize it into a bounded executable plan, enforce a controller budget, run dry-run or controlled real backtests, write controller ledgers/summaries, expose status inspection, and preserve safety fields. The latest alpha result is still not promotable: Sharpe remains below 0.7 and max drawdown remains worse than -0.35.

---

## 1. Current state

v3 is implemented and verified. It can:

```text
run one Harvest cycle
analyze real backtest result
attribute failure
update route state
choose research branch
write v3_next_cycle_plan.json/.md
```

Latest relevant result:

```text
cycle_0050:
  oos_class: fail
  research_decision: cost_robustness_branch
  executed_backtest_count: 81
  blocker summary:
    - drawdown_concentrated_by_window
    - drawdown_concentrated_by_signal
    - zero_cost_only_best
    - cost_sensitivity
    - possible_portfolio_construction_issue
  v3 next plan:
    - restrict_costs: [30, 60]
    - turnover >= 0.4 quantile
    - prefer_cost_robust
```

But v3 still stops after writing the plan. It does not automatically read that plan, execute `cycle_0051`, evaluate it, then continue to `cycle_0052` under governance.

---

## 2. v4 scope

### In scope

v4 should implement:

1. A top-level autonomous controller module.
2. A materializer that converts `v3_next_cycle_plan.json` into an executable `correction_plan.json`.
3. Budget and stop-policy enforcement before every controlled execution.
4. Multi-cycle autonomous loop with real backtest execution only behind explicit CLI flag.
5. Controller ledger artifacts showing every decision, skip, stop, and metric-bearing run.
6. Status/inspection script for the latest controller run.
7. Tests proving safety gates, budget caps, materialization, stop conditions, and non-placeholder execution.

### Out of scope

v4 must **not** implement:

- live trading;
- broker/order integration;
- automatic promotion to production;
- automatic timer/service enabling;
- broad daemon restoration;
- external data/API expansion;
- arbitrary LLM-written factor code;
- self-modifying code or v5 meta-improvement.

---

## 3. Target runtime behavior

Target command:

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 5 \
  --max-backtests 500 \
  --allow-controlled-execution \
  --stop-on-data-request \
  --stop-on-route-stop \
  --stop-on-manual-review
```

Dry-run command:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 3 \
  --max-backtests 200
```

Expected loop:

```text
read latest completed cycle
  -> load v3_next_cycle_plan.json
  -> validate plan_status / branch / manual gate / data request
  -> materialize executable correction plan
  -> fingerprint and semantic duplicate check
  -> budget gate
  -> controlled real backtest if allowed
  -> analyze + OOS + failure attribution + route state + research decision
  -> write v3 artifacts for new cycle
  -> append controller ledger event
  -> continue / switch / stop / block
```

---

## 4. Artifact contract

Each controller run writes:

```text
artifacts/harvest_agent/controller_runs/<controller_run_id>/
  controller_config.json
  controller_ledger.jsonl
  controller_summary.json
  controller_summary.md
  budget_state.json
  stop_state.json
  latest_decision.json
```

Each cycle still writes normal Harvest artifacts:

```text
artifacts/harvest_agent/cycle_XXXX/
  correction_plan.json/.md
  experiment_fingerprint.json
  semantic_signature.json
  runs/value_quality_cost_sensitivity_v1/result.json/.md
  result_analysis.json/.md
  diagnosis.json/.md
  comparison.json/.md
  oos_validation.json/.md
  failure_attribution.json/.md
  route_state.json/.md
  research_decision.json/.md
  portfolio_branch_plan.json/.md
  data_request.json/.md
  v3_next_cycle_plan.json/.md
  evidence_ledger.json/.md
  verdict.json/.md
  next_cycle_plan.json/.md
```

A controller ledger event should contain at least:

```json
{
  "schema_version": 1,
  "controller_run_id": "controller_20260525T000000Z",
  "event_index": 1,
  "cycle_id": "cycle_0051",
  "based_on_cycle": "cycle_0050",
  "branch": "cost_robustness_branch",
  "plan_status": "planned",
  "gate_decision": "allow_controlled_execution",
  "executed_backtest_count": 54,
  "budget_remaining_backtests": 446,
  "oos_class": "fail",
  "research_decision": "risk_reduction_branch",
  "manual_approval_required": false,
  "stop_reason": null,
  "artifact_dir": "artifacts/harvest_agent/cycle_0051"
}
```

---

## 5. Stop and budget policy

v4 must stop before execution when any of these conditions is true:

```text
manual_approval_required == true and --stop-on-manual-review
v3_next_cycle_plan.plan_status in {blocked, stopped}
research_decision == data_request and --stop-on-data-request
research_decision == stop_route and --stop-on-route-stop
semantic duplicate cannot be resolved within max attempts
remaining cycle budget == 0
estimated next backtests > remaining backtest budget
same branch fails more than max_consecutive_branch_failures
same mechanism route fails more than max_route_failures
```

Default controller policy:

```json
{
  "max_cycles": 3,
  "max_backtests": 300,
  "max_attempts_per_cycle": 5,
  "max_consecutive_branch_failures": 2,
  "max_route_failures": 3,
  "allow_controlled_execution": false,
  "stop_on_data_request": true,
  "stop_on_route_stop": true,
  "stop_on_manual_review": true,
  "no_timer": true,
  "no_daemon": true,
  "no_live_trading": true,
  "no_automatic_promotion": true
}
```

---

## 6. Implementation tasks

### Task 1: Add controller policy dataclass

**Objective:** Create a typed policy object so all budgets and safety gates are explicit.

**Files:**

- Create: `src/factor_lab/harvest_controller_policy.py`
- Test: `tests/test_harvest_controller_policy.py`

**Implementation sketch:**

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HarvestControllerPolicy:
    max_cycles: int = 3
    max_backtests: int = 300
    max_attempts_per_cycle: int = 5
    max_consecutive_branch_failures: int = 2
    max_route_failures: int = 3
    allow_controlled_execution: bool = False
    stop_on_data_request: bool = True
    stop_on_route_stop: bool = True
    stop_on_manual_review: bool = True
    no_timer: bool = True
    no_daemon: bool = True
    no_live_trading: bool = True
    no_automatic_promotion: bool = True

    def validate(self) -> None:
        if self.max_cycles < 0:
            raise ValueError("max_cycles must be >= 0")
        if self.max_backtests < 0:
            raise ValueError("max_backtests must be >= 0")
        if not self.no_live_trading:
            raise ValueError("v4 controller cannot enable live trading")
        if not self.no_automatic_promotion:
            raise ValueError("v4 controller cannot enable automatic promotion")
```

**Tests:**

- default policy is dry-run and safe;
- negative budgets fail;
- live trading / auto promotion cannot be enabled.

---

### Task 2: Add v3 plan loader and validator

**Objective:** Load the latest `v3_next_cycle_plan.json` and classify it as executable, blocked, stopped, or manual-review.

**Files:**

- Create: `src/factor_lab/harvest_v3_plan_loader.py`
- Test: `tests/test_harvest_v3_plan_loader.py`

**Implementation sketch:**

```python
def load_latest_v3_next_plan(root: Path) -> dict[str, Any] | None:
    latest_path = root / "artifacts/harvest_agent/latest_cycle.json"
    if not latest_path.exists():
        return None
    latest = json.loads(latest_path.read_text())
    cycle_id = latest.get("cycle_id")
    if not cycle_id:
        return None
    plan_path = root / "artifacts/harvest_agent" / cycle_id / "v3_next_cycle_plan.json"
    if not plan_path.exists():
        return None
    plan = json.loads(plan_path.read_text())
    plan["_source_path"] = str(plan_path)
    return plan


def classify_v3_next_plan(plan: dict[str, Any]) -> dict[str, Any]:
    status = plan.get("plan_status")
    if status == "blocked":
        return {"decision": "block", "reason": "plan_status_blocked"}
    if status == "stopped":
        return {"decision": "stop", "reason": "plan_status_stopped"}
    if plan.get("manual_approval_required"):
        return {"decision": "manual_review", "reason": "manual_approval_required"}
    return {"decision": "executable", "reason": "planned"}
```

**Tests:**

- missing latest cycle returns `None`;
- blocked plan stops;
- stopped plan stops;
- manual review plan does not execute;
- planned cost branch is executable.

---

### Task 3: Add v3 plan materializer

**Objective:** Convert `v3_next_cycle_plan.json` into an executable correction plan compatible with `run_plan_backtest()`.

**Files:**

- Create: `src/factor_lab/harvest_v3_plan_materializer.py`
- Test: `tests/test_harvest_v3_plan_materializer.py`

**Implementation sketch:**

```python
def materialize_v3_next_plan(
    v3_plan: dict[str, Any],
    *,
    next_cycle_id: str,
    dataset_path: str,
    mechanism_route: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actions = []
    portfolio_construction = []
    for item in v3_plan.get("experiments") or []:
        if item.get("type") == "action":
            actions.append(dict(item.get("action") or {}))
        elif item.get("type") == "portfolio_construction":
            portfolio_construction.append(dict(item.get("config") or {}))

    if not actions and v3_plan.get("plan_status") == "planned":
        actions.append({"type": "restrict_costs", "cost_bps_values": [30, 60]})

    return {
        "schema_version": 2,
        "cycle_id": next_cycle_id,
        "based_on_cycle": v3_plan.get("based_on_cycle"),
        "plan_status": "planned",
        "objective": f"v4 materialized {v3_plan.get('branch')} branch",
        "dataset_path": dataset_path,
        "mechanism_route": mechanism_route or {},
        "actions": actions,
        "portfolio_construction": portfolio_construction,
        "research_decision": {
            "decision": v3_plan.get("branch"),
            "rationale": v3_plan.get("rationale") or [],
            "expected_information_gain": v3_plan.get("expected_information_gain"),
        },
        "executable": True,
        "success_criteria": v3_plan.get("success_criteria") or {
            "sharpe_min": 0.7,
            "max_drawdown_min": -0.35,
            "positive_at_cost_bps": 30,
            "min_ok_windows": 2,
        },
    }
```

**Tests:**

- cost branch materializes `restrict_costs`, `turnover >= 0.4`, `prefer_cost_robust`;
- portfolio branch preserves `portfolio_construction` configs;
- materialized plan is fingerprintable;
- blocked/stopped plans are not materialized by this function.

---

### Task 4: Add budget estimator and budget state

**Objective:** Estimate the number of backtest combinations before execution and prevent exceeding the controller budget.

**Files:**

- Create: `src/factor_lab/harvest_controller_budget.py`
- Test: `tests/test_harvest_controller_budget.py`

**Implementation sketch:**

```python
def estimate_backtest_count(plan: dict[str, Any]) -> int:
    signals = _last_action_value(plan, "set_signal_columns", "signal_columns", ["industry_relative_book_yield"])
    costs = _last_action_value(plan, "restrict_costs", "cost_bps_values", [0, 30, 60])
    holdings = _last_action_value(plan, "set_holding_counts", "holding_counts", [50, 75, 100])
    windows = _last_action_value(plan, "set_windows", "year_windows", DEFAULT_WINDOWS)
    return max(0, len(signals) * len(costs) * len(holdings) * len(windows))


def budget_gate(*, estimated_backtests: int, used_backtests: int, max_backtests: int) -> dict[str, Any]:
    remaining = max_backtests - used_backtests
    if estimated_backtests > remaining:
        return {"decision": "block", "reason": "backtest_budget_exceeded", "remaining": remaining}
    return {"decision": "allow", "reason": "within_budget", "remaining": remaining - estimated_backtests}
```

**Tests:**

- default initial matrix estimates 27 or 81 depending signal/action inputs;
- `restrict_costs=[30,60]` reduces count;
- budget gate blocks when estimate exceeds remaining;
- dry-run still records estimated count but executed count remains zero.

---

### Task 5: Add controller ledger writer

**Objective:** Persist every controller decision in append-only JSONL plus summary JSON/MD.

**Files:**

- Create: `src/factor_lab/harvest_controller_ledger.py`
- Test: `tests/test_harvest_controller_ledger.py`

**Implementation sketch:**

```python
def append_controller_event(run_dir: Path, event: dict[str, Any]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    with (run_dir / "controller_ledger.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def write_controller_summary(run_dir: Path, events: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "schema_version": 1,
        "controller_run_id": run_dir.name,
        "cycles_run": sum(1 for e in events if e.get("cycle_id")),
        "executed_backtest_count": sum(int(e.get("executed_backtest_count") or 0) for e in events),
        "stop_reason": next((e.get("stop_reason") for e in reversed(events) if e.get("stop_reason")), None),
        "started_systemd_daemon": False,
        "scheduled_timer_enabled": False,
    }
    ...
    return summary
```

**Tests:**

- JSONL append is stable;
- summary totals cycles and backtests;
- safety fields are always false;
- stop reason is preserved.

---

### Task 6: Add autonomous controller core

**Objective:** Implement the v4 loop without changing the existing v3 loop public behavior.

**Files:**

- Create: `src/factor_lab/harvest_autonomous_research_controller.py`
- Test: `tests/test_harvest_autonomous_research_controller.py`

**Implementation sketch:**

```python
def run_harvest_autonomous_research_controller(
    *,
    root: str | Path = ".",
    policy: HarvestControllerPolicy | None = None,
) -> dict[str, Any]:
    policy = policy or HarvestControllerPolicy()
    policy.validate()

    controller_run_id = make_controller_run_id()
    run_dir = Path(root) / "artifacts/harvest_agent/controller_runs" / controller_run_id
    events = []
    used_backtests = 0

    for event_index in range(policy.max_cycles):
        latest_plan = load_latest_v3_next_plan(Path(root))
        if latest_plan is None:
            # first-cycle fallback may call existing run_harvest_evolution_once dry-run or baseline.
            ...

        classification = classify_v3_next_plan(latest_plan)
        if classification["decision"] != "executable":
            events.append(stop_event(...))
            break

        next_cycle_id = allocate_next_cycle_id(root)
        materialized = materialize_v3_next_plan(...)
        estimated = estimate_backtest_count(materialized)
        gate = budget_gate(estimated_backtests=estimated, used_backtests=used_backtests, max_backtests=policy.max_backtests)
        if gate["decision"] != "allow":
            events.append(stop_event(...))
            break

        cycle_result = run_materialized_harvest_cycle(
            root=root,
            plan=materialized,
            allow_controlled_execution=policy.allow_controlled_execution,
        )
        used_backtests += int(cycle_result.get("executed_backtest_count") or 0)
        events.append(cycle_event(...))

        if should_stop_after_cycle(cycle_result, policy):
            events.append(stop_event(...))
            break

    return write_controller_summary(run_dir, events)
```

Important: `run_materialized_harvest_cycle()` should share the artifact-writing logic of `run_harvest_evolution_once()` rather than forking a second incompatible cycle writer. Prefer extracting common “execute plan and write v3 artifacts” code from `harvest_evolution_loop.py` into a helper.

**Tests:**

- dry-run controller executes zero real backtests but writes ledger;
- controlled controller executes real metric-bearing backtests;
- controller stops on manual review;
- controller stops on data request;
- controller stops when budget exceeded;
- controller does not start systemd daemon/timer;
- controller does not mark automatic promotion.

---

### Task 7: Refactor cycle execution helper out of `harvest_evolution_loop.py`

**Objective:** Avoid duplicated artifact logic between the old v3 loop and the new v4 controller.

**Files:**

- Modify: `src/factor_lab/harvest_evolution_loop.py`
- Possibly create: `src/factor_lab/harvest_cycle_runner.py`
- Test: `tests/test_harvest_cycle_runner.py`
- Keep existing: `tests/test_harvest_evolution_loop.py`

**Required helper API:**

```python
def run_harvest_cycle_from_plan(
    *,
    root: str | Path,
    plan: dict[str, Any],
    previous_cycle_id: str | None,
    allow_controlled_execution: bool,
) -> dict[str, Any]:
    """Execute one already-materialized plan and write the standard Harvest artifacts."""
```

**Acceptance criteria:**

- Existing `run_harvest_evolution_loop()` still passes all current tests.
- New controller uses the same helper.
- No duplicated v3 artifact-writing block remains except thin wrappers.

---

### Task 8: Add controller CLI script

**Objective:** Expose v4 as an explicit one-shot CLI, not a timer or daemon.

**Files:**

- Create: `scripts/run_harvest_autonomous_research_controller.py`
- Test: `tests/test_run_harvest_autonomous_research_controller.py`

**CLI contract:**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 5 \
  --max-backtests 500 \
  --allow-controlled-execution \
  --stop-on-data-request \
  --stop-on-route-stop \
  --stop-on-manual-review
```

**Expected JSON stdout:**

```json
{
  "schema_version": 1,
  "controller_status": "complete",
  "controller_run_id": "controller_...",
  "cycles_requested": 5,
  "cycles_run": 3,
  "executed_backtest_count": 162,
  "stop_reason": "route_stop",
  "started_systemd_daemon": false,
  "scheduled_timer_enabled": false,
  "artifacts_dir": "artifacts/harvest_agent/controller_runs/controller_..."
}
```

---

### Task 9: Add controller status inspector

**Objective:** Make it easy to inspect the latest autonomous controller run without reading raw JSONL.

**Files:**

- Create: `scripts/inspect_harvest_controller_status.py`
- Test: `tests/test_inspect_harvest_controller_status.py`

**Command:**

```bash
PYTHONPATH=src .venv/bin/python scripts/inspect_harvest_controller_status.py
```

**Output should include:**

```text
latest controller_run_id
cycles_run
executed_backtest_count
latest cycle_id
latest branch
latest oos_class
latest research_decision
stop_reason
safety state: timer inactive / service inactive / no live trading / no promotion
```

---

### Task 10: Add non-placeholder verification guard

**Objective:** Prevent v4 from reporting “real research” if a run artifact is placeholder or lacks metrics.

**Files:**

- Create: `src/factor_lab/harvest_real_execution_guard.py`
- Test: `tests/test_harvest_real_execution_guard.py`

**Implementation sketch:**

```python
def validate_real_backtest_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("status") == "simulated_controlled_placeholder":
        return {"valid": False, "reason": "placeholder_status"}
    summary = result.get("summary") or {}
    execution = result.get("execution") or {}
    executed = execution.get("executed_count") or summary.get("executed_count") or result.get("executed_backtest_count")
    best = result.get("best_result") or {}
    if not executed or not best:
        return {"valid": False, "reason": "missing_metric_bearing_result"}
    return {"valid": True, "reason": "metric_bearing_result"}
```

**Acceptance criteria:**

- Controlled v4 run refuses to count placeholder results as executed research.
- User-facing summary separates:
  - loop/plumbing validation;
  - controlled execution validation;
  - real alpha research progress.

---

## 7. Verification plan

### Targeted tests

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_controller_policy.py \
  tests/test_harvest_v3_plan_loader.py \
  tests/test_harvest_v3_plan_materializer.py \
  tests/test_harvest_controller_budget.py \
  tests/test_harvest_controller_ledger.py \
  tests/test_harvest_cycle_runner.py \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_run_harvest_autonomous_research_controller.py \
  tests/test_inspect_harvest_controller_status.py \
  tests/test_harvest_real_execution_guard.py -q
```

### Regression tests

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_evolution_loop.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_v3_next_plan.py \
  tests/test_harvest_research_decision.py \
  tests/test_harvest_route_state.py \
  tests/test_harvest_failure_attribution.py \
  tests/test_harvest_backtest_runner.py \
  tests/test_harvest_comparative_evaluator.py -q
```

### Compile check

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_controller_policy.py \
  src/factor_lab/harvest_v3_plan_loader.py \
  src/factor_lab/harvest_v3_plan_materializer.py \
  src/factor_lab/harvest_controller_budget.py \
  src/factor_lab/harvest_controller_ledger.py \
  src/factor_lab/harvest_cycle_runner.py \
  src/factor_lab/harvest_autonomous_research_controller.py \
  src/factor_lab/harvest_real_execution_guard.py \
  scripts/run_harvest_autonomous_research_controller.py \
  scripts/inspect_harvest_controller_status.py
```

### Dry-run smoke

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 2 \
  --max-backtests 200
```

Expected:

```text
cycles_run >= 1
executed_backtest_count = 0
controller ledger exists
started_systemd_daemon = false
scheduled_timer_enabled = false
```

### Controlled one-cycle smoke

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 1 \
  --max-backtests 120 \
  --allow-controlled-execution
```

Expected:

```text
cycles_run = 1
executed_backtest_count > 0
latest run result.json has real matrix metrics
status != simulated_controlled_placeholder
controller ledger records branch and OOS result
```

### Controlled multi-cycle smoke

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 3 \
  --max-backtests 300 \
  --allow-controlled-execution
```

Expected:

```text
cycles_run <= 3
executed_backtest_count <= 300
fingerprints are non-duplicate or stop reason explains halt
manual/data/stop gates respected
no timer/service enabled
no promotion written
```

### Safety check

```bash
systemctl --user is-active factor-lab-harvest-agent.timer || true
systemctl --user is-active factor-lab-harvest-agent.service || true
systemctl --user is-active factor-lab-research-daemon.service || true
```

Expected:

```text
factor-lab-harvest-agent.timer: inactive or unknown
factor-lab-harvest-agent.service: inactive or unknown
no broad daemon restoration caused by v4
```

---

## 8. User-facing reporting standard

Every v4 run report must separate:

### Engineering/controller result

```text
controller_run_id
cycles_requested
cycles_run
stop_reason
budget used / remaining
artifacts_dir
```

### Real research execution result

```text
executed_backtest_count
best signal
best window
best cost_bps
total_return
Sharpe
max_drawdown
OOS class
pass/fail thresholds
```

### Autonomy result

```text
branch sequence, e.g.
cycle_0051: cost_robustness_branch -> fail
cycle_0052: risk_reduction_branch -> fail
cycle_0053: stop_route/data_request/budget_exhausted
```

### Safety result

```text
started_systemd_daemon=false
scheduled_timer_enabled=false
live_trading=false
automatic_promotion=false
```

Do **not** call the alpha successful unless OOS/promotion criteria pass. It is acceptable for v4 engineering to succeed while the alpha route fails.

---

## 9. Acceptance criteria

v4 is complete only when all of these are true:

1. The controller can start from `cycle_0050`’s `v3_next_cycle_plan.json` and autonomously create at least one subsequent cycle.
2. Dry-run mode writes plans/ledger but executes zero backtests.
3. Controlled mode executes real metric-bearing backtests, not placeholders.
4. Backtest budget is enforced before each execution.
5. Manual-review, data-request, route-stop, duplicate, and budget stop conditions halt the loop.
6. Standard v3 artifacts are still written per cycle.
7. Controller artifacts summarize the whole autonomous run.
8. Existing v3 tests still pass.
9. No timer, daemon, live trading, broker/order path, or automatic promotion is enabled.
10. User-facing summary distinguishes engineering success from alpha success.

---

## 10. Follow-up after v4

If v4 works but repeatedly stops for the same blocker, the next plan should be v5:

```text
Harvest Agent v5 Research System Self-Improvement Plan
```

v5 should remain plan-first and human-gated. It may propose changes to route policy, data requirements, portfolio construction, or research governance, but it should not automatically modify core research code without explicit approval and code review.
