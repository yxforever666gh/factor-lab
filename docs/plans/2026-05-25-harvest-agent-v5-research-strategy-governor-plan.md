# Harvest Agent v5 Research Strategy Governor Implementation Plan

> **For Hermes:** Use `subagent-driven-development` or strict TDD to implement this plan task-by-task. Keep the implementation Hermes-native, local-artifact driven, simulation/backtest-only, bounded, auditable, and rollback-safe. Do **not** enable timers, daemons, live trading, broker/order paths, automatic promotion, broad external data/API expansion, arbitrary LLM-written factor code, or self-modifying code in this phase.

**Goal:** Upgrade Harvest Agent from v4 “按 v3 plan 自动连续执行” to v5 “根据多周期失败证据主动治理研究策略”，让系统在重复失败时不再机械循环，而是能识别研究浪费、冻结无效分支、切换机制路线、收缩/扩展实验空间、发出 data request 或 stop route。

**Architecture:** Add a deterministic `harvest_strategy_governor` layer above v4 controller and below human approval. v5 reads controller ledgers, cycle artifacts, route state, OOS validation, failure attribution, semantic fingerprints, and research lessons; produces a bounded `v5_strategy_plan.json` that v4 can consume as a strategy override/input. v5 is a strategy-policy layer, not a code-writing or trading layer: it changes planned research actions and budgets only through explicit artifacts and tests.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, JSON/Markdown artifacts under `artifacts/harvest_agent/strategy_runs/`, optional config under `configs/harvest_strategy_governor.json`, pytest tests under `tests/`, docs under `docs/plans/`.

---

## 0. Current state and motivation

v4 is implemented and verified:

```text
latest v4 controlled run: controller_20260525T013539Z
cycles_run: 2
executed_backtest_count: 72
stop_reason: backtest_budget_exceeded
branch_sequence: portfolio_construction_branch -> cost_robustness_branch
latest_cycle: cycle_0057
oos_class: fail
research_decision: portfolio_construction_branch
best Sharpe: 0.315312
max_drawdown: -0.576326
```

Engineering outcome:

```text
v3_next_cycle_plan -> v4 materializer -> budget gate -> real controlled backtest -> OOS/failure attribution -> next v3 plan
```

works.

Alpha outcome:

```text
still failed: Sharpe < 0.7 and max_drawdown < -0.35
```

The problem is now no longer “can the agent execute the next cycle?” It can. The problem is “will it keep cycling through low-information branch repairs?” v5 should address that by governing research strategy across cycles.

---

## 1. v5 scope

### In scope

v5 should implement:

1. Multi-cycle evidence aggregation across Harvest cycles and controller runs.
2. Detection of repeated low-information loops such as alternating `portfolio_construction_branch` / `cost_robustness_branch` without OOS improvement.
3. Route-level strategy decisions:
   - `continue_with_constraints`
   - `shrink_search_space`
   - `switch_mechanism_route`
   - `request_data`
   - `stop_route`
   - `manual_review`
4. Experiment-space governance:
   - cap repeated branches;
   - prefer non-duplicate hypotheses;
   - block exact semantic repeats;
   - enforce minimum expected information gain;
   - set per-strategy backtest budgets.
5. A strategy plan artifact that v4 can read before materializing the next cycle.
6. Status/inspection script for the latest v5 strategy run.
7. Tests proving deterministic decisions, safety boundaries, and v4 integration.

### Out of scope

v5 must **not** implement:

- live trading;
- broker/order integration;
- automatic promotion;
- automatic timer/service enabling;
- broad daemon restoration;
- external data/API fetching;
- arbitrary LLM-written factor code;
- self-modifying code;
- automatic edits to source code;
- unbounded autonomous exploration.

---

## 2. Target runtime behavior

Dry-run strategy review:

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python scripts/run_harvest_strategy_governor.py \
  --lookback-cycles 8 \
  --max-next-backtests 120
```

Write strategy plan artifact:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_strategy_governor.py \
  --lookback-cycles 8 \
  --max-next-backtests 120 \
  --write
```

Run v4 with latest v5 strategy plan enabled:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 3 \
  --max-backtests 120 \
  --allow-controlled-execution \
  --use-latest-strategy-plan \
  --stop-on-data-request \
  --stop-on-route-stop \
  --stop-on-manual-review
```

Status inspection:

```bash
PYTHONPATH=src .venv/bin/python scripts/inspect_harvest_strategy_status.py
```

Expected flow:

```text
read controller ledgers + recent cycle artifacts
  -> aggregate strategy evidence
  -> detect repeated branch/route/failure loops
  -> score expected information gain for possible next strategies
  -> choose bounded strategy decision
  -> write v5_strategy_plan.json/.md
  -> v4 controller optionally consumes strategy plan
  -> v4 materializes a safer next correction plan
```

---

## 3. Artifact contract

Each v5 strategy run writes:

```text
artifacts/harvest_agent/strategy_runs/<strategy_run_id>/
  strategy_config.json
  strategy_evidence.json
  strategy_evidence.md
  strategy_decision.json
  strategy_decision.md
  v5_strategy_plan.json
  v5_strategy_plan.md
  strategy_summary.json
  strategy_summary.md
```

Latest pointer:

```text
artifacts/harvest_agent/latest_strategy_run.json
```

The strategy plan should have this shape:

```json
{
  "schema_version": 1,
  "strategy_run_id": "strategy_20260525T000000Z",
  "based_on_controller_run_id": "controller_20260525T013539Z",
  "based_on_cycle_id": "cycle_0057",
  "plan_status": "planned",
  "strategy_decision": "shrink_search_space",
  "reason_codes": [
    "repeated_oos_failures",
    "branch_loop_detected",
    "drawdown_not_improving"
  ],
  "allowed_branches": ["risk_reduction_branch"],
  "blocked_branches": ["cost_robustness_branch", "portfolio_construction_branch"],
  "route_action": {
    "type": "continue_with_constraints",
    "mechanism_id": "industry_relative_value"
  },
  "experiment_constraints": {
    "max_next_backtests": 120,
    "allowed_cost_bps": [30, 60],
    "allowed_holding_counts": [75, 100],
    "require_non_duplicate_semantic_hash": true,
    "require_expected_information_gain_min": "medium"
  },
  "stop_conditions": [
    "next_cycle_oos_class_fail_without_sharpe_improvement",
    "max_drawdown_worse_than_-0.55",
    "semantic_duplicate_detected"
  ],
  "manual_approval_required": false,
  "safety": {
    "no_timer": true,
    "no_daemon": true,
    "no_live_trading": true,
    "no_automatic_promotion": true
  }
}
```

If v5 chooses data request or route stop:

```json
{
  "plan_status": "blocked",
  "strategy_decision": "request_data",
  "manual_approval_required": true,
  "data_request": {
    "missing_fields": ["cashflow_quality", "leverage", "earnings_revision"],
    "reason": "current primitives cannot test value-trap avoidance mechanism"
  }
}
```

or:

```json
{
  "plan_status": "stopped",
  "strategy_decision": "stop_route",
  "manual_approval_required": true,
  "reason_codes": ["route_exhausted", "semantic_repeat_limit_reached"]
}
```

---

## 4. Strategy decision policy

v5 should be deterministic and conservative.

### Evidence inputs

Read recent cycle artifacts:

```text
artifacts/harvest_agent/cycle_*/
  result_analysis.json
  diagnosis.json
  oos_validation.json
  failure_attribution.json
  route_state.json
  research_decision.json
  semantic_signature.json
  mechanism_route.json
  v3_next_cycle_plan.json
```

Read controller artifacts:

```text
artifacts/harvest_agent/controller_runs/*/
  controller_summary.json
  controller_ledger.jsonl
  latest_decision.json
  budget_state.json
  stop_state.json
```

Optional knowledge inputs:

```text
knowledge/harvest_research_lessons.md
knowledge/harvest_route_state.json
knowledge/harvest_data_requests.json
knowledge/mechanism_lessons.md
knowledge/data_blockers.json
```

### Decision classes

Use these strategy decisions:

```text
continue_with_constraints
shrink_search_space
switch_mechanism_route
request_data
stop_route
manual_review
```

### Initial deterministic policy

1. If latest route state is `stop`, emit `stop_route`.
2. If required route fields are blocked/missing, emit `request_data`.
3. If the same route has >= 4 consecutive OOS fails and no near miss, emit `switch_mechanism_route` if another ready route exists; otherwise `manual_review`.
4. If branches alternate between the same two branches for >= 3 cycles with no Sharpe/drawdown improvement, emit `shrink_search_space` and block those branches for one cycle.
5. If drawdown is worsening while Sharpe is not improving, prefer `risk_reduction_branch` and cap backtests.
6. If cost robustness repeatedly fails at positive costs, block zero-cost-only evidence and require positive-cost candidates.
7. If portfolio construction repeatedly fails to monetize positive IC, require a new construction hypothesis or stop the route.
8. If no rule fires, emit `continue_with_constraints`.

### Improvement metrics

Define improvement conservatively:

```text
sharpe_improved = latest_best_sharpe >= prior_best_sharpe + 0.05
max_drawdown_improved = latest_max_drawdown >= prior_max_drawdown + 0.03
near_miss = sharpe >= 0.6 and max_drawdown >= -0.4
```

Do not call alpha progress if only `executed_backtest_count` increases.

---

## 5. Proposed files

Create:

```text
src/factor_lab/harvest_strategy_evidence.py
src/factor_lab/harvest_strategy_policy.py
src/factor_lab/harvest_strategy_plan.py
src/factor_lab/harvest_strategy_governor.py
scripts/run_harvest_strategy_governor.py
scripts/inspect_harvest_strategy_status.py
tests/test_harvest_strategy_evidence.py
tests/test_harvest_strategy_policy.py
tests/test_harvest_strategy_plan.py
tests/test_harvest_strategy_governor.py
tests/test_run_harvest_strategy_governor.py
tests/test_inspect_harvest_strategy_status.py
```

Modify:

```text
src/factor_lab/harvest_autonomous_research_controller.py
src/factor_lab/harvest_v3_plan_materializer.py
scripts/run_harvest_autonomous_research_controller.py
tests/test_harvest_autonomous_research_controller.py
tests/test_harvest_v3_plan_materializer.py
tests/test_run_harvest_autonomous_research_controller.py
docs/plans/2026-05-25-harvest-agent-v5-research-strategy-governor-plan.md
```

Optional config:

```text
configs/harvest_strategy_governor.json
```

---

## 6. Implementation tasks

### Task 1: Add strategy evidence loader

**Objective:** Load recent Harvest cycles and controller runs into a normalized evidence object.

**Files:**

- Create: `src/factor_lab/harvest_strategy_evidence.py`
- Test: `tests/test_harvest_strategy_evidence.py`

**Step 1: Write failing tests**

Create tests that build temp artifacts:

```python
def test_collect_strategy_evidence_reads_recent_cycles_and_controller_runs(tmp_path):
    # arrange: write cycle_0056/cycle_0057 and controller ledger
    evidence = collect_strategy_evidence(tmp_path, lookback_cycles=2)
    assert evidence["latest_cycle_id"] == "cycle_0057"
    assert evidence["latest_controller_run_id"] == "controller_test"
    assert len(evidence["cycles"]) == 2
    assert evidence["branch_sequence"] == ["portfolio_construction_branch", "cost_robustness_branch"]
```

Also test missing artifacts:

```python
def test_collect_strategy_evidence_handles_missing_artifacts(tmp_path):
    evidence = collect_strategy_evidence(tmp_path, lookback_cycles=5)
    assert evidence["cycles"] == []
    assert evidence["latest_cycle_id"] is None
```

**Step 2: Run tests to verify failure**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_evidence.py -q
```

Expected: fail because module/function does not exist.

**Step 3: Implement minimal loader**

Implement:

```python
def collect_strategy_evidence(root: str | Path = ".", lookback_cycles: int = 8) -> dict[str, Any]:
    ...
```

The returned object should include:

```text
schema_version
latest_cycle_id
latest_controller_run_id
cycles[]
controller_runs[]
branch_sequence
failure_blocker_counts
semantic_hash_counts
route_counts
```

**Step 4: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_evidence.py -q
```

Expected: pass.

---

### Task 2: Add branch-loop and low-information detectors

**Objective:** Detect repeated branch loops, semantic repeats, and no-improvement patterns.

**Files:**

- Modify: `src/factor_lab/harvest_strategy_evidence.py`
- Test: `tests/test_harvest_strategy_evidence.py`

**Step 1: Write failing tests**

```python
def test_detect_branch_loop_for_alternating_failed_branches():
    evidence = {
        "branch_sequence": [
            "portfolio_construction_branch",
            "cost_robustness_branch",
            "portfolio_construction_branch",
            "cost_robustness_branch",
        ],
        "cycles": [
            {"oos_class": "fail", "best_sharpe": 0.45, "max_drawdown": -0.49},
            {"oos_class": "fail", "best_sharpe": 0.31, "max_drawdown": -0.57},
        ],
    }
    loops = detect_strategy_loops(evidence)
    assert "branch_loop_detected" in loops["reason_codes"]
```

```python
def test_detect_no_improvement_when_sharpe_and_drawdown_worsen():
    loops = detect_strategy_loops({"cycles": [
        {"best_sharpe": 0.45, "max_drawdown": -0.49, "oos_class": "fail"},
        {"best_sharpe": 0.31, "max_drawdown": -0.57, "oos_class": "fail"},
    ]})
    assert "drawdown_not_improving" in loops["reason_codes"]
```

**Step 2: Implement**

Add:

```python
def detect_strategy_loops(evidence: dict[str, Any]) -> dict[str, Any]:
    ...
```

Return:

```json
{
  "loop_detected": true,
  "reason_codes": ["branch_loop_detected", "drawdown_not_improving"],
  "blocked_branches": ["portfolio_construction_branch", "cost_robustness_branch"]
}
```

**Step 3: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_evidence.py -q
```

---

### Task 3: Add deterministic strategy policy

**Objective:** Convert evidence into a strategy decision.

**Files:**

- Create: `src/factor_lab/harvest_strategy_policy.py`
- Test: `tests/test_harvest_strategy_policy.py`

**Step 1: Write tests for each decision class**

Required tests:

```text
test_policy_stops_when_route_state_stop
test_policy_requests_data_when_missing_required_fields
test_policy_switches_route_after_repeated_oos_failures
test_policy_shrinks_search_space_on_branch_loop
test_policy_prefers_risk_reduction_when_drawdown_worsens
test_policy_continues_with_constraints_when_no_blocker
```

Example:

```python
def test_policy_shrinks_search_space_on_branch_loop():
    decision = decide_strategy({
        "latest_cycle_id": "cycle_0057",
        "current_route_status": "active",
        "ready_alternative_routes": [],
        "loop_analysis": {
            "loop_detected": True,
            "reason_codes": ["branch_loop_detected", "drawdown_not_improving"],
            "blocked_branches": ["portfolio_construction_branch", "cost_robustness_branch"],
        },
        "cycles": [
            {"oos_class": "fail", "best_sharpe": 0.45, "max_drawdown": -0.49},
            {"oos_class": "fail", "best_sharpe": 0.31, "max_drawdown": -0.57},
        ],
    })
    assert decision["strategy_decision"] == "shrink_search_space"
    assert "risk_reduction_branch" in decision["allowed_branches"]
```

**Step 2: Implement**

Add:

```python
def decide_strategy(evidence: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    ...
```

Decision output should include:

```text
strategy_decision
plan_status
reason_codes
allowed_branches
blocked_branches
route_action
experiment_constraints
manual_approval_required
safety
```

**Step 3: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_policy.py -q
```

---

### Task 4: Add v5 strategy plan writer

**Objective:** Write `v5_strategy_plan.json/.md` from strategy decision and evidence.

**Files:**

- Create: `src/factor_lab/harvest_strategy_plan.py`
- Test: `tests/test_harvest_strategy_plan.py`

**Step 1: Write tests**

```python
def test_build_strategy_plan_contains_required_contract_fields():
    plan = build_strategy_plan(
        strategy_run_id="strategy_test",
        evidence={"latest_cycle_id": "cycle_0057", "latest_controller_run_id": "controller_test"},
        decision={"strategy_decision": "shrink_search_space", "plan_status": "planned"},
        max_next_backtests=120,
    )
    assert plan["schema_version"] == 1
    assert plan["strategy_run_id"] == "strategy_test"
    assert plan["based_on_cycle_id"] == "cycle_0057"
    assert plan["experiment_constraints"]["max_next_backtests"] == 120
    assert plan["safety"]["no_live_trading"] is True
```

```python
def test_write_strategy_plan_writes_json_and_markdown(tmp_path):
    run_dir = tmp_path / "artifacts/harvest_agent/strategy_runs/strategy_test"
    write_strategy_plan(run_dir, {"schema_version": 1, "strategy_decision": "manual_review"})
    assert (run_dir / "v5_strategy_plan.json").exists()
    assert (run_dir / "v5_strategy_plan.md").exists()
```

**Step 2: Implement**

Functions:

```python
def build_strategy_plan(...): ...
def write_strategy_plan(run_dir: str | Path, plan: dict[str, Any]) -> None: ...
def write_strategy_summary(run_dir: str | Path, evidence: dict[str, Any], decision: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]: ...
```

**Step 3: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_plan.py -q
```

---

### Task 5: Add strategy governor orchestrator

**Objective:** Combine evidence, policy, and plan writer into one callable v5 governor.

**Files:**

- Create: `src/factor_lab/harvest_strategy_governor.py`
- Test: `tests/test_harvest_strategy_governor.py`

**Step 1: Write tests**

```python
def test_strategy_governor_dry_run_does_not_write_latest_pointer(tmp_path):
    summary = run_harvest_strategy_governor(tmp_path, lookback_cycles=2, write=False)
    assert summary["strategy_status"] == "dry_run"
    assert not (tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").exists()
```

```python
def test_strategy_governor_write_creates_strategy_artifacts(tmp_path):
    # arrange minimal failed cycles/controller ledger
    summary = run_harvest_strategy_governor(tmp_path, lookback_cycles=2, max_next_backtests=120, write=True)
    run_dir = tmp_path / summary["artifacts_dir"]
    assert (run_dir / "strategy_evidence.json").exists()
    assert (run_dir / "strategy_decision.json").exists()
    assert (run_dir / "v5_strategy_plan.json").exists()
    assert (tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").exists()
```

**Step 2: Implement**

Add:

```python
def run_harvest_strategy_governor(
    root: str | Path = ".",
    lookback_cycles: int = 8,
    max_next_backtests: int = 120,
    write: bool = False,
    strategy_run_id: str | None = None,
) -> dict[str, Any]:
    ...
```

Dry-run should return the proposed plan and summary but should not write `latest_strategy_run.json`.

**Step 3: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_harvest_strategy_governor.py -q
```

---

### Task 6: Add CLI scripts

**Objective:** Expose v5 strategy governor and status inspection via scripts.

**Files:**

- Create: `scripts/run_harvest_strategy_governor.py`
- Create: `scripts/inspect_harvest_strategy_status.py`
- Test: `tests/test_run_harvest_strategy_governor.py`
- Test: `tests/test_inspect_harvest_strategy_status.py`

**Step 1: Write CLI tests**

Use `subprocess.run` against temp root.

Required checks:

```text
--help exits 0
without --write prints JSON and does not write latest pointer
with --write creates latest_strategy_run.json
inspect prints latest strategy decision, based_on_cycle_id, plan_status, safety fields
```

**Step 2: Implement scripts**

`run_harvest_strategy_governor.py` args:

```text
--root
--lookback-cycles
--max-next-backtests
--write
```

`inspect_harvest_strategy_status.py` args:

```text
--root
```

**Step 3: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_run_harvest_strategy_governor.py \
  tests/test_inspect_harvest_strategy_status.py \
  -q
```

---

### Task 7: Integrate v5 strategy plan into v4 materialization

**Objective:** Allow v4 controller to optionally read the latest v5 strategy plan and apply safe constraints before materializing the next cycle.

**Files:**

- Modify: `src/factor_lab/harvest_autonomous_research_controller.py`
- Modify: `src/factor_lab/harvest_v3_plan_materializer.py`
- Modify: `scripts/run_harvest_autonomous_research_controller.py`
- Test: `tests/test_harvest_autonomous_research_controller.py`
- Test: `tests/test_harvest_v3_plan_materializer.py`
- Test: `tests/test_run_harvest_autonomous_research_controller.py`

**Step 1: Write tests**

```python
def test_controller_applies_latest_strategy_plan_when_enabled(tmp_path):
    # arrange latest v3 plan plus latest_strategy_run.json pointing to shrink_search_space plan
    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=120),
        use_latest_strategy_plan=True,
    )
    correction_plan = json.loads((tmp_path / "artifacts/harvest_agent/cycle_0051/correction_plan.json").read_text())
    assert correction_plan["strategy_plan_id"] == "strategy_test"
    assert correction_plan["controller_constraints"]["max_next_backtests"] == 120
```

```python
def test_strategy_plan_blocked_stops_controller_before_execution(tmp_path):
    # arrange v5 plan_status=blocked / strategy_decision=request_data
    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=120, allow_controlled_execution=True),
        use_latest_strategy_plan=True,
    )
    assert summary["cycles_run"] == 0
    assert summary["stop_reason"] == "strategy_plan_blocked"
```

**Step 2: Implement loader**

Add helper in controller or new small module:

```python
def load_latest_strategy_plan(root: str | Path = ".") -> dict[str, Any] | None:
    ...
```

**Step 3: Apply constraints**

Materializer should support optional:

```python
materialize_v3_next_plan(..., strategy_plan: dict[str, Any] | None = None)
```

Apply only safe deterministic constraints:

```text
max_next_backtests
allowed_branches / blocked_branches
allowed_cost_bps
allowed_holding_counts
require_non_duplicate_semantic_hash metadata
```

Do not let strategy plan inject arbitrary expressions or code.

**Step 4: Add CLI flag**

```text
--use-latest-strategy-plan
```

Default: false, preserving v4 behavior.

**Step 5: Verify**

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_harvest_v3_plan_materializer.py \
  tests/test_run_harvest_autonomous_research_controller.py \
  -q
```

---

### Task 8: Add strategy-aware controlled smoke

**Objective:** Prove v5 can generate a strategy plan from current failed evidence and v4 can consume it in dry-run without mutating latest real cycle.

**Files:**

- Modify tests as needed.
- Update this plan document with implementation status.

**Step 1: Run strategy governor dry-run**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_strategy_governor.py \
  --lookback-cycles 8 \
  --max-next-backtests 120
```

Expected:

```text
strategy_status=dry_run
strategy_decision in {shrink_search_space, switch_mechanism_route, request_data, manual_review}
no latest_strategy_run.json written
```

**Step 2: Write strategy plan**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_strategy_governor.py \
  --lookback-cycles 8 \
  --max-next-backtests 120 \
  --write
```

Expected artifacts:

```text
artifacts/harvest_agent/strategy_runs/<strategy_run_id>/v5_strategy_plan.json
artifacts/harvest_agent/latest_strategy_run.json
```

**Step 3: Run v4 dry-run with strategy plan**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 1 \
  --max-backtests 120 \
  --use-latest-strategy-plan
```

Expected:

```text
executed_backtest_count=0
stop_reason=dry_run_complete
latest_cycle.json remains latest controlled metric-bearing cycle
correction_plan contains strategy_plan_id / controller_constraints
```

**Step 4: Optional controlled smoke only after dry-run passes**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_autonomous_research_controller.py \
  --max-cycles 1 \
  --max-backtests 120 \
  --allow-controlled-execution \
  --use-latest-strategy-plan \
  --stop-on-data-request \
  --stop-on-route-stop \
  --stop-on-manual-review
```

Expected:

```text
real metric-bearing backtests if strategy plan_status=planned
or cycles_run=0 with explicit strategy stop reason if blocked/stopped/manual
no timer/service/live trading/promotion
```

---

## 7. Verification commands

Targeted py_compile:

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_strategy_evidence.py \
  src/factor_lab/harvest_strategy_policy.py \
  src/factor_lab/harvest_strategy_plan.py \
  src/factor_lab/harvest_strategy_governor.py \
  src/factor_lab/harvest_autonomous_research_controller.py \
  src/factor_lab/harvest_v3_plan_materializer.py \
  scripts/run_harvest_strategy_governor.py \
  scripts/inspect_harvest_strategy_status.py \
  scripts/run_harvest_autonomous_research_controller.py
```

Targeted tests:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_strategy_evidence.py \
  tests/test_harvest_strategy_policy.py \
  tests/test_harvest_strategy_plan.py \
  tests/test_harvest_strategy_governor.py \
  tests/test_run_harvest_strategy_governor.py \
  tests/test_inspect_harvest_strategy_status.py \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_harvest_v3_plan_materializer.py \
  tests/test_run_harvest_autonomous_research_controller.py \
  -q
```

Regression slice:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_controller_policy.py \
  tests/test_harvest_v3_plan_loader.py \
  tests/test_harvest_v3_plan_materializer.py \
  tests/test_harvest_controller_budget.py \
  tests/test_harvest_controller_ledger.py \
  tests/test_harvest_real_execution_guard.py \
  tests/test_harvest_cycle_runner.py \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_run_harvest_autonomous_research_controller.py \
  tests/test_inspect_harvest_controller_status.py \
  tests/test_harvest_evolution_loop.py \
  -q
```

Safety checks:

```bash
systemctl --user is-active factor-lab-harvest-agent.timer || true
systemctl --user is-active factor-lab-harvest-agent.service || true
systemctl --user is-active factor-lab-research-daemon.service || true
```

Expected:

```text
Harvest timer/service remain inactive.
Research daemon may already be active, but v5 must not start or modify it.
No live trading, broker path, or automatic promotion artifacts are created.
```

---

## 8. Acceptance criteria

v5 is complete only when:

- strategy governor can read current v4/cycle artifacts and produce a `v5_strategy_plan.json`;
- strategy decisions are deterministic and tested for all decision classes;
- repeated branch loops and no-improvement patterns are detected;
- data request and stop route decisions block v4 before execution;
- planned strategy decisions can constrain v4 materialization without arbitrary code injection;
- dry-run v4 with strategy plan does not advance `latest_cycle.json`;
- controlled v4 with strategy plan executes real metric-bearing backtests only behind `--allow-controlled-execution`;
- controller/strategy summaries separate engineering success from alpha success;
- tests and py_compile pass;
- no timer, daemon, live trading, broker/order path, or automatic promotion is enabled.

---

## 9. Reporting requirements

When reporting v5 results, always separate:

1. **Strategy governance success:** Did v5 detect loop/failure patterns and write a coherent strategy plan?
2. **Controller execution success:** Did v4 consume the strategy plan and obey constraints?
3. **Controlled execution success:** Did real metric-bearing backtests run when explicitly allowed?
4. **Alpha outcome:** Did Sharpe, drawdown, OOS class, and pass gate actually improve?
5. **Safety state:** Timer/service/live trading/promotion all remain disabled.

Never say “alpha improved” because the system ran more cycles. Only report alpha progress if metrics improved against prior controlled cycles.

---

## 10. First implementation recommendation

Start with the smallest useful v5 slice:

```text
Task 1 evidence loader
Task 2 loop/no-improvement detectors
Task 3 strategy policy
Task 4 strategy plan writer
Task 5 strategy governor CLI
```

Only after that passes, integrate v4 consumption.

---

## 11. Implementation status — 2026-05-25

Implemented in this pass:

- `src/factor_lab/harvest_strategy_evidence.py`
  - collects recent cycle/controller evidence;
  - aggregates branch sequence, failure blockers, semantic hash counts, route counts;
  - detects repeated OOS failure, Sharpe/drawdown non-improvement, semantic repeat, and alternating branch-loop patterns.
- `src/factor_lab/harvest_strategy_policy.py`
  - deterministic conservative decision policy for `continue_with_constraints`, `shrink_search_space`, `switch_mechanism_route`, `request_data`, and `stop_route`.
- `src/factor_lab/harvest_strategy_plan.py`
  - builds and writes `v5_strategy_plan.json/.md` plus strategy summaries.
- `src/factor_lab/harvest_strategy_governor.py`
  - dry-run/write orchestrator;
  - latest strategy plan loader for v4 integration.
- `scripts/run_harvest_strategy_governor.py`
- `scripts/inspect_harvest_strategy_status.py`
- v4 integration:
  - `scripts/run_harvest_autonomous_research_controller.py --use-latest-strategy-plan`;
  - `run_harvest_autonomous_research_controller(..., use_latest_strategy_plan=True)`;
  - `materialize_v3_next_plan(..., strategy_plan=...)` applies only safe deterministic constraints and records `strategy_plan_id` / `controller_constraints`.

Verification completed:

```text
py_compile target slice: pass
pytest targeted + Harvest v4 regression slice: 55 passed
strategy governor dry-run: strategy_status=dry_run, latest pointer not written
strategy governor write: wrote strategy_20260525T015844Z
strategy status inspect: strategy_status=available
v4 strategy dry-run: cycles_run=1, executed_backtest_count=0, stop_reason=dry_run_complete
latest controlled metric-bearing cycle pointer remained cycle_0057
no timer/service/live trading/promotion enabled by this implementation
```

Current live v5 decision from existing evidence:

```text
strategy_decision=shrink_search_space
based_on_cycle_id=cycle_0057
based_on_controller_run_id=controller_20260525T013539Z
reason_codes=[repeated_oos_failures, sharpe_not_improving, drawdown_not_improving, semantic_repeat_limit_reached]
allowed_branches=[risk_reduction_branch]
max_next_backtests=120
```

The likely first real strategy decision from current evidence should be one of:

```text
shrink_search_space
manual_review
switch_mechanism_route
```

because the latest observed sequence has repeated OOS failures, branch cycling, worsening drawdown, and no promotable alpha.
