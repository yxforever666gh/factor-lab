# Autonomous Strategy Lab Redesign Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. Keep the first run dry-run/artifact-only. Do not enable timers, daemons, queue writes, live trading, auto-promotion, model/provider pinning, or broad external data fetching.

**Goal:** Replace the current “generate many factor variants then backtest” loop with a portfolio-aware autonomous research system that can diagnose why strategies fail, choose the next research action, and stop or request new data/mechanisms when evidence says more backtests are low value.

**Architecture:** Build an `Autonomous Strategy Lab` as a deterministic controller around existing Factor Lab evidence. It reads factor evaluation history, candidate status, risk-reduction artifacts, Harvest cycle artifacts, and knowledge files; produces a strategy decision artifact; then optionally materializes a bounded experiment plan. The key change is that the agent optimizes a full research objective — alpha + risk + data sufficiency + information gain — rather than only factor expression generation.

**Tech Stack:** Python 3, SQLite, existing Factor Lab artifacts, JSON/Markdown reports, pytest. Initial MVP lives in `scripts/run_autonomous_strategy_lab.py` and `src/factor_lab/autonomous_strategy_lab.py`, then can be wired into existing Harvest/v4/v5 controllers only after dry-run verification.

---

## 0. Design position

The current system already has many pieces of autonomy, but it still behaves too much like a high-throughput factor factory. The failure mode is not only execution. It is objective design:

- It can run backtests, but it does not reliably decide that the current search space is exhausted.
- It can generate factor candidates, but many are near-duplicates or portfolio-construction fragile.
- It can diagnose blockers, but the diagnosis does not yet become a clean next research contract.
- It tests factor signals before it knows whether portfolio/risk constraints make the route viable.

The redesigned system should treat each cycle as a scientific decision:

```text
Evidence -> Diagnosis -> Hypothesis -> Cheap screen -> Controlled experiment -> Verdict -> Learning -> Next action / stop
```

The default next action must sometimes be **do not run another backtest**.

---

## 1. Target loop

```text
AutonomousStrategyLab.run_once()
  -> collect_evidence()
  -> diagnose_failure_modes()
  -> score_research_routes()
  -> choose_strategy_decision()
  -> materialize_next_plan(dry-run by default)
  -> write artifacts
```

Decision types:

- `continue_route_with_constraints`
- `repair_portfolio_construction`
- `switch_mechanism_route`
- `request_data`
- `stop_route`
- `manual_review`

Hard safety defaults:

- `dry_run=true` unless explicitly overridden.
- `queue_write_allowed=false` by default.
- `automation_allowed=false` until at least one dry-run + one controlled run pass.
- no systemd/timer changes.
- no live trading.
- no auto-promotion.
- no model/provider pinning.

---

## 2. Research objective

A candidate route is only worth running if it scores well on all four axes:

1. **Signal evidence** — IC, split robustness, OOS class, pass rate.
2. **Portfolio viability** — drawdown, turnover, cost sensitivity, capacity proxy.
3. **Information gain** — novelty, semantic distance from previous failed cycles, falsifiability.
4. **Data sufficiency** — enough fields and coverage to test the economic mechanism.

A route with decent IC but impossible drawdown should not receive more expression variants. It should move to portfolio construction repair, new risk controls, or a mechanism switch.

---

## 3. MVP artifact contract

Create:

```text
artifacts/autonomous_strategy_lab/latest_decision.json
artifacts/autonomous_strategy_lab/latest_decision.md
```

JSON shape:

```json
{
  "schema_version": 1,
  "run_id": "strategy_lab_YYYYMMDDTHHMMSSZ",
  "mode": "dry_run",
  "evidence_summary": {},
  "diagnosis": {},
  "route_scores": [],
  "decision": "request_data",
  "reason_codes": [],
  "next_plan": {},
  "safety": {
    "queue_write_allowed": false,
    "automation_allowed": false,
    "live_trading_enabled": false
  }
}
```

---

## 4. MVP scoring rules

Start deterministic and simple:

### Drawdown blocker

If latest risk-reduction repair says:

```text
repair_status = blocked_no_drawdown_safe_candidate
best_available_max_drawdown < drawdown_limit
```

then block automatic continuation of the same route unless the next plan changes portfolio construction or mechanism/data.

### Search-space exhaustion

If candidate history shows many rejected/fragile/testing candidates and no promotion-safe route, raise:

```text
search_space_low_information
```

### Data insufficiency

If latest Harvest cycles show repeated:

```text
oos_class = insufficient_data
repeated_blockers includes no_ok_rows
```

then prefer `request_data` or `switch_mechanism_route` over more backtests.

### Strategy choice priority

1. If drawdown blocker persists and no safe candidate exists: `manual_review` or `request_data`.
2. If data insufficiency repeats: `request_data`.
3. If semantic repeats dominate: `switch_mechanism_route` or `shrink_search_space`.
4. Only if risk and data are acceptable: `continue_route_with_constraints`.

---

## 5. Task plan

### Task 1: Add dry-run prototype script

**Objective:** Produce an artifact-backed autonomous decision without queue writes or source integration.

**Files:**
- Create: `scripts/run_autonomous_strategy_lab.py`
- Output: `artifacts/autonomous_strategy_lab/latest_decision.{json,md}`

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_strategy_lab.py
```

Expected:

- exit code 0
- writes JSON and Markdown artifacts
- `queue_write_allowed=false`
- decision is one of the allowed decision types

### Task 2: Extract reusable module

**Objective:** Move prototype logic to importable module.

**Files:**
- Create: `src/factor_lab/autonomous_strategy_lab.py`
- Test: `tests/test_autonomous_strategy_lab.py`

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_autonomous_strategy_lab.py -q
```

### Task 3: Add route registry

**Objective:** Make mechanism choices explicit rather than embedded in script literals.

**Files:**
- Create: `configs/autonomous_strategy_routes.json`
- Modify: `src/factor_lab/autonomous_strategy_lab.py`
- Test: `tests/test_autonomous_strategy_lab_routes.py`

### Task 4: Add cheap-screen planner

**Objective:** Generate cheap next experiments before full backtests.

Examples:

- data coverage screen
- feature availability screen
- risk feasibility screen
- route novelty screen

**Files:**
- Modify: `src/factor_lab/autonomous_strategy_lab.py`
- Test: `tests/test_autonomous_strategy_lab_planner.py`

### Task 5: Controlled execution adapter

**Objective:** Convert a decision into at most N controlled backtests, still dry-run first.

**Files:**
- Create: `src/factor_lab/autonomous_strategy_execution_adapter.py`
- Create: `scripts/run_autonomous_strategy_lab_controlled.py`
- Test: `tests/test_autonomous_strategy_execution_adapter.py`

Guardrails:

- explicit `--allow-controlled-execution` required
- hard cap on backtests
- no daemon/timer/systemd
- no auto-promotion

### Task 6: Integrate with Harvest only after standalone MVP passes

**Objective:** Let Harvest consume `latest_decision.json` as a policy input, not as a replacement for safety gates.

**Files:**
- Modify: `src/factor_lab/harvest_autonomous_research_controller.py`
- Test: relevant Harvest controller tests

---

## 6. Acceptance criteria

MVP is acceptable when:

1. A dry-run reads current project evidence and writes `latest_decision.{json,md}`.
2. The decision correctly identifies the current drawdown blocker.
3. The decision refuses queue writes and automation by default.
4. The next plan is specific: data request, mechanism switch, or bounded portfolio repair — not vague “run more”.
5. Targeted tests pass before any controlled execution is attempted.

---

## 7. Initial expected decision for current project state

Given current evidence:

- latest risk-reduction repair has `blocked_no_drawdown_safe_candidate`;
- best available max drawdown is worse than the -35% limit;
- latest Harvest cycles include repeated `insufficient_data/no_ok_rows`;
- current controlled restart dry-run has `would_run_count=0`;

The first dry-run should likely choose:

```text
decision = request_data 或 manual_review
```

with a next plan like:

```text
Do not run another same-route backtest. Write blocker report, request new mechanism/data, then only run a capped cheap screen.
```
