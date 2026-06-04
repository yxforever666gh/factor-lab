# Harvest Agent Self-Correction Loop Implementation Plan

> **For Hermes:** Use test-driven-development and implement this plan task-by-task. Keep the loop Hermes-native, local-artifact driven, bounded, simulation/backtest-only, and auditable.

**Goal:** Upgrade Harvest Agent from a fixed cycle executor into a result-driven self-correction research loop that analyzes prior backtests, diagnoses failure modes, writes executable repair plans, runs changed experiments, compares outcomes, blocks duplicates, updates knowledge, and decides the next action.

**Architecture:** Add deterministic v1 modules around the existing Harvest artifact namespace: result analyzer → diagnostician → self-correction planner → experiment fingerprint gate → filtered backtest runner → comparative evaluator → evolution loop. The v1 loop remains bounded to the existing value-quality/bucket-aware route and uses a small controlled action vocabulary rather than arbitrary code generation.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, artifacts under `artifacts/harvest_agent/`, pytest tests under `tests/`, existing `small_institutional_backtest_matrix` runner and Harvest knowledge/report utilities.

---

## Acceptance Criteria

1. Given a completed Harvest cycle with `result.json`, the analyzer emits structured diagnostics including drawdown, Sharpe, cost sensitivity, window concentration, and promotion readiness.
2. The diagnostician maps diagnostics to failure classes and root-cause hypotheses.
3. The planner generates a machine-readable executable repair plan using only allowed v1 actions.
4. The executor applies plan filters/ranking constraints to real backtests and writes real metrics, not placeholders.
5. The fingerprint gate blocks exact duplicate plans and forces variation across cycles.
6. The comparative evaluator compares baseline vs candidate metrics and decides continue/modify/pivot/manual review.
7. A 3+ cycle evolution loop produces non-identical fingerprints and result-driven plans.
8. No timer, daemon, live trading, broker, or external API path is started.

## V1 Action Vocabulary

Supported plan actions only:

- `add_filter`: quantile filter on `volatility_20`, `turnover`, `total_mv`, `pb`, `roe`.
- `restrict_costs`: evaluate only a selected set of cost bps values.
- `set_holding_counts`: restrict holdings to selected counts.
- `set_signal_columns`: select from known columns.
- `prefer_cost_robust`: rank candidates with positive cost robustness rather than 0 bps-only best row.
- `reverse_signal`: falsification variant for selected signals.

## Tasks

### Task 1: Result analyzer

Create `src/factor_lab/harvest_result_analyzer.py` and `tests/test_harvest_result_analyzer.py`.

The analyzer reads a cycle result JSON and returns:

- `best_total_return`
- `best_sharpe`
- `best_max_drawdown`
- `best_cost_bps`
- `cost_sensitive`
- `window_concentration_risk`
- `drawdown_too_high`
- `sharpe_too_low`
- `promotion_ready`

### Task 2: Diagnostician

Create `src/factor_lab/harvest_diagnostician.py` and `tests/test_harvest_diagnostician.py`.

Map analyzer diagnostics to failure classes:

- `drawdown_too_high`
- `weak_risk_adjusted_return`
- `zero_cost_best_only`
- `window_concentration`
- `not_promotion_ready`

### Task 3: Self-correction planner

Create `src/factor_lab/harvest_self_correction_planner.py` and `tests/test_harvest_self_correction_planner.py`.

Generate executable plans. For example:

- drawdown too high → add `volatility_20 <= p60` filter.
- zero cost best only → restrict costs to `[30, 60]` and prefer cost robust.
- weak Sharpe → test alternative value-quality signals.
- window concentration → require all standard windows.

### Task 4: Fingerprint gate

Create `src/factor_lab/harvest_experiment_fingerprint.py` and tests.

Hash dataset path + signals + windows + holding counts + costs + filters + ranking rule + reverse flags. Block exact duplicates found in prior cycle artifacts.

### Task 5: Filtered backtest runner

Extend `small_institutional_backtest_matrix` or create `src/factor_lab/harvest_backtest_runner.py` so executable plans can filter the dataset before running real backtests.

### Task 6: Comparative evaluator

Create `src/factor_lab/harvest_comparative_evaluator.py` and tests.

Compare baseline and candidate result summaries. Emit:

- metric deltas
- improvements
- regressions
- decision: `continue_modified_route`, `pivot_or_stop`, `manual_review_for_promotion`, or `continue_same_mainline`

### Task 7: Evolution loop

Create `src/factor_lab/harvest_evolution_loop.py` and `scripts/run_harvest_evolution_loop.py`.

Each cycle must write:

- `result_analysis.json/.md`
- `diagnosis.json/.md`
- `correction_plan.json/.md`
- `experiment_fingerprint.json`
- `comparison.json/.md`
- updated `evidence_ledger.json/.md`
- updated `verdict.json/.md`
- updated `next_cycle_plan.json/.md`

### Task 8: Verification

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_result_analyzer.py \
  tests/test_harvest_diagnostician.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_experiment_fingerprint.py \
  tests/test_harvest_backtest_runner.py \
  tests/test_harvest_comparative_evaluator.py \
  tests/test_harvest_evolution_loop.py -q
```

Then run:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_evolution_loop.py --cycles 3 --allow-controlled-execution
```

Verify fingerprints differ and each plan is driven by previous metrics.

---

## Implementation Status — 2026-05-24 UTC

Status: **implemented and verification complete**.

Implemented self-correction components:

- Task 1 result analyzer: `src/factor_lab/harvest_result_analyzer.py`
- Task 2 diagnostician: `src/factor_lab/harvest_diagnostician.py`
- Task 3 self-correction planner: `src/factor_lab/harvest_self_correction_planner.py`
- Task 4 fingerprint gate: `src/factor_lab/harvest_experiment_fingerprint.py`
- Task 5 filtered backtest runner: `src/factor_lab/harvest_backtest_runner.py`
- Task 6 comparative evaluator: `src/factor_lab/harvest_comparative_evaluator.py`
- Task 7 evolution loop: `src/factor_lab/harvest_evolution_loop.py`, `scripts/run_harvest_evolution_loop.py`

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
  src/factor_lab/harvest_result_analyzer.py \
  src/factor_lab/harvest_diagnostician.py \
  src/factor_lab/harvest_self_correction_planner.py \
  src/factor_lab/harvest_experiment_fingerprint.py \
  src/factor_lab/harvest_backtest_runner.py \
  src/factor_lab/harvest_comparative_evaluator.py \
  src/factor_lab/harvest_evolution_loop.py \
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
  scripts/render_harvest_agent_timer.py \
  scripts/run_harvest_evolution_loop.py
```

Result: `compile_ok`.

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
  tests/test_harvest_agent_timer_render.py \
  tests/test_harvest_result_analyzer.py \
  tests/test_harvest_diagnostician.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_experiment_fingerprint.py \
  tests/test_harvest_backtest_runner.py \
  tests/test_harvest_comparative_evaluator.py \
  tests/test_harvest_evolution_loop.py -q
```

Result: `73 passed, 1 warning`. The warning is the existing pandas `FutureWarning` from `defensive_quality_experiments.py`.

Runtime smoke checks completed:

- `scripts/run_harvest_agent_once.py --dry-run` completed `cycle_0046` with `executed_count=0`, `started_systemd_daemon=false`, and `scheduled_timer_enabled=false`.
- `scripts/write_harvest_report.py` wrote the latest Harvest report for `cycle_0046`.
- `scripts/render_harvest_agent_timer.py --preview` rendered service/timer preview artifacts only under `artifacts/harvest_agent/timer_preview/`.
- `systemctl --user is-active factor-lab-harvest-agent.timer factor-lab-harvest-agent.service` returned `inactive` / `inactive`.
- `scripts/dry_run_controlled_restart.py` returned `would_run_count=0`.
- `scripts/audit_runtime_takeover.py` continued to recommend `['pause_broad_daemon', 'allow_controlled_only_daemon']`.

Evolution-loop acceptance evidence:

- A prior controlled 3-cycle run produced `cycle_0043`, `cycle_0044`, and `cycle_0045`.
- Fingerprints differed across all three cycles:
  - `cycle_0043`: `434fb19324deeaf464cddcfd`
  - `cycle_0044`: `60016627c0d683e1ebbe2870`
  - `cycle_0045`: `dca7204aae9b5b5eb30a1769`
- Plans changed based on prior diagnostics: baseline experiment → volatility/cost/turnover filters → adjusted volatility/holding/signal constraints.
- Best observed route improved from `industry_relative_book_yield` baseline to `industry_relative_earnings_yield`, but remained below promotion thresholds; verdict correctly stayed conservative (`continue_modified_route`, not promotion).

Safety state after implementation:

- No live trading path added.
- No broker/order path added.
- No broad daemon restoration added.
- No Harvest timer/service enabled.
- Controlled execution still requires explicit CLI approval.
- Harvest artifacts remain isolated under `artifacts/harvest_agent/`.
- Knowledge updates remain local under `knowledge/`.
