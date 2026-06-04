# Harvest Agent v2 Research Enhancement Implementation Plan

> **For Hermes:** Use test-driven-development for each code change. Keep v2 Hermes-native, local-artifact driven, simulation/backtest-only, bounded, and auditable. Do not enable timers, daemons, live trading, broker/order paths, external data expansion, or automatic promotion.

**Goal:** Upgrade Harvest Agent from v1 exact-fingerprint self-correction into a safer v2 research loop with semantic duplicate detection, bounded mechanism route selection, stricter OOS/robustness validation, and richer inspectable reports.

**Architecture:** Add deterministic v2 modules around the existing v1 loop: semantic plan signature → mechanism route registry → OOS robustness validator → v2 plan enricher/report. v2 remains a controlled research enhancer, not a free-form factor generator.

**Tech Stack:** Python modules under `src/factor_lab/`, scripts under `scripts/`, configs under `configs/`, artifacts under `artifacts/harvest_agent/`, pytest tests under `tests/`.

---

## Safety boundaries

1. No live trading.
2. No broker/order path.
3. No automatic paper promotion.
4. No timer/service enablement.
5. No broad daemon restoration.
6. No external data/API expansion.
7. No arbitrary LLM-generated factor code in v2.
8. Controlled execution still requires explicit CLI flag.
9. Promotion remains manual-review only.

---

## V2 acceptance criteria

1. Plans with equivalent research semantics are detected as duplicates even if action order, labels, or minor quantile values differ.
2. Mechanism routes are selected from a bounded registry, not arbitrary generated names.
3. The self-correction planner can enrich plans with mechanism route metadata while preserving v1 action vocabulary.
4. OOS robustness validation classifies pass/fail/near-miss using split coverage, Sharpe, drawdown, cost robustness, and window consistency.
5. Evolution-loop artifacts include `semantic_signature.json`, `mechanism_route.json`, and `oos_validation.json`.
6. Dry-run smoke completes with `started_systemd_daemon=false` and `scheduled_timer_enabled=false`.
7. Targeted Harvest tests pass.

---

## Task 1: Semantic duplicate detector

**Objective:** Detect duplicate-equivalent research plans beyond exact fingerprint matching.

**Files:**
- Create: `src/factor_lab/harvest_semantic_duplicate.py`
- Test: `tests/test_harvest_semantic_duplicate.py`

**Behavior:**
- Normalize action order by action type.
- Normalize signal aliases/family semantics.
- Bucket quantile values to coarse bands.
- Ignore labels and transient cycle fields.
- Return a stable semantic signature and hash.
- Scan prior `semantic_signature.json` artifacts for duplicates.

---

## Task 2: Bounded mechanism route registry

**Objective:** Make v2 route changes mechanism-driven but bounded.

**Files:**
- Create: `configs/harvest_mechanism_routes.json`
- Create: `src/factor_lab/harvest_mechanism_routes.py`
- Test: `tests/test_harvest_mechanism_routes.py`

**Routes:**
- `industry_relative_value`
- `value_quality_no_distress`
- `value_momentum_confirmation`
- `cost_robust_value_quality`
- `low_volatility_value_quality`

**Behavior:**
- Load registry from config.
- Select a route based on failure classes.
- Return required fields, allowed signals, filters, and rationale.
- Block unsupported/missing routes.

---

## Task 3: OOS robustness validator

**Objective:** Classify candidate evidence under stricter validation than best-row metrics.

**Files:**
- Create: `src/factor_lab/harvest_oos_validator.py`
- Test: `tests/test_harvest_oos_validator.py`

**Behavior:**
- Read a result payload from `build_small_institutional_backtest_matrix`.
- Count total/ok windows and cost-positive windows.
- Extract best Sharpe, max drawdown, total return, and cost robustness.
- Classify as `pass`, `near_miss`, `fail`, or `insufficient_data`.
- Require conservative manual-review output for promotion-like cases.

---

## Task 4: Enrich self-correction plans with v2 mechanism metadata

**Objective:** Keep v1 actions executable while adding v2 route context.

**Files:**
- Modify: `src/factor_lab/harvest_self_correction_planner.py`
- Modify: `src/factor_lab/harvest_evolution_loop.py`
- Test: `tests/test_harvest_self_correction_planner.py`
- Test: `tests/test_harvest_evolution_loop.py`

**Behavior:**
- Add optional `mechanism_route` metadata to correction plans.
- Restrict selected signal columns to allowed route signals where appropriate.
- Write `mechanism_route.json/.md` per cycle.
- Do not expand action vocabulary beyond v1 without tests.

---

## Task 5: Integrate semantic duplicate and OOS validation artifacts

**Objective:** Make v2 evidence inspectable and anti-repeat stronger.

**Files:**
- Modify: `src/factor_lab/harvest_evolution_loop.py`
- Modify: `src/factor_lab/harvest_experiment_fingerprint.py` if needed
- Test: `tests/test_harvest_evolution_loop.py`

**Behavior:**
- Write `semantic_signature.json` before execution.
- If exact duplicate exists, vary plan as v1 already does.
- If semantic duplicate exists, vary route/attempt before execution.
- Write `oos_validation.json/.md` after execution.
- Verdict reasoning includes OOS class and semantic-duplicate status.

---

## Verification commands

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_semantic_duplicate.py \
  src/factor_lab/harvest_mechanism_routes.py \
  src/factor_lab/harvest_oos_validator.py \
  src/factor_lab/harvest_self_correction_planner.py \
  src/factor_lab/harvest_evolution_loop.py \
  scripts/run_harvest_evolution_loop.py

PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_semantic_duplicate.py \
  tests/test_harvest_mechanism_routes.py \
  tests/test_harvest_oos_validator.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_experiment_fingerprint.py \
  tests/test_harvest_evolution_loop.py -q

PYTHONPATH=src .venv/bin/python scripts/run_harvest_evolution_loop.py --cycles 1
systemctl --user is-active factor-lab-harvest-agent.timer factor-lab-harvest-agent.service || true
```

Expected safety state:

- Dry-run cycle completes.
- No controlled execution unless explicitly flagged.
- No timer/service enabled.
- No live/broker path added.
- V2 artifacts are written under `artifacts/harvest_agent/<cycle_id>/`.

---

## Implementation Status — 2026-05-24 UTC

Status: **implemented and verification complete**.

Implemented v2 components:

- Semantic duplicate detector: `src/factor_lab/harvest_semantic_duplicate.py`
- Bounded mechanism route registry: `configs/harvest_mechanism_routes.json`, `src/factor_lab/harvest_mechanism_routes.py`
- OOS robustness validator: `src/factor_lab/harvest_oos_validator.py`
- Planner enrichment: `src/factor_lab/harvest_self_correction_planner.py` now attaches `mechanism_route` metadata and restricts selected signals to route-allowed signals when applicable.
- Evolution-loop integration: `src/factor_lab/harvest_evolution_loop.py` now writes `semantic_signature.json`, `mechanism_route.json/.md`, and `oos_validation.json/.md` per cycle.

Verification completed:

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/harvest_semantic_duplicate.py \
  src/factor_lab/harvest_mechanism_routes.py \
  src/factor_lab/harvest_oos_validator.py \
  src/factor_lab/harvest_experiment_fingerprint.py \
  src/factor_lab/harvest_self_correction_planner.py \
  src/factor_lab/harvest_evolution_loop.py \
  scripts/run_harvest_evolution_loop.py

PYTHONPATH=src .venv/bin/python -m pytest [Harvest targeted suite] -q
```

Result: `82 passed, 1 warning`. The warning is the pre-existing pandas `FutureWarning` from `defensive_quality_experiments.py`.

Additional focused v2 regression slice after final semantic rounding cleanup:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_harvest_semantic_duplicate.py \
  tests/test_harvest_mechanism_routes.py \
  tests/test_harvest_oos_validator.py \
  tests/test_harvest_self_correction_planner.py \
  tests/test_harvest_evolution_loop.py -q
```

Result: `12 passed`.

Smoke evidence:

- Dry-run v2 evolution completed `cycle_0047` with `executed_backtest_count=0`, `started_systemd_daemon=false`, and `scheduled_timer_enabled=false`.
- Controlled one-cycle v2 evolution completed `cycle_0048` with `executed_backtest_count=81`, `started_systemd_daemon=false`, and `scheduled_timer_enabled=false`.
- `cycle_0048` wrote:
  - `semantic_signature.json`
  - `mechanism_route.json`
  - `oos_validation.json`
  - existing v1 artifacts including `correction_plan.json`, `result_analysis.json`, `diagnosis.json`, `comparison.json`, `evidence_ledger.json`, `verdict.json`, and `next_cycle_plan.json`.

Latest controlled v2 research result (`cycle_0048`):

- `mechanism_id`: `industry_relative_value`
- `semantic_hash`: `fcd4f1cd72dc25c4ac96ff1e`
- `oos_class`: `fail`
- `executed_backtest_count`: `81`
- Best row:
  - signal: `industry_relative_book_yield`
  - window: `2021-2022`
  - holding_count: `50`
  - cost_bps: `0`
  - total_return: `1.717723`
  - sharpe: `0.668238`
  - max_drawdown: `-0.406628`
- OOS validation reasons:
  - `sharpe_below_threshold`
  - `drawdown_below_threshold`
- Promotion/manual-review flag: `false`

Safety state after v2:

- `factor-lab-harvest-agent.timer`: `inactive`
- `factor-lab-harvest-agent.service`: `inactive`
- No live trading path added.
- No broker/order path added.
- No broad daemon restoration added.
- No timer/service enabled.
- Controlled execution still requires explicit CLI approval.
