# Recent Research Cleanup Inventory and Plan

> **Scope:** inspection only. No research code/artifact deletion or quarantine has been performed.

**Goal:** identify recent Factor Lab research outputs, code, artifacts, and docs that are obsolete, superseded, broken, or safe to archive; then define a conservative cleanup plan with verification gates.

**Inspection timestamp:** 2026-06-02

---

## 1. Inspection commands run

```bash
cd /home/admin/factor-lab

git status --porcelain=v1

# Categorize modified/untracked files by research line, mtime, size.
python3 - <<'PY'
# summarized git status by category and recent mtime
PY

# Inspect recent artifacts/knowledge/docs/configs by mtime and size.
python3 - <<'PY'
# listed recent top-level artifacts, knowledge, docs/plans, configs
PY

# Read current status artifacts:
# - artifacts/autonomous_strategy_lab/status_report.md
# - artifacts/autonomous_strategy_lab/cache_extension_plan.md
# - artifacts/autonomous_strategy_lab/controlled_execution_decision.md
# - artifacts/harvest_agent/controller_runs/controller_20260601T221417Z/controller_summary.md
# - artifacts/harvest_agent/v3_status.md
# - artifacts/harvest_agent/cycle_0060/verdict.md
# - artifacts/small_institutional_simulation/risk_reduction_repair.md

PYTHONPATH=src .venv/bin/python -m pytest tests -q
```

---

## 2. Current working tree inventory

`git status --porcelain=v1` currently reports:

```text
206 changed/untracked paths
12 modified tracked files
194 untracked files
```

Categorized count:

| Category | Count | Approx role |
|---|---:|---|
| `harvest` | 126 | Harvest Agent V2/V3/V4/V5/controller/evolution loop modules, scripts, tests, plans, knowledge |
| `autonomous_strategy` | 44 | Autonomous Strategy Lab modules, worker contracts, cache extension, coverage/execution adapters |
| `small_institutional/correction` | 22 | Hermes correction loop, risk-reduction executor/plans, simulated portfolio diagnostics |
| `webui` | 5 | Newly added data-source WebUI settings work |
| `knowledge` | 3 | Recent knowledge summaries/data blockers/research waste updates |
| `plans` | 2 | Recent ASL + data-source plan docs |
| `other` | 4 | Misc docs/status |

Untracked code/doc line-count summary:

```text
autonomous_strategy: 44 files, ~4017 lines
harvest:             126 files, ~11948 lines
correction/small:    17 code files, ~2287 lines
webui_data_sources:   3 files, ~678 lines
other:                4 docs, ~1806 lines
```

---

## 3. Current research-state conclusions

### 3.1 Autonomous Strategy Lab is the current active research direction

Current status artifact:

`artifacts/autonomous_strategy_lab/status_report.md`

Key state:

```text
status: blocked
decision: request_data
coverage_overall_status: blocked
execution_status: blocked
cache_extension_status: dry_run_plan_written
cache_extension_action: fetch_required
controlled_execution_started: False
controlled_execution_allowed: False
queue_write_allowed: False
timer_enable_allowed: False
```

Blocking fields:

```text
pb_history_756d: insufficient_history (50/97, ratio=0.515464)
pe_ttm_history_756d: insufficient_history (50/97, ratio=0.515464)
```

Interpretation:

- ASL is not stale; it is the newest decision layer.
- It correctly stops at `request_data` rather than forcing same-route backtests.
- Its code should be retained, but old preview/test-only worker artifacts can be archived after preserving latest summary artifacts.

### 3.2 Harvest Controller is now gated by ASL and did not run new cycles

Current controller summary:

`artifacts/harvest_agent/controller_runs/controller_20260601T221417Z/controller_summary.md`

```text
controller_status: complete
cycles_run: 0
executed_backtest_count: 0
stop_reason: autonomous_strategy_lab_request_data
started_systemd_daemon: False
scheduled_timer_enabled: False
```

Interpretation:

- The Harvest controller is now downstream/consumer of ASL decisions.
- Recent Harvest V3/V4/V5 modules may be partly superseded by ASL, but not all are safe to remove because tests/scripts/docs still reference them.
- Cleanup should first distinguish:
  - active bridge code used by ASL/Harvest controller;
  - old standalone Harvest evolution/controller code no longer invoked;
  - docs/plans that are historical but still referenced by skills/memory.

### 3.3 Harvest cycle artifacts show repeated insufficient-data / same-route repair

`artifacts/harvest_agent/v3_status.md`:

```text
cycle_0056: route=industry_relative_value oos=fail decision=cost_robustness_branch
cycle_0057: route=industry_relative_value oos=fail decision=portfolio_construction_branch
cycle_0058: route=industry_relative_value oos=insufficient_data decision=repair_same_route
cycle_0059: route=industry_relative_value oos=insufficient_data decision=repair_same_route
cycle_0060: route=industry_relative_value oos=insufficient_data decision=repair_same_route
```

`cycle_0060/verdict.md` still says:

```text
decision: continue_modified_route
next_action: v3:repair_same_route
```

But the newer ASL/controller status says stop with `autonomous_strategy_lab_request_data`.

Interpretation:

- Cycle-local `continue_modified_route` is stale relative to global ASL state.
- Latest authoritative state should be ASL status + Harvest controller stop reason.
- Old cycle-local action artifacts should be archived or explicitly marked historical to avoid future agents reading them as current instructions.

### 3.4 Small-institutional / correction line remains blocked, not promotable

`artifacts/small_institutional_simulation/risk_reduction_repair.md`:

```text
repair_status: blocked_no_drawdown_safe_candidate
drawdown_limit: -0.35
best_available_max_drawdown: -0.475431
automation_allowed: False
candidate_count: 0
```

Interpretation:

- Risk-reduction experiment produced a valid blocker, not a usable strategy.
- Keep latest blocker summary and policy logic if still used.
- Archive detailed intermediate simulation/diagnostic artifacts after preserving the final blocker report.

---

## 4. Test-suite health and stale-code signal

Full test run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests -q
```

Result:

```text
1094 passed, 6 failed
```

Failures:

1. `tests/test_value_route_scorecard.py::test_scorecard_roles_and_tail_degradation`
   - `tail_degradation_ratio` is `None`; test expects numeric `0.504`.
   - Likely stale assumption or missing fixture/data calculation path.

2. Five failures in `tests/test_verify_de_openclaw_runtime.py`
   - Test attempts to load:
     `scripts/verify_de_hermes_native_runtime.py`
   - Current existing file is:
     `scripts/verify_de_openclaw_runtime.py`
   - This is a clear stale rename/migration mismatch.

Important stale-code finding:

`tests/test_verify_de_openclaw_runtime.py` has a stale name but tests the newer file name `verify_de_hermes_native_runtime.py`; meanwhile `scripts/verify_de_openclaw_runtime.py` docstring already says “de-HermesNative”. This should be cleaned in the next pass by either:

- renaming `scripts/verify_de_openclaw_runtime.py` → `scripts/verify_de_hermes_native_runtime.py`, and leaving a compatibility shim if scripts still call the old name; or
- updating tests and allowed reference lists consistently if the old name is intentionally retained.

Recommendation: rename to `verify_de_hermes_native_runtime.py` and add a tiny compatibility wrapper at the old path for one release cycle.

---

## 5. Artifact inventory and cleanup candidates

Recent top-level artifact sizes of interest:

| Path | Size / note | Cleanup status |
|---|---:|---|
| `artifacts/harvest_agent/` | ~184M | Archive old cycle runs after preserving latest summaries/status |
| `artifacts/research_daemon_status_history.jsonl` | ~171M | Rotate/compress; not research evidence needing live size |
| `artifacts/factor_lab.db` | ~869M | Do not delete; DB retention/VACUUM needs separate plan |
| `artifacts/llm_usage_ledger.jsonl` | ~11M | Keep; useful cost ledger |
| `artifacts/runtime_observations.jsonl` | ~6.5M | Can rotate/compress if old |
| `artifacts/autonomous_strategy_lab/` | ~420K | Keep current; archive worker previews older than latest verdict |
| `artifacts/small_institutional_simulation/` | ~276K | Keep latest blocker summary, archive detail runs |
| `artifacts/hermes_correction/` | ~200K | Keep current correction state |

Cleanup interpretation:

- High-value cleanup is **not code deletion first**; it is reducing stale authority and artifact noise.
- The largest easy artifact cleanup is rotating/compressing `research_daemon_status_history.jsonl` and old Harvest cycles, but DB cleanup must be separate and cautious.

---

## 6. Proposed cleanup categories

### A. Keep / current active line

Retain and stabilize:

- `src/factor_lab/autonomous_strategy_lab*.py`
- `src/factor_lab/autonomous_strategy_*coverage_preflight.py`
- `src/factor_lab/autonomous_strategy_execution_adapter.py`
- `src/factor_lab/autonomous_strategy_cache_extension.py`
- ASL routes/workers/configs that produce current `request_data` decision
- `/data-sources` WebUI files from the latest task
- Latest ASL artifacts:
  - `status_report.{md,json}`
  - `cache_extension_plan.{md,json}`
  - `controlled_execution_decision.{md,json}`
  - `historical_valuation_coverage_preflight.{md,json}`

Reason: these represent the newest active architecture and current blocker.

### B. Mark historical / archive after summary extraction

Candidates:

- Older Harvest V2/V3/V4/V5 plan docs:
  - `docs/plans/2026-05-22-harvest-agent-autonomous-research-loop-plan.md`
  - `docs/plans/2026-05-24-harvest-agent-v2-research-enhancement-plan.md`
  - `docs/plans/2026-05-25-harvest-agent-self-correction-loop-plan.md`
  - `docs/plans/2026-05-25-harvest-agent-v3-research-intelligence-plan.md`
  - `docs/plans/2026-05-25-harvest-agent-v4-autonomous-research-controller-plan.md`
  - `docs/plans/2026-05-25-harvest-agent-v5-research-strategy-governor-plan.md`

Caution: the Factor Lab skill currently references some Harvest docs as procedural memory. Do **not** move/delete these until the skill references are updated or a consolidated `docs/archive/harvest/README.md` redirect is created.

### C. Stale / broken naming cleanup

High-confidence cleanup:

- `scripts/verify_de_openclaw_runtime.py` vs `tests/test_verify_de_openclaw_runtime.py` expecting `verify_de_hermes_native_runtime.py`.

Plan:

1. Create `scripts/verify_de_hermes_native_runtime.py` with current verifier content.
2. Replace `scripts/verify_de_openclaw_runtime.py` with a compatibility shim that imports/calls the new script.
3. Update `ALLOWED_BLOCKED_REFERENCE_FILES` to include both paths temporarily.
4. Run `tests/test_verify_de_openclaw_runtime.py`.
5. Later rename test file to `test_verify_de_hermes_native_runtime.py` after confirming all callers are updated.

### D. Stale current-instruction artifacts

Candidates to archive or mark historical:

- Harvest cycle artifacts where local verdict says continue/repair but global ASL status says request_data/blocked.
- Specifically cycle artifacts under:
  - `artifacts/harvest_agent/cycle_0056/`
  - `artifacts/harvest_agent/cycle_0057/`
  - `artifacts/harvest_agent/cycle_0058/`
  - `artifacts/harvest_agent/cycle_0059/`
  - `artifacts/harvest_agent/cycle_0060/`

Plan:

- Keep `v3_status.md`, latest controller summary, latest ASL status.
- Move cycle directories into `artifacts/archive/harvest_agent_cycles_YYYYMMDD/` or compress to `artifacts/archive/harvest_agent_cycles_0056_0060.tar.zst`.
- Add a top-level `artifacts/harvest_agent/README.md` saying the authoritative current state is ASL `request_data` and controller stopped with `autonomous_strategy_lab_request_data`.

### E. Blocked small-institutional outputs

Candidates:

- detailed risk-reduction matrix outputs under `artifacts/small_institutional_simulation/`
- intermediate correction artifacts older than current `blocked_no_drawdown_safe_candidate`

Plan:

- Preserve final blocker summaries:
  - `risk_reduction_repair.{md,json}`
  - current correction state in `artifacts/hermes_correction/current_state.*`, if present
- Archive raw/intermediate runs.
- Keep code only if referenced by tests/current correction state. Otherwise mark modules as historical or move under an archive namespace in a later pass.

### F. Runtime logs / status ledgers

Candidates:

- `artifacts/research_daemon_status_history.jsonl` (~171M)
- older portions of `artifacts/runtime_observations.jsonl`
- repeated old status `.log` files from May 31

Plan:

- Rotate/compress logs by date.
- Keep last 7 days uncompressed.
- Move older logs to `artifacts/archive/logs/` as `.zst` or `.gz`.
- Do not touch `factor_lab.db` in this cleanup pass.

---

## 7. Proposed cleanup execution plan

### Phase 0 — Safety snapshot

**Objective:** capture current state before moving anything.

Commands:

```bash
cd /home/admin/factor-lab
mkdir -p artifacts/cleanup_inventory_2026-06-02

git status --porcelain=v1 > artifacts/cleanup_inventory_2026-06-02/git_status_before.txt
find artifacts -maxdepth 2 -type f -printf '%s\t%TY-%Tm-%Td %TH:%TM\t%p\n' \
  | sort -nr | head -200 > artifacts/cleanup_inventory_2026-06-02/largest_recent_artifacts.txt

PYTHONPATH=src .venv/bin/python -m pytest tests/test_webui_data_source_settings.py tests/test_webui_llm_settings.py tests/test_settings_env_file.py -q \
  | tee artifacts/cleanup_inventory_2026-06-02/webui_settings_tests_before.txt
```

### Phase 1 — Fix the obvious stale verifier naming

**Objective:** remove one clear full-suite failure caused by stale filename mismatch.

Steps:

1. Copy/rename `scripts/verify_de_openclaw_runtime.py` to `scripts/verify_de_hermes_native_runtime.py`.
2. Replace old file with compatibility wrapper:

```python
#!/usr/bin/env python3
from verify_de_hermes_native_runtime import main

if __name__ == "__main__":
    raise SystemExit(main())
```

3. Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_verify_de_openclaw_runtime.py -q
```

Expected: 5 verifier tests pass.

### Phase 2 — Resolve or classify value-route scorecard failure

**Objective:** determine whether `tail_degradation_ratio=None` is a stale expected value, missing fixture, or real regression.

Steps:

1. Inspect `src/factor_lab/value_route_scorecard.py` and input artifacts.
2. Decide:
   - if missing data is expected after ASL/request-data shift, update test to assert explicit missing-data semantics; or
   - if value should be computed, fix the calculation/fixture.
3. Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_value_route_scorecard.py -q
```

### Phase 3 — Archive stale Harvest cycle artifacts, not code yet

**Objective:** remove stale instruction noise while preserving evidence.

Dry-run plan:

```bash
mkdir -p artifacts/archive/harvest_agent_cycles_2026-06-02
for d in artifacts/harvest_agent/cycle_0056 artifacts/harvest_agent/cycle_0057 artifacts/harvest_agent/cycle_0058 artifacts/harvest_agent/cycle_0059 artifacts/harvest_agent/cycle_0060; do
  [ -d "$d" ] && echo "would archive $d"
done
```

Write mode only after review:

```bash
mv artifacts/harvest_agent/cycle_0056 artifacts/archive/harvest_agent_cycles_2026-06-02/
mv artifacts/harvest_agent/cycle_0057 artifacts/archive/harvest_agent_cycles_2026-06-02/
mv artifacts/harvest_agent/cycle_0058 artifacts/archive/harvest_agent_cycles_2026-06-02/
mv artifacts/harvest_agent/cycle_0059 artifacts/archive/harvest_agent_cycles_2026-06-02/
mv artifacts/harvest_agent/cycle_0060 artifacts/archive/harvest_agent_cycles_2026-06-02/
```

Then write `artifacts/harvest_agent/README.md` stating current authoritative state.

### Phase 4 — Archive preview/obsolete ASL worker artifacts

**Objective:** keep latest ASL status but archive preview-only worker attempts.

Candidates:

- `artifacts/autonomous_strategy_lab/workers/worker_preview_test/`
- `artifacts/autonomous_strategy_lab/workers/worker_preview_test2/`
- keep or summarize `worker_preview_final_check/` because latest status references it.

Dry-run first.

### Phase 5 — Rotate runtime logs

**Objective:** reduce artifact noise/size without deleting research evidence.

Candidates:

- `artifacts/research_daemon_status_history.jsonl` (~171M)
- `artifacts/runtime_observations.jsonl` (~6.5M)

Plan:

```bash
mkdir -p artifacts/archive/logs_2026-06-02
cp artifacts/research_daemon_status_history.jsonl artifacts/archive/logs_2026-06-02/research_daemon_status_history.before_rotation.jsonl
python3 - <<'PY'
# keep recent tail / rotate old lines after inspecting timestamp format
PY
```

Do not truncate blindly until timestamp parsing is verified.

### Phase 6 — Code cleanup audit for Harvest modules

**Objective:** only after tests are green and stale artifacts are archived, decide whether Harvest code should remain, be consolidated, or move to archive.

Steps:

1. Build import/caller map for `src/factor_lab/harvest_*.py` and `scripts/*harvest*`.
2. Categorize each file:
   - active bridge used by ASL/controller/WebUI;
   - historical but tested;
   - unused experimental code;
   - docs-only reference.
3. Remove/relocate only files with zero runtime references and obsolete tests.
4. Run targeted Harvest/ASL tests after each batch.

Recommended command for the audit:

```bash
python3 - <<'PY'
# AST/import graph for harvest/autonomous_strategy modules
PY
```

---

## 8. Cleanup priority list

### Highest confidence / do first

1. Fix stale verifier file name mismatch.
2. Resolve/classify `value_route_scorecard` test failure.
3. Add top-level state note to Harvest artifacts marking ASL as authoritative.
4. Archive old Harvest cycle directories after preserving summaries.
5. Archive ASL worker preview attempts except latest final-check evidence.

### Medium confidence / do after tests are green

1. Rotate large daemon status history logs.
2. Archive small-institutional detailed intermediate outputs while preserving final blocker.
3. Consolidate old Harvest plan docs only after updating skill references.

### Do not do yet

1. Do not delete `factor_lab.db`.
2. Do not delete Tushare cache.
3. Do not remove Harvest code modules until caller/import map is built and tests are green.
4. Do not move docs referenced by `~/.hermes/skills/factor-lab/SKILL.md` without patching the skill references.
5. Do not enable daemon/timer/queue writes as part of cleanup.

---

## 9. Expected verification after cleanup

Minimum targeted verification:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_verify_de_openclaw_runtime.py \
  tests/test_value_route_scorecard.py \
  tests/test_autonomous_strategy_lab.py \
  tests/test_autonomous_strategy_lab_report.py \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_webui_data_source_settings.py \
  -q
```

Then:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests -q
```

Target:

- No failures introduced by cleanup.
- Existing 6 failures should be reduced to 0 if Phases 1–2 are completed.
- Current ASL authoritative status remains `request_data` / no queue write / no controlled execution.

---

## 10. Recommended next action

Proceed with **Phase 1 + Phase 2 only first**:

1. Fix stale verifier filename mismatch.
2. Resolve or reclassify the value-route scorecard failing test.
3. Re-run full suite.

Only after the suite is green should artifact/archive cleanup begin. This avoids mixing “cleanup” with unresolved test breakage and prevents deleting evidence needed to diagnose the failures.
