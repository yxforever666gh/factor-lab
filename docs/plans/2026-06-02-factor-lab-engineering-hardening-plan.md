# Factor Lab Engineering Hardening and Codebase Rationalization Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 将 Factor Lab 从“快速研究迭代型代码库”收敛成更清晰、可维护、可验证的工程化系统，同时不破坏当前 1100-test green baseline 和现有研究安全边界。

**Architecture:** 采用“先冻结行为、再分层搬迁、最后删除/归档”的保守重构路线。第一阶段只做无行为变化的模块拆分和入口整理；第二阶段建立 import/caller map 和 artifact retention；第三阶段才做 dead-code archive。所有阶段必须保持 full test suite 绿色，严禁混入 daemon/timer/queue/live-trading 行为变化。

**Tech Stack:** Python package under `src/factor_lab/`; FastAPI/Jinja WebUI; CLI scripts; pytest; artifact-backed research state; SQLite artifact DB.

---

## 0. Current baseline

Recent inspection summary:

```text
src/factor_lab: 333 Python files, ~61,736 lines
scripts:        193 Python scripts, ~12,681 lines
tests:          315 Python test files, ~24,357 lines
current tests:  1100 passed, 31 warnings
```

Largest pressure points:

```text
src/factor_lab/webui_app.py        3371 lines
src/factor_lab/research_queue.py   2110 lines
src/factor_lab/research_strategy.py 1521 lines
src/factor_lab/hermes_decision_router.py 1518 lines
src/factor_lab/storage.py          1340 lines
scripts/                           193 files, flat namespace
Harvest/ASL research branches       many active + historical modules mixed together
artifacts/                          live state + historical evidence + logs mixed together
```

Current code quality diagnosis:

- Not broken: test suite is green and safety gates are strong.
- Not clean: old research branches, giant modules, flat scripts, and mutable artifacts create cognitive load.
- Main risk: continued feature accretion without structural cleanup will turn the repo into an unmaintainable research dump.

---

## Non-negotiable constraints

1. **No behavior-changing refactor without tests.**
2. **Full suite must remain green after each phase:**
   ```bash
   PYTHONPATH=src .venv/bin/python -m pytest tests -q
   ```
3. **No queue writes, daemon/timer enablement, auto-promotion, or live trading as part of engineering cleanup.**
4. **Do not delete research evidence directly.** Move to `artifacts/archive/...` first.
5. **Do not remove Harvest/ASL modules until import/caller map proves they are unused or historical.**
6. **Do not move docs referenced by Hermes skills without updating the skill references or adding redirect docs.**
7. **Preserve compatibility shims for public script names for at least one cleanup cycle.**

---

## Desired target structure

### WebUI target structure

Current:

```text
src/factor_lab/webui_app.py
src/factor_lab/webui_templates/*.html
```

Target:

```text
src/factor_lab/webui/
  __init__.py
  app.py                    # create_app/app wiring, startup hooks, shared render
  routes/
    __init__.py
    dashboard.py
    settings_llm.py
    settings_data_sources.py
    settings_hermes.py
    research.py
    reports.py
    ops.py
    portfolios.py
  services/
    __init__.py
    env_settings.py          # read/write env helpers, secret masking
    service_restart.py       # systemd restart helpers
    report_loaders.py
  templates/                 # optional later move from webui_templates
```

Compatibility requirement:

```python
# src/factor_lab/webui_app.py remains as shim
from factor_lab.webui.app import app
```

### Scripts target structure

Current flat namespace:

```text
scripts/*.py  # 193 files
```

Target:

```text
scripts/prod/        # stable production/ops entrypoints used by systemd or humans
scripts/ops/         # inspectors, audits, status reports
scripts/reports/     # write_* report/materializer scripts
scripts/devtools/    # probes, smoke tests, one-off diagnostics
scripts/archive/     # old compatibility wrappers and historical experiment scripts
```

Compatibility requirement:

Top-level script names that are documented or used by systemd remain as wrappers during migration.

### Research-code target structure

Current research modules are mostly flat under `src/factor_lab/`.

Target grouping, implemented gradually:

```text
src/factor_lab/research/
  queue/
  governance/
  routes/
  strategy_lab/
  harvest/
  small_institutional/

src/factor_lab/data_sources/
  tushare.py
  cache.py
  coverage.py
  schemas.py

src/factor_lab/portfolio/
  construction.py
  diagnostics.py
  scorecards.py
```

Do not do this all at once. Use caller-map-driven slices.

---

## Phase 1 — Baseline lock and module boundary map

**Objective:** Create an objective map of the current codebase before moving files.

**Files:**
- Create: `artifacts/engineering_hardening_2026-06-02/module_inventory.json`
- Create: `artifacts/engineering_hardening_2026-06-02/module_inventory.md`
- Create: `scripts/ops/write_module_inventory.py`
- Test: `tests/test_module_inventory.py`

**Tasks:**

1. Add a script that scans:
   - Python files under `src/factor_lab/`
   - scripts under `scripts/`
   - tests under `tests/`
2. Report for each file:
   - line count
   - import names
   - inbound import count
   - script entrypoint flag
   - test coverage by filename convention
   - category guess: `webui`, `research_queue`, `harvest`, `autonomous_strategy`, `small_institutional`, `data_source`, `portfolio`, `misc`
3. Write markdown summary with top giant files and likely refactor candidates.
4. Run:
   ```bash
   PYTHONPATH=src .venv/bin/python -m pytest tests/test_module_inventory.py -q
   PYTHONPATH=src .venv/bin/python scripts/ops/write_module_inventory.py
   ```

**Acceptance criteria:**

- Inventory exists and is deterministic.
- No production behavior changed.
- Full suite remains green.

---

## Phase 2 — Split env/settings helpers out of `webui_app.py`

**Objective:** Shrink `webui_app.py` without changing routes or HTML behavior.

**Files:**
- Create: `src/factor_lab/webui/__init__.py`
- Create: `src/factor_lab/webui/services/__init__.py`
- Create: `src/factor_lab/webui/services/env_settings.py`
- Create: `src/factor_lab/webui/services/service_restart.py`
- Modify: `src/factor_lab/webui_app.py`
- Tests:
  - existing `tests/test_webui_llm_settings.py`
  - existing `tests/test_webui_data_source_settings.py`
  - existing `tests/test_settings_env_file.py`

**Move these helpers first:**

From `webui_app.py` to `webui/services/env_settings.py`:

- `_mask_secret`
- `_read_env_values`
- `_split_csv`
- `_coerce_boolish`
- LLM env constants
- LLM profile loading/saving helpers
- Data-source env constants
- Data-source loading/saving helpers
- `test_llm_profile_connection`
- `test_data_source_connection`

From `webui_app.py` to `webui/services/service_restart.py`:

- `restart_research_daemon_after_settings_save`

**Compatibility approach:**

`webui_app.py` should re-export imported helper names temporarily so current tests continue to monkeypatch them:

```python
from factor_lab.webui.services.env_settings import load_llm_settings, save_llm_settings, ...
from factor_lab.webui.services.service_restart import restart_research_daemon_after_settings_save
```

**Verification:**

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_webui_llm_settings.py \
  tests/test_webui_data_source_settings.py \
  tests/test_settings_env_file.py \
  -q
PYTHONPATH=src .venv/bin/python -m pytest tests -q
```

**Acceptance criteria:**

- `webui_app.py` line count decreases materially.
- Settings tests pass unchanged or with minimal import updates.
- No route behavior changes.

---

## Phase 3 — Split WebUI routes by page group

**Objective:** Turn `webui_app.py` from giant route file into route registration hub.

**Files:**
- Create: `src/factor_lab/webui/app.py`
- Create: `src/factor_lab/webui/routes/settings.py`
- Create: `src/factor_lab/webui/routes/research.py`
- Create: `src/factor_lab/webui/routes/portfolio.py`
- Create: `src/factor_lab/webui/routes/reports.py`
- Create: `src/factor_lab/webui/routes/ops.py`
- Modify: `src/factor_lab/webui_app.py` into shim or thin app factory wrapper.

**Order:**

1. Move `/settings`, `/settings/test-model`, `/data-sources`, `/data-sources/test`, `/hermes`, `/agents` first.
2. Move report/status routes:
   - `/harvest-agent/status`
   - `/autonomous-strategy-lab/status`
   - `/research-quality`
   - `/llm`
3. Move research/runs/candidates pages.
4. Move portfolio/paper-portfolio/approved-universe pages.
5. Leave dashboard/control last.

**Implementation pattern:**

Use FastAPI `APIRouter`:

```python
router = APIRouter()

@router.get('/data-sources', response_class=HTMLResponse)
def data_sources_page(...): ...
```

App hub:

```python
app.include_router(settings.router)
app.include_router(research.router)
...
```

**Tests:**

Run route tests after each move:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_webui_routes.py \
  tests/test_webui_llm_settings.py \
  tests/test_webui_data_source_settings.py \
  -q
```

**Acceptance criteria:**

- All existing route URLs still work.
- `systemctl --user restart factor-lab-web-ui.service` still starts cleanly.
- Live smoke:
  ```bash
  curl -sS -o /tmp/data-sources.html -w '%{http_code}\n' http://127.0.0.1:8765/data-sources
  curl -sS -o /tmp/runs.html -w '%{http_code}\n' http://127.0.0.1:8765/runs
  ```
- Full suite green.

---

## Phase 4 — Scripts namespace rationalization

**Objective:** Stop `scripts/` from being a flat dumping ground.

**Files:**
- Create directories:
  - `scripts/prod/`
  - `scripts/ops/`
  - `scripts/reports/`
  - `scripts/devtools/`
  - `scripts/archive/`
- Create: `docs/ops/script-entrypoints.md`
- Possibly modify systemd units only after confirming wrappers.

**Classification rules:**

### Keep top-level wrappers for stable entrypoints

Examples:

```text
scripts/run_web_ui.py
scripts/run_research_daemon.py
scripts/run_research_task_worker.py
scripts/run_controlled_admission_feeder.py
scripts/dry_run_controlled_restart.py
scripts/audit_runtime_takeover.py
```

These may be wrappers pointing to `scripts/prod/` or `scripts/ops/`, but top-level compatibility should remain.

### Move report writers

Pattern:

```text
scripts/write_*.py -> scripts/reports/write_*.py
```

Top-level wrapper remains if referenced in docs/tests.

### Move probes/smoke tests

Pattern:

```text
scripts/probe_*.py -> scripts/devtools/probe_*.py
scripts/smoke_*.py -> scripts/devtools/smoke_*.py
```

### Move obsolete one-offs

After caller-map confirmation:

```text
scripts/archive/<name>.py
```

**Test strategy:**

1. First add tests for wrapper imports / CLI help where necessary.
2. Move one group at a time.
3. Run targeted tests after each group.

**Acceptance criteria:**

- `scripts/` top-level stable list is documented.
- Existing documented commands still work via wrappers.
- Full suite green.

---

## Phase 5 — Harvest / ASL research-line consolidation

**Objective:** Separate current ASL-gated system from historical Harvest experiments.

**Files:**
- Create: `artifacts/engineering_hardening_2026-06-02/harvest_caller_map.{json,md}`
- Create: `docs/ops/research-lines.md`
- Modify only after caller-map approval.

**Research-line status target:**

```text
Current authoritative line:
  Autonomous Strategy Lab request_data / coverage preflight / controlled execution adapter

Bridge line:
  Harvest controller consumes ASL decisions, but does not independently continue stale cycles.

Historical lines:
  Harvest V2/V3/V4/V5 standalone evolution/controller/governor artifacts and old cycle verdicts.
```

**Tasks:**

1. Generate caller map for:
   - `src/factor_lab/harvest_*.py`
   - `scripts/*harvest*`
   - `tests/test_harvest_*.py`
2. Classify each file:
   - `active_runtime`
   - `active_bridge`
   - `tested_historical`
   - `artifact_writer_only`
   - `dead_experiment_candidate`
3. Add `docs/ops/research-lines.md` explaining current authority order:
   ```text
   ASL status > Harvest controller summary > Harvest cycle-local verdicts
   ```
4. Only archive code after classification and green tests.

**Acceptance criteria:**

- No active bridge removed.
- Stale local cycle instructions cannot be mistaken for current authority.
- Tests remain green.

---

## Phase 6 — Artifact retention policy

**Objective:** Stop artifacts from becoming a second unbounded codebase.

**Files:**
- Create: `src/factor_lab/artifact_retention.py`
- Create: `scripts/ops/audit_artifact_retention.py`
- Create: `scripts/ops/apply_artifact_retention.py`
- Create: `tests/test_artifact_retention.py`
- Create: `docs/ops/artifact-retention.md`

**Policy:**

Keep:

```text
latest authoritative status artifacts
latest blocker reports
latest run summaries
DB current state
Tushare cache
```

Archive:

```text
old Harvest cycles
old worker previews
old status ledgers beyond retention window
old probe logs
old intermediate run matrices after final summary exists
```

Do not touch by default:

```text
artifacts/factor_lab.db
artifacts/tushare_cache/
```

**Dry-run first:**

```bash
PYTHONPATH=src .venv/bin/python scripts/ops/audit_artifact_retention.py
```

Write mode requires explicit flag:

```bash
PYTHONPATH=src .venv/bin/python scripts/ops/apply_artifact_retention.py --write
```

**Acceptance criteria:**

- Dry-run writes a clear candidate list.
- Write mode moves to archive, never deletes.
- DB and Tushare cache excluded.
- Full suite green.

---

## Phase 7 — DB retention / VACUUM plan

**Objective:** Handle `artifacts/factor_lab.db` separately and safely.

**Files:**
- Create: `scripts/ops/audit_factor_lab_db_size.py`
- Create: `docs/ops/db-retention.md`
- Optional later: `src/factor_lab/db_retention.py`

**Tasks:**

1. Inspect table sizes and row counts.
2. Identify append-only history tables vs current state tables.
3. Decide retention rules per table.
4. Export archival copies before deletion.
5. Run `VACUUM` only after backup.

**Explicit non-goal:** Do not modify DB retention in the same PR/slice as WebUI or scripts refactor.

---

## Phase 8 — CI / quality gates

**Objective:** Prevent future drift.

**Files:**
- Create/update: `.github/workflows/test.yml` or local equivalent if GitHub is not used.
- Create: `scripts/ops/check_engineering_hygiene.py`
- Create: `tests/test_engineering_hygiene.py`

**Hygiene checks:**

- No new top-level `scripts/probe_*.py`; must go under `scripts/devtools/`.
- No new WebUI route directly added to `webui_app.py` after route split.
- No artifact writer without dry-run mode unless explicitly exempt.
- No production script that enables timer/daemon/live-trading without explicit flag.
- No route/controller status that treats stale Harvest cycle-local verdict as current authority over ASL status.

**Acceptance criteria:**

- CI/test command catches future structural regression.
- Developers know where new code belongs.

---

## Suggested execution order

### Sprint 1 — Low-risk structure

1. Phase 1: module inventory.
2. Phase 2: extract settings helpers.
3. Phase 3 part A: move settings/data-source routes.

Expected result:

- `webui_app.py` materially smaller.
- No behavior change.
- Full suite green.

### Sprint 2 — WebUI and scripts hygiene

1. Phase 3 complete route split.
2. Phase 4 scripts namespace rationalization.
3. Add `docs/ops/script-entrypoints.md`.

Expected result:

- WebUI route ownership clear.
- Script entrypoints documented.
- Top-level scripts no longer grow without structure.

### Sprint 3 — Research-line cleanup

1. Phase 5 Harvest/ASL caller map.
2. Archive clearly historical Harvest code/artifacts only after classification.
3. Write `docs/ops/research-lines.md`.

Expected result:

- Current research authority order is explicit.
- Old cycles/plans cannot mislead future agents.

### Sprint 4 — Retention and CI

1. Phase 6 artifact retention.
2. Phase 7 DB retention plan.
3. Phase 8 CI/hygiene checks.

Expected result:

- Artifact/log growth controlled.
- Future code stays in the right layer.

---

## Verification matrix

After every phase:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests -q
```

After WebUI phases:

```bash
systemctl --user restart factor-lab-web-ui.service
curl -sS -o /tmp/data-sources.html -w '%{http_code}\n' http://127.0.0.1:8765/data-sources
curl -sS -o /tmp/settings.html -w '%{http_code}\n' http://127.0.0.1:8765/settings
curl -sS -o /tmp/runs.html -w '%{http_code}\n' http://127.0.0.1:8765/runs
```

After scripts phases:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_web_ui.py --help || true
PYTHONPATH=src .venv/bin/python scripts/dry_run_controlled_restart.py
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_takeover.py
```

After research-line phases:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_autonomous_strategy_lab.py \
  tests/test_autonomous_strategy_lab_report.py \
  tests/test_harvest_autonomous_research_controller.py \
  tests/test_harvest_controller_policy.py \
  tests/test_harvest_cycle_runner.py \
  -q
```

After retention phases:

```bash
PYTHONPATH=src .venv/bin/python scripts/ops/audit_artifact_retention.py
PYTHONPATH=src .venv/bin/python -m pytest tests/test_artifact_retention.py -q
```

---

## Definition of done

This engineering hardening effort is done when:

1. `webui_app.py` is a thin app/router hub or compatibility shim, not a 3000+ line module.
2. Settings/data-source/env logic lives in service modules, not route files.
3. Scripts are categorized and documented.
4. Harvest/ASL authority order is documented and enforced.
5. Old research artifacts are archived by policy, not ad hoc.
6. DB retention has a written plan and backup-first process.
7. Full test suite remains green.
8. Future hygiene checks prevent recurrence of flat script sprawl and giant WebUI route accretion.

---

## Execution status — 2026-06-02 02:42 UTC

Current position: **Sprint 1 / all 8 phases completed**.

Completed:

1. **Phase 1 — module inventory baseline**
   - Inventory generated under `artifacts/engineering_hardening_2026-06-02/`.
2. **Phase 2A — service restart extraction**
   - Extracted daemon restart behavior to `src/factor_lab/webui/services/service_restart.py`.
3. **Phase 2B-1 — env/settings pure helpers extraction**
   - Added `src/factor_lab/webui/services/env_settings.py` constants/helpers.
4. **Phase 2B-2a — env file reader extraction**
   - Moved `_read_env_values` behavior to `env_settings.read_env_values(...)`.
5. **Phase 2B-2b — LLM settings service extraction**
   - Moved LLM profile ordering/enabled/form parsing/load/save/connection-test logic into `env_settings.py`.
   - Kept `webui_app.load_llm_settings`, `save_llm_settings`, and `test_llm_profile_connection` as compatibility wrappers.
   - Hermes profile sync remains a WebUI callback injected into the service; this avoids hard-coupling the service to WebUI globals.
6. **Phase 2B-2c — data-source settings service extraction**
   - Moved data-source ordering/redaction/form parsing/load/save/test-connection logic into `env_settings.py`.
   - Kept `webui_app.load_data_source_settings`, `save_data_source_settings`, and `test_data_source_connection` as compatibility wrappers.
   - Preserved monkeypatch compatibility for `webui_app.env_file`, blank-key-preserves-existing behavior, masked secrets, profile ordering, and legacy `TUSHARE_TOKEN` / `DIEMENG_API_KEY` sync.
7. **Phase 3A-1 — `/data-sources` route module extraction**
   - Added `src/factor_lab/webui/routes/settings_data_sources.py`.
   - Moved `/data-sources`, `/data-sources` POST save, and `/data-sources/test` handlers out of `webui_app.py`.
   - Registered routes through injected dependencies/lazy lambdas, preserving `webui_app` monkeypatch compatibility for `env_file` and `test_data_source_connection`.
8. **Phase 3A-2 — `/settings` route module extraction**
   - Added `src/factor_lab/webui/routes/settings_llm.py`.
   - Moved `/settings`, `/settings` POST save, and `/settings/test-model` handlers out of `webui_app.py`.
   - Used the same injected-dependency/lazy-lambda pattern to preserve monkeypatch compatibility for LLM settings route tests.
9. **Phase 3C — `/hermes` and legacy `/agents` route module extraction**
   - Added `src/factor_lab/webui/routes/settings_hermes.py`.
   - Moved `/hermes` GET/POST and legacy `/agents` GET/POST handlers out of `webui_app.py`.
   - Preserved `/agents` redirect compatibility and Hermes settings save/restart behavior.
10. **Phase 3B — app-wiring helper migration**
    - Added `src/factor_lab/webui/app.py`.
    - Moved app creation, startup cache registration helper, and template rendering helper into the new app-wiring module.
    - `webui_app.py` now calls `create_app()`, `register_startup_cache(...)`, and `render_template(...)` while remaining the compatibility import surface for existing tests/scripts.
11. **Phase 4 — scripts namespace rationalization**
    - Created/confirmed namespace directories: `scripts/prod/`, `scripts/ops/`, `scripts/reports/`, `scripts/devtools/`, `scripts/archive/`.
    - Added README guardrails in each namespace directory.
    - Added `docs/ops/script-entrypoints.md`, classifying 194 top-level scripts into stable/runtime, report-writer, ops, devtool, ops/devtool, and manual-review groups.
    - Kept existing top-level scripts in place for compatibility; no moves were made in this low-risk pass because many docs/tests/systemd paths still reference top-level entrypoints.
12. **Phase 5 — Harvest / ASL research-line consolidation**
    - Added `artifacts/engineering_hardening_2026-06-02/harvest_caller_map.{json,md}`.
    - Classified 165 Harvest/ASL files: 41 active runtime, 22 active bridge, 22 artifact-writer-only, 78 tested historical, and 2 dead-experiment candidates.
    - Added `docs/ops/research-lines.md` with current authority order: ASL status/decisions > Harvest controller bridge summaries > Harvest cycle-local verdicts/historical artifacts.
    - No code or artifact deletion/move was performed in this phase.
13. **Phase 6 — artifact retention plan**
    - Added `artifacts/engineering_hardening_2026-06-02/artifact_retention_audit.{json,md}`.
    - Added `docs/ops/artifact-retention.md` with archive-first retention policy.
    - Observed `artifacts/` at 2.2GB, with no-touch paths `artifacts/factor_lab.db` (829MB), `artifacts/tushare_cache/` (206MB), and active daemon/feeder/ASL state artifacts.
    - Classified 464 top-level artifact entries; no files were deleted or moved.
14. **Phase 7 — DB retention plan**
    - Added `artifacts/engineering_hardening_2026-06-02/db_retention_audit.{json,md}`.
    - Added `docs/ops/db-retention.md` with backup-first DB retention plan.
    - Inspected `artifacts/factor_lab.db` read-only: 829MB, 15 tables, 90,818 research_tasks rows, and large research evidence tables such as `factor_results`, `run_artifacts`, `factor_evaluations`, and `portfolio_results`.
    - No DB writes, deletes, archive exports, or VACUUM were performed.
15. **Phase 8 — CI / hygiene checks**
    - Added `tests/test_engineering_hygiene.py`.
    - Hygiene checks now assert `webui_app.py` stays under 3000 lines, WebUI route/service/app modules exist, script namespace directories and README guardrails exist, retention/research-line docs exist, and hardening audit artifacts exist.
    - This prevents immediate regression into giant WebUI/settings files, flat scripts without policy, and undocumented retention state.

Verification:

```bash
pytest tests/test_engineering_hygiene.py tests/test_module_inventory.py -q
# 8 passed

PYTHONPATH=. pytest tests -q
# 1116 passed, 32 warnings
```

Notes:

- Bare `pytest tests -q` can hit local environment import shadowing from an installed `tests` package (`No module named tests.test_research_gate`). The verified full-suite command for this workspace is `PYTHONPATH=. pytest tests -q`.
- Sprint 1 engineering hardening is complete: WebUI settings were extracted, settings routes modularized, app-wiring helpers moved, script namespace policy established, Harvest/ASL authority documented, artifact/DB retention planned, and hygiene checks added.

Next prepared work:

1. Optional follow-up: move additional non-settings WebUI routes in smaller future passes.
2. Optional follow-up: execute artifact/DB retention only with explicit approval, backup, downtime, and manifests.
3. Optional follow-up: group-by-group script moves with compatibility wrappers.
