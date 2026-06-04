# Recent Research Cleanup Execution Report

**Date:** 2026-06-02

## Completed

1. Safety snapshot created:
   - `artifacts/cleanup_inventory_2026-06-02/git_status_before.txt`
   - `artifacts/cleanup_inventory_2026-06-02/largest_recent_artifacts.txt`
   - `artifacts/cleanup_inventory_2026-06-02/webui_settings_tests_before.txt`

2. Fixed stale verifier naming mismatch:
   - Added canonical script: `scripts/verify_de_hermes_native_runtime.py`
   - Converted old script to compatibility shim: `scripts/verify_de_openclaw_runtime.py`
   - Updated verifier concept-reference scan to recognize both `hermes_native` and `HermesNative`/`hermesnative` spelling.
   - Verification: `tests/test_verify_de_openclaw_runtime.py` -> 5 passed.

3. Fixed brittle value-route scorecard test:
   - `tests/test_value_route_scorecard.py` now builds deterministic temp artifacts instead of relying on mutable live artifacts.
   - Root cause: live `artifacts/value_route_followups/` was absent, so `tail_degradation_ratio` was correctly `None`; the test was stale because it depended on historical artifacts.
   - Verification: `tests/test_value_route_scorecard.py` -> 1 passed.

4. Full suite restored green before artifact archiving:
   - `PYTHONPATH=src .venv/bin/python -m pytest tests -q`
   - Result: 1100 passed, 31 warnings.

5. Safe artifact archival performed, using move-to-archive rather than deletion:
   - Archived Harvest cycles:
     - `artifacts/harvest_agent/cycle_0056`
     - `artifacts/harvest_agent/cycle_0057`
     - `artifacts/harvest_agent/cycle_0058`
     - `artifacts/harvest_agent/cycle_0059`
     - `artifacts/harvest_agent/cycle_0060`
   - Destination: `artifacts/archive/harvest_agent_cycles_2026-06-02/` (~42M)
   - Archived old ASL worker previews:
     - `artifacts/autonomous_strategy_lab/workers/worker_preview_test`
     - `artifacts/autonomous_strategy_lab/workers/worker_preview_test2`
   - Destination: `artifacts/archive/autonomous_strategy_worker_previews_2026-06-02/` (~100K)
   - Added authority note: `artifacts/harvest_agent/README.md`
   - Dry-run/execution log: `artifacts/cleanup_inventory_2026-06-02/archive_dry_run.txt`
   - Size report: `artifacts/cleanup_inventory_2026-06-02/archive_sizes_after.txt`

6. Post-archive verification:
   - Targeted cleanup tests: 30 passed.
   - Full suite after archive: 1100 passed, 31 warnings.

## Not completed / intentionally deferred

1. Runtime log rotation deferred:
   - `artifacts/research_daemon_status_history.jsonl` is large (~171M) but may be actively written by the daemon.
   - Need timestamp-aware rotation and service/file-handle check before truncation.

2. `factor_lab.db` cleanup deferred:
   - Large (~869M), but it is the research database and must not be deleted.
   - Needs separate DB retention/VACUUM plan.

3. Harvest code-module consolidation deferred:
   - Harvest source/tests remain extensive and referenced by green tests.
   - Need import/caller map before removing or moving any code.

4. Old Harvest plan doc archival deferred:
   - Some docs are referenced by the Factor Lab skill/procedural memory.
   - Move only after skill references are patched or redirect docs are created.

## Current verification baseline

```text
PYTHONPATH=src .venv/bin/python -m pytest tests -q
1100 passed, 31 warnings
```

## Recommended next step

Perform a dedicated **runtime log + DB retention plan**:

1. Check active daemon/service and open file handles.
2. Parse timestamps in `research_daemon_status_history.jsonl`.
3. Keep recent tail uncompressed, archive older rows as compressed logs.
4. Inspect DB tables and decide retention/VACUUM strategy separately.
