# Factor Lab artifact retention policy

Updated: 2026-06-02

## Goal

Control artifact growth without losing active runtime state, current authority evidence, Tushare cache, or database history.

## Hard no-touch paths

- `artifacts/factor_lab.db` — Phase 7 only, backup-first.
- `artifacts/tushare_cache/` — preserve to avoid repeated provider calls.
- Active daemon/feeder/ASL current state artifacts.
- `artifacts/engineering_hardening_2026-06-02/` while this hardening run is active.

## Retention classes

| Class | Examples | Action |
|---|---|---|
| live_state_preserve | heartbeat/current ASL/feeder/runtime state | Preserve |
| cache_preserve | `tushare_cache` | Preserve |
| db_no_touch | `factor_lab.db` | Phase 7 backup-first plan only |
| current_cleanup_evidence_preserve | hardening artifacts, cleanup manifests | Preserve while current |
| historical_candidate_archive_review | generated/recent/rolling candidate runs | Candidate for archive after manifest |
| diagnostic_or_probe_archive_review | mvp/probe/audit/debug/sample outputs | Candidate for archive after age/open-file check |
| evidence_preserve_or_review | research evidence not clearly stale | Review before archive |

## Archive-first procedure

1. Confirm active runtime state: systemd services, processes, pending/running DB tasks.
2. Check whether candidate paths are open with `lsof +D <path>` when available.
3. Write a manifest listing source path, size, classification, reason, timestamp, and target path.
4. Move to `artifacts/archive/<topic>_<date>/`, never direct-delete.
5. Run targeted tests and a smoke check after moving.
6. Only after a cooling-off period and explicit approval should archived material be deleted.

## Phase 6 audit artifacts

- `artifacts/engineering_hardening_2026-06-02/artifact_retention_audit.json`
- `artifacts/engineering_hardening_2026-06-02/artifact_retention_audit.md`
