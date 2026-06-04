# Factor Lab DB retention plan

Updated: 2026-06-02

## Scope

This plan covers `artifacts/factor_lab.db`. Phase 7 is plan/audit only: no rows are deleted, no VACUUM is run, and no write transaction is opened.

## Current audit

- DB size: **829MB**
- Table count: **15**
- manual_review: 5 tables / 109336 rows
- research_evidence_preserve_or_archive_by_age: 9 tables / 1893667 rows
- task_history_archive_candidate: 1 tables / 90818 rows

## Hard safety rules

1. Make an on-disk backup before any DB write, delete, archive, or VACUUM.
2. Stop or drain active daemon/worker processes before any retention write.
3. Never touch `pending` or `running` `research_tasks` rows.
4. Export archived rows to JSONL/SQLite sidecar before deleting from the live DB.
5. Run integrity checks before and after retention.
6. VACUUM only after backup, archive export, successful tests, and an explicit downtime window.

## Backup-first procedure

```bash
cd /home/admin/factor-lab
mkdir -p artifacts/db_backups
sqlite3 artifacts/factor_lab.db ".backup artifacts/db_backups/factor_lab_$(date -u +%Y%m%dT%H%M%SZ).db"
sqlite3 artifacts/factor_lab.db "PRAGMA integrity_check;"
```

## Candidate retention policy

- `research_tasks`: only finished/failed rows older than a chosen retention window are candidates; preserve pending/running.
- Research evidence tables: preserve by default; if archived, export to evidence sidecar first.
- State/config tables: preserve.
- Manual-review tables: inspect schema and callers before action.

## Phase 7 audit artifacts

- `artifacts/engineering_hardening_2026-06-02/db_retention_audit.json`
- `artifacts/engineering_hardening_2026-06-02/db_retention_audit.md`
