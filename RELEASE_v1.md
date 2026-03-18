# Factor Lab v1.0

## What v1.0 means

Factor Lab v1.0 is the first stable, end-to-end version of the research platform.

It can:
- run real-data factor workflows on Tushare data
- evaluate raw and neutralized factor efficacy
- track time-split robustness
- score factors and pick cluster representatives
- maintain a candidate pool and graveyard
- backtest multiple simple long/short portfolio variants
- store workflow runs in SQLite
- generate markdown and HTML reports
- build an index page and latest-summary artifact
- run a scheduled cycle entrypoint for recurring execution
- detect candidate/graveyard changes between recent runs

## Default operator entrypoints

### Run one workflow

```bash
python3 scripts/run_tushare_workflow.py
```

### Run the full scheduled cycle manually

```bash
python3 scripts/run_scheduled_cycle.py
```

### Rebuild reports only

```bash
python3 scripts/build_sqlite_report.py
python3 scripts/build_html_report.py
python3 scripts/build_index_page.py
python3 scripts/build_run_summary.py
python3 scripts/build_change_report.py
```

## Key output artifacts

- `artifacts/factor_lab.db`
- `artifacts/report.html`
- `artifacts/index.html`
- `artifacts/sqlite_report.md`
- `artifacts/latest_summary.txt`
- `artifacts/change_report.md`
- `artifacts/tushare_workflow/`
- `artifacts/tushare_batch/`

## What is intentionally not in v1.0

- live trading
- production-grade queue orchestration
- cost-aware optimizer
- LLM-driven ideation/review loop
- automatic cron registration without operator choice

## Suggested next step after v1.0

Add optional automation layers:
- cron scheduling
- notifications
- LLM review/planning
- richer dashboards

## Operational note

The current workspace runtime has evolved beyond the original v1.0 baseline.
Primary operation is now centered on a long-running research daemon supervised by `systemd --user`, with queue-driven orchestration, follow-up task generation, heartbeats, and health/queue dashboards.
See `RUNBOOK.md` for the current operating model.
