# Factor Lab

A reproducible factor research platform, now packaged as v1.0.

## What this project includes

- synthetic sample market/fundamental data generator
- Tushare-backed real A-share data workflow
- structured factor definitions
- single-factor evaluation pipeline
- simple pass/fail gate
- time-split robustness check
- industry + size neutralized factor check
- factor correlation matrix for de-duplication clues
- simple long/short composite portfolio backtest
- candidate pool / graveyard outputs
- factor scoring and correlation-cluster representatives
- experiment ledger per workflow run
- SQLite experiment store for runs, factors, portfolios, and artifacts
- SQL views for common research queries
- basic task state tracking per workflow
- markdown + HTML reporting from the SQLite store
- run fingerprinting / rerun linkage in the experiment store
- batch runner for multiple workflow configs with comparison output
- scheduled cycle entrypoint for recurring runs
- index page and latest-summary artifact for quick glance usage
- change detection between recent finished runs
- experiment artifacts written to `artifacts/`

## Project layout

- `src/factor_lab/` — framework code
- `configs/first_workflow.json` — synthetic first batch
- `configs/tushare_workflow.json` — real-data workflow config
- `configs/tushare_batch.json` — batch workflow config
- `scripts/run_first_workflow.py` — synthetic workflow entrypoint
- `scripts/run_tushare_workflow.py` — Tushare workflow entrypoint
- `scripts/run_tushare_batch.py` — batch runner entrypoint
- `artifacts/` — generated outputs (runtime-only, not intended for source control)

## Install

```bash
python3 -m pip install -e .
```

For tests:

```bash
python3 -m pip install -e '.[dev]'
```

## Local secrets

Create a local `.env` file:

```bash
cp .env.example .env
```

Then set:

```bash
TUSHARE_TOKEN=your_token_here
```

`.env` is ignored by git.

## Run

### 1) Synthetic smoke test

```bash
python3 scripts/run_first_workflow.py
```

Outputs:

- `artifacts/first_workflow/results.json`
- `artifacts/first_workflow/summary.md`

### 2) Real Tushare workflow

```bash
python3 scripts/run_tushare_workflow.py
```

### 3) Local web UI

```bash
python3 scripts/run_web_ui.py
```

Defaults to `127.0.0.1:8765`.
Override with `WEB_UI_HOST` and `WEB_UI_PORT` in `.env` or the shell environment.

### 4) Autonomous research daemon

Main long-running research runtime:

```bash
python3 scripts/run_research_daemon.py
```

Install/enable the user service:

```bash
./scripts/install_research_daemon_service.sh
```

See `RUNBOOK.md` for the full operating model, health checks, and troubleshooting steps.

Outputs:

- `artifacts/tushare_workflow/dataset.csv`
- `artifacts/tushare_workflow/results.json`
- `artifacts/tushare_workflow/split_results.json`
- `artifacts/tushare_workflow/factor_correlation.csv`
- `artifacts/tushare_workflow/neutralized_results.json`
- `artifacts/tushare_workflow/portfolio_results.json`
- `artifacts/tushare_workflow/candidate_pool.json`
- `artifacts/tushare_workflow/factor_graveyard.json`
- `artifacts/tushare_workflow/factor_scores.json`
- `artifacts/tushare_workflow/cluster_representatives.json`
- `artifacts/tushare_workflow/experiment_ledger.json`
- `artifacts/tushare_workflow/task_state.json`
- `artifacts/tushare_workflow/summary.md`
- `artifacts/tushare_batch/batch_summary.json`
- `artifacts/tushare_batch/batch_comparison.json`
- `artifacts/factor_lab.db`
- `artifacts/sqlite_report.md`
- `artifacts/report.html`
- `artifacts/index.html`
- `artifacts/latest_summary.txt`
- `artifacts/change_report.md`

## Current real-data factor set

- `momentum_20`
- `earnings_yield`
- `book_yield`
- `roe` (derived ROE-like profitability = `(E/P)/(B/P)`)
- `size_inv`
- `turnover_shock_5_20`
- `momentum_20 + earnings_yield`

## Notes

This is the v1.0 baseline release.

What it does well now:
- prove the research workflow end-to-end
- swap between sample and Tushare data
- generate reproducible experiment artifacts
- provide first-pass robustness and de-dup clues
- compare raw factor efficacy versus industry/size-neutralized efficacy
- run a simple composite long/short portfolio sanity check
- classify factors into a candidate pool versus graveyard
- run multiple workflow configs through one batch entrypoint

What it does not do yet:
- transaction cost model
- portfolio optimizer
- production task queue / scheduler
- factor library / graveyard persistence
- richer robustness tests beyond simple half-split
