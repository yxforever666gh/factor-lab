# Factor Research MVP

A minimal, reproducible framework for factor research.

## What this project now includes

- synthetic sample market/fundamental data generator
- Tushare-backed A-share data workflow
- structured factor definitions with safe expression evaluation
- single-factor evaluation pipeline
- simple pass/fail gate
- v0.2 research add-ons:
  - time-split robustness check
  - factor correlation matrix for de-duplication
- experiment artifacts written to `artifacts/`

## Project layout

- `src/factor_lab/` — framework code
- `configs/first_workflow.json` — synthetic example workflow
- `configs/tushare_workflow.json` — first real-data workflow
- `scripts/run_first_workflow.py` — synthetic workflow entrypoint
- `scripts/run_tushare_workflow.py` — Tushare workflow entrypoint
- `artifacts/` — generated outputs

## Local secrets

Create `.env` with:

```bash
TUSHARE_TOKEN=your_token_here
```

`.env` is ignored by git.

## Run

### Synthetic workflow

```bash
python3 scripts/run_first_workflow.py
```

### Tushare workflow

```bash
python3 scripts/run_tushare_workflow.py
```

## Outputs

Each workflow writes a dataset snapshot and research artifacts such as:

- `results.json`
- `split_results.json`
- `summary.md`
- `factor_correlation.csv`

## Current scope

This is still a research MVP, not a production trading engine. The goal is to make the first research loop reproducible and extensible before adding real portfolio construction, scheduling, and large-scale batch search.
