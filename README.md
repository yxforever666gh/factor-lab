# Factor Research MVP

A minimal, reproducible framework for factor research.

## What this project includes

- synthetic sample market/fundamental data generator
- Tushare-backed real A-share data workflow
- structured factor definitions
- single-factor evaluation pipeline
- simple pass/fail gate
- time-split robustness check
- factor correlation matrix for de-duplication clues
- experiment artifacts written to `artifacts/`

## Project layout

- `src/factor_lab/` — framework code
- `configs/first_workflow.json` — synthetic first batch
- `configs/tushare_workflow.json` — real-data workflow config
- `scripts/run_first_workflow.py` — synthetic workflow entrypoint
- `scripts/run_tushare_workflow.py` — Tushare workflow entrypoint
- `artifacts/` — generated outputs

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

Outputs:

- `artifacts/tushare_workflow/dataset.csv`
- `artifacts/tushare_workflow/results.json`
- `artifacts/tushare_workflow/split_results.json`
- `artifacts/tushare_workflow/factor_correlation.csv`
- `artifacts/tushare_workflow/summary.md`

## Current real-data factor set

- `momentum_20`
- `earnings_yield`
- `book_yield`
- `size_inv`
- `turnover_shock_5_20`
- `momentum_20 + earnings_yield`

## Notes

This is still an MVP.

What it does well now:
- prove the research workflow end-to-end
- swap between sample and Tushare data
- generate reproducible experiment artifacts
- provide first-pass robustness and de-dup clues

What it does not do yet:
- industry / size neutralization
- transaction cost model
- portfolio optimizer
- production task queue / scheduler
- factor library / graveyard persistence
