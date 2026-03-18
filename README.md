# Factor Research MVP

A minimal, reproducible framework for factor research.

## What this MVP includes

- synthetic sample market/fundamental data generator
- structured factor definitions
- single-factor evaluation pipeline
- simple pass/fail gate
- experiment artifacts written to `artifacts/`

## Project layout

- `src/factor_lab/` — framework code
- `configs/first_workflow.json` — first batch of factor experiments
- `scripts/run_first_workflow.py` — entrypoint to execute the first workflow
- `artifacts/` — generated outputs

## Run

```bash
python3 scripts/run_first_workflow.py
```

## Current workflow

The first workflow generates sample data for a small equity universe, evaluates several factors (momentum, value, quality, turnover surprise), then writes:

- `artifacts/first_workflow/results.json`
- `artifacts/first_workflow/summary.md`

This is intentionally small and deterministic so the core research flow is easy to verify before swapping in real market data.
