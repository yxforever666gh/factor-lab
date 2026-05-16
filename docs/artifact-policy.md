# Artifact Policy

This policy defines which Factor Lab files should be committed to Git, which files should remain local runtime state, and which generated outputs may be promoted into curated snapshots.

## Tracked in Git

Commit files that are small, deterministic, and useful for review or reproducible tests:

- Small deterministic golden fixtures under `tests/fixtures/`.
- Human-written reports, policies, plans, and design notes under `docs/`.
- Minimal example configuration files under `configs/`.

Tracked files should avoid machine-local paths, credentials, tokens, large market datasets, and nondeterministic runtime output.

## Not tracked in Git

Runtime artifacts are local working state and should not be newly committed. In particular, keep these out of Git:

- SQLite databases and sidecar files:
  - `artifacts/**/*.db`
  - `artifacts/**/*.db-wal`
  - `artifacts/**/*.db-shm`
- Generated run payloads and result files:
  - `artifacts/**/dataset.csv`
  - `artifacts/**/results.json`
  - `artifacts/**/portfolio_results.json`
  - `artifacts/diagnostics/*.json`
- Generated candidate run directories, including `artifacts/generated_candidate_*/`, `artifacts/generated_*/`, and opportunity generated batch runs.
- Tushare cache and Tushare workflow/batch outputs.
- Feature-store parquet outputs and their generated metadata sidecars.

These paths may contain large data, nondeterministic outputs, local cache state, or frequently changing runtime state. Do not use `git rm` or delete local artifacts merely to enforce this policy; ignore rules only affect newly untracked files.

## Snapshot candidates

Generated outputs may be committed only after being deliberately curated into stable, small snapshots:

- Copy compact summaries or minimized examples into `tests/fixtures/` when they are needed as deterministic golden fixtures.
- Copy human-readable summaries into `docs/snapshots/` when they are useful for documentation or review history.

Snapshot candidates should be stripped of large datasets, volatile timestamps when practical, secrets, credentials, and machine-specific paths. Prefer concise summaries over full runtime directories.
