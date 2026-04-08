# Research Runtime Hardening Plan

Status: in progress
Owner: OpenClaw assistant
Updated: 2026-04-03

## Goal

Prevent one bad research task family from stalling the whole autonomous runtime.

## P0

- Validate generated configs before execution and reject malformed lineage.
- Add generated batch preflight validation.
- Classify failures into transient vs deterministic.
- Do not retry deterministic failures.
- Quarantine deterministic task outputs and mark them clearly.
- Avoid global stall when only one task family is unhealthy.

## P1

- Add schema_version and migration path for generated artifacts.
- Add dependency graph metadata for generated factors.
- Teach the planner to avoid repeatedly injecting unhealthy branches.
- Isolate queue lanes so exploration failures do not block validation.
- Surface root cause and blocked lane information in the UI.

## P2

- Add auto-heal for common malformed generated artifacts when safe.
- Add compatibility tests for historical artifacts.
- Add failure dashboard and blocked-lane visibility.
- Tie promotion more tightly to medium/long window validation.

## Done So Far

- Recorded the plan in `memory/2026-04-03.md`.
- Added generated batch preflight validation.
- Added deterministic failure classification.
- Added deterministic no-retry / quarantine path.
- Added fail-safe tests for generated batch validation.
- Restarted the daemon and manually recovered stuck generated batches.
- Implemented task-family scoped circuit handling.
- Added daemon recycle safeguards: batch workers default to 1, daemon exits after task budget, daemon exits on RSS threshold.
- Moved heavy `workflow` / `batch` / `generated_batch` execution onto short-lived subprocess workers.

## Next Slice

- Observe whether RSS stays bounded under the new subprocess model.
- Budget guard is now pushed earlier for risky probe opportunities with no target candidates.
- Report refresh is now throttled; next consider separating refresh onto its own worker path.
- Add blocked-lane / root-cause visibility to the UI.
