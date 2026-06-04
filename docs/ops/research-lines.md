# Factor Lab research lines and authority order

Updated: 2026-06-02

## Current authority order

1. **Autonomous Strategy Lab (ASL)** decisions and status artifacts are current authority for autonomous research direction, especially `request_data`, coverage blockers, and controlled-execution adapter decisions.
2. **Harvest controller bridge** artifacts may consume/reflect ASL decisions, but should not override ASL blockers or resume stale same-route cycles independently.
3. **Harvest cycle-local verdicts** from V2/V3/V4/V5 experiments are historical unless explicitly promoted by current ASL/controller evidence.

## Current line

- Autonomous Strategy Lab request-data / coverage-preflight / controlled execution adapter.
- Use ASL status pages/artifacts before interpreting older Harvest cycle reports.

## Bridge line

- Harvest autonomous research controller and related policy/budget/evidence modules are bridge/operational context.
- Bridge code may summarize or consume ASL decisions, but should not independently authorize broad daemon/timer/live-trading behavior.

## Historical lines

- Harvest V2/V3/V4/V5 standalone evolution/controller/governor artifacts and cycle-local verdicts.
- Treat stale `continue_modified_route` / same-route verdicts as historical when ASL says `request_data` or coverage is blocked.

## Caller map

See:

- `artifacts/engineering_hardening_2026-06-02/harvest_caller_map.json`
- `artifacts/engineering_hardening_2026-06-02/harvest_caller_map.md`

## Safety policy

- Do not delete or archive Harvest/ASL code based only on the classification map.
- Review caller samples and tests before moving any file.
- Archive stale artifacts under `artifacts/archive/...`; do not direct-delete evidence.
- Do not touch `artifacts/factor_lab.db` in this phase.
