
## 2026-04-30 Direction diagnostics after controlled value smoke

- Daemon remained inactive during paired original/inverted value-route workflows.
- Original routes had positive Rank IC but negative top-bottom spread.
- Inverting the signal did not fix spread: all three inverted spreads were also negative and worse.
- A synthetic portfolio convention test confirms `evaluate_long_short_portfolio` is high-signal minus low-signal under the tested setup.
- Current conclusion: do not restore broad daemon execution. The next issue is not a simple global sign flip; investigate mechanism/data/window/portfolio construction quality and why positive IC is not monetized by the current long-short construction.
- Runtime worker now has a final `workflow_admission_adapter` guard before `run_workflow`, and controlled restart dry-run currently finds zero workflow tasks safe to run.

## 2026-04-30 Controlled daemon-ready bucket-aware tasks

After bucket-aware validation passed, three workflow tasks were prepared for controlled daemon restart. `dry_run_controlled_restart.py` reported `would_run_count=3` and `blocked_count=2`; would-run tasks are admitted bucket-aware value routes. Daemon remains inactive until explicit restart confirmation.

## 2026-04-30 Controlled restart monitoring lesson

Starting the systemd research daemon reached `active` but spun CPU without claiming tasks; it was stopped and returned to inactive. Manual `run_orchestrator` rounds completed the admitted bucket-aware workflows. The first bucket-aware workflow triggered an old generic generated_batch follow-up; this was blocked and `research_queue._enqueue_followups_for_workflow` now suppresses generic follow-ups for `source=bucket_aware_controlled_validation` / `portfolio_construction.mode=bucket_pair`. Future controlled daemon restart needs a stricter queue filter and investigation of the systemd startup spin.

## Daemon safe controlled runner lesson — 2026-04-30

Systemd `active` is not enough evidence that Factor Lab research runtime is healthy. A safe daemon restart must prove: controlled restart dry-run has admitted workflow(s), task claim occurs, heartbeat/status advances, no unsafe generated/recent follow-up is emitted, and the runner exits or sleeps intentionally when idle/no-claimable. Added bounded controls in `scripts/run_research_daemon.py`, a one-shot runner `scripts/run_controlled_orchestrator_once.py`, daemon probe `scripts/probe_research_daemon_once.py`, and systemd environment audit `scripts/audit_research_daemon_systemd_env.py`. Current safe state remains daemon inactive when `would_run_count=0`.

## Controlled restart Phase F lesson — 2026-04-30

Safe-runner tests passed, but Phase F showed systemd daemon is still not safe as a long-running workflow executor: during controlled probes it can update heartbeat and process admitted work, yet stopping the service while worker subprocesses are active can time out and require SIGKILL. Use `scripts/run_controlled_orchestrator_once.py` for admitted tasks. Do not restore broad/systemd daemon until worker subprocess cancellation and one-shot/timer service semantics are fixed. Final safe state after cleanup: daemon inactive, controlled dry-run `would_run_count=0`.
