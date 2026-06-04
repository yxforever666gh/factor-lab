# Harvest Agent operations

Harvest Agent is a safe, local, auditable research-loop wrapper for Factor Lab. It runs one bounded cycle at a time under `artifacts/harvest_agent/` and updates compact local knowledge under `knowledge/`.

## Safety boundaries

- No live trading or broker/order path.
- No broad daemon restoration.
- No timer/cron/systemd enablement by these scripts.
- Controlled execution requires `--allow-controlled-execution` and is capped to one experiment in the Phase 10-12 orchestrator/timer preview path.
- Reports and knowledge notes summarize durable conclusions only; detailed run records stay in artifacts.

## One dry-run cycle

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_agent_once.py --dry-run
```

This writes state snapshot, charter, proposals, reviewer decision, deterministic gate, execution manifest, evidence ledger, verdict, knowledge update, next-cycle plan, latest-cycle pointer, and report.

## Optional controlled single experiment

```bash
PYTHONPATH=src .venv/bin/python scripts/run_harvest_agent_once.py --allow-controlled-execution --max-experiments 1
```

This remains local and does not start a daemon or schedule.

## Knowledge and report

```bash
PYTHONPATH=src .venv/bin/python scripts/update_harvest_knowledge.py
PYTHONPATH=src .venv/bin/python scripts/write_harvest_report.py
```

Artifacts:

- `artifacts/harvest_agent/latest_cycle.json`
- `artifacts/harvest_agent/report.json`
- `artifacts/harvest_agent/report.md`

Knowledge:

- `knowledge/harvest_agent.md`
- `knowledge/mechanism_lessons.md`
- `knowledge/data_blockers.json`
- `knowledge/research_waste.md`

## Timer preview only

```bash
PYTHONPATH=src .venv/bin/python scripts/render_harvest_agent_timer.py --preview
```

The renderer writes preview unit text under `artifacts/harvest_agent/timer_preview/`. It does **not** install or enable a timer. Manual approval is required before scheduling.

## WebUI status hook

If the FastAPI WebUI is running, `GET /harvest-agent/status` returns the same safety-oriented report payload.
