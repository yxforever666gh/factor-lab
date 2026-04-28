# Factor Lab Runbook

## Current operating mode

Factor Lab is no longer primarily driven by cron.

### Main loop

The main research loop is now:

- `scripts/run_research_daemon.py` — long-running research daemon
- `systemd/factor-lab-research-daemon.service` — user service that keeps the daemon alive

This daemon is the primary engine for 24/7 operation.
It:

- watches the SQLite research queue
- claims pending tasks
- executes workflow / batch / generated_batch / diagnostic tasks
- writes heartbeats
- writes daemon status
- idles when no work is available
- continues after restarts via systemd

### Supporting automation

Cron is now secondary.

- `factor-lab research orchestrator` cron job — **disabled**
- `factor-lab llm cycle` cron job — lightweight support check, not primary research execution

## What is running the system?

### Primary runtime

- **Daemon:** `scripts/run_research_daemon.py`
- **Supervisor:** `systemd --user`
- **Service name:** `factor-lab-research-daemon.service`

### Main data/control files

- Queue DB: `artifacts/factor_lab.db`
- Heartbeats: `artifacts/system_heartbeat.jsonl`
- Daemon status: `artifacts/research_daemon_status.json`
- Web UI: `http://127.0.0.1:8765`

## How to check if the system is healthy

### 1) Check daemon health

```bash
systemctl --user status factor-lab-research-daemon.service
```

Good signs:

- `Active: active (running)`
- recent CPU time increasing
- no restart loop

### 2) Check daemon status file

```bash
cat artifacts/research_daemon_status.json
```

Look for:

- recent `updated_at_utc`
- `state` is `running`, `idle`, or `guardrail`
- `last_processed` exists and changes over time

### 3) Check heartbeats

```bash
tail -n 30 artifacts/system_heartbeat.jsonl
```

Look for:

- `research_daemon started`
- repeated `research_orchestrator started/finished`
- no long silent gaps
- no repeated `circuit_open`

### 4) Check the UI

Recommended pages:

- `/health` — overall health, stall alerts, daemon status, knowledge gain summary
- `/research` — queue state, task lineage, knowledge gain, parent tasks
- `/weekly` — weekly trend summary

## Normal states

These are normal and not failures:

- queue temporarily empty
- daemon state = `idle`
- orchestrator heartbeat says `idle`
- cron delivery disabled (`delivery.mode=none`)
- old cron run history showing previous announce/channel errors

## Warning signs

Investigate if you see any of the following:

### Daemon-level issues

- `systemctl --user status` not active
- daemon status file not updating
- daemon repeatedly restarts
- `state = failed` for a long time

### Queue-level issues

- `/research` shows many pending tasks but no finished tasks
- same retry task reappears repeatedly
- exploration tasks pile up without useful gain
- no baseline tasks appear after queue empties

### Health-level issues

- `/health` stall red light = yes
- `/health` failure alert = yes
- `no_significant_information_gain` keeps increasing while exploration remains active

### Guardrail-level issues

- heartbeats contain repeated `circuit_open`
- recent failures trigger continuous pausing

## First-response troubleshooting

### Problem: daemon is dead

Run:

```bash
systemctl --user restart factor-lab-research-daemon.service
systemctl --user status factor-lab-research-daemon.service
```

Then verify:

```bash
cat artifacts/research_daemon_status.json
```

### Problem: queue seems empty and nothing is moving

Seed the queue manually:

```bash
python3 scripts/seed_research_queue.py
```

Then either wait for the daemon to pick tasks up, or manually run one step:

```bash
python3 scripts/run_research_orchestrator.py
```

### Problem: UI is down

Restart the web UI manually:

```bash
set -a; source .env; set +a; python3 scripts/run_web_ui.py
```

For background mode, use the existing local process pattern or another service wrapper.

### Problem: too many failures

Check recent heartbeats and queue:

```bash
tail -n 50 artifacts/system_heartbeat.jsonl
```

and

- open `/research`
- inspect failed tasks and `last_error`

If the issue is data/provider related, let the guardrail pause the system rather than forcing repeated retries.

## Manual operator commands

### Seed queue

```bash
python3 scripts/seed_research_queue.py
```

### Run one orchestrator step

```bash
python3 scripts/run_research_orchestrator.py
```

### Run daemon manually in foreground

```bash
python3 scripts/run_research_daemon.py
```

### Install/reinstall the systemd user service

```bash
./scripts/install_research_daemon_service.sh
```

## Current architecture summary

### Main path

1. daemon wakes
2. queue inspected
3. task claimed
4. task executed
5. follow-up tasks generated
6. heartbeat + status updated
7. daemon sleeps briefly or idles

### Task types currently supported

- `workflow`
- `batch`
- `generated_batch`
- `diagnostic`

### Guardrails currently present

- empty-queue reseed
- retry task creation
- consecutive-failure circuit-open pause
- exploration throttling after repeated no-gain outcomes
- health-page stall and warning indicators

## Versioning note

Public docs may still mention v1.0 baseline.
Operationally, the current system is beyond that baseline and behaves like an early `v2.x alpha` autonomous research runtime.
