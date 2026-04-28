# Daemon Heartbeat Schema

`artifacts/research_daemon_heartbeat.json` is a lightweight runtime snapshot written by `scripts/run_research_daemon.py` whenever daemon status is updated.

The file is designed for WebUI/control pages and smoke tests. It must be cheap to read and safe to parse even while the daemon is running.

## Schema

```json
{
  "timestamp": "2026-04-28T00:00:00+00:00",
  "pid": 12345,
  "project_root": "/home/admin/factor-lab",
  "provider": "real_llm",
  "state": "running",
  "queue": {
    "pending": 0,
    "running": 1,
    "finished_24h": 120,
    "failed_24h": 3
  },
  "current_task": {
    "id": "...",
    "task_type": "workflow",
    "status": "running",
    "started_at_utc": "...",
    "created_at_utc": "..."
  },
  "last_injection": {},
  "skip_reasons_24h": {},
  "processed_tasks_total": 12,
  "rss_mb": 1024
}
```

## Rules

- The heartbeat is runtime state and must not be committed to Git.
- Writes are atomic: daemon writes a temporary JSON file and then replaces the target.
- Missing DB or locked DB must not crash the daemon; queue fields should fall back to zero or include a short `error` field.
- API keys, tokens, prompts, and raw LLM responses must never be written to this heartbeat.
- WebUI pages may treat this file as optional; absence should show “暂无 heartbeat 文件”.

## Verification

```bash
python3 -m json.tool artifacts/research_daemon_heartbeat.json
```
