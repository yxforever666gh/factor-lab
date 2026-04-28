# OpenClaw Reference Policy

## Purpose

Factor Lab is now expected to run as an independent project rooted at:

```text
/home/admin/factor-lab
```

This policy separates OpenClaw references that are still valid architectural concepts from references that would reintroduce a runtime dependency on the old OpenClaw workspace.

## Allowed: OpenClaw agent-role architecture

Factor Lab may keep OpenClaw-inspired agent-role architecture concepts, including:

- role-based research agents;
- planner/reviewer/failure-analyst separation;
- agent brief and agent response terminology;
- compatibility notes that explain historical migration context;
- tests or verifier fixtures that contain old strings only to prove blocked references are detected.

Allowed concept references must not cause the running system to import code, load environment files, spawn processes, or read artifacts from the old OpenClaw workspace.

## Blocked: OpenClaw workspace path

Runtime code and service files must not depend on the old workspace path:

```text
/home/admin/.openclaw/workspace
```

Blocked examples:

```ini
WorkingDirectory=/home/admin/.openclaw/workspace
EnvironmentFile=/home/admin/.openclaw/workspace/.env
Environment=PYTHONPATH=/home/admin/.openclaw/workspace/src
ExecStart=/usr/bin/python3 /home/admin/.openclaw/workspace/scripts/run_research_daemon.py
```

The runtime verifier treats such references as failures in source, scripts, and systemd files.

## Blocked by default: OpenClaw CLI event delivery

Direct OpenClaw CLI event delivery is not part of the default Factor Lab runtime. If compatibility is retained, it must be:

- explicitly gated by an environment variable;
- disabled by default;
- non-fatal when the CLI is missing;
- documented as compatibility behavior, not a core dependency.

Acceptable compatibility pattern:

```text
RESEARCH_DAEMON_WAKE_EVENTS=0
```

and code paths that return disabled/unavailable instead of crashing.

## Provider policy

### Current default provider

```text
real_llm
```

`real_llm` is the default decision/brief provider for the independent Factor Lab runtime.

### Compatibility provider

```text
openclaw_agent
```

`openclaw_agent` is considered a migration/compatibility provider. It must not be the default in production daemon service configuration unless a deliberate compatibility experiment is being run and documented.

## Agent vs provider distinction

OpenClaw-style agents and model providers are different layers.

Provider layer responsibilities:

- `base_url`;
- model name;
- API key lookup;
- timeout/retry;
- request/response transport.

Agent layer responsibilities:

- role identity;
- prompt contract;
- required inputs;
- output schema;
- permission boundaries;
- decision interpretation;
- auditability.

A provider fallback is not a replacement for the old agent-role architecture.

## Verification

Run:

```bash
python3 scripts/verify_de_openclaw_runtime.py
```

Expected healthy output:

```text
PASS webui_path
PASS daemon_path
PASS provider
PASS no_old_workspace_process
PASS no_blocked_openclaw_refs
WARN allowed_openclaw_concept_refs=N
```

A warning for allowed concept references is acceptable. A failure for old workspace path, old environment file, or old Python path is not acceptable.

## Completion-report rule

Any future migration or de-OpenClaw completion report must include actual verifier output, not just a prose claim that migration is complete.
