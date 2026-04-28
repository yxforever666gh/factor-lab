# Factor Lab

Factor Lab is an experimental automated quantitative factor research and backtesting platform. It combines data preparation, factor evaluation, robustness checks, candidate lifecycle management, an autonomous research daemon, and a local FastAPI Web UI.

> **Status:** research/engineering system, not production trading software. Nothing in this repository is financial advice.

## Features

- Synthetic sample-data workflow for quick local validation.
- Tushare-backed A-share data workflow.
- Factor evaluation, time-split robustness, neutralization, correlation/de-duplication, and portfolio sanity checks.
- Candidate pool / watchlist / graveyard style research lifecycle artifacts.
- SQLite-backed experiment store and reports.
- Autonomous research daemon and queue orchestration.
- Local Web UI for health, runs, LLM/provider settings, agents, and read-only control status.
- Runtime hardening utilities:
  - `scripts/verify_de_openclaw_runtime.py`
  - `scripts/build_queue_explanation.py`
  - `scripts/smoke_test_factor_lab.py`

## Project layout

```text
src/factor_lab/                 Core Python package
configs/                        Workflow configs
scripts/                        CLI/runtime entrypoints
systemd/                        User service templates
src/factor_lab/webui_templates/ FastAPI/Jinja Web UI templates
tests/                          Pytest tests
docs/                           Plans, policies, runbooks, design notes
artifacts/                      Local runtime outputs; not intended for source control
```

## Install

```bash
python3 -m pip install -e .
```

For development/tests:

```bash
python3 -m pip install -e '.[dev]'
```

## Configuration

Create a local `.env` from the template:

```bash
cp .env.example .env
```

Then fill in local-only values such as:

```env
TUSHARE_TOKEN=replace-me
FACTOR_LAB_LLM_BASE_URL=https://example.com/v1
FACTOR_LAB_LLM_MODEL=replace-me
FACTOR_LAB_LLM_API_KEY=replace-me
```

Real `.env` files and API keys must not be committed.

## Common commands

### Synthetic smoke workflow

```bash
python3 scripts/run_first_workflow.py
```

### Tushare workflow

```bash
python3 scripts/run_tushare_workflow.py
```

### Local Web UI

```bash
python3 scripts/run_web_ui.py
```

Default URL:

```text
http://127.0.0.1:8765/
```

Useful pages:

```text
/          lightweight overview
/control   read-only runtime control/status
/health    health diagnostics
/runs      workflow runs
/agents    agent-role settings
/llm       LLM/provider status
```

### Research daemon

```bash
python3 scripts/run_research_daemon.py
```

Or install the user service if appropriate:

```bash
./scripts/install_research_daemon_service.sh
```

## Verification

Run focused tests and runtime checks:

```bash
pytest tests/test_verify_de_openclaw_runtime.py \
       tests/test_webui_routes.py \
       tests/test_daemon_heartbeat.py \
       tests/test_queue_explanation.py \
       tests/test_smoke_script.py -q

python3 scripts/verify_de_openclaw_runtime.py
python3 scripts/build_queue_explanation.py
python3 scripts/smoke_test_factor_lab.py
```

## Runtime artifacts policy

`artifacts/` is local runtime space. Large generated outputs, SQLite databases, Tushare caches, diagnostics, parquet feature stores, and generated candidate runs should not be committed.

See:

```text
docs/artifact-policy.md
```

Use small deterministic fixtures under `tests/fixtures/` or curated snapshots under `docs/snapshots/` if examples are needed.

## OpenClaw migration note

Factor Lab is expected to run independently from the old OpenClaw workspace. OpenClaw-inspired agent-role architecture concepts may remain, but runtime dependencies on `/home/admin/.openclaw/workspace` should not.

See:

```text
docs/openclaw-reference-policy.md
scripts/verify_de_openclaw_runtime.py
```

## License

Add a `LICENSE` file before public release if you want this project to be open-source under a specific license.
