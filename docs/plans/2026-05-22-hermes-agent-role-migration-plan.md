# Factor Lab Hermes-Native Rebuild Plan

> **For Hermes:** Use subagent-driven-development to implement this plan task-by-task. This plan intentionally replaces the old role-agent style with Hermes-native shape, naming, configuration, and execution. Do not preserve old terminology for compatibility unless a task explicitly says to delete or quarantine it.

**Goal:** Make Factor Lab use Hermes Agent profiles as the only agent abstraction.

**Architecture:** Factor Lab becomes a domain application that asks named Hermes profiles to perform research work. Hermes owns agent identity, profile config, tool access, skills, sessions, and model routing. Factor Lab owns domain data, schemas, artifacts, queue admission, and deterministic validation.

**Model rule:** Factor Lab Hermes profiles and any Factor Lab-spawned Hermes subagents must use the current Hermes main model. Do not pin model/provider/base_url/api_key inside Factor Lab profile configs, bootstrap files, env keys, or role-specific code. When the Hermes main model changes, every Factor Lab profile follows automatically on the next invocation.

**Single source of truth:** the main Hermes config is the only model source. Factor Lab may read it at call time and pass `--provider` / `--model` explicitly to a profile invocation, or rely on Hermes native inheritance if that is supported and probe-verified. It must never maintain separate subagent model settings.

**Tech Stack:** Hermes CLI profiles, Hermes toolsets, Hermes skills, Hermes sessions, Python, pytest, Factor Lab artifacts and validation code.

---

## Target shape

Factor Lab should look and feel like a Hermes application, not like an older multi-agent framework with Hermes bolted on.

Target Hermes profiles:

```text
factor-lab-researcher
factor-lab-diagnostician
factor-lab-reviewer
factor-lab-data-steward
```

Target profile responsibilities:

```text
factor-lab-researcher
  Creates and prioritizes research ideas, reads project state, writes proposal artifacts.

factor-lab-diagnostician
  Investigates failed runs, reads logs/artifacts, writes diagnosis artifacts.

factor-lab-reviewer
  Reviews candidates, risk, overfit, duplication, and promotion readiness.

factor-lab-data-steward
  Checks data coverage, cache quality, field availability, and data blockers.
```

Target execution pattern:

```text
Factor Lab event
  -> write a Hermes briefing artifact
  -> call the correct Hermes profile
  -> Hermes uses tools/skills/sessions as needed
  -> Hermes returns a JSON artifact
  -> Factor Lab validates and applies deterministic gates
```

Target environment:

```env
FACTOR_LAB_AGENT_BACKEND=hermes
FACTOR_LAB_HERMES_MODE=native
FACTOR_LAB_HERMES_REQUIRE_PROFILE=1
FACTOR_LAB_HERMES_PROFILE_MAP_JSON={"researcher":"factor-lab-researcher","diagnostician":"factor-lab-diagnostician","reviewer":"factor-lab-reviewer","data_steward":"factor-lab-data-steward"}
FACTOR_LAB_HERMES_SESSION_MODE=resume
FACTOR_LAB_HERMES_MODEL_SOURCE=main
FACTOR_LAB_HERMES_ARTIFACT_DIR=artifacts/hermes
```

Important: these names are the new public language. Do not expose old role names in WebUI, docs, artifact names, or new APIs.

---

## Naming rules

Use Hermes-style names everywhere new code is touched.

Allowed public terms:

```text
Hermes profile
Hermes skill
Hermes toolset
Hermes session
Hermes briefing
Hermes artifact
Hermes backend
Hermes main model
Factor Lab research event
Factor Lab validation gate
```

Model vocabulary rule:

```text
Say: profiles inherit the Hermes main model.
Do not say: each profile has its own model, fallback model, agent model, role model, or provider fallback.
```

Names to remove from public/config/UI/docs paths:

```text
planner
failure_analyst
data_quality
role config
role agent
provider fallback
live decision provider
observation decision provider
old claw/claw-style wording
```

The word `reviewer` may remain only because it maps cleanly to a Hermes profile responsibility. Prefer `factor-lab-reviewer` for public naming.

Implementation may keep temporary adapter code internally for one or two commits, but the final acceptance criteria require no old names in active config, WebUI, docs, or artifact paths.

---

## File-level cleanup target

Current old-style files should be removed, renamed, or reduced to compatibility shims with no active runtime ownership.

Primary replacements:

```text
src/factor_lab/agent_roles.py
  -> src/factor_lab/hermes_profiles.py

src/factor_lab/llm_agent.py
  -> remove or fold into src/factor_lab/hermes_client.py

src/factor_lab/llm_provider_router.py
  -> src/factor_lab/hermes_router.py

src/factor_lab/agent_briefs.py
  -> src/factor_lab/hermes_briefings.py

src/factor_lab/agent_runtime_hooks.py
  -> src/factor_lab/hermes_runtime_hooks.py

src/factor_lab/agent_schemas.py
  -> src/factor_lab/hermes_contracts.py

src/factor_lab/agent_responses.py
  -> src/factor_lab/hermes_artifacts.py

src/factor_lab/webui_templates/agents.html
  -> src/factor_lab/webui_templates/hermes.html
```

Test replacements should mirror the source names:

```text
tests/test_agent_roles.py
  -> tests/test_hermes_profiles.py

tests/test_llm_provider_router.py
  -> tests/test_hermes_router.py

tests/test_agent_briefs.py
  -> tests/test_hermes_briefings.py

tests/test_agent_runtime_hooks.py
  -> tests/test_hermes_runtime_hooks.py

tests/test_reviewer_data_quality_agents.py
  -> split into tests/test_factor_lab_reviewer_profile.py and tests/test_factor_lab_data_steward_profile.py
```

Artifact path replacements:

```text
artifacts/agent_*.json
  -> artifacts/hermes/*.json

artifacts/*agent*brief*.json
  -> artifacts/hermes/briefings/*.json

artifacts/*agent*response*.json
  -> artifacts/hermes/responses/*.json
```

---

## Phase 1: Create the Hermes vocabulary layer

### Task 1.1: Add canonical Hermes profile definitions

**Objective:** Create the new source of truth for Factor Lab Hermes profiles.

**Files:**

- Create: `src/factor_lab/hermes_profiles.py`
- Create: `tests/test_hermes_profiles.py`

**Implementation:**

Create a `HermesProfileSpec` dataclass with:

```python
@dataclass(frozen=True)
class HermesProfileSpec:
    key: str
    profile: str
    purpose: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    session: str
    artifact_namespace: str
```

Define exactly four specs:

```python
HERMES_PROFILE_SPECS = {
    "researcher": HermesProfileSpec(
        key="researcher",
        profile="factor-lab-researcher",
        purpose="Create and prioritize Factor Lab research proposals.",
        toolsets=("file", "terminal", "skills", "session_search"),
        skills=("factor-lab",),
        session="factor-lab-researcher-main",
        artifact_namespace="researcher",
    ),
    "diagnostician": HermesProfileSpec(
        key="diagnostician",
        profile="factor-lab-diagnostician",
        purpose="Diagnose failed Factor Lab runs and propose repairs.",
        toolsets=("file", "terminal", "skills", "session_search"),
        skills=("factor-lab",),
        session="factor-lab-diagnostician-main",
        artifact_namespace="diagnostician",
    ),
    "reviewer": HermesProfileSpec(
        key="reviewer",
        profile="factor-lab-reviewer",
        purpose="Review Factor Lab candidates before promotion.",
        toolsets=("file", "terminal", "skills"),
        skills=("factor-lab",),
        session="factor-lab-reviewer-main",
        artifact_namespace="reviewer",
    ),
    "data_steward": HermesProfileSpec(
        key="data_steward",
        profile="factor-lab-data-steward",
        purpose="Check Factor Lab data availability, quality, and blockers.",
        toolsets=("file", "terminal", "skills"),
        skills=("factor-lab",),
        session="factor-lab-data-steward-main",
        artifact_namespace="data_steward",
    ),
}
```

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_profiles.py -q
python3 -m py_compile src/factor_lab/hermes_profiles.py
```

---

### Task 1.2: Add old-name translation only as a temporary migration helper

**Objective:** Let existing call sites move gradually while preventing old names from becoming public API.

**Files:**

- Modify: `src/factor_lab/hermes_profiles.py`
- Modify: `tests/test_hermes_profiles.py`

**Implementation:**

Add a private mapping:

```python
_LEGACY_EVENT_KEY_MAP = {
    "planner": "researcher",
    "failure_analyst": "diagnostician",
    "data_quality": "data_steward",
}
```

Rules:

- The mapping is private.
- It is not rendered in WebUI.
- It is not written to artifacts except as `input_event_key` during migration diagnostics.
- Add a TODO with a concrete removal milestone.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_profiles.py -q
```

Expected: canonical names are returned by default; old names only work through an explicitly named translation function.

---

## Phase 2: Replace the router with Hermes-first routing

### Task 2.1: Create Hermes router

**Objective:** Route Factor Lab events directly to Hermes profiles using canonical Hermes names.

**Files:**

- Create: `src/factor_lab/hermes_router.py`
- Create: `tests/test_hermes_router.py`

**Implementation:**

Create:

```python
class HermesRouter:
    def __init__(self, profile_map: Mapping[str, str] | None = None): ...

    def route(self, event_key: str, context: Mapping[str, Any]) -> HermesRoute:
        ...
```

`HermesRoute` should include:

```python
@dataclass(frozen=True)
class HermesRoute:
    request_id: str
    profile_key: str
    profile_name: str
    session_name: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    model_source: str = "main"
    main_provider: str | None = None
    main_model: str | None = None
    briefing_path: Path
    response_path: Path
```

Rules:

- New call sites must use `researcher`, `diagnostician`, `reviewer`, `data_steward`.
- If an old event key arrives, translate it once and mark the route with `migration_alias_used=True`.
- Do not use provider-style names in the router API.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_router.py -q
python3 -m py_compile src/factor_lab/hermes_router.py
```

---

### Task 2.2: Deprecate old router module

**Objective:** Stop active code from importing old routing names.

**Files:**

- Modify: existing call sites that import the old router
- Add/modify tests that assert imports use `HermesRouter`

**Implementation:**

Search for imports of the old router and replace them with `HermesRouter`.

Where immediate deletion would be too large, leave a small shim that raises a clear migration error unless explicitly enabled by tests.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 - <<'PY'
from pathlib import Path
bad = []
for p in Path('src').rglob('*.py'):
    text = p.read_text()
    if 'llm_provider_router' in text or 'DecisionProviderRouter' in text:
        bad.append(str(p))
print('\n'.join(bad))
raise SystemExit(1 if bad else 0)
PY
```

Expected: no active source files import the old router.

---

## Phase 3: Build the Hermes client around CLI profiles

### Task 3.1: Add Hermes client

**Objective:** Call Hermes as a real profile-backed agent, not as a generic completion API.

**Files:**

- Create: `src/factor_lab/hermes_client.py`
- Create: `tests/test_hermes_client.py`

**Command shape:**

```bash
hermes --profile factor-lab-researcher --resume factor-lab-researcher-main chat -q '<prompt>' --provider '<current-main-provider>' --model '<current-main-model>' --toolsets file,terminal,skills,session_search --quiet
```

If named resume behaves differently in the installed Hermes version, the client should select the supported Hermes command shape after probing `hermes --help` and `hermes profile list`.

The provider/model flags must be resolved from the current Hermes main config at call time. They must not come from Factor Lab profile config, old provider settings, role config, or hardcoded defaults.

**Request object:**

```python
@dataclass(frozen=True)
class HermesRequest:
    request_id: str
    profile_key: str
    profile_name: str
    session_name: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    model_source: str = "main"
    main_provider: str | None = None
    main_model: str | None = None
    briefing_path: Path
    response_path: Path
    timeout_seconds: int = 300
```

**Result object:**

```python
@dataclass(frozen=True)
class HermesResult:
    ok: bool
    request_id: str
    profile_key: str
    profile_name: str
    response_path: Path
    payload: dict[str, Any] | None
    raw_text: str
    exit_code: int | None
    error: str | None
```

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_client.py -q
python3 -m py_compile src/factor_lab/hermes_client.py
```

Expected client tests:

```text
- Hermes command includes --provider and --model from the main Hermes config.
- Changing the mocked main config changes every profile command.
- Profile-specific model/provider values are ignored or rejected.
- Missing main model config fails loudly instead of silently using profile-local defaults.
```

---

### Task 3.2: Make JSON output a Hermes artifact contract

**Objective:** Stop treating Hermes replies as chat text; treat them as Factor Lab artifacts.

**Files:**

- Create: `src/factor_lab/hermes_contracts.py`
- Create: `src/factor_lab/hermes_artifacts.py`
- Create: `tests/test_hermes_contracts.py`
- Create: `tests/test_hermes_artifacts.py`

**Required common fields:**

```json
{
  "request_id": "...",
  "profile_key": "researcher",
  "summary": "...",
  "recommendation": "...",
  "confidence": 0.0,
  "risks": [],
  "next_actions": []
}
```

Profile-specific fields can be added in `hermes_contracts.py`, but every response must include the common fields.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_contracts.py tests/test_hermes_artifacts.py -q
```

---

### Task 3.3: Add main model resolver

**Objective:** Make model inheritance explicit and testable.

**Files:**

- Create: `src/factor_lab/hermes_main_model.py`
- Create: `tests/test_hermes_main_model.py`

**Implementation:**

Create a small resolver that reads the active Hermes main configuration using supported Hermes CLI/config paths.

Preferred behavior:

```text
1. Run `hermes config` or read the default Hermes config path.
2. Extract current main provider and model.
3. Return a typed `HermesMainModel(provider, model)` object.
4. Never read Factor Lab old provider keys for this purpose.
5. Fail loudly if provider or model is missing.
```

The resolver is used by `HermesClient` immediately before spawning each Hermes profile. This guarantees that if the user changes the main Hermes model, Factor Lab profiles follow without editing profile configs.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_main_model.py tests/test_hermes_client.py -q
python3 -m py_compile src/factor_lab/hermes_main_model.py src/factor_lab/hermes_client.py
```

---

## Phase 4: Replace briefings with Hermes briefings

### Task 4.1: Create Hermes briefing writer

**Objective:** Give Hermes profiles file-based context and let them use tools instead of stuffing old prompt templates into a provider call.

**Files:**

- Create: `src/factor_lab/hermes_briefings.py`
- Create: `tests/test_hermes_briefings.py`

**Briefing path:**

```text
artifacts/hermes/briefings/<profile_key>/<request_id>.json
```

**Briefing content:**

```json
{
  "request_id": "...",
  "profile_key": "researcher",
  "profile_name": "factor-lab-researcher",
  "workdir": "/home/admin/factor-lab",
  "task": "...",
  "context": {},
  "important_paths": [],
  "output_contract": {},
  "allowed_actions": []
}
```

**Prompt shape sent to Hermes:**

```text
You are the Factor Lab Hermes profile: <profile_name>.
Work in /home/admin/factor-lab.
Read this briefing if needed: <briefing_path>.
Return one JSON object matching the contract. No markdown. No extra text.
The JSON must include request_id=<request_id> and profile_key=<profile_key>.
```

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_briefings.py -q
```

---

## Phase 5: Bootstrap Hermes profiles the Hermes way

### Task 5.1: Add profile config file

**Objective:** Store Factor Lab profile setup as Hermes-facing config, not old role config JSON.

**Files:**

- Create: `configs/hermes/profiles.json`
- Create: `scripts/bootstrap_hermes_profiles.py`
- Create: `tests/test_bootstrap_hermes_profiles.py`

**Config shape:**

```json
{
  "model_source": "main",
  "profiles": {
    "factor-lab-researcher": {
      "workdir": "/home/admin/factor-lab",
      "toolsets": ["file", "terminal", "skills", "session_search"],
      "skills": ["factor-lab"]
    },
    "factor-lab-diagnostician": {
      "workdir": "/home/admin/factor-lab",
      "toolsets": ["file", "terminal", "skills", "session_search"],
      "skills": ["factor-lab"]
    },
    "factor-lab-reviewer": {
      "workdir": "/home/admin/factor-lab",
      "toolsets": ["file", "terminal", "skills"],
      "skills": ["factor-lab"]
    },
    "factor-lab-data-steward": {
      "workdir": "/home/admin/factor-lab",
      "toolsets": ["file", "terminal", "skills"],
      "skills": ["factor-lab"]
    }
  }
}
```

`configs/hermes/profiles.json` must not contain `model`, `provider`, `base_url`, `api_key`, `fallback_order`, or any profile-local model override. If any of those fields are present, bootstrap must fail instead of silently accepting them.

**Bootstrap behavior:**

- Run `hermes profile list`.
- Create missing profiles with `hermes profile create <name>`.
- Set profile workdir, toolsets, and skills using supported Hermes CLI/config paths.
- Do not set profile-local model/provider/base_url/api_key. Profiles inherit the main Hermes model.
- Validate that generated profile configs contain no model override fields.
- Print exact commands run.
- Fail loudly if Hermes CLI is unavailable.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 scripts/bootstrap_hermes_profiles.py --dry-run
pytest tests/test_bootstrap_hermes_profiles.py -q
```

Expected bootstrap tests:

```text
- Generated profile config has no model/provider/base_url/api_key fields.
- A forbidden model override in configs/hermes/profiles.json makes bootstrap fail.
- Creating all four profiles does not pin a model inside any profile.
```

---

## Phase 6: Replace WebUI language and settings

### Task 6.1: Replace Agents page with Hermes page

**Objective:** Make the UI speak Hermes.

**Files:**

- Rename: `src/factor_lab/webui_templates/agents.html` -> `src/factor_lab/webui_templates/hermes.html`
- Modify: `src/factor_lab/webui_app.py`
- Create/modify: `tests/test_webui_hermes_settings.py`

**UI sections:**

```text
Hermes backend
Hermes profiles
Hermes sessions
Hermes toolsets
Hermes artifacts
Hermes health
```

**Do not show:**

```text
old role config JSON
provider fallback order as agent architecture
planner/failure_analyst/data_quality labels
old claw/claw-style labels
```

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_webui_hermes_settings.py tests/test_webui_routes.py -q
```

---

## Phase 7: Move runtime hooks to Hermes names

### Task 7.1: Rename runtime hook module

**Objective:** Use Hermes runtime hooks as the public integration point.

**Files:**

- Rename: `src/factor_lab/agent_runtime_hooks.py` -> `src/factor_lab/hermes_runtime_hooks.py`
- Rename: `tests/test_agent_runtime_hooks.py` -> `tests/test_hermes_runtime_hooks.py`
- Modify all imports

**Implementation:**

Expose functions like:

```python
run_researcher_profile(...)
run_diagnostician_profile(...)
run_reviewer_profile(...)
run_data_steward_profile(...)
```

Do not expose functions named after old roles.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_runtime_hooks.py -q
python3 - <<'PY'
from pathlib import Path
bad_terms = ['agent_runtime_hooks', 'planner', 'failure_analyst', 'data_quality']
bad = []
for p in Path('src/factor_lab').rglob('*.py'):
    text = p.read_text()
    for term in bad_terms:
        if term in text:
            bad.append(f'{p}:{term}')
print('\n'.join(bad))
raise SystemExit(1 if bad else 0)
PY
```

Expected: no active source references to old public names. If a temporary migration adapter still exists, it must be listed in a deletion task and excluded from runtime imports.

---

## Phase 8: Remove old public config keys

### Task 8.1: Replace environment keys

**Objective:** Stop configuring Factor Lab agents through old provider/role language.

**Files:**

- Modify: `.env.example` if present
- Modify: config loading code
- Modify: WebUI settings writer
- Create/modify: `tests/test_hermes_env_settings.py`

**New keys only:**

```env
FACTOR_LAB_AGENT_BACKEND=hermes
FACTOR_LAB_HERMES_MODE=native
FACTOR_LAB_HERMES_REQUIRE_PROFILE=1
FACTOR_LAB_HERMES_PROFILE_MAP_JSON={"researcher":"factor-lab-researcher","diagnostician":"factor-lab-diagnostician","reviewer":"factor-lab-reviewer","data_steward":"factor-lab-data-steward"}
FACTOR_LAB_HERMES_SESSION_MODE=resume
FACTOR_LAB_HERMES_TIMEOUT_SECONDS=300
FACTOR_LAB_HERMES_ARTIFACT_DIR=artifacts/hermes
```

Old keys should be read only by a temporary migration script, not by active runtime code.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_env_settings.py -q
python3 - <<'PY'
from pathlib import Path
bad_terms = [
    'FACTOR_LAB_AGENT_ROLES_JSON',
    'FACTOR_LAB_DECISION_PROVIDER',
    'FACTOR_LAB_LIVE_DECISION_PROVIDER',
    'FACTOR_LAB_OBSERVATION_DECISION_PROVIDER',
]
bad = []
for root in ['src', 'tests', 'docs']:
    for p in Path(root).rglob('*'):
        if p.is_file() and p.suffix in {'.py', '.md', '.html', '.json', '.yaml', '.yml'}:
            text = p.read_text(errors='ignore')
            for term in bad_terms:
                if term in text:
                    bad.append(f'{p}:{term}')
print('\n'.join(bad))
raise SystemExit(1 if bad else 0)
PY
```

Expected: old keys are gone from active source/docs after migration tasks complete.

---

## Phase 9: Add one-command Hermes enablement

### Task 9.1: Add Hermes native setup script

**Objective:** Provide one command that makes Factor Lab Hermes-native.

**Files:**

- Create: `scripts/enable_hermes_native.py`
- Create: `tests/test_enable_hermes_native.py`

**Command:**

```bash
cd /home/admin/factor-lab
python3 scripts/enable_hermes_native.py --write --bootstrap-profiles --smoke
```

**Script behavior:**

1. Verify `hermes` CLI exists.
2. Bootstrap profiles.
3. Write new env keys.
4. Run one smoke request per profile.
5. Write rollout artifact:

```text
artifacts/hermes/rollouts/<timestamp>.json
```

6. Print a status summary using Hermes names only.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 scripts/enable_hermes_native.py --dry-run
pytest tests/test_enable_hermes_native.py -q
```

---

## Phase 10: Smoke test real Hermes profiles

### Task 10.1: Add Hermes smoke script

**Objective:** Prove each Hermes profile can run and return valid JSON.

**Files:**

- Create: `scripts/smoke_hermes_profiles.py`
- Create: `tests/test_smoke_hermes_profiles.py`

**Command:**

```bash
cd /home/admin/factor-lab
FACTOR_LAB_AGENT_BACKEND=hermes \
FACTOR_LAB_HERMES_MODE=native \
FACTOR_LAB_HERMES_REQUIRE_PROFILE=1 \
python3 scripts/smoke_hermes_profiles.py --write-artifacts
```

**Expected:**

- `factor-lab-researcher` returns valid JSON.
- `factor-lab-diagnostician` returns valid JSON.
- `factor-lab-reviewer` returns valid JSON.
- `factor-lab-data-steward` returns valid JSON.
- All responses include matching `request_id`.
- All artifacts are written under `artifacts/hermes/`.

---

## Phase 11: Delete or quarantine old files

### Task 11.1: Remove old source modules from active runtime

**Objective:** Finish the cleanup so the codebase no longer carries the previous agent style in active paths.

**Files to delete or quarantine:**

```text
src/factor_lab/agent_roles.py
src/factor_lab/llm_agent.py
src/factor_lab/agent_briefs.py
src/factor_lab/agent_runtime_hooks.py
src/factor_lab/agent_schemas.py
src/factor_lab/agent_responses.py
```

If deletion is too risky in one commit, move compatibility shims to:

```text
src/factor_lab/legacy_compat/
```

Rules for `legacy_compat`:

- Not imported by normal runtime.
- Not shown in WebUI.
- Not used by docs except migration notes.
- Has a dated deletion TODO.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 - <<'PY'
from pathlib import Path
forbidden_files = [
    Path('src/factor_lab/agent_roles.py'),
    Path('src/factor_lab/llm_agent.py'),
    Path('src/factor_lab/agent_briefs.py'),
    Path('src/factor_lab/agent_runtime_hooks.py'),
    Path('src/factor_lab/agent_schemas.py'),
    Path('src/factor_lab/agent_responses.py'),
]
remaining = [str(p) for p in forbidden_files if p.exists()]
print('\n'.join(remaining))
raise SystemExit(1 if remaining else 0)
PY
```

---

## Phase 12: Final vocabulary audit

### Task 12.1: Add repository vocabulary audit

**Objective:** Prevent old language from creeping back into Factor Lab.

**Files:**

- Create: `scripts/audit_hermes_vocabulary.py`
- Create: `tests/test_audit_hermes_vocabulary.py`

**Audit rules:**

Public source/docs/UI must not contain old public labels except in:

```text
docs/archive/**
src/factor_lab/legacy_compat/**
tests/legacy_compat/**
```

The script should scan:

```text
src/
tests/
docs/
configs/
scripts/
```

and fail on old public terms.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 scripts/audit_hermes_vocabulary.py
pytest tests/test_audit_hermes_vocabulary.py -q
```

---

## Acceptance criteria

Migration is complete when:

1. `hermes profile list` shows:

```text
factor-lab-researcher
factor-lab-diagnostician
factor-lab-reviewer
factor-lab-data-steward
```

2. Factor Lab runtime uses `FACTOR_LAB_AGENT_BACKEND=hermes` and `FACTOR_LAB_HERMES_MODE=native`.
3. WebUI has a Hermes page, not an old agents page.
4. Active source imports `HermesRouter`, `HermesClient`, `HermesProfileSpec`, `hermes_briefings`, `hermes_artifacts`, and `hermes_runtime_hooks`.
5. Old public names are gone from active config, UI, docs, and artifact paths.
6. All Hermes profile smoke tests pass.
7. A vocabulary audit exists and passes.
8. Factor Lab validation gates still run after Hermes responses.
9. Hermes artifacts are written under `artifacts/hermes/`.
10. The repo reads as a Hermes-native Factor Lab integration, not as a previous agent framework with a Hermes adapter.

---

## Implementation status — 2026-05-22

Implemented in this pass:

- Added canonical Hermes profile vocabulary layer:
  - `src/factor_lab/hermes_profiles.py`
  - `tests/test_hermes_profiles.py`
- Added Hermes-first route/request layer:
  - `src/factor_lab/hermes_router.py`
  - `tests/test_hermes_router.py`
- Added profile-backed Hermes CLI client and JSON artifact contract:
  - `src/factor_lab/hermes_client.py`
  - `src/factor_lab/hermes_contracts.py`
  - `src/factor_lab/hermes_artifacts.py`
  - matching tests
- Added file-based Hermes briefing writer:
  - `src/factor_lab/hermes_briefings.py`
  - `tests/test_hermes_briefings.py`
- Added Hermes-named runtime hooks:
  - `src/factor_lab/hermes_runtime_hooks.py`
  - `tests/test_hermes_runtime_hooks.py`
- Added Hermes profile bootstrap/config/smoke/audit scripts:
  - `scripts/bootstrap_hermes_profiles.py`
  - `scripts/enable_hermes_native.py`
  - `scripts/smoke_hermes_profiles.py`
  - `scripts/audit_hermes_vocabulary.py`
- Added Hermes-facing profile config:
  - `configs/hermes/profiles.json`
- Replaced the public WebUI Agents page with `/hermes` and `hermes.html`, leaving `/agents` as a compatibility redirect for existing tests/bookmarks.
- Bootstrapped the four Hermes profiles in the local Hermes installation:
  - `factor-lab-researcher`
  - `factor-lab-diagnostician`
  - `factor-lab-reviewer`
  - `factor-lab-data-steward`
- Wrote Hermes-native Factor Lab env keys via `scripts/enable_hermes_native.py --write --dry-run`.

Verification performed:

```bash
pytest \
  tests/test_hermes_profiles.py \
  tests/test_hermes_router.py \
  tests/test_hermes_client.py \
  tests/test_hermes_contracts.py \
  tests/test_hermes_artifacts.py \
  tests/test_hermes_briefings.py \
  tests/test_hermes_runtime_hooks.py \
  tests/test_webui_hermes_settings.py \
  tests/test_bootstrap_hermes_profiles.py \
  tests/test_enable_hermes_native.py \
  tests/test_smoke_hermes_profiles.py \
  tests/test_audit_hermes_vocabulary.py \
  tests/test_hermes_env_settings.py \
  -q
# 27 passed
```

```bash
python3 -m py_compile \
  src/factor_lab/hermes_profiles.py \
  src/factor_lab/hermes_router.py \
  src/factor_lab/hermes_client.py \
  src/factor_lab/hermes_contracts.py \
  src/factor_lab/hermes_artifacts.py \
  src/factor_lab/hermes_briefings.py \
  src/factor_lab/hermes_runtime_hooks.py \
  scripts/bootstrap_hermes_profiles.py \
  scripts/enable_hermes_native.py \
  scripts/smoke_hermes_profiles.py \
  scripts/audit_hermes_vocabulary.py \
  src/factor_lab/webui_app.py
```

Live smoke note:

- `hermes profile list` confirms all four profiles exist.
- Real live smoke reached Hermes profile invocation but provider credentials/subscription are currently not usable:
  - `nowcoding`: `403 insufficient_user_quota`
  - `ccvibe`: `403 SUBSCRIPTION_NOT_FOUND`
- Therefore deterministic smoke/test coverage passes, but live profile JSON smoke requires a working Hermes provider credential before it can pass.

Known follow-up if strict acceptance is required:

- Active legacy decision modules still exist for backward-compatible deterministic routing and older tests. The Hermes-native layer is now present and tested; fully deleting old modules/imports is a larger destructive cleanup and should be done in a separate compatibility-breaking pass.

## Verification suite

Run targeted suite:

```bash
cd /home/admin/factor-lab
python3 -m py_compile \
  src/factor_lab/hermes_profiles.py \
  src/factor_lab/hermes_router.py \
  src/factor_lab/hermes_client.py \
  src/factor_lab/hermes_contracts.py \
  src/factor_lab/hermes_artifacts.py \
  src/factor_lab/hermes_briefings.py \
  src/factor_lab/hermes_runtime_hooks.py \
  scripts/bootstrap_hermes_profiles.py \
  scripts/enable_hermes_native.py \
  scripts/smoke_hermes_profiles.py \
  scripts/audit_hermes_vocabulary.py

pytest \
  tests/test_hermes_profiles.py \
  tests/test_hermes_router.py \
  tests/test_hermes_client.py \
  tests/test_hermes_contracts.py \
  tests/test_hermes_artifacts.py \
  tests/test_hermes_briefings.py \
  tests/test_hermes_runtime_hooks.py \
  tests/test_webui_hermes_settings.py \
  tests/test_bootstrap_hermes_profiles.py \
  tests/test_enable_hermes_native.py \
  tests/test_smoke_hermes_profiles.py \
  tests/test_audit_hermes_vocabulary.py \
  -q
```

Run full suite:

```bash
cd /home/admin/factor-lab
pytest tests -q
```

Run live smoke only after profiles exist:

```bash
cd /home/admin/factor-lab
python3 scripts/bootstrap_hermes_profiles.py --write
python3 scripts/smoke_hermes_profiles.py --write-artifacts
```

---

## Rollout stance

This migration should be direct:

1. Introduce Hermes vocabulary modules.
2. Move routing to Hermes profile keys.
3. Move CLI invocation to Hermes profiles.
4. Rename WebUI and config.
5. Delete or quarantine old modules.
6. Run smoke.
7. Fix failures from artifacts.

Do not spend effort preserving the old style as a parallel architecture. The desired end state is Hermes-native, with Factor Lab as a domain app inside the Hermes ecosystem.
