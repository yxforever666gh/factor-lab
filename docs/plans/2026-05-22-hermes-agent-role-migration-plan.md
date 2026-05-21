# Factor Lab Role Agents to Hermes-Native Agent Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. This is the aggressive migration plan requested by the user: prefer full Hermes Agent ownership over conservative compatibility. Keep rollback switches, but do not optimize for gradualism.

**Goal:** Turn Factor Lab’s current role agents (`planner`, `failure_analyst`, `reviewer`, `data_quality`) into real Hermes Agent-backed autonomous role runtimes, with Hermes profiles, tool access, session continuity, and role-specific execution policies as the default decision path.

**Architecture:** Factor Lab keeps `AgentRoleConfig` only as the role registry and schema contract. Actual role execution moves from in-process `real_llm` calls to Hermes profile invocations. Each role becomes a Hermes-native agent identity with its own profile, prompt, skills, toolsets, memory/session policy, workdir, and model/provider configuration. Factor Lab becomes the domain runtime and validator; Hermes becomes the agent layer.

**Tech Stack:** Python, pytest, Factor Lab `DecisionProviderRouter`, Hermes CLI profiles, Hermes session resume, Hermes toolsets, JSON response contracts, WebUI env-backed settings, daemon/runtime hooks.

---

## Current state

Current Factor Lab roles are defined in:

- `src/factor_lab/agent_roles.py`

Current runtime routing is in:

- `src/factor_lab/llm_provider_router.py`

Current roles:

- `planner`: research planning, next tasks, candidate expansion.
- `failure_analyst`: failed run/candidate diagnosis and repair suggestions.
- `reviewer`: candidate quality review, duplicate/overfit/fragility review.
- `data_quality`: data source, field, cache, coverage, and Tushare diagnostics.

Today these are not real Hermes agents. They are role configs whose prompts are injected into a provider request. The real execution path is `DecisionProviderRouter -> real_llm -> provider/model fallback`.

The migration target is deliberately stronger:

```text
Factor Lab decision/event
  -> role registry / schema contract
  -> Hermes profile for that role
  -> Hermes agent uses tools / skills / workdir / session as needed
  -> JSON decision artifact
  -> Factor Lab validates schema and applies deterministic admission/runtime rules
```

---

## Target state: full Hermes-native mode

Default production mode after migration:

```env
FACTOR_LAB_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_LIVE_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_HERMES_AGENT_MODE=full
FACTOR_LAB_HERMES_REQUIRE_AGENT=1
```

Role profile mapping:

```json
{
  "planner": "factor-lab-planner",
  "failure_analyst": "factor-lab-failure-analyst",
  "reviewer": "factor-lab-reviewer",
  "data_quality": "factor-lab-data-quality"
}
```

Each profile is a real Hermes profile, not just a model alias.

Expected profile characteristics:

- Separate Hermes profile home under `~/.hermes/profiles/<profile>`.
- `terminal.cwd=/home/admin/factor-lab`.
- Factor Lab skill available or explicitly loaded.
- Role-specific system/personality prompt stored as profile instructions or injected per call.
- Role-specific allowed toolsets.
- Role-specific session policy.
- Role-specific model/provider can differ from other roles.

Role tool policy in aggressive mode:

- `planner`: `file`, `terminal`, `skills`, `session_search`, optionally `web` later.
- `failure_analyst`: `file`, `terminal`, `skills`, `session_search`.
- `reviewer`: `file`, `terminal`, `skills`.
- `data_quality`: `file`, `terminal`, `skills`.

Default stance: allow tools because these are supposed to be agents, not prompt wrappers. Factor Lab still validates outputs and blocks unsafe workflow admission.

---

## Important design decisions

### 1. Hermes is the agent layer, not a provider synonym

The WebUI and config must distinguish:

- LLM provider/profile/model: where tokens come from.
- Hermes agent profile: which autonomous runtime identity handles a Factor Lab role.
- Factor Lab role: what responsibility/schema/event type is being handled.

Do not call `factor-lab-planner` a provider. It is an agent profile.

### 2. `real_llm` becomes fallback/debug, not the target

After this migration, `real_llm` should be used for:

- emergency fallback when `FACTOR_LAB_HERMES_REQUIRE_AGENT=0`;
- A/B diagnostics;
- tests that should not spawn Hermes;
- bootstrap if profiles are missing.

It should not remain the primary architecture.

### 3. Stateful Hermes sessions are part of the target

The previous conservative plan preferred stateless `hermes chat -q`. This aggressive plan uses durable role sessions by default after the smoke phase.

Recommended session keys:

```text
factor-lab-planner-main
factor-lab-failure-analyst-main
factor-lab-reviewer-main
factor-lab-data-quality-main
```

Each role should be able to accumulate domain context across calls. To reduce state poisoning, Factor Lab must include request IDs and require every JSON output to cite the request ID it is answering.

### 4. Factor Lab remains the deterministic safety boundary

Hermes can reason and inspect files, but Factor Lab still owns:

- JSON schema validation;
- workflow admission;
- queue writes;
- daemon task claiming;
- budget gates;
- duplicate control;
- controlled-only runtime policy;
- result ledger and artifacts.

Aggressive does not mean letting a chat transcript directly mutate research queues without validation.

### 5. Failed Hermes calls should produce artifacts, not silent fallback

When a Hermes role fails, write an artifact containing:

- request ID;
- decision type;
- role name;
- profile name;
- command used;
- exit code;
- stdout/stderr tail;
- parse/validation error;
- fallback path used, if any.

This makes “出了错事后再改” practical.

---

## New config surface

Add environment/config keys:

```env
FACTOR_LAB_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_LIVE_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_HERMES_AGENT_MODE=full
FACTOR_LAB_HERMES_REQUIRE_AGENT=1
FACTOR_LAB_HERMES_PROFILE_MAP_JSON={"planner":"factor-lab-planner","failure_analyst":"factor-lab-failure-analyst","reviewer":"factor-lab-reviewer","data_quality":"factor-lab-data-quality"}
FACTOR_LAB_HERMES_SESSION_MODE=resume
FACTOR_LAB_HERMES_TIMEOUT_SECONDS=300
FACTOR_LAB_HERMES_MAX_OUTPUT_CHARS=60000
FACTOR_LAB_HERMES_ARTIFACT_DIR=artifacts/hermes_agent_decisions
FACTOR_LAB_HERMES_FALLBACK_PROVIDER=real_llm
```

Mode meanings:

- `FACTOR_LAB_HERMES_AGENT_MODE=off`: never use Hermes.
- `shadow`: call Hermes and existing provider, use existing provider result, persist diff.
- `hybrid`: use Hermes for selected roles, fallback allowed.
- `full`: use Hermes for all roles.

Require flag:

- `FACTOR_LAB_HERMES_REQUIRE_AGENT=1`: fail loudly if Hermes fails or profile is missing.
- `0`: fallback to `FACTOR_LAB_HERMES_FALLBACK_PROVIDER`.

Aggressive target is `full + require_agent=1` after tests and smoke.

---

## Implementation phases

## Phase 0: Replace the conservative plan with an aggressive inventory

### Task 0.1: Add Hermes migration inventory script

**Objective:** Produce a preflight report that tells whether Factor Lab can enter Hermes-native mode.

**Files:**

- Create: `scripts/audit_hermes_agent_migration.py`
- Create: `tests/test_audit_hermes_agent_migration.py`

**Script must report:**

- current decision provider envs;
- loaded `AgentRoleConfig` roles and decision types;
- expected Hermes profile map;
- whether `hermes` CLI exists;
- output of `hermes profile list` parsed enough to detect required profiles;
- current daemon status;
- current WebUI health if reachable;
- whether `artifacts/hermes_agent_decisions` is writable.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 scripts/audit_hermes_agent_migration.py
pytest tests/test_audit_hermes_agent_migration.py -q
```

Expected: report clearly says either `ready_for_full_hermes=false` with blockers, or `true`.

---

## Phase 1: Create Hermes profile bootstrapper

### Task 1.1: Add profile bootstrap script

**Objective:** Create/update the four Hermes profiles reproducibly.

**Files:**

- Create: `scripts/bootstrap_hermes_role_profiles.py`
- Create: `configs/hermes_role_profiles.json`
- Create: `tests/test_bootstrap_hermes_role_profiles.py`

**Config shape:**

```json
{
  "profiles": {
    "planner": {
      "profile": "factor-lab-planner",
      "workdir": "/home/admin/factor-lab",
      "skills": ["factor-lab"],
      "toolsets": ["file", "terminal", "skills", "session_search"],
      "session": "factor-lab-planner-main"
    },
    "failure_analyst": {
      "profile": "factor-lab-failure-analyst",
      "workdir": "/home/admin/factor-lab",
      "skills": ["factor-lab"],
      "toolsets": ["file", "terminal", "skills", "session_search"],
      "session": "factor-lab-failure-analyst-main"
    },
    "reviewer": {
      "profile": "factor-lab-reviewer",
      "workdir": "/home/admin/factor-lab",
      "skills": ["factor-lab"],
      "toolsets": ["file", "terminal", "skills"],
      "session": "factor-lab-reviewer-main"
    },
    "data_quality": {
      "profile": "factor-lab-data-quality",
      "workdir": "/home/admin/factor-lab",
      "skills": ["factor-lab"],
      "toolsets": ["file", "terminal", "skills"],
      "session": "factor-lab-data-quality-main"
    }
  }
}
```

**Implementation notes:**

- Use `hermes profile create <name>` if missing.
- Use `hermes config set ... --profile <name>` only if the CLI supports it; otherwise document/manual-write profile config with clear artifact output.
- Do not hide CLI failures. Return non-zero if any required profile cannot be created.
- Bootstrap script may be idempotent.

**Verification:**

```bash
cd /home/admin/factor-lab
python3 scripts/bootstrap_hermes_role_profiles.py --dry-run
python3 scripts/bootstrap_hermes_role_profiles.py --write
hermes profile list
python3 scripts/audit_hermes_agent_migration.py
pytest tests/test_bootstrap_hermes_role_profiles.py -q
```

Expected: four profiles exist and audit can find them.

---

## Phase 2: Add HermesAgentClient

### Task 2.1: Implement command builder and request envelope

**Objective:** Build a deterministic Hermes invocation layer without touching `DecisionProviderRouter` yet.

**Files:**

- Create: `src/factor_lab/hermes_agent_client.py`
- Create: `tests/test_hermes_agent_client.py`

**Core API:**

```python
@dataclass(frozen=True)
class HermesAgentRequest:
    request_id: str
    role_name: str
    decision_type: str
    profile: str
    session_name: str | None
    system_prompt: str
    schema: dict[str, Any] | None
    context: dict[str, Any]
    timeout_seconds: int

@dataclass(frozen=True)
class HermesAgentResult:
    ok: bool
    request_id: str
    role_name: str
    profile: str
    payload: dict[str, Any] | None
    raw_text: str
    command: list[str]
    exit_code: int | None
    error: str | None
    artifact_path: str | None
```

**Command target:**

Use Hermes CLI one-shot first, but include session resume support:

```bash
hermes --profile factor-lab-planner --resume factor-lab-planner-main chat -q '<prompt>' --toolsets file,terminal,skills,session_search --quiet
```

If `--resume` with arbitrary names is unreliable, fall back to `--continue <name>` or stateless `chat -q`, but preserve the client abstraction.

**Prompt envelope requirements:**

The prompt sent to Hermes must include:

- role name;
- decision type;
- request ID;
- required JSON-only response instruction;
- Factor Lab schema if available;
- compact context JSON;
- explicit instruction: do not return markdown fences;
- explicit instruction: include `request_id` in JSON.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_agent_client.py -q
python3 -m py_compile src/factor_lab/hermes_agent_client.py
```

---

## Phase 3: Wire Hermes into DecisionProviderRouter as first-class provider

### Task 3.1: Add `hermes_agent` provider path

**Objective:** Make `DecisionProviderRouter(provider="hermes_agent")` execute the matching Hermes profile.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`
- Test: `tests/test_llm_provider_router_hermes_agent.py`

**Expected behavior:**

```python
router = DecisionProviderRouter(provider="hermes_agent")
payload = router.generate("planner", context)
```

Must:

- call `select_agent_role(decision_type)`;
- map role to Hermes profile;
- call `HermesAgentClient.generate(...)`;
- parse JSON;
- validate using existing validation path;
- attach metadata:
  - `provider: hermes_agent`
  - `agent_backend: hermes`
  - `agent_role`
  - `hermes_profile`
  - `hermes_request_id`
  - `hermes_artifact_path`
  - `fallback_used` if relevant.

**Aggressive fallback rule:**

- If `FACTOR_LAB_HERMES_REQUIRE_AGENT=1`, raise/return explicit provider failure. Do not silently use `real_llm`.
- If `0`, fallback to `FACTOR_LAB_HERMES_FALLBACK_PROVIDER`.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_llm_provider_router_hermes_agent.py -q
pytest tests/test_webui_agent_settings.py tests/test_webui_llm_settings.py -q
python3 -m py_compile src/factor_lab/llm_provider_router.py src/factor_lab/hermes_agent_client.py
```

---

## Phase 4: Add shadow and diff mode, but use it briefly

### Task 4.1: Implement shadow comparison artifacts

**Objective:** Allow one short A/B run before switching default to full Hermes.

**Files:**

- Create: `src/factor_lab/hermes_shadow_compare.py`
- Create: `tests/test_hermes_shadow_compare.py`
- Modify: `src/factor_lab/llm_provider_router.py`

**Behavior:**

If `FACTOR_LAB_HERMES_AGENT_MODE=shadow`:

- call `hermes_agent`;
- call existing configured provider;
- use existing provider result for runtime;
- write diff artifact under `artifacts/hermes_agent_decisions/shadow/`.

Diff should compare:

- schema validity;
- major fields;
- confidence/risk fields;
- execution time;
- parse errors;
- tool usage hints if captured.

Aggressive instruction: shadow mode is for one smoke window only. Do not let it become permanent architecture.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_shadow_compare.py tests/test_llm_provider_router_hermes_agent.py -q
```

---

## Phase 5: WebUI settings for Hermes-native agents

### Task 5.1: Expose Hermes agent backend separately from LLM settings

**Objective:** Make the WebUI reflect that these are real agents, not provider aliases.

**Files:**

- Modify: `src/factor_lab/webui_app.py`
- Modify: `src/factor_lab/webui_templates/settings.html`
- Test: `tests/test_webui_agent_settings.py`
- Test: `tests/test_webui_llm_settings.py`

**UI fields:**

- Decision backend: `real_llm`, `hermes_agent`, `mock`, `heuristic`.
- Hermes agent mode: `off`, `shadow`, `hybrid`, `full`.
- Require Hermes agent: true/false.
- Role profile map JSON.
- Session mode: `stateless`, `resume`.
- Timeout seconds.
- Artifact dir.

**Copy requirement:**

UI must say:

```text
Hermes Agent profiles are autonomous role runtimes. They are not the same thing as LLM provider profiles.
```

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_webui_agent_settings.py tests/test_webui_llm_settings.py tests/test_webui_routes.py -q
```

---

## Phase 6: Role-by-role live smoke, fast switch to full mode

### Task 6.1: Add live smoke script

**Objective:** Run one controlled Hermes call for each role and verify valid JSON artifacts.

**Files:**

- Create: `scripts/smoke_hermes_role_agents.py`
- Test: `tests/test_smoke_hermes_role_agents.py`

**Smoke contexts:**

- planner: ask for next research action from a tiny synthetic ledger summary.
- failure_analyst: diagnose a synthetic failed run.
- reviewer: review a synthetic candidate with duplicate/fragility hints.
- data_quality: diagnose a synthetic missing-field/cache issue.

**Run:**

```bash
cd /home/admin/factor-lab
FACTOR_LAB_DECISION_PROVIDER=hermes_agent \
FACTOR_LAB_HERMES_REQUIRE_AGENT=1 \
python3 scripts/smoke_hermes_role_agents.py --write-artifacts
```

Expected:

- all four roles return parseable JSON;
- each includes matching `request_id`;
- each passes schema or clearly reports schema mismatch artifact;
- artifacts written under `artifacts/hermes_agent_decisions/smoke/`.

After smoke passes, switch config target to:

```env
FACTOR_LAB_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_LIVE_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=hermes_agent
FACTOR_LAB_HERMES_AGENT_MODE=full
FACTOR_LAB_HERMES_REQUIRE_AGENT=1
```

---

## Phase 7: Make Hermes agents own runtime hooks

### Task 7.1: Route existing runtime hooks through Hermes by default

**Objective:** Ensure `agent_runtime_hooks.py` and planner pipeline use Hermes-native roles.

**Files:**

- Modify: `src/factor_lab/agent_runtime_hooks.py`
- Modify: `src/factor_lab/research_planner_pipeline.py`
- Test: `tests/test_agent_runtime_hooks.py`
- Test: `tests/test_research_planner_pipeline_hermes_agent.py`

**Expected behavior:**

- observation/data-quality hooks use `hermes_agent` unless explicitly overridden;
- planner uses `hermes_agent` in live mode;
- reviewer/failure-analysis calls record Hermes metadata;
- no call path accidentally hardcodes `real_llm` except explicit health/test endpoints.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_agent_runtime_hooks.py tests/test_research_planner_pipeline_hermes_agent.py -q
```

---

## Phase 8: Let planner Hermes Agent inspect project state directly

### Task 8.1: Add project-state briefing file instead of huge prompts

**Objective:** Feed Hermes agents compact pointers and let them inspect files with tools.

**Files:**

- Create: `src/factor_lab/hermes_briefing.py`
- Create: `tests/test_hermes_briefing.py`
- Modify: `src/factor_lab/hermes_agent_client.py`

**Briefing artifact:**

For every agent request, write:

```text
artifacts/hermes_agent_decisions/requests/<request_id>.json
```

Include:

- decision type;
- role;
- context summary;
- relevant artifact paths;
- latest ledger paths;
- DB query hints;
- schema requirement;
- allowed action boundary.

Then prompt Hermes with the path:

```text
Read artifacts/hermes_agent_decisions/requests/<request_id>.json if needed. Return only the JSON decision object.
```

This makes Hermes a real tool-using agent instead of a passive JSON completion engine.

**Verification:**

```bash
cd /home/admin/factor-lab
pytest tests/test_hermes_briefing.py tests/test_hermes_agent_client.py -q
```

---

## Phase 9: Daemon integration in Hermes-native mode

### Task 9.1: Use Hermes role agents inside controlled daemon execution

**Objective:** Allow the controlled Factor Lab daemon to call Hermes role agents for planning/review/failure/data-quality decisions.

**Files:**

- Modify: `scripts/run_research_daemon.py`
- Modify: `scripts/run_research_task_worker.py`
- Modify: `src/factor_lab/research_queue.py`
- Test: `tests/test_research_daemon_hermes_agent_mode.py`
- Test: `tests/test_run_research_task_worker_hermes_metadata.py`

**Rules:**

- Controlled admission remains enforced.
- Hermes planner may propose, but queue writes still pass gates.
- Hermes reviewer may reject or flag, but deterministic gate remains final.
- Hermes failure analyst may propose recovery, but recovery tasks still pass governance.
- Every task touched by Hermes stores metadata with role/profile/request ID.

**Aggressive runtime target:**

Once smoke passes, the daemon should be able to operate in controlled mode with Hermes agents as the normal brain.

**Verification:**

```bash
cd /home/admin/factor-lab
FACTOR_LAB_DECISION_PROVIDER=hermes_agent \
FACTOR_LAB_HERMES_AGENT_MODE=full \
FACTOR_LAB_HERMES_REQUIRE_AGENT=1 \
python3 scripts/dry_run_controlled_restart.py

pytest tests/test_research_daemon_hermes_agent_mode.py tests/test_run_research_task_worker_hermes_metadata.py -q
```

---

## Phase 10: Remove old OpenClaw compatibility from the active path

### Task 10.1: Audit and quarantine old OpenClaw naming/path dependencies

**Objective:** Make Hermes the explicit agent architecture and leave OpenClaw only as historical compatibility if truly needed.

**Files:**

- Create: `scripts/audit_legacy_agent_paths.py`
- Create: `tests/test_audit_legacy_agent_paths.py`
- Modify docs/config references as needed.

**Search terms:**

- `openclaw`
- `claw`
- old workspace paths
- old provider fallbacks that imply OpenClaw ownership

**Expected result:**

- No active runtime path depends on OpenClaw agent semantics.
- Any remaining string is marked historical/compatibility.
- Hermes profiles are documented as the new agent architecture.

---

## Phase 11: Operational rollout

### Task 11.1: One-command rollout script

**Objective:** Provide one explicit command to enter full Hermes-native mode.

**Files:**

- Create: `scripts/enable_hermes_native_agents.py`
- Create: `tests/test_enable_hermes_native_agents.py`

**Behavior:**

The script should:

1. run migration audit;
2. bootstrap profiles if requested;
3. run role smoke;
4. update `.env` only with `--write`;
5. write `artifacts/hermes_agent_decisions/rollout_<timestamp>.json`;
6. print exact rollback command.

**Run:**

```bash
cd /home/admin/factor-lab
python3 scripts/enable_hermes_native_agents.py --dry-run
python3 scripts/enable_hermes_native_agents.py --write --bootstrap-profiles --smoke
```

**Rollback:**

```bash
cd /home/admin/factor-lab
python3 scripts/enable_hermes_native_agents.py --rollback-real-llm --write
```

Rollback should restore:

```env
FACTOR_LAB_DECISION_PROVIDER=real_llm
FACTOR_LAB_LIVE_DECISION_PROVIDER=real_llm
FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=real_llm
FACTOR_LAB_HERMES_AGENT_MODE=off
FACTOR_LAB_HERMES_REQUIRE_AGENT=0
```

---

## Acceptance criteria

The migration is complete when all are true:

1. `hermes profile list` shows all four Factor Lab role profiles.
2. `scripts/audit_hermes_agent_migration.py` reports `ready_for_full_hermes=true`.
3. `scripts/smoke_hermes_role_agents.py --write-artifacts` succeeds for all four roles.
4. `DecisionProviderRouter(provider="hermes_agent")` works for all role decision types.
5. WebUI exposes Hermes Agent profile settings separately from LLM provider/model settings.
6. Runtime hooks and planner pipeline default to Hermes when env says `hermes_agent`.
7. Hermes decision artifacts are written for success and failure cases.
8. `FACTOR_LAB_HERMES_REQUIRE_AGENT=1` prevents silent fallback.
9. Controlled daemon dry-run still respects workflow admission and governance.
10. Old OpenClaw semantics are not part of the active role-agent architecture.

---

## Test and verification suite

Minimum targeted suite:

```bash
cd /home/admin/factor-lab
python3 -m py_compile \
  src/factor_lab/hermes_agent_client.py \
  src/factor_lab/hermes_briefing.py \
  src/factor_lab/hermes_shadow_compare.py \
  src/factor_lab/llm_provider_router.py \
  src/factor_lab/agent_runtime_hooks.py \
  src/factor_lab/research_planner_pipeline.py \
  scripts/audit_hermes_agent_migration.py \
  scripts/bootstrap_hermes_role_profiles.py \
  scripts/smoke_hermes_role_agents.py \
  scripts/enable_hermes_native_agents.py

pytest \
  tests/test_hermes_agent_client.py \
  tests/test_hermes_briefing.py \
  tests/test_hermes_shadow_compare.py \
  tests/test_llm_provider_router_hermes_agent.py \
  tests/test_audit_hermes_agent_migration.py \
  tests/test_bootstrap_hermes_role_profiles.py \
  tests/test_smoke_hermes_role_agents.py \
  tests/test_enable_hermes_native_agents.py \
  tests/test_webui_agent_settings.py \
  tests/test_agent_runtime_hooks.py \
  tests/test_research_planner_pipeline_hermes_agent.py \
  -q
```

Before any daemon restart:

```bash
cd /home/admin/factor-lab
python3 scripts/dry_run_controlled_restart.py
python3 scripts/audit_runtime_takeover.py
systemctl --user is-active factor-lab-research-daemon.service || true
```

Full confidence suite:

```bash
cd /home/admin/factor-lab
pytest tests -q
```

---

## Failure policy

Because this is intentionally aggressive, failure handling should optimize for diagnosis over hiding problems.

Default in full mode:

- Missing Hermes CLI: fail.
- Missing role profile: fail.
- Hermes non-zero exit: fail with artifact.
- JSON parse error: fail with artifact.
- Schema mismatch: fail with artifact.
- Timeout: fail with artifact.
- Wrong `request_id`: fail with artifact.

Fallback only when:

```env
FACTOR_LAB_HERMES_REQUIRE_AGENT=0
```

This is important. If the system silently falls back to `real_llm`, it will look migrated while still using the old architecture.

---

## Rollout stance

Recommended rollout after implementation:

1. Bootstrap profiles.
2. Run all tests.
3. Run four-role smoke.
4. Run `shadow` for one short check only.
5. Switch directly to `full + require_agent=1`.
6. Let failures produce artifacts.
7. Fix role prompts/tool permissions/schema handling from the artifacts.

This matches the user’s requested posture: move fully to Hermes Agent, accept breakage, then repair from observed failures.

---

## Notes for implementers

- Do not confuse `happyClaw.Pro`, `ai-continue`, or `nowcoding` with role agents. Those are provider/profile/model execution choices, not Factor Lab role identities.
- Do not treat `AgentRoleConfig` as obsolete. It remains the contract registry, but not the executor.
- Do not let Hermes write directly to the research queue without Factor Lab governance.
- Do not dump raw run history into Hermes memory. Use artifacts and Factor Lab knowledge files.
- Prefer explicit artifacts over logs-only debugging.
- Use request IDs everywhere.
- If profile/session behavior in Hermes CLI differs from assumptions, patch `HermesAgentClient` and document the actual supported command shape.
