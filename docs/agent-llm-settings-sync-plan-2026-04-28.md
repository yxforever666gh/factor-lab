# Agent / LLM Settings Synchronization Fix Plan

> **For Hermes:** Use test-driven-development and systematic-debugging. Implement this plan task-by-task and verify with focused tests plus live WebUI checks.

**Goal:** Fix the bug where `/settings` 大模型 fallback/profile changes do not stay synchronized with `/agents` Agent Role fallback settings, and close similar stale-profile-name cases.

**Architecture:** Treat LLM profiles as the source of truth for available provider names and the global provider fallback order. Agent roles may keep role-specific ordering, but their `llm_fallback_order` must never silently point at deleted/renamed/disabled profiles. On LLM settings save, reconcile existing Agent Role fallback lists against the old and new LLM profile order; on Agent settings load/render, expose available profile names and stale-order warnings so operators can see mismatches before saving.

**Tech Stack:** FastAPI + Jinja2 WebUI, env-file backed settings, `factor_lab.agent_roles.AgentRoleConfig`, pytest/TestClient.

---

## Root-cause findings

1. `/settings` save writes:
   - `FACTOR_LAB_LLM_PROFILES_JSON`
   - `FACTOR_LAB_LLM_FALLBACK_ORDER`
   - legacy primary model keys

2. `/agents` save writes a separate role JSON:
   - `FACTOR_LAB_AGENT_ROLES_JSON`
   - `FACTOR_LAB_AGENT_ROLE_ORDER`

3. Runtime uses role-specific fallback first:
   - `DecisionProviderRouter._call_real_llm()` calls `_real_llm_profiles(agent_role.llm_fallback_order)`.
   - Therefore if `FACTOR_LAB_AGENT_ROLES_JSON` has stale role fallback names, the new global LLM fallback order from `/settings` is not necessarily used.

4. Similar stale cases found:
   - LLM profile rename/delete can leave Agent roles pointing to missing profile names.
   - Disabling a model profile can leave Agent roles pointing to a disabled profile name.
   - Agent settings page currently provides a free-text fallback field without showing the available profile names or stale-name warnings.

## Invariants after the fix

1. LLM profiles remain connection/provider settings; Agent roles remain responsibility settings.
2. Global LLM fallback order is the default provider order.
3. Agent roles may still have role-specific order, but every name in `role.llm_fallback_order` must be an available enabled LLM profile name after LLM settings save.
4. If a role's order was previously empty, equal to the old global fallback, or becomes invalid after filtering stale names, it should adopt the new global fallback order.
5. If a role has a valid custom subset/order, preserve it but remove stale/disabled names.
6. Secrets must not be echoed in HTML or logs.

---

## Task 1: Add failing regression tests for LLM-save → Agent-role sync

**Objective:** Prove that changing the global model fallback order through `/settings` updates existing Agent roles that were using the old global order.

**Files:**
- Modify: `tests/test_webui_llm_settings.py`

**Test cases:**

1. `test_save_llm_settings_syncs_agent_role_fallback_when_roles_used_old_global_order`
   - Env starts with profiles `primary,backup` and agent roles whose `llm_fallback_order` is `primary,backup`.
   - Save LLM settings with numeric order making fallback `backup,primary`.
   - Assert `FACTOR_LAB_AGENT_ROLES_JSON` in env/file now has role fallback `backup,primary`.

2. `test_save_llm_settings_filters_stale_agent_role_profile_names_but_preserves_custom_order`
   - Env starts with profiles `primary,backup,third` and agent role fallback `third,backup`.
   - Save LLM settings with only `primary,backup` enabled.
   - Assert role fallback becomes `backup` rather than `third,backup`, preserving the valid custom subset/order.

**RED command:**

```bash
python -m pytest tests/test_webui_llm_settings.py::test_save_llm_settings_syncs_agent_role_fallback_when_roles_used_old_global_order tests/test_webui_llm_settings.py::test_save_llm_settings_filters_stale_agent_role_profile_names_but_preserves_custom_order -q
```

**Expected RED:** fail because `save_llm_settings()` currently does not update `FACTOR_LAB_AGENT_ROLES_JSON`.

---

## Task 2: Implement role fallback reconciliation helper

**Objective:** Add a pure helper that computes corrected role fallback orders from old global order, new global order, and enabled profile names.

**Files:**
- Modify: `src/factor_lab/webui_app.py`

**Implementation shape:**

Add helpers near the existing agent-role helpers:

```python
def _split_csv(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _enabled_profile_names(profiles: list[dict[str, Any]]) -> list[str]:
    return [str(p.get("name") or "").strip() for p in profiles if str(p.get("name") or "").strip() and bool(p.get("enabled", True))]


def _reconcile_role_fallback_order(role_order, old_global_order, new_global_order, enabled_names):
    # if old/empty/default/invalid -> new global; if custom -> filter stale names
```

Then add:

```python
def _sync_agent_roles_with_llm_profiles(existing_values, profiles, old_fallback_order, new_fallback_order) -> dict[str, str]:
    ...
    return {
        "FACTOR_LAB_AGENT_ROLES_JSON": agent_roles_to_json(updated_roles),
        "FACTOR_LAB_AGENT_ROLE_ORDER": existing order or joined role names,
    }
```

Rules:
- Normalize CSV/list inputs.
- Available names are enabled profiles only.
- New global fallback is filtered to available names.
- Role order equal to old global, empty, or fully invalid becomes new global.
- Role-specific custom order keeps valid names in the role's order and drops stale names.
- If there are no enabled profile names, do not destroy role fallback order; leave roles unchanged.

**GREEN command:**

```bash
python -m pytest tests/test_webui_llm_settings.py::test_save_llm_settings_syncs_agent_role_fallback_when_roles_used_old_global_order tests/test_webui_llm_settings.py::test_save_llm_settings_filters_stale_agent_role_profile_names_but_preserves_custom_order -q
```

---

## Task 3: Wire reconciliation into LLM settings save

**Objective:** When `/settings` saves profiles/fallback, update Agent Role env keys in the same env-file write.

**Files:**
- Modify: `src/factor_lab/webui_app.py`

**Implementation notes:**
- Capture `old_fallback_order` from `existing_values`/`os.environ` before overwriting `FACTOR_LAB_LLM_FALLBACK_ORDER`.
- After profiles/fallback are derived, call `_sync_agent_roles_with_llm_profiles(...)`.
- Merge returned agent env keys into `requested`.
- Include `AGENT_ROLE_ENV_KEYS` in `managed_keys` only when sync returns values, so existing role JSON lines can be updated in place and missing lines appended if roles already exist in env/process.
- Update `os.environ` for synced role env keys together with LLM env keys.

**Verification:** same tests as Task 2 plus:

```bash
python -m pytest tests/test_webui_llm_settings.py tests/test_webui_agent_settings.py -q
```

---

## Task 4: Add Agent page visibility for available profiles and stale fallback names

**Objective:** Prevent hidden mismatches by showing available model profiles and warning when Agent fallback references missing/disabled profiles.

**Files:**
- Modify: `tests/test_webui_agent_settings.py`
- Modify: `src/factor_lab/webui_app.py`
- Modify: `src/factor_lab/webui_templates/agents.html`

**Test case:**

`test_agents_page_warns_about_stale_fallback_profile_names`
- Env has LLM profiles `primary` only and Agent role fallback `missing,primary`.
- GET `/agents`.
- Assert page contains available profile `primary` and stale/missing profile name `missing` in a warning.

**Implementation:**
- Add `_agent_fallback_diagnostics(settings)` or equivalent helper.
- `agents_page()` passes:
  - `available_profile_names`
  - `agent_fallback_warnings`
- Template renders a small card above the form:
  - “可用大模型 Profiles: primary, backup”
  - if warnings exist, list role name + stale names.
- Add `<datalist id="llm-profile-names">` and attach it to fallback inputs where browser support helps, while retaining plain text input.

**Verification:**

```bash
python -m pytest tests/test_webui_agent_settings.py::test_agents_page_warns_about_stale_fallback_profile_names -q
```

---

## Task 5: Run focused verification and live WebUI smoke check

**Objective:** Verify behavior did not regress and deployed WebUI renders.

**Commands:**

```bash
python -m pytest tests/test_webui_llm_settings.py tests/test_webui_agent_settings.py tests/test_llm_provider_router.py -q
systemctl --user restart factor-lab-web-ui.service
sleep 8
curl -sS -o /tmp/factor-lab-agents.html -w '%{http_code} %{size_download}\n' http://127.0.0.1:8765/agents
curl -sS -o /tmp/factor-lab-settings.html -w '%{http_code} %{size_download}\n' http://127.0.0.1:8765/settings
```

**Acceptance:**
- Focused tests pass.
- `/agents` and `/settings` return HTTP 200.
- `/agents` page includes “可用大模型 Profiles”.
- No API keys are printed.

---

## Non-goals

- Do not remove role-specific fallback capability.
- Do not add budget/token hard limits.
- Do not expose secrets in HTML.
- Do not change provider endpoint behavior except via existing API format/profile settings.
