# Factor Lab direct_model Compact Context Implementation Plan

> **For Hermes:** Use test-driven-development for implementation. The user explicitly requested a plan first and then execution. Do not add budget protection or hard max-input guards.

**Goal:** Make Factor Lab's `direct_model` decision-agent path use compact context by default, reducing planner/failure analyst input size from roughly 112k-149k tokens per cycle to roughly 8k-11k tokens per cycle.

**Architecture:** Reuse the existing compacting logic already used by the HermesNative Gateway path: `_compact_context_for_prompt()`. Add a small real-LLM prompt payload builder that selects compact by default, allows raw context only via explicit environment variable, and records compaction metadata in model responses. No truncation, no max-character budget, and no prompt-size failure logic.

**Tech Stack:** Python, pytest, existing `HermesDecisionRouter` in `src/factor_lab/hermes_decision_router.py`.

---

## Requirements

1. `direct_model` must default to compact context.
2. `direct_model` must support raw context only when explicitly configured:
   - `FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE=raw`
   - or `FACTOR_LAB_LLM_CONTEXT_MODE=raw`
3. No input budget protection:
   - do not add `FACTOR_LAB_LLM_MAX_INPUT_CHARS`
   - do not truncate JSON strings
   - do not raise because prompt is large
4. Record prompt compaction metadata in returned payload under `direct_model_prompt_meta`.
5. Apply the same compact payload across all three real LLM API formats:
   - Anthropic messages
   - OpenAI Responses
   - OpenAI Chat Completions
6. Add regression tests so future changes cannot silently revert `direct_model` back to full raw context.

---

## Current Problem

In `src/factor_lab/hermes_decision_router.py`, `_call_direct_model_profile()` currently builds user prompt with the full context:

```python
user_prompt = json.dumps(
    {
        "decision_type": decision_type,
        "context": context,
        "required_output_schema": self._decision_schema_hint(decision_type),
    },
    ensure_ascii=False,
)
```

Observed current scale:

```text
planner direct_model input:          ~56k-75k tokens
failure analyst direct_model input:  ~55k-74k tokens
combined decision cycle:         ~112k-149k tokens
```

The existing HermesNative path already compacts context via:

```python
self._compact_context_for_prompt(decision_type, context)
```

Observed compact scale:

```text
planner compact input:          ~2k-3k tokens
failure analyst compact input:  ~6k-8k tokens
combined compact cycle:         ~8k-11k tokens
```

---

## Task 1: Add failing tests for compact default and raw opt-in

**Objective:** Prove `direct_model` currently sends raw context and therefore fails the new expected compact behavior.

**Files:**
- Modify: `tests/test_hermes_decision_router.py`

**Step 1: Add helper context inside tests**

Use a large planner context that includes many `candidate_pool_tasks` so compacting must introduce omitted counts and significantly shrink the prompt.

**Step 2: Add test: default direct_model prompt uses compact context**

Expected behavior:

- captured user prompt contains `"context_mode": "compact"`
- captured user prompt contains `context_compaction`
- captured user prompt is much smaller than the raw equivalent
- response payload contains `direct_model_prompt_meta.context_mode == "compact"`

**Step 3: Add test: raw context is opt-in**

Set:

```python
monkeypatch.setenv("FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE", "raw")
```

Expected behavior:

- captured user prompt contains `"context_mode": "raw"`
- raw-only marker from the source context is present
- response payload contains `direct_model_prompt_meta.context_mode == "raw"`

**Step 4: Run tests and verify RED**

Run:

```bash
python -m pytest tests/test_hermes_decision_router.py::test_direct_model_uses_compact_context_by_default tests/test_hermes_decision_router.py::test_direct_model_raw_context_mode_is_opt_in -q
```

Expected: FAIL because `_call_direct_model_profile()` does not yet emit `context_mode`, `context_compaction`, or `direct_model_prompt_meta`.

---

## Task 2: Add real LLM context mode and prompt payload builder

**Objective:** Add the small reusable logic needed by `_call_direct_model_profile()`.

**Files:**
- Modify: `src/factor_lab/hermes_decision_router.py`

**Step 1: Add `_direct_model_context_mode()`**

```python
def _direct_model_context_mode(self) -> str:
    raw = (
        os.environ.get("FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE")
        or os.environ.get("FACTOR_LAB_LLM_CONTEXT_MODE")
        or "compact"
    ).strip().lower()
    if raw in {"raw", "full", "full_context"}:
        return "raw"
    return "compact"
```

**Step 2: Add `_direct_model_prompt_payload()`**

```python
def _direct_model_prompt_payload(
    self,
    decision_type: str,
    context: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    mode = self._direct_model_context_mode()
    raw_context_chars = len(json.dumps(context, ensure_ascii=False))

    if mode == "raw":
        prompt_context = context
    else:
        prompt_context = self._compact_context_for_prompt(decision_type, context)

    prompt_context_chars = len(json.dumps(prompt_context, ensure_ascii=False))
    prompt_meta = {
        "context_mode": mode,
        "raw_context_chars": raw_context_chars,
        "prompt_context_chars": prompt_context_chars,
        "estimated_raw_tokens_4c": raw_context_chars // 4,
        "estimated_prompt_tokens_4c": prompt_context_chars // 4,
        "reduction_ratio": round(
            1.0 - (prompt_context_chars / raw_context_chars),
            4,
        ) if raw_context_chars else 0.0,
    }

    return (
        {
            "decision_type": decision_type,
            "context_mode": mode,
            "context": prompt_context,
            "required_output_schema": self._decision_schema_hint(decision_type),
            "context_compaction": prompt_meta,
        },
        prompt_meta,
    )
```

---

## Task 3: Route direct_model through compact payload

**Objective:** Replace full-context prompt construction with compact prompt construction.

**Files:**
- Modify: `src/factor_lab/hermes_decision_router.py`

**Step 1: Replace current `user_prompt` builder**

Change `_call_direct_model_profile()` from full context JSON to:

```python
prompt_payload, prompt_meta = self._direct_model_prompt_payload(decision_type, context)
user_prompt = json.dumps(prompt_payload, ensure_ascii=False)
```

**Step 2: Attach metadata to response**

After successful parse:

```python
payload.setdefault("direct_model_prompt_meta", prompt_meta)
```

Place this near existing defaults:

```python
payload.setdefault("schema_version", self._decision_schema_version(decision_type))
payload.setdefault("agent_name", f"{decision_type}-real-llm")
payload.setdefault("decision_source", "direct_model")
payload.setdefault("decision_context_id", context.get("context_id"))
payload.setdefault("direct_model_prompt_meta", prompt_meta)
```

---

## Task 4: Verify tests pass

**Objective:** Confirm compact mode works and raw remains opt-in.

**Commands:**

```bash
python -m pytest tests/test_hermes_decision_router.py::test_direct_model_uses_compact_context_by_default tests/test_hermes_decision_router.py::test_direct_model_raw_context_mode_is_opt_in -q
```

Expected: PASS.

Then run the full router test file:

```bash
python -m pytest tests/test_hermes_decision_router.py -q
```

Expected: PASS.

---

## Task 5: Measure before/after prompt size using current artifacts

**Objective:** Verify actual current artifact contexts now compact for direct_model.

**Command:**

```bash
PYTHONPATH=src python - <<'PY'
from pathlib import Path
import json
from factor_lab.hermes_decision_router import HermesDecisionRouter

root = Path('/home/admin/factor-lab')
router = HermesDecisionRouter(provider='direct_model')
for rel, dtype in [
    ('artifacts/planner_decision_context.json', 'planner'),
    ('artifacts/failure_decision_context.json', 'diagnostician'),
]:
    ctx = json.loads((root / rel).read_text())
    payload, meta = router._direct_model_prompt_payload(dtype, ctx)
    prompt = json.dumps(payload, ensure_ascii=False)
    print(rel)
    print(meta)
    print('prompt_chars', len(prompt), 'est_tokens_4c', len(prompt)//4)
PY
```

Expected approximate result:

```text
planner: prompt around 8k chars / ~2k tokens
failure: prompt around 25k chars / ~6k tokens
```

---

## Task 6: Final report

**Objective:** Report exact changed files, test commands, and measured token reduction.

Include:

- plan file path
- modified source path
- modified test path
- test results
- measured prompt sizes
- note: no budget protection was added per user instruction

---

## Acceptance Criteria

- [ ] Plan saved to `docs/real-llm-compact-context-plan-2026-04-28.md`
- [ ] New tests fail before implementation
- [ ] `direct_model` defaults to compact context
- [ ] `FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE=raw` preserves raw mode
- [ ] Returned payload includes `direct_model_prompt_meta`
- [ ] No budget protection added
- [ ] Router tests pass
- [ ] Current artifact measurement confirms large reduction
