# Factor Lab LLM Token Usage Tracking Implementation Plan

> **For Hermes:** Use test-driven-development if executing this plan. This document is a plan only unless the user explicitly asks to execute it.

**Goal:** Add reliable LLM token/usage tracking for Factor Lab decision agents without adding tokenizer dependencies or changing compact-context behavior.

**Architecture:** Extract provider-reported usage from real LLM API responses, combine it with existing prompt compaction metadata, attach the result to each decision payload, and append an audit row to an append-only JSONL ledger. This provides both exact provider usage when available and local estimated usage when providers omit usage.

**Tech Stack:** Python stdlib only, existing `DecisionProviderRouter`, pytest.

---

## Scope

### In scope

1. Parse token usage from real LLM API responses:
   - OpenAI Chat Completions style: `prompt_tokens`, `completion_tokens`, `total_tokens`
   - OpenAI Responses style: `input_tokens`, `output_tokens`, `total_tokens`
   - Anthropic Messages style: `input_tokens`, `output_tokens`
2. Attach normalized usage to returned real LLM payload under:
   - `real_llm_usage`
3. Extend existing prompt metadata under:
   - `real_llm_prompt_meta`
4. Append one JSON object per real LLM attempt to:
   - `artifacts/llm_usage_ledger.jsonl`
5. Include failed attempts where an HTTP error response has parseable usage, if available.
6. Add tests for usage normalization and ledger writing.
7. No new dependencies.
8. No hard token budget or prompt truncation.

### Out of scope

1. Installing `tiktoken` or `tokenizers`.
2. Provider billing API integration.
3. WebUI charts.
4. Changing model/provider fallback behavior.
5. Changing compact-context rules.
6. Adding max input guards.

---

## Existing Context

Current compact implementation in:

```text
src/factor_lab/llm_provider_router.py
```

already records prompt metadata like:

```json
{
  "context_mode": "compact",
  "raw_context_chars": 467500,
  "prompt_context_chars": 7802,
  "estimated_raw_tokens_4c": 116875,
  "estimated_prompt_tokens_4c": 1950,
  "reduction_ratio": 0.9833
}
```

But this only estimates context tokens. It does not capture real provider usage, output tokens, retries, fallback attempts, or daily totals.

---

## Data Model

### `real_llm_usage` payload field

Each successful real LLM response should include:

```json
"real_llm_usage": {
  "api_format": "openai|openai_responses|anthropic",
  "prompt_tokens": 123,
  "completion_tokens": 45,
  "total_tokens": 168,
  "usage_source": "provider|missing|partial",
  "raw_usage": {
    "prompt_tokens": 123,
    "completion_tokens": 45,
    "total_tokens": 168
  }
}
```

Normalization rules:

| Provider field | Normalized field |
|---|---|
| `prompt_tokens` | `prompt_tokens` |
| `input_tokens` | `prompt_tokens` |
| `completion_tokens` | `completion_tokens` |
| `output_tokens` | `completion_tokens` |
| `total_tokens` | `total_tokens` |

If `total_tokens` is missing but prompt/completion are present:

```python
total_tokens = prompt_tokens + completion_tokens
```

If no usage exists:

```json
{
  "api_format": "openai",
  "prompt_tokens": null,
  "completion_tokens": null,
  "total_tokens": null,
  "usage_source": "missing",
  "raw_usage": {}
}
```

### `real_llm_prompt_meta` extension

Add final prompt-level fields:

```json
"user_prompt_chars": 8750,
"estimated_user_prompt_tokens_4c": 2187
```

These are different from `prompt_context_chars` because final user prompt also includes:

- `decision_type`
- `context_mode`
- `required_output_schema`
- `context_compaction`

### Ledger file

Append-only path:

```text
artifacts/llm_usage_ledger.jsonl
```

One line per real LLM HTTP attempt.

Success row example:

```json
{
  "created_at_utc": "2026-04-28T00:00:00+00:00",
  "success": true,
  "decision_type": "planner",
  "provider": "real_llm",
  "profile_name": "ai-continue",
  "model": "gpt-5.5",
  "base_url": "https://rayplus.site",
  "api_format": "openai",
  "context_mode": "compact",
  "raw_context_chars": 467500,
  "prompt_context_chars": 7802,
  "user_prompt_chars": 8750,
  "estimated_user_prompt_tokens_4c": 2187,
  "usage": {
    "prompt_tokens": 2301,
    "completion_tokens": 902,
    "total_tokens": 3203,
    "usage_source": "provider"
  },
  "latency_ms": 19718,
  "error_type": null,
  "error_message": null
}
```

Failure row example:

```json
{
  "created_at_utc": "2026-04-28T00:00:00+00:00",
  "success": false,
  "decision_type": "planner",
  "provider": "real_llm",
  "profile_name": "nowcoding",
  "model": "gpt-5.4",
  "base_url": "https://nowcoding.ai/v1",
  "api_format": "openai",
  "context_mode": "compact",
  "raw_context_chars": 467500,
  "prompt_context_chars": 7802,
  "user_prompt_chars": 8750,
  "estimated_user_prompt_tokens_4c": 2187,
  "usage": {
    "prompt_tokens": null,
    "completion_tokens": null,
    "total_tokens": null,
    "usage_source": "missing"
  },
  "latency_ms": 531,
  "error_type": "http_error:403",
  "error_message": "insufficient_user_quota"
}
```

---

## Implementation Tasks

## Task 1: Add usage extraction unit tests

**Objective:** Define exact usage normalization behavior before implementation.

**Files:**

- Modify: `tests/test_llm_provider_router.py`

**Tests to add:**

```python
def test_extract_llm_usage_normalizes_openai_chat_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
        "openai",
    )
    assert usage["prompt_tokens"] == 10
    assert usage["completion_tokens"] == 5
    assert usage["total_tokens"] == 15
    assert usage["usage_source"] == "provider"
```

```python
def test_extract_llm_usage_normalizes_responses_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 20, "output_tokens": 7, "total_tokens": 27}},
        "openai_responses",
    )
    assert usage["prompt_tokens"] == 20
    assert usage["completion_tokens"] == 7
    assert usage["total_tokens"] == 27
```

```python
def test_extract_llm_usage_normalizes_anthropic_usage_without_total():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 30, "output_tokens": 8}},
        "anthropic",
    )
    assert usage["prompt_tokens"] == 30
    assert usage["completion_tokens"] == 8
    assert usage["total_tokens"] == 38
```

```python
def test_extract_llm_usage_handles_missing_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage({}, "openai")
    assert usage["prompt_tokens"] is None
    assert usage["completion_tokens"] is None
    assert usage["total_tokens"] is None
    assert usage["usage_source"] == "missing"
```

**Run RED:**

```bash
python -m pytest tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_openai_chat_usage tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_responses_usage tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_anthropic_usage_without_total tests/test_llm_provider_router.py::test_extract_llm_usage_handles_missing_usage -q
```

Expected: FAIL because `_extract_llm_usage()` does not exist yet.

---

## Task 2: Implement `_extract_llm_usage()`

**Objective:** Normalize provider usage formats into one stable schema.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`

**Implementation:**

```python
def _coerce_int_or_none(self, value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_llm_usage(self, raw: dict[str, Any], api_format: str) -> dict[str, Any]:
    usage = raw.get("usage") or {}
    if not isinstance(usage, dict):
        usage = {}

    prompt_tokens = self._coerce_int_or_none(usage.get("prompt_tokens") or usage.get("input_tokens"))
    completion_tokens = self._coerce_int_or_none(usage.get("completion_tokens") or usage.get("output_tokens"))
    total_tokens = self._coerce_int_or_none(usage.get("total_tokens"))

    if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
        total_tokens = prompt_tokens + completion_tokens

    if prompt_tokens is None and completion_tokens is None and total_tokens is None:
        source = "missing"
    elif prompt_tokens is None or completion_tokens is None or total_tokens is None:
        source = "partial"
    else:
        source = "provider"

    return {
        "api_format": api_format,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "usage_source": source,
        "raw_usage": usage,
    }
```

**Run GREEN:**

```bash
python -m pytest tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_openai_chat_usage tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_responses_usage tests/test_llm_provider_router.py::test_extract_llm_usage_normalizes_anthropic_usage_without_total tests/test_llm_provider_router.py::test_extract_llm_usage_handles_missing_usage -q
```

Expected: PASS.

---

## Task 3: Add prompt meta final prompt char fields

**Objective:** Track actual final user prompt size, not only compact context size.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`
- Modify: `tests/test_llm_provider_router.py`

**Implementation detail:**

In `_call_real_llm_profile()` after building `user_prompt`:

```python
prompt_meta = dict(prompt_meta)
prompt_meta.update({
    "user_prompt_chars": len(user_prompt),
    "estimated_user_prompt_tokens_4c": len(user_prompt) // 4,
})
```

**Test:** Extend existing compact tests to assert:

```python
assert payload["real_llm_prompt_meta"]["user_prompt_chars"] > 0
assert payload["real_llm_prompt_meta"]["estimated_user_prompt_tokens_4c"] > 0
```

---

## Task 4: Attach usage to successful real LLM payloads

**Objective:** Make successful agent responses self-contained with usage data.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`
- Modify: `tests/test_llm_provider_router.py`

**Implementation:**

After `raw` is loaded and before returning payload:

```python
usage = self._extract_llm_usage(raw, api_format)
...
payload.setdefault("real_llm_usage", usage)
```

Because `_call_real_llm_profile()` has three API branches, ensure `raw` remains available after all branches. It already does.

**Tests:**

Update existing fake responses:

- OpenAI chat test returns:

```json
"usage": {"prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15}
```

Assert:

```python
assert payload["real_llm_usage"]["total_tokens"] == 15
```

- Anthropic test returns:

```json
"usage": {"input_tokens": 21, "output_tokens": 6}
```

Assert:

```python
assert payload["real_llm_usage"]["total_tokens"] == 27
```

- OpenAI Responses test returns:

```json
"usage": {"input_tokens": 31, "output_tokens": 9, "total_tokens": 40}
```

Assert:

```python
assert payload["real_llm_usage"]["prompt_tokens"] == 31
assert payload["real_llm_usage"]["completion_tokens"] == 9
assert payload["real_llm_usage"]["total_tokens"] == 40
```

---

## Task 5: Add ledger writer tests

**Objective:** Define append-only usage ledger behavior.

**Files:**

- Modify: `tests/test_llm_provider_router.py`

**Test requirements:**

Use `monkeypatch` to redirect artifacts path if project has path helpers suitable for this. If difficult, monkeypatch router's ledger path method after adding it in Task 6.

Test shape:

```python
def test_append_llm_usage_ledger_writes_jsonl(monkeypatch, tmp_path):
    router = DecisionProviderRouter(provider="real_llm")
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)

    router._append_llm_usage_ledger({...})
    router._append_llm_usage_ledger({...})

    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 2
    assert rows[0]["decision_type"] == "planner"
```

Run RED first. Expected: FAIL because ledger methods do not exist.

---

## Task 6: Implement ledger writer

**Objective:** Append usage rows safely without introducing external dependencies.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`

**Implementation:**

```python
def _llm_usage_ledger_path(self) -> Path:
    return artifacts_dir() / "llm_usage_ledger.jsonl"


def _append_llm_usage_ledger(self, row: dict[str, Any]) -> None:
    path = self._llm_usage_ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
```

Do not fail the LLM call if ledger writing fails. Use a guarded helper:

```python
def _try_append_llm_usage_ledger(self, row: dict[str, Any]) -> None:
    try:
        self._append_llm_usage_ledger(row)
    except Exception:
        return
```

---

## Task 7: Build usage ledger row helper

**Objective:** Centralize row shape so success/failure logging remains consistent.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`

**Implementation:**

```python
def _build_llm_usage_ledger_row(
    self,
    *,
    success: bool,
    decision_type: str,
    profile: dict[str, Any],
    api_format: str,
    prompt_meta: dict[str, Any],
    usage: dict[str, Any],
    latency_ms: int,
    error_type: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "decision_type": decision_type,
        "provider": "real_llm",
        "profile_name": profile.get("name") or "default",
        "model": profile.get("model") or self.model,
        "base_url": profile.get("base_url"),
        "api_format": api_format,
        "context_mode": prompt_meta.get("context_mode"),
        "raw_context_chars": prompt_meta.get("raw_context_chars"),
        "prompt_context_chars": prompt_meta.get("prompt_context_chars"),
        "user_prompt_chars": prompt_meta.get("user_prompt_chars"),
        "estimated_user_prompt_tokens_4c": prompt_meta.get("estimated_user_prompt_tokens_4c"),
        "usage": {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "usage_source": usage.get("usage_source"),
        },
        "latency_ms": latency_ms,
        "error_type": error_type,
        "error_message": error_message,
    }
```

---

## Task 8: Write ledger rows on successful attempts

**Objective:** Ensure every successful real LLM call creates one usage ledger entry.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`
- Modify: `tests/test_llm_provider_router.py`

**Implementation approach:**

In `_call_real_llm_profile()`:

1. Record start time near the beginning:

```python
started = time.perf_counter()
```

2. After raw response is parsed and usage extracted:

```python
latency_ms = int((time.perf_counter() - started) * 1000)
usage = self._extract_llm_usage(raw, api_format)
self._try_append_llm_usage_ledger(
    self._build_llm_usage_ledger_row(
        success=True,
        decision_type=decision_type,
        profile=profile,
        api_format=api_format,
        prompt_meta=prompt_meta,
        usage=usage,
        latency_ms=latency_ms,
    )
)
```

3. Attach to payload:

```python
payload.setdefault("real_llm_usage", usage)
```

**Test:** Use fake `urlopen`, monkeypatch ledger path, call `_call_real_llm_profile()`, read ledger and assert one row.

---

## Task 9: Write ledger rows on HTTP failures when possible

**Objective:** Capture failed provider attempts, especially quota/rate-limit failures in fallback chains.

**Files:**

- Modify: `src/factor_lab/llm_provider_router.py`
- Modify: `tests/test_llm_provider_router.py`

**Implementation detail:**

HTTP error handling currently raises:

```python
raise RuntimeError(f"http_error:{exc.code}:{body}") from exc
```

Enhance each HTTPError branch to:

1. read body
2. try `json.loads(body)`
3. extract usage if body has usage
4. append ledger row with:
   - `success=False`
   - `error_type=f"http_error:{exc.code}"`
   - `error_message=body[:1000]`

Use missing usage if body has no usage.

Keep the raised error message unchanged enough that existing tests still pass.

---

## Task 10: Add simple aggregation script or helper test target

**Objective:** Make token totals easy to inspect from CLI without WebUI work.

**Files:**

- Create: `scripts/summarize_llm_usage.py`
- Test optionally: `tests/test_summarize_llm_usage.py`

**Script behavior:**

```bash
python scripts/summarize_llm_usage.py --ledger artifacts/llm_usage_ledger.jsonl --days 1
```

Output:

```text
rows=12 success=10 failed=2
prompt_tokens=12345 completion_tokens=2345 total_tokens=14690
estimated_user_prompt_tokens_4c=15000
by_decision_type:
  planner total_tokens=8000 rows=5
  failure_analyst total_tokens=6690 rows=5
by_model:
  gpt-5.5 total_tokens=14690 rows=10
```

Keep this script stdlib-only.

This task can be deferred if the first implementation should stay minimal.

---

## Verification Commands

### Targeted tests

```bash
python -m pytest tests/test_llm_provider_router.py -q
```

Expected: all pass.

### Related full test subset

```bash
python -m pytest tests/test_llm_provider_router.py tests/test_agent_briefs.py tests/test_decision_ab_judge.py -q
```

Expected: all pass or only unrelated pre-existing failures.

### Manual ledger smoke test

Run a fake or real decision call, then:

```bash
tail -n 5 artifacts/llm_usage_ledger.jsonl
```

Expected: JSONL rows with `usage` and prompt metadata.

### Current artifact usage estimate smoke

```bash
PYTHONPATH=src python - <<'PY'
from pathlib import Path
import json
from factor_lab.llm_provider_router import DecisionProviderRouter
root = Path('/home/admin/factor-lab')
router = DecisionProviderRouter(provider='real_llm')
for rel, dtype in [
    ('artifacts/planner_decision_context.json', 'planner'),
    ('artifacts/failure_decision_context.json', 'failure_analyst'),
]:
    ctx = json.loads((root / rel).read_text())
    payload, meta = router._real_llm_prompt_payload(dtype, ctx)
    prompt = json.dumps(payload, ensure_ascii=False)
    print(rel, len(prompt), len(prompt)//4, meta)
PY
```

Expected: compact prompt sizes remain small; token tracking does not change compact behavior.

---

## Acceptance Criteria

- [ ] `real_llm_usage` appears on successful real LLM payloads.
- [ ] OpenAI Chat usage normalizes correctly.
- [ ] OpenAI Responses usage normalizes correctly.
- [ ] Anthropic usage normalizes correctly.
- [ ] Missing usage is represented with null token fields and `usage_source="missing"`.
- [ ] `real_llm_prompt_meta` includes final `user_prompt_chars` and `estimated_user_prompt_tokens_4c`.
- [ ] `artifacts/llm_usage_ledger.jsonl` is append-only and receives one row per real LLM HTTP attempt.
- [ ] HTTP failures append failure rows when reachable.
- [ ] No tokenizer dependencies added.
- [ ] No budget protection added.
- [ ] Existing compact-context tests continue passing.

---

## Operational Notes

1. Provider usage is authoritative when present.
2. Some proxy providers may omit `usage`; for those calls, use local estimates from `real_llm_prompt_meta` and ledger estimate fields.
3. Fallback attempts should be counted separately because each successful HTTP model attempt can consume tokens.
4. 403 quota errors often do not consume model tokens, but recording them helps explain latency and fallback behavior.
5. The ledger may grow over time; rotation or aggregation can be planned later.
