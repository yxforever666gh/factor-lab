# WebUI Data Source Settings Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 在 Factor Lab WebUI 增加“数据源设置”界面，支持配置 Tushare 等多个数据源的 Key/参数、启用/禁用、拖动排序，并安全同步到运行时环境变量。

**Architecture:** 采用现有 `src/factor_lab/webui_app.py` 的 env-backed settings 模式：新增数据源 profile 结构化 JSON env，按顺序同步 legacy 单数据源变量（如 `TUSHARE_TOKEN`），同时保留无密钥回显、空密钥保留、`.env` 注释/无关变量保留、当前进程 env 更新。WebUI 增加独立 `/data-sources` 页面，导航中与“大模型设置/Hermes 设置”并列，前端用 HTML5 drag-and-drop 调整行顺序并写回隐藏 order 字段。

**Tech Stack:** FastAPI + Jinja2 模板；`.env`/`os.environ` 配置；pytest + FastAPI TestClient；少量原生 JavaScript（不引入新前端依赖）。

---

## Scope / Acceptance Criteria

### 必须实现

1. WebUI 新增 `/data-sources` 页面，侧边栏显示“数据源设置”。
2. 页面支持至少 5 行数据源 profile slot，可新增多个数据源。
3. 每行字段：顺序、拖动手柄、启用、数据源类型、显示名称、Token/API Key、当前 Key 状态、说明/可选参数。
4. 支持 Tushare token 输入，并同步 legacy `TUSHARE_TOKEN`。
5. 支持“多个数据源 + 顺序优先级”：保存后结构化写入 env，运行时可读取排序后的 enabled profiles。
6. 支持拖动行改变顺序；无 JS 时仍可通过数字 order 改顺序。
7. Secret 安全：HTML 不回显真实 key；password 输入留空；留空保存保留同名同类型 profile 的旧 key。
8. 保存 `.env` 时保留注释和无关变量，凭证文件权限尽量设置为 `0600`。
9. 保存后更新当前 WebUI 进程 `os.environ`；是否重启 daemon 要明确展示结果。
10. 测试覆盖 helper、保存逻辑、页面渲染、拖动所需字段、POST 行为。

### 暂不实现（避免 YAGNI）

1. 不做完整 secret manager/OAuth。
2. 不在保存时默认请求外部数据源；连接测试可作为独立按钮，且不保存未提交表单。
3. 不改 Factor Lab 的 provider 选择语义为自动多源混合；本期只做配置入口和 legacy 变量同步。
4. 不把数据源配置和 LLM provider/profile 混在同一个页面。

---

## Proposed Env Contract

新增 env keys：

```text
FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=[{"name":"primary-tushare","source_type":"tushare","api_key":"...","enabled":true,"notes":"","extra":{}}]
FACTOR_LAB_DATA_SOURCE_ORDER=primary-tushare,backup-diemeng
FACTOR_LAB_PRIMARY_DATA_SOURCE=tushare
```

Legacy sync：

```text
TUSHARE_TOKEN=<first enabled tushare profile api_key>
DIEMENG_API_KEY=<first enabled diemeng profile api_key, if present>
```

Profile shape：

```json
{
  "name": "primary-tushare",
  "source_type": "tushare",
  "api_key": "secret-token",
  "enabled": true,
  "notes": "A-share daily/basic cache source",
  "extra": {}
}
```

Loaded settings returned to templates must redact secrets：

```json
{
  "profiles": [
    {
      "name": "primary-tushare",
      "source_type": "tushare",
      "api_key": "",
      "api_key_configured": true,
      "api_key_masked": "abcd...wxyz",
      "enabled": true,
      "notes": "..."
    }
  ],
  "order": "primary-tushare",
  "env_file": "/home/admin/factor-lab/.env"
}
```

---

## Task 1: Add data-source settings helper tests

**Objective:** 先用测试锁定数据源 profile 的加载、排序、密钥遮蔽、保存和 legacy sync 行为。

**Files:**
- Modify: `tests/test_webui_llm_settings.py` 或新建 `tests/test_webui_data_source_settings.py`
- No production edits yet

**Step 1: Write failing tests**

Create focused tests:

- `test_load_data_source_settings_masks_keys_and_reads_env_file`
- `test_save_data_source_settings_preserves_unrelated_env_and_updates_runtime_env`
- `test_save_data_source_settings_keeps_existing_key_when_blank`
- `test_save_data_source_settings_uses_numeric_order_fields`
- `test_save_data_source_settings_syncs_first_enabled_tushare_to_legacy_token`
- `test_save_data_source_settings_ignores_fully_blank_rows`

Expected assertions:

```python
settings = webui_app.load_data_source_settings()
assert settings["profiles"][0]["api_key"] == ""
assert settings["profiles"][0]["api_key_masked"] == "tush...cret"

webui_app.save_data_source_settings({...})
assert "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=" in env_file.read_text()
assert webui_app.os.environ["TUSHARE_TOKEN"] == "new-token"
```

**Step 2: Run tests to verify failure**

Run:

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python -m pytest tests/test_webui_data_source_settings.py -q
```

Expected: FAIL because `load_data_source_settings` / `save_data_source_settings` do not exist.

---

## Task 2: Implement data-source settings helpers

**Objective:** 在 `webui_app.py` 中实现纯 helper，不涉及页面路由。

**Files:**
- Modify: `src/factor_lab/webui_app.py`
- Test: `tests/test_webui_data_source_settings.py`

**Implementation outline:**

Add constants near existing `LLM_ENV_KEYS` area:

```python
DATA_SOURCE_ENV_KEYS = [
    "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
    "FACTOR_LAB_DATA_SOURCE_ORDER",
    "FACTOR_LAB_PRIMARY_DATA_SOURCE",
    "TUSHARE_TOKEN",
    "DIEMENG_API_KEY",
]

DATA_SOURCE_TYPE_OPTIONS = [
    {"value": "tushare", "label": "Tushare"},
    {"value": "diemeng", "label": "Diemeng / 迪蒙"},
    {"value": "custom", "label": "Custom"},
]
```

Add helpers:

```python
def _load_data_source_profiles(values: dict[str, str]) -> tuple[list[dict[str, Any]], str]: ...
def _data_source_profiles_from_form(form: dict[str, str], existing_profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]: ...
def _first_enabled_source_profile(profiles: list[dict[str, Any]], source_type: str | None = None) -> dict[str, Any]: ...
def load_data_source_settings() -> dict[str, Any]: ...
def save_data_source_settings(form: dict[str, str]) -> dict[str, Any]: ...
```

Important behavior:

- Use env file values before stale `os.environ` when a target env file is explicitly monkeypatched in tests.
- Preserve existing key by stable identity `(source_type, name)` when submitted key field is blank.
- Sort by numeric order fields if present, else by `FACTOR_LAB_DATA_SOURCE_ORDER`.
- `FACTOR_LAB_DATA_SOURCE_ORDER` includes enabled profiles only.
- Sync `TUSHARE_TOKEN` from first enabled Tushare profile.
- Sync `DIEMENG_API_KEY` from first enabled Diemeng profile.
- Do not delete legacy tokens if no enabled profile of that type exists unless the user explicitly clears/deletes it; safer initial behavior is preserve existing legacy key.

**Step 3: Run helper tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_webui_data_source_settings.py -q
```

Expected: PASS.

---

## Task 3: Add `/data-sources` routes

**Objective:** 暴露 GET/POST routes，使用 `application/x-www-form-urlencoded` 手动解析，避免新增 `python-multipart` 依赖。

**Files:**
- Modify: `src/factor_lab/webui_app.py`
- Test: `tests/test_webui_data_source_settings.py`

**Implementation outline:**

```python
@app.get("/data-sources", response_class=HTMLResponse)
def data_sources_page(saved: str | None = None, restart: str | None = None):
    settings = load_data_source_settings()
    profile_slots = list(settings.get("profiles") or [])
    while len(profile_slots) < 5:
        profile_slots.append({"name": "", "source_type": "tushare", "api_key_masked": "未配置", "enabled": True, "notes": ""})
    return render(
        "data_sources.html",
        title="数据源设置",
        settings=settings,
        profile_slots=profile_slots,
        source_type_options=DATA_SOURCE_TYPE_OPTIONS,
        saved=saved == "1",
        restart_ok=restart == "1",
        restart_failed=restart == "0",
    )

@app.post("/data-sources")
async def data_sources_save(request: Request):
    body = (await request.body()).decode("utf-8")
    parsed = parse_qs(body, keep_blank_values=True)
    save_data_source_settings({key: values[-1] if values else "" for key, values in parsed.items()})
    restart_result = restart_research_daemon_after_settings_save()
    restart_flag = "1" if restart_result.get("ok") else "0"
    return RedirectResponse(url=f"/data-sources?saved=1&restart={restart_flag}", status_code=303)
```

**Tests:**

- GET `/data-sources` returns 200.
- POST `/data-sources` returns 303 and writes expected env keys.
- Password input value is empty in rendered HTML.

---

## Task 4: Create `data_sources.html` template with draggable rows

**Objective:** 新增视觉上与现有 settings 页面一致的数据源配置表，支持拖动排序和数字排序 fallback。

**Files:**
- Create: `src/factor_lab/webui_templates/data_sources.html`
- Modify: `src/factor_lab/webui_templates/base.html`
- Test: `tests/test_webui_data_source_settings.py`

**Template requirements:**

- Extends `base.html`.
- Saved banner text: “已保存数据源设置”。
- Shows current env file path.
- Table columns: 拖动、顺序、启用、类型、名称、Token/API Key、当前 Key、说明。
- Each row has `draggable="true"`, a drag handle, and hidden/number order field.
- Token input: `type="password"`, `value=""`, `autocomplete="new-password"`, placeholder `留空保留`.
- Current key column uses `api_key_masked` only.
- Include no external JS dependency.

Add small script at bottom:

```html
<script>
(function () {
  const tbody = document.querySelector('[data-source-profile-rows]');
  if (!tbody) return;
  let dragged = null;
  function renumber() {
    tbody.querySelectorAll('tr').forEach((row, index) => {
      const input = row.querySelector('input[data-order-input]');
      if (input) input.value = String(index + 1);
    });
  }
  tbody.querySelectorAll('tr').forEach((row) => {
    row.addEventListener('dragstart', () => { dragged = row; row.classList.add('dragging'); });
    row.addEventListener('dragend', () => { row.classList.remove('dragging'); dragged = null; renumber(); });
    row.addEventListener('dragover', (event) => {
      event.preventDefault();
      if (!dragged || dragged === row) return;
      const rect = row.getBoundingClientRect();
      const after = event.clientY > rect.top + rect.height / 2;
      tbody.insertBefore(dragged, after ? row.nextSibling : row);
    });
  });
})();
</script>
```

**Navigation:**

In `base.html`, add:

```html
<a href="/data-sources">数据源设置</a>
```

near “大模型设置”。

**Tests:**

Assert response contains:

- `数据源设置`
- `draggable="true"`
- `data-source-profile-rows`
- `Tushare`
- no raw secret string.

---

## Task 5: Optional unsaved connection test route for data sources

**Objective:** 提供“测试数据源”按钮，但不把测试动作绑定到保存动作，避免意外外部请求。

**Files:**
- Modify: `src/factor_lab/webui_app.py`
- Modify: `src/factor_lab/webui_templates/data_sources.html`
- Test: `tests/test_webui_data_source_settings.py`

**Design:**

Route:

```python
@app.post("/data-sources/test", response_class=HTMLResponse)
async def data_sources_test(request: Request): ...
```

Behavior:

- Reads unsaved submitted row.
- Does not write `.env`.
- For `tushare`, only performs a minimal metadata/probe if key is provided; otherwise returns “Token 未填写”。
- All errors are displayed redacted.
- Never prints key in HTML/log artifacts.

Acceptance test:

- Monkeypatch `test_data_source_connection(profile)` to avoid real network.
- POST `/data-sources/test` returns 200 with fake success.
- Env file remains unchanged.

If implementation time is limited, this task can be postponed without blocking the core settings UI.

---

## Task 6: Wire runtime read path only where safe

**Objective:** 确保现有 Tushare provider continues to work via `TUSHARE_TOKEN` while allowing future code to read ordered data-source profiles.

**Files:**
- Modify only if needed: `src/factor_lab/settings.py` or `src/factor_lab/webui_app.py`
- Do not modify backtest/provider behavior beyond env sync in this phase.

Plan:

1. Keep `TushareDataProvider()` unchanged because it already calls `get_required_env("TUSHARE_TOKEN")`.
2. Because save syncs `TUSHARE_TOKEN`, existing workflows continue to work.
3. Add a pure helper only if useful for future use:

```python
def load_data_source_profiles_from_env() -> list[dict[str, Any]]: ...
```

4. Do not implement automatic fallback across Tushare/Diemeng in this phase; that needs a separate data-layer plan.

Verification:

```bash
PYTHONPATH=src .venv/bin/python - <<'PY'
from factor_lab.tushare_provider import TushareDataProvider
print('provider still expects TUSHARE_TOKEN')
PY
```

No external Tushare fetch should run in this verification.

---

## Task 7: Full verification and live WebUI smoke

**Objective:** 用测试和本地 HTTP 验证页面、保存、密钥遮蔽和服务健康。

**Commands:**

```bash
cd /home/admin/factor-lab
PYTHONPATH=src .venv/bin/python -m py_compile src/factor_lab/webui_app.py src/factor_lab/settings.py
PYTHONPATH=src .venv/bin/python -m pytest tests/test_webui_data_source_settings.py tests/test_webui_llm_settings.py tests/test_settings_env_file.py -q
```

If broad impact looks low, run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_webui_routes.py tests/test_feature_schema.py tests/test_autonomous_strategy_cache_extension.py -q
```

Live smoke, if WebUI service is available:

```bash
curl -sS -o /tmp/data-sources.html -w '%{http_code}\n' http://127.0.0.1:8765/data-sources
curl -sS -o /tmp/data-sources-post.html -w '%{http_code} %{redirect_url}\n' \
  -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'source_order_0=1&source_enabled_0=on&source_type_0=tushare&source_name_0=primary-tushare&source_api_key_0=&source_notes_0=existing' \
  http://127.0.0.1:8765/data-sources
```

Expected:

- GET returns `200`.
- POST returns `303` to `/data-sources?saved=1&restart=...`.
- HTML contains masked key only.
- `.env` contains `FACTOR_LAB_DATA_SOURCE_PROFILES_JSON` and preserves unrelated lines.
- `.env` permission is `600` if chmod succeeds.

---

## Risk Notes

1. **Secret leakage risk:** highest risk. Never put raw token into `value=`, logs, test failure messages, or page output.
2. **Wrong env precedence:** tests should monkeypatch `env_file()` and ensure file values win over stale process env for the target file.
3. **Deleting tokens accidentally:** blank key must preserve existing key; blank row deletes only if all identifying fields are blank.
4. **Daemon restart semantics:** saving WebUI env updates the current process immediately, but daemon reads env on restart. Show restart result clearly.
5. **Tushare rate limits:** connection test must be opt-in and minimal; saving settings must not call Tushare.
6. **Scope creep:** multiple data-source fallback at data-loading level is a later architecture task. This plan only adds safe configuration and legacy sync.

---

## Suggested Implementation Order

1. Helper tests → helper implementation.
2. Routes → template → navigation.
3. Optional test button.
4. Targeted tests + live smoke.
5. Only after this passes, discuss whether to wire true runtime multi-source fallback beyond `TUSHARE_TOKEN` legacy sync.
