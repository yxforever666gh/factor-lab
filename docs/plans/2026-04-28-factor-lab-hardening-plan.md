# Factor Lab Hardening Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 将 Factor Lab 从“可运行的自动化研究原型”收敛为“运行态清晰、结果可信、可维护、可验收”的自动化量化研究系统。

**Architecture:** 本计划不优先扩展新功能，而是先治理边界：源码态与运行态分离、de-OpenClaw 验收、WebUI 性能与控制面、daemon 可观测性、数据/回测可信度审计、Agent role 与 LLM provider 解耦。所有变更必须以小步提交、可验证命令、回滚安全为原则推进。

**Tech Stack:** Python, FastAPI, Uvicorn, SQLite/WAL, systemd user services, Tushare, pandas/parquet, Jinja templates, pytest, shell/systemctl/journalctl.

---

## 0. 当前已知问题基线

截至 2026-04-28 的实机检查：

- `git status --porcelain` 显示约 1137 个变更项：668 deleted、323 modified、146 untracked。
- `git ls-files artifacts | wc -l` 显示 `artifacts/` 下约 5739 个文件被 Git 跟踪。
- WebUI 服务运行于 `/home/admin/factor-lab/scripts/run_web_ui.py`。
- Research daemon 已运行于 `/home/admin/factor-lab/scripts/run_research_daemon.py`，agent briefs 使用 `--provider real_llm`。
- `/health`、`/runs`、`/agents`、`/settings`、`/llm` 可访问。
- `/` 首页 5 秒超时，存在 dashboard 重查询/重 artifact 读取风险。
- `/control` 返回 404，控制面不完整。
- 源码/脚本/文档仍存在 OpenClaw 引用，需要分类为“架构概念保留”或“旧 workspace/CLI 残留”。

---

## 总体阶段

1. **Phase A — 仓库与 artifacts 治理**：把源码、配置、测试样例、运行产物分开。
2. **Phase B — de-OpenClaw 运行态验收**：确认新路径、新 `.env`、新 provider、保留 agent-role 架构。
3. **Phase C — WebUI 性能与控制面修复**：首页不超时，增加 daemon/control 可观测入口。
4. **Phase D — Research daemon 可观测性与防空转**：让队列、worker、跳过原因、stale task 可解释。
5. **Phase E — 数据与回测可信度审计**：未来函数、成本、幸存者偏差、多重检验、因子重复治理。
6. **Phase F — Agent role/provider 边界硬化**：明确角色、schema、权限、验证方式。
7. **Phase G — 验收、文档与持续运行规则**：形成 smoke test、daily check、release checklist。

---

# Phase A — 仓库与 Artifacts 治理

## Task A1: 生成 artifacts 资产清单

**Objective:** 先只盘点，不删除任何文件，形成 artifacts 分类依据。

**Files:**
- Create: `artifacts_inventory.json`
- Create: `docs/artifact-inventory-2026-04-28.md`

**Steps:**

1. 运行脚本扫描 `artifacts/`：

```bash
python3 - <<'PY'
from pathlib import Path
import json, hashlib
root = Path('artifacts')
rows = []
for p in root.rglob('*'):
    if p.is_file():
        rel = str(p)
        rows.append({
            'path': rel,
            'suffix': p.suffix,
            'size': p.stat().st_size,
            'top_dir': rel.split('/')[1] if '/' in rel else '',
        })
Path('artifacts_inventory.json').write_text(json.dumps(rows, indent=2, ensure_ascii=False))
print('files', len(rows), 'bytes', sum(r['size'] for r in rows))
PY
```

2. 生成 Markdown 汇总：按目录、后缀、大小 Top 50。
3. 验证：

```bash
python3 -m json.tool artifacts_inventory.json >/dev/null
```

4. Commit：

```bash
git add artifacts_inventory.json docs/artifact-inventory-2026-04-28.md
git commit -m "docs: inventory factor lab artifacts"
```

---

## Task A2: 制定 artifact policy

**Objective:** 明确哪些文件进 Git，哪些只留本地，哪些进入样例快照。

**Files:**
- Create: `docs/artifact-policy.md`
- Modify: `.gitignore`

**Policy Draft:**

```markdown
# Artifact Policy

## Tracked
- Small deterministic golden fixtures under `tests/fixtures/`
- Human-written reports under `docs/`
- Minimal example configs under `configs/`

## Not tracked
- `artifacts/**/*.db`
- `artifacts/**/*.db-wal`
- `artifacts/**/*.db-shm`
- `artifacts/**/dataset.csv`
- `artifacts/**/results.json`
- `artifacts/**/portfolio_results.json`
- `artifacts/diagnostics/*.json`
- generated candidate run directories
- Tushare cache and feature-store parquet outputs

## Snapshot candidates
- Curated small summaries copied to `tests/fixtures/` or `docs/snapshots/`
```

**`.gitignore` additions candidate:**

```gitignore
# Factor Lab runtime artifacts
artifacts/*.db
artifacts/*.db-wal
artifacts/*.db-shm
artifacts/diagnostics/
artifacts/generated_candidate_*/
artifacts/generated_*/
artifacts/opportunity_generated_batch_run/
artifacts/tushare_cache/
artifacts/tushare_batch/
artifacts/tushare_workflow/
artifacts/feature_store/*.parquet
artifacts/feature_store/*.meta.json
```

**Verification:**

```bash
git check-ignore -v artifacts/factor_lab.db || true
git check-ignore -v artifacts/diagnostics/example.json || true
```

**Important:** 不在本任务执行 `git rm`。只写 policy 和 ignore。

---

## Task A3: 分离 tracked runtime artifacts

**Objective:** 将 runtime artifacts 从 Git 索引移除，但不删除本地文件。

**Files:**
- Modify: Git index only

**Steps:**

1. 先 dry-run：

```bash
git ls-files artifacts | wc -l
git ls-files artifacts | sed -n '1,120p'
```

2. 选择第一批最安全类型执行：

```bash
git rm --cached artifacts/factor_lab.db artifacts/factor_lab.db-wal artifacts/factor_lab.db-shm || true
git rm --cached -r artifacts/diagnostics || true
git rm --cached -r artifacts/generated_candidate_* || true
```

3. 验证本地文件仍存在：

```bash
test -f artifacts/factor_lab.db && echo "db still exists"
```

4. Commit：

```bash
git add .gitignore docs/artifact-policy.md
git commit -m "chore: stop tracking runtime artifacts"
```

**Risk Control:** 每批不要超过 1 类 artifact，避免一次提交过大不可 review。

---

# Phase B — de-OpenClaw 运行态验收

## Task B1: 建立 de-OpenClaw 验收脚本

**Objective:** 用机器检查替代主观“迁移完成”。

**Files:**
- Create: `scripts/verify_de_openclaw_runtime.py`
- Test: `tests/test_verify_de_openclaw_runtime.py`

**Script requirements:**

- 检查 systemd WebUI ExecStart 是否包含 `/home/admin/factor-lab`。
- 检查 daemon ExecStart 是否包含 `/home/admin/factor-lab`。
- 检查运行进程不从 `/home/admin/.openclaw/workspace` 启动。
- 检查 daemon provider 为 `real_llm` 或配置允许的 provider。
- 把 OpenClaw 引用分成：
  - allowed: agent-role 文档/兼容层；
  - blocked: old workspace path、openclaw CLI 硬调用、旧 env 文件。

**Run:**

```bash
python3 scripts/verify_de_openclaw_runtime.py
```

**Expected:**

```text
PASS webui_path
PASS daemon_path
PASS provider
PASS no_old_workspace_process
WARN allowed_openclaw_concept_refs=N
PASS no_blocked_openclaw_refs
```

---

## Task B2: 整理 OpenClaw 引用分类文档

**Objective:** 避免把 agent 架构和旧 workspace 依赖混为一谈。

**Files:**
- Create: `docs/openclaw-reference-policy.md`

**Content requirements:**

- `OpenClaw agent-role architecture`：可保留为设计思想。
- `OpenClaw workspace path`：禁止运行期依赖。
- `OpenClaw CLI event`：默认禁止，除非显式兼容层开关。
- `openclaw_agent provider`：迁移期兼容项，默认禁用。
- `real_llm provider`：当前正式 provider。

---

# Phase C — WebUI 性能与控制面修复

## Task C1: 修复首页超时

**Objective:** 首页 `/` 在 1 秒内返回，慢数据异步化或读取预计算 snapshot。

**Files:**
- Modify: `src/factor_lab/webui_app.py`
- Modify: relevant templates under `src/factor_lab/webui_templates/`
- Test: `tests/test_webui_routes.py`

**Steps:**

1. 写测试：请求 `/`，断言 status 200，耗时小于 1 秒。
2. 定位首页 handler 中的重读文件/重 SQLite 查询。
3. 将重数据改为：
   - 读取 `artifacts/webui_dashboard_snapshot.json`；或
   - 使用 try/timeout/fallback；或
   - 首页只展示轻量摘要，详细数据链接到 `/runs`、`/agents`。
4. 单个 artifact 读取失败时只显示 warning，不阻塞首页。

**Verification:**

```bash
python3 - <<'PY'
import urllib.request, time
for p in ['/', '/health', '/runs', '/agents', '/settings', '/llm']:
    t=time.time()
    r=urllib.request.urlopen('http://127.0.0.1:8765'+p, timeout=3)
    r.read(100)
    print(p, r.status, round(time.time()-t, 3))
PY
```

Expected: 所有核心页面 200，`/` 小于 1 秒。

---

## Task C2: 增加 `/control` 页面只读版本

**Objective:** 先提供 daemon/control 的只读控制台，不立即允许危险操作。

**Files:**
- Modify: `src/factor_lab/webui_app.py`
- Create/Modify: `src/factor_lab/webui_templates/control.html`
- Test: `tests/test_webui_routes.py`

**Page fields:**

- WebUI service status
- daemon service status
- daemon PID
- daemon start path
- provider
- current queue counts
- latest task id/status
- stale running tasks count
- latest daemon heartbeat timestamp
- links: `/runs`, `/agents`, `/settings`, `/health`

**Route:**

```python
@app.get('/control', response_class=HTMLResponse)
```

**Verification:**

```bash
curl -fsS http://127.0.0.1:8765/control | grep -E "Daemon|Provider|Queue"
```

---

## Task C3: 添加安全操作接口的设计，不先开放执行

**Objective:** 防止 WebUI 控制面直接引入危险 side effects。

**Files:**
- Create: `docs/webui-control-safety-design.md`

**Design required:**

- 哪些操作允许：refresh snapshot、enqueue baseline、mark stale task。
- 哪些操作必须二次确认：restart daemon、cancel task。
- 哪些操作禁止：删除 artifacts、清空 DB。
- 每个操作写 audit log。
- POST-only，不用 GET 做副作用。

---

# Phase D — Research Daemon 可观测性与防空转

## Task D1: 定义 daemon heartbeat schema

**Objective:** daemon 周期性写轻量 heartbeat，WebUI 和脚本都能读取。

**Files:**
- Create: `docs/daemon-heartbeat-schema.md`
- Modify: `scripts/run_research_daemon.py`
- Test: `tests/test_daemon_heartbeat.py`

**Schema:**

```json
{
  "timestamp": "2026-04-28T00:00:00+08:00",
  "pid": 12345,
  "project_root": "/home/admin/factor-lab",
  "provider": "real_llm",
  "queue": {
    "pending": 0,
    "running": 1,
    "finished_24h": 120,
    "failed_24h": 3
  },
  "current_task": {
    "id": "...",
    "type": "workflow",
    "started_at": "..."
  },
  "last_injection": {
    "timestamp": "...",
    "source": "planner",
    "count": 2
  },
  "skip_reasons_24h": {
    "recently_finished_same_fingerprint": 12
  }
}
```

**Verification:**

```bash
python3 -m json.tool artifacts/research_daemon_heartbeat.json
```

---

## Task D2: 建立队列解释报告

**Objective:** 当队列为空时，系统必须说明“为什么为空”。

**Files:**
- Create: `scripts/build_queue_explanation.py`
- Output: `artifacts/research_queue_explanation.json`
- Test: `tests/test_queue_explanation.py`

**Report fields:**

- pending/running counts
- deficits by category
- last planner run
- last opportunity injection
- top skip reasons
- cooldown-blocked examples
- stale running tasks
- recommendation: `wait`, `reseed`, `repair`, `inspect_data`, `inspect_llm`

---

## Task D3: stale running task policy

**Objective:** 防止任务卡住但进程仍活着。

**Files:**
- Create: `docs/stale-task-policy.md`
- Modify: relevant queue/runtime module
- Test: runtime state tests

**Policy:**

- workflow task > N minutes: stale warning
- diagnostic task > N minutes: stale warning
- worker PID missing: mark stale
- stale task 不直接删除，先标记并生成 repair recommendation

---

# Phase E — 数据与回测可信度审计

## Task E1: 数据时间对齐审计

**Objective:** 明确每个数据字段在何时可得，防止未来函数。

**Files:**
- Create: `docs/data-availability-policy.md`
- Create: `scripts/audit_data_alignment.py`
- Output: `artifacts/data_alignment_audit.json`

**Audit checks:**

- target return 是否严格向后；
- daily_basic 是否按 trade_date 使用；
- 财务字段是否有 report_date / ann_date 约束；
- universe 是否在当时可得；
- 停牌/缺失是否处理。

---

## Task E2: 回测交易成本准入标准

**Objective:** approved 因子必须通过成本后验证。

**Files:**
- Create: `docs/backtest-cost-policy.md`
- Modify: factor approval/scoring code path
- Test: scoring/approval tests

**Minimum criteria:**

- gross IC / spread 只作为初筛；
- net return / net spread 必须纳入晋级；
- turnover 超阈值降级；
- capacity/liquidity 风险写入 risk profile；
- 成本假设写入 summary。

---

## Task E3: 多重检验和重复因子治理

**Objective:** 防止参数变体刷榜。

**Files:**
- Create: `docs/multiple-testing-and-factor-dedup-policy.md`
- Modify: clustering/scoring/approval path
- Test: candidate dedup tests

**Rules:**

- 与已 approved 因子相关性过高：不能直接 approved；
- 同 family 内只允许代表因子晋级；
- 新因子必须说明相对已有因子的增量；
- LLM rationale 不作为统计证据。

---

# Phase F — Agent Role / Provider 边界硬化

## Task F1: Agent role registry 固化

**Objective:** 每个 agent 有固定职责、输入、输出、权限。

**Files:**
- Modify: `src/factor_lab/agent_roles.py`
- Create: `docs/agent-role-registry.md`
- Test: `tests/test_agent_roles.py`

**Required roles:**

- data_auditor
- factor_researcher
- backtest_reviewer
- risk_reviewer
- failure_analyst
- planner
- decision_reviewer

**Each role schema:**

```json
{
  "role": "backtest_reviewer",
  "can_enqueue_tasks": false,
  "can_approve_factor": false,
  "required_inputs": ["summary", "metrics", "cost_assumptions"],
  "required_outputs": ["verdict", "evidence", "risks", "recommended_next_step"]
}
```

---

## Task F2: Provider router 只负责模型调用，不负责 agent 决策

**Objective:** 防止 provider 和 agent 混淆。

**Files:**
- Modify: `src/factor_lab/llm_provider_router.py`
- Modify: `src/factor_lab/llm_bridge.py`
- Test: `tests/test_llm_provider_router.py`

**Rule:**

- provider router: base_url/model/api_key/timeout/retry
- agent layer: role/prompt/schema/permissions/decision interpretation
- 禁止在 provider router 里写 agent-specific business logic

---

## Task F3: Agent output schema validation

**Objective:** LLM 输出必须结构化、可拒绝、可审计。

**Files:**
- Modify: `src/factor_lab/llm_schema_validation.py`
- Test: `tests/test_llm_schema_validation.py`

**Minimum validation:**

- verdict 必须来自枚举；
- evidence 必须引用指标或 artifact；
- unsupported claim 进入 warnings；
- 缺少 required field 时拒绝入库；
- rejected response 写入 diagnostics。

---

# Phase G — 验收、文档与持续运行规则

## Task G1: 建立一键 smoke test

**Objective:** 每次改动后快速判断系统是否仍可用。

**Files:**
- Create: `scripts/smoke_test_factor_lab.py`
- Test: `tests/test_smoke_script.py`

**Checks:**

- import `factor_lab`
- DB 可打开
- WebUI routes 可访问
- daemon heartbeat 可读
- de-OpenClaw runtime pass
- LLM provider config 可解析但不泄露 key
- Tushare cache status 可读

**Run:**

```bash
python3 scripts/smoke_test_factor_lab.py
```

---

## Task G2: 建立 release checklist

**Objective:** 防止文档再次过度乐观。

**Files:**
- Create: `docs/release-checklist.md`

**Checklist:**

- `git status` 只有预期源码/文档变更；
- `pytest` 通过；
- `scripts/smoke_test_factor_lab.py` 通过；
- WebUI `/` 小于 1 秒；
- daemon 路径为 `/home/admin/factor-lab`；
- provider 为预期 provider；
- no blocked OpenClaw references；
- artifacts policy 没有违反；
- DB backup 完成；
- release notes 包含已知问题。

---

## Task G3: 建立 daily operational report

**Objective:** 让 24/7 系统每天自动总结是否真的有研究进展。

**Files:**
- Create: `scripts/build_daily_ops_report.py`
- Output: `artifacts/daily_ops_report.md`

**Report sections:**

- tasks finished/failed
- new candidates
- promoted/demoted/graveyard factors
- top skip reasons
- data health
- LLM health
- daemon restarts
- WebUI route health
- recommended human action

---

# Implementation Order

建议严格按以下顺序执行：

1. A1 inventory
2. A2 artifact policy
3. A3 分批 untrack runtime artifacts
4. B1 de-OpenClaw runtime verifier
5. B2 OpenClaw reference policy
6. C1 首页超时修复
7. C2 `/control` 只读页
8. D1 daemon heartbeat
9. D2 queue explanation
10. G1 smoke test
11. E1/E2/E3 研究可信度审计
12. F1/F2/F3 Agent/provider 边界硬化
13. G2/G3 release 与 daily ops

---

# Acceptance Criteria

本计划完成后，项目必须满足：

- `git status` 不再被 daemon 运行产物持续污染。
- `artifacts/` 中 runtime 文件不再默认进 Git。
- WebUI `/`、`/health`、`/runs`、`/agents`、`/settings`、`/llm`、`/control` 全部 200。
- 首页 `/` 在正常机器上 1 秒内返回。
- daemon heartbeat 可读，且显示 project root、provider、队列状态。
- de-OpenClaw verifier 通过：无旧 workspace 运行期依赖。
- OpenClaw agent-role 概念和旧 OpenClaw workspace/CLI 依赖被明确区分。
- 队列为空时能解释原因，而不是只显示 empty。
- 因子晋级必须经过数据可得性、成本、重复性和稳健性检查。
- LLM/Agent 输出必须结构化并可审计。
- release checklist 可复用，completion report 必须绑定实机输出。

---

# Non-goals

本计划暂不做：

- 新增更多因子算子；
- 新增更多 LLM provider；
- 大规模替换 SQLite；
- 重写整个 WebUI；
- 直接上实盘交易；
- 删除历史数据文件。

先治理边界和可信度，再继续扩展。
