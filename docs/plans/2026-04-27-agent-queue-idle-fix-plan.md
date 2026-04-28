# Agent Queue Idle / 空转修复实施计划

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 修复 Factor Lab daemon 在 `pending=0/running=0` 时被误判为 queue stall、补任务过于保守、探索任务被风控压制后短时间空转的问题，让 Agent 运行从“小批量跑完后空等”变为“健康空闲可区分、可持续补充非重复研究任务”。

**Architecture:** 不直接放宽所有风控，也不盲目重复 baseline。新增“健康空闲 vs 真空转”的判定层，改造 repair snapshot/repair agent 的 queue_stall 依据；同时增强 refill fallback：当 baseline/validated tasks 被 fingerprint cooldown 拦住时，优先生成轻量窗口变体或 opportunity exploration，而不是重复 reseed 同一个 fingerprint。

**Tech Stack:** Python 3.10+, SQLite-backed `ExperimentStore`, pytest, Factor Lab artifacts JSON runtime state, systemd user services.

---

## 0. 当前诊断结论

### 现象

当前 daemon 和 Agent 都在运行，但出现周期性：

1. planner/opportunity 注入少量任务；
2. 任务很快完成；
3. `pending=0`、`running=0`；
4. Repair Agent 报 `queue_stall`；
5. `reseed_queue` 尝试补 baseline，但 `seeded_task_ids=[]`；
6. 下一轮又依赖 planner/opportunity 注入少量任务。

### 已确认状态

- daemon 正常：`factor-lab-research-daemon.service active (running)`。
- 最新状态文件持续更新：`artifacts/research_daemon_status.json`。
- repair snapshot 中：
  - `queue_counts.pending = 0`
  - `queue_counts.running = 0`
  - `queue_counts.finished = 200`
  - `failure_state.consecutive_failures = 0`
  - `heartbeat_gap.seconds_since_last ≈ 8s`
- planner 最新可注入任务数：`planner_injected=1`。
- opportunity 最新可注入任务数：`opportunity_injected=2`。
- baseline reseed 失败原因不是异常，而是最近同 fingerprint 已完成，被 cooldown/dedupe 拦住。

### 根因假设

**Root Cause A — queue_stall 判定过窄：**
现在 Repair Agent 主要看到 `pending=0/running=0` 就倾向判空转，但没有结合最近是否刚成功注入/完成任务、heartbeat 是否新鲜、planner 是否刚有注入。

**Root Cause B — baseline reseed 没有解释性结果：**
`enqueue_baseline_tasks()` 返回 `[]` 时调用方不知道是 cooldown、budget、fingerprint duplicate，还是文件/配置异常。Repair Agent 只能看到 `seeded 0 baseline tasks`，无法区分“健康防重复”与“真补种失败”。

**Root Cause C — refill fallback 太依赖固定 baseline：**
当 standard baseline / validation 被 repeat cooldown 拦住时，系统虽有 `maybe_expand_research_space()`，但当前场景下探索经常被 `low_recent_yield`、`rss_risk`、`low_confidence` 压住，导致 refill 的可持续性不足。

**Root Cause D — real LLM Agent 退化会降低任务多样性：**
Planner / Failure Analyst 当前可运行，但最近响应显示 `degraded_to_heuristic=true` 或 provider 侧配置不完整；heuristic 更容易重复选择同类诊断/验证任务，进一步触发 fingerprint cooldown。

---

## 1. 非目标 / 约束

本计划不做以下事情：

- 不关闭 `recently_finished_same_fingerprint` 去重。
- 不无脑缩短所有 cooldown。
- 不强行高并发跑 Tushare / batch。
- 不把低质量 exploration 全部放开。
- 不改交易/因子计算逻辑。
- 不在没有测试的情况下直接改 daemon 主循环。

修复目标是：**更准确地区分健康 idle、冷却 idle、真实 stall，并在真实 stall 时提供安全的非重复补任务路径。**

---

## 2. 验收标准

### 功能验收

1. 当最近 2 分钟内有任务完成或有 planner/opportunity 注入时，`pending=0/running=0` 不应被标记为 `queue_stall`，而应标记为 `healthy_idle` 或 `cooldown_idle`。
2. 当队列为空且最近无注入、无完成、无 heartbeat、无 cooldown 解释时，才判为 `queue_stall`。
3. `reseed_queue` 返回 0 时，repair action plan 必须包含原因计数，例如：
   - `repeat_blocked`
   - `budget_full`
   - `config_missing`
   - `enqueue_error`
4. 当 baseline 被 repeat cooldown 拦住时，orchestrator 应尝试至少一种非重复 fallback：
   - variant window workflow；或
   - safe diagnostic opportunity；或
   - lightweight exploration task。
5. 最新 `repair_runtime_snapshot.json` 中应包含：
   - `queue_liveness.state`
   - `queue_liveness.reason`
   - `queue_liveness.last_task_finished_age_seconds`
   - `queue_liveness.last_injection_age_seconds`
   - `queue_liveness.recent_injected_count`
6. 系统不能因新逻辑无限重复注入同 fingerprint。

### 测试验收

以下命令必须通过：

```bash
pytest tests/test_repair_runtime_liveness.py -v
pytest tests/test_research_queue_reseed_diagnostics.py -v
pytest tests/test_research_queue_refill_fallback.py -v
pytest tests/test_research_planner_validate.py tests/test_research_opportunity_autonomy.py -v
```

如时间允许，额外跑：

```bash
pytest tests/test_research_daemon_recycle.py tests/test_research_queue_fail_safes.py -v
```

---

## 3. 实施任务

### Task 1: 为 queue liveness 写失败测试

**Objective:** 先定义“健康 idle 不等于 queue stall”的行为。

**Files:**
- Create: `tests/test_repair_runtime_liveness.py`
- Inspect/Modify later: `src/factor_lab/repair_runtime.py`

**Step 1: 创建测试文件**

写入：

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from factor_lab.repair_runtime import classify_queue_liveness


def iso_age(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def test_empty_queue_with_recent_finished_task_is_healthy_idle():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[
            {
                "status": "finished",
                "finished_at_utc": iso_age(45),
                "created_at_utc": iso_age(90),
                "task_type": "diagnostic",
                "worker_note": "diagnostic finished: opportunity_diagnose",
            }
        ],
        refill_state={"planner_injected": 1, "opportunity_injected": 2, "updated_at_utc": iso_age(50)},
        heartbeat_gap={"available": True, "seconds_since_last": 8},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "healthy_idle"
    assert result["is_queue_stall"] is False
    assert "recent_activity" in result["reason"]


def test_empty_queue_with_no_recent_activity_is_queue_stall():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[
            {
                "status": "finished",
                "finished_at_utc": iso_age(900),
                "created_at_utc": iso_age(930),
                "task_type": "workflow",
                "worker_note": "workflow finished",
            }
        ],
        refill_state={"planner_injected": 0, "opportunity_injected": 0, "updated_at_utc": iso_age(900)},
        heartbeat_gap={"available": True, "seconds_since_last": 900},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "queue_stall"
    assert result["is_queue_stall"] is True


def test_empty_queue_during_repeat_cooldown_is_cooldown_idle():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[],
        refill_state={
            "planner_injected": 0,
            "opportunity_injected": 0,
            "updated_at_utc": iso_age(30),
            "repeat_blocked_count": 2,
        },
        heartbeat_gap={"available": True, "seconds_since_last": 20},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "cooldown_idle"
    assert result["is_queue_stall"] is False
```

**Step 2: 运行测试，确认失败**

```bash
pytest tests/test_repair_runtime_liveness.py -v
```

Expected: FAIL — `classify_queue_liveness` 不存在。

---

### Task 2: 实现 `classify_queue_liveness`

**Objective:** 新增可复用的队列活性分类函数。

**Files:**
- Modify: `src/factor_lab/repair_runtime.py`
- Test: `tests/test_repair_runtime_liveness.py`

**Step 1: 在 `repair_runtime.py` 添加 helper**

建议实现：

```python
from datetime import datetime, timezone
from typing import Any


def _parse_iso_utc(value: str | None):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _age_seconds(value: str | None) -> float | None:
    dt = _parse_iso_utc(value)
    if dt is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())


def classify_queue_liveness(
    *,
    queue_counts: dict[str, Any],
    recent_research_tasks: list[dict[str, Any]],
    refill_state: dict[str, Any] | None = None,
    heartbeat_gap: dict[str, Any] | None = None,
    failure_state: dict[str, Any] | None = None,
    recent_activity_seconds: int = 120,
) -> dict[str, Any]:
    refill_state = refill_state or {}
    heartbeat_gap = heartbeat_gap or {}
    failure_state = failure_state or {}

    pending = int(queue_counts.get("pending") or 0)
    running = int(queue_counts.get("running") or 0)
    active = pending + running

    latest_finished_age = None
    for task in recent_research_tasks:
        if task.get("status") != "finished":
            continue
        age = _age_seconds(task.get("finished_at_utc") or task.get("created_at_utc"))
        if age is None:
            continue
        latest_finished_age = age if latest_finished_age is None else min(latest_finished_age, age)

    refill_age = _age_seconds(refill_state.get("updated_at_utc"))
    recent_injected_count = int(refill_state.get("planner_injected") or 0) + int(refill_state.get("opportunity_injected") or 0)
    heartbeat_seconds = heartbeat_gap.get("seconds_since_last")
    try:
        heartbeat_seconds = float(heartbeat_seconds)
    except Exception:
        heartbeat_seconds = None

    if active > 0:
        return {
            "state": "active",
            "is_queue_stall": False,
            "reason": "pending_or_running_tasks",
            "active_count": active,
            "last_task_finished_age_seconds": latest_finished_age,
            "last_injection_age_seconds": refill_age,
            "recent_injected_count": recent_injected_count,
        }

    recent_task = latest_finished_age is not None and latest_finished_age <= recent_activity_seconds
    recent_injection = recent_injected_count > 0 and refill_age is not None and refill_age <= recent_activity_seconds
    fresh_heartbeat = heartbeat_seconds is not None and heartbeat_seconds <= recent_activity_seconds
    repeat_blocked = int(refill_state.get("repeat_blocked_count") or 0) > 0

    if recent_task or recent_injection or fresh_heartbeat:
        return {
            "state": "healthy_idle",
            "is_queue_stall": False,
            "reason": "recent_activity",
            "active_count": 0,
            "last_task_finished_age_seconds": latest_finished_age,
            "last_injection_age_seconds": refill_age,
            "recent_injected_count": recent_injected_count,
        }

    if repeat_blocked or bool(failure_state.get("cooldown_active")):
        return {
            "state": "cooldown_idle",
            "is_queue_stall": False,
            "reason": "cooldown_or_repeat_blocked",
            "active_count": 0,
            "last_task_finished_age_seconds": latest_finished_age,
            "last_injection_age_seconds": refill_age,
            "recent_injected_count": recent_injected_count,
        }

    return {
        "state": "queue_stall",
        "is_queue_stall": True,
        "reason": "empty_queue_without_recent_activity",
        "active_count": 0,
        "last_task_finished_age_seconds": latest_finished_age,
        "last_injection_age_seconds": refill_age,
        "recent_injected_count": recent_injected_count,
    }
```

**Step 2: 运行测试**

```bash
pytest tests/test_repair_runtime_liveness.py -v
```

Expected: PASS。

---

### Task 3: 把 liveness 接入 repair snapshot

**Objective:** 让 Repair Agent 使用 `queue_liveness`，而不是只看 pending/running。

**Files:**
- Modify: `src/factor_lab/repair_runtime.py`
- Test: `tests/test_repair_runtime_liveness.py`

**Step 1: 找到 `build_repair_runtime_snapshot`**

在 `src/factor_lab/repair_runtime.py` 中找到构造 snapshot 的函数。

**Step 2: 读取 refill state**

在 snapshot 构造处读取：

```python
refill_state = _read_json(ARTIFACTS / "research_queue_refill_state.json", {})
```

如果文件内已有 `_read_json` helper，复用；不要重复造轮子。

**Step 3: 加入 queue_liveness 字段**

在返回 payload 前加入：

```python
queue_liveness = classify_queue_liveness(
    queue_counts=queue_counts,
    recent_research_tasks=recent_research_tasks,
    refill_state=refill_state,
    heartbeat_gap=heartbeat_gap,
    failure_state=failure_state,
)

payload["queue_liveness"] = queue_liveness
```

**Step 4: 扩展测试**

在 `tests/test_repair_runtime_liveness.py` 增加 snapshot 级别测试。如果现有 `build_repair_runtime_snapshot` 依赖真实 DB，可用 monkeypatch 替换 store 方法，最小验证输出包含 `queue_liveness`。

**Step 5: 运行测试**

```bash
pytest tests/test_repair_runtime_liveness.py -v
```

Expected: PASS。

---

### Task 4: 修改 Repair Agent 的 queue_stall 判定

**Objective:** Repair Agent 只在 `queue_liveness.is_queue_stall=true` 时建议 `reseed_queue`。

**Files:**
- Modify: `src/factor_lab/repair_agent_engine.py` 或实际包含 `build_repair_response` 的文件
- Modify if needed: `src/factor_lab/agent_briefs.py`
- Test: `tests/test_repair_runtime_liveness.py`

**Step 1: 定位 repair response 构造函数**

搜索：

```bash
python3 - <<'PY'
from pathlib import Path
for p in Path('src/factor_lab').glob('*.py'):
    text = p.read_text(errors='ignore')
    if 'build_repair_response' in text or 'queue_stall' in text:
        print(p)
PY
```

**Step 2: 修改判断逻辑**

伪代码：

```python
queue_liveness = inputs.get("runtime_snapshot", {}).get("queue_liveness") or inputs.get("queue_liveness") or {}

if queue_liveness.get("is_queue_stall"):
    incident_type = "queue_stall"
    repair_mode = "repair"
    recommended_actions = [... reseed_queue ...]
elif queue_liveness.get("state") in {"healthy_idle", "cooldown_idle"}:
    incident_type = "unknown"
    repair_mode = "observe"
    recommended_actions = [mark_incident_only]
```

**Step 3: 增加测试**

新增测试：

```python
def test_repair_agent_observes_healthy_idle_instead_of_reseed():
    response = build_repair_response({
        "context_id": "test",
        "inputs": {
            "runtime_snapshot": {
                "queue_liveness": {"state": "healthy_idle", "is_queue_stall": False, "reason": "recent_activity"},
                "queue_counts": {"pending": 0, "running": 0, "finished": 100, "failed": 0},
            }
        }
    }, source_label="heuristic")

    assert response["repair_mode"] == "observe"
    assert all(a["action_type"] != "reseed_queue" for a in response["recommended_actions"])
```

根据实际函数签名调整 import。

**Step 4: 运行测试**

```bash
pytest tests/test_repair_runtime_liveness.py -v
```

Expected: PASS。

---

### Task 5: 给 baseline reseed 增加诊断返回

**Objective:** 让系统知道 reseed 0 的原因，而不是只有 `[]`。

**Files:**
- Modify: `src/factor_lab/research_queue.py:568-612`
- Modify: `src/factor_lab/repair_playbooks.py`
- Test: `tests/test_research_queue_reseed_diagnostics.py`

**Step 1: 创建测试文件**

Create: `tests/test_research_queue_reseed_diagnostics.py`

测试目标：当 `recently_finished_same_fingerprint=True` 时，诊断返回 `repeat_blocked_count=2`。

```python
from __future__ import annotations

from factor_lab import research_queue


class DummyStore:
    def list_research_tasks(self, limit=300):
        return []

    def enqueue_research_task(self, **kwargs):
        raise AssertionError("should not enqueue when repeat-blocked")


def test_enqueue_baseline_tasks_with_diagnostics_reports_repeat_blocked(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "tushare_workflow.json").write_text('{"a": 1}', encoding="utf-8")
    (tmp_path / "configs" / "tushare_batch.json").write_text('{"b": 2}', encoding="utf-8")

    monkeypatch.setattr(research_queue, "recently_finished_same_fingerprint", lambda *a, **kw: True)
    result = research_queue.enqueue_baseline_tasks_with_diagnostics(DummyStore())

    assert result["task_ids"] == []
    assert result["repeat_blocked_count"] == 2
    assert result["skipped"][0]["reason"] == "recently_finished_same_fingerprint"
```

**Step 2: 实现新函数，不破坏旧接口**

在 `research_queue.py` 中保留旧函数：

```python
def enqueue_baseline_tasks(store: ExperimentStore) -> list[str]:
    return enqueue_baseline_tasks_with_diagnostics(store)["task_ids"]
```

新增：

```python
def enqueue_baseline_tasks_with_diagnostics(store: ExperimentStore) -> dict[str, Any]:
    seeds = [...]  # 复用原 seeds
    budget = queue_budget_snapshot(store)
    task_ids = []
    skipped = []

    for seed in seeds:
        category = _category_from_note(seed["worker_note"])
        if category == "baseline" and budget["baseline"] >= _max_pending_baseline():
            skipped.append({"seed": seed["worker_note"], "reason": "budget_full"})
            continue
        if category == "validation" and budget["validation"] >= _max_pending_validation():
            skipped.append({"seed": seed["worker_note"], "reason": "budget_full"})
            continue
        try:
            cfg = json.loads(Path(seed["config_path"]).read_text(encoding="utf-8"))
        except FileNotFoundError:
            skipped.append({"seed": seed["worker_note"], "reason": "config_missing", "config_path": seed["config_path"]})
            continue
        fingerprint = f"{seed['task_type']}::{config_fingerprint(cfg)}::{seed['output_dir']}"
        if recently_finished_same_fingerprint(...):
            skipped.append({"seed": seed["worker_note"], "reason": "recently_finished_same_fingerprint", "fingerprint": fingerprint})
            continue
        task_id = store.enqueue_research_task(...)
        task_ids.append(task_id)
        budget[category] += 1

    return {
        "task_ids": task_ids,
        "skipped": skipped,
        "repeat_blocked_count": len([s for s in skipped if s["reason"] == "recently_finished_same_fingerprint"]),
        "budget_blocked_count": len([s for s in skipped if s["reason"] == "budget_full"]),
        "config_missing_count": len([s for s in skipped if s["reason"] == "config_missing"]),
    }
```

注意：`recently_finished_same_fingerprint(...)` 参数必须与现有 `enqueue_baseline_tasks` 保持一致。

**Step 3: 运行测试**

```bash
pytest tests/test_research_queue_reseed_diagnostics.py -v
```

Expected: PASS。

---

### Task 6: 把 reseed diagnostics 写入 refill state / repair action plan

**Objective:** 让 Repair Agent 和 snapshot 能解释为什么 reseed 0。

**Files:**
- Modify: `src/factor_lab/research_queue.py`
- Modify: `src/factor_lab/repair_playbooks.py`
- Test: `tests/test_research_queue_reseed_diagnostics.py`

**Step 1: 修改 orchestrator 中调用 reseed 的位置**

在 `run_orchestrator()` 内当前调用：

```python
seeded = enqueue_baseline_tasks(store)
```

保守替换为：

```python
reseed_result = enqueue_baseline_tasks_with_diagnostics(store)
seeded = reseed_result["task_ids"]
```

并在 `_mark_refill_attempt` 里加入可选字段：

```python
"repeat_blocked_count": int((reseed_result or {}).get("repeat_blocked_count") or 0),
"reseed_skipped": (reseed_result or {}).get("skipped") or [],
```

如果不想大改 `_mark_refill_attempt` 参数，可新增 `_write_reseed_diagnostics(reseed_result)` 到 `artifacts/research_queue_reseed_diagnostics.json`，repair snapshot 再读取。

**推荐低风险方案：**新增 `artifacts/research_queue_reseed_diagnostics.json`，避免频繁修改 `_mark_refill_attempt` 签名。

**Step 2: 修改 repair playbook 的 reseed action**

在 `execute_repair_actions` 处理 `reseed_queue` 的分支里使用新函数，并把诊断放进 action row：

```python
result = enqueue_baseline_tasks_with_diagnostics(store)
seeded = result["task_ids"]
row = {
    "action_type": "reseed_queue",
    "target": target,
    "status": "ok" if seeded else "failed_no_effect",
    "seeded_task_ids": seeded,
    "effect_count": len(seeded),
    "effect_summary": f"seeded {len(seeded)} baseline tasks",
    "reseed_diagnostics": result,
}
```

**Step 3: 测试 action plan 包含诊断**

在测试中断言：

```python
assert action["reseed_diagnostics"]["repeat_blocked_count"] == 2
```

**Step 4: 运行测试**

```bash
pytest tests/test_research_queue_reseed_diagnostics.py -v
```

Expected: PASS。

---

### Task 7: 添加 refill fallback 的最小策略测试

**Objective:** 当 baseline repeat-blocked 时，系统尝试非重复 fallback，而不是只报 no_effect。

**Files:**
- Create: `tests/test_research_queue_refill_fallback.py`
- Modify: `src/factor_lab/research_queue.py`

**Step 1: 写测试**

```python
from __future__ import annotations

from factor_lab import research_queue


class DummyStore:
    def __init__(self):
        self.enqueued = []

    def list_research_tasks(self, limit=300):
        return []

    def enqueue_research_task(self, **kwargs):
        task_id = f"task-{len(self.enqueued)+1}"
        self.enqueued.append({"task_id": task_id, **kwargs})
        return task_id


def test_refill_fallback_uses_expansion_when_baseline_repeat_blocked(monkeypatch):
    store = DummyStore()
    monkeypatch.setattr(
        research_queue,
        "enqueue_baseline_tasks_with_diagnostics",
        lambda _store: {"task_ids": [], "repeat_blocked_count": 2, "skipped": [{"reason": "recently_finished_same_fingerprint"}]},
    )
    monkeypatch.setattr(
        research_queue,
        "maybe_expand_research_space",
        lambda _store, max_new_tasks=4, allow_repeat=False: ["expanded-1"],
    )

    result = research_queue.refill_empty_queue_with_fallback(store)

    assert result["task_ids"] == ["expanded-1"]
    assert result["source"] == "expand_research_space"
```

**Step 2: 实现 helper**

在 `research_queue.py` 增加：

```python
def refill_empty_queue_with_fallback(store: ExperimentStore, *, allow_repeat_expand: bool = True) -> dict[str, Any]:
    reseed = enqueue_baseline_tasks_with_diagnostics(store)
    if reseed["task_ids"]:
        return {"source": "baseline_reseed", "task_ids": reseed["task_ids"], "reseed_diagnostics": reseed}

    expanded = maybe_expand_research_space(store, max_new_tasks=4, allow_repeat=allow_repeat_expand)
    if expanded:
        return {"source": "expand_research_space", "task_ids": expanded, "reseed_diagnostics": reseed}

    return {"source": "none", "task_ids": [], "reseed_diagnostics": reseed}
```

**Step 3: 逐步替换 orchestrator 中散落的 reseed/expand 逻辑**

只替换最简单分支：

```python
elif can_reseed_baseline(store):
    refill_result = refill_empty_queue_with_fallback(store)
    seeded = refill_result["task_ids"]
    if seeded:
        ...
```

不要一次性重构所有分支。

**Step 4: 运行测试**

```bash
pytest tests/test_research_queue_refill_fallback.py -v
```

Expected: PASS。

---

### Task 8: 降低 Repair Agent 的 false positive 噪音

**Objective:** 对 `healthy_idle` / `cooldown_idle` 只记录 observe，不触发 repair failed/no_effect。

**Files:**
- Modify: `src/factor_lab/repair_agent_engine.py`
- Modify: `src/factor_lab/repair_verifier.py` if necessary
- Test: `tests/test_repair_runtime_liveness.py`

**Step 1: 对 healthy/cooldown idle 返回 observe**

逻辑：

```python
if queue_liveness.get("state") in {"healthy_idle", "cooldown_idle"}:
    return {
        "incident_type": "unknown",
        "severity": "low",
        "repair_mode": "observe",
        "suspected_root_causes": [],
        "recommended_actions": [
            {
                "action_type": "mark_incident_only",
                "target": "none",
                "reason": f"queue is {queue_liveness['state']}: {queue_liveness.get('reason')}",
                "risk_level": "low",
            }
        ],
        ...
    }
```

**Step 2: 避免 verifier 把 observe 误报成 failed**

如果 `repair_verification.json` 对 `mark_incident_only` 已经支持 `noop/verified`，无需修改。

**Step 3: 测试**

```bash
pytest tests/test_repair_runtime_liveness.py -v
```

Expected: PASS。

---

### Task 9: Agent provider 配置检查计划，不直接修 provider

**Objective:** 记录并验证 real LLM 退化原因，但不把 provider 修复混入 queue idle 修复。

**Files:**
- Modify: `docs/plans/2026-04-27-agent-queue-idle-fix-plan.md` only if findings change
- Inspect: `artifacts/llm_provider_health_live.json`
- Inspect: `artifacts/agent_responses.json`

**Step 1: 验证当前 provider 状态**

```bash
python3 - <<'PY'
import json
from pathlib import Path
for f in ['artifacts/llm_provider_health_live.json', 'artifacts/agent_responses.json']:
    p = Path(f)
    print('\n###', f)
    d = json.loads(p.read_text())
    print(json.dumps(d.get('agent_responses', d), ensure_ascii=False, indent=2)[:3000])
PY
```

**Step 2: 判断是否与 queue idle 修复相关**

只记录：

- `configured_provider`
- `effective_source`
- `degraded_to_heuristic`
- `fallback_reason`
- missing env，例如 `FACTOR_LAB_OPENCLAW_*`

**Step 3: 不在本轮修 provider**

Provider 修复应单独出计划，避免把队列活性修复和 Agent 接入修复耦合。

---

### Task 10: 本地集成验证

**Objective:** 在不破坏长期 daemon 的前提下验证修复。

**Files:**
- No source changes unless tests fail

**Step 1: 跑单元测试**

```bash
pytest tests/test_repair_runtime_liveness.py -v
pytest tests/test_research_queue_reseed_diagnostics.py -v
pytest tests/test_research_queue_refill_fallback.py -v
```

Expected: 全部 PASS。

**Step 2: 跑相关回归**

```bash
pytest tests/test_research_opportunity_autonomy.py tests/test_research_daemon_recycle.py tests/test_research_queue_fail_safes.py -v
```

Expected: 全部 PASS；若旧测试不存在或环境缺依赖，记录原因。

**Step 3: 手动跑一次 orchestrator**

```bash
python3 - <<'PY'
from factor_lab.research_queue import run_orchestrator
print(run_orchestrator(max_tasks=1))
PY
```

Expected:

- 不抛异常；
- 如果队列为空但最近有 activity，repair response 不应建议 `reseed_queue`；
- `artifacts/repair_runtime_snapshot.json` 出现 `queue_liveness`。

**Step 4: 检查 artifacts**

```bash
python3 - <<'PY'
import json
from pathlib import Path
for f in ['artifacts/repair_runtime_snapshot.json', 'artifacts/repair_agent_response.json', 'artifacts/repair_action_plan.json']:
    print('\n###', f)
    d = json.loads(Path(f).read_text())
    print(json.dumps({
        'queue_liveness': d.get('queue_liveness'),
        'incident_type': d.get('incident_type'),
        'repair_mode': d.get('repair_mode'),
        'actions': d.get('actions'),
    }, ensure_ascii=False, indent=2))
PY
```

Expected:

- `healthy_idle` / `cooldown_idle` 时不出现 failed reseed 噪音；
- 真 queue stall 才会出现 `reseed_queue`。

---

## 4. 风险与回滚

### 风险 1: 过度放松 queue_stall 导致真停滞没被修

缓解：

- `healthy_idle` 只在最近任务/注入/heartbeat 新鲜时成立；
- 超过 `recent_activity_seconds` 仍为空则回到 `queue_stall`。

### 风险 2: fallback 生成过多重复任务

缓解：

- 不移除 fingerprint cooldown；
- fallback 仍走 `maybe_expand_research_space()` 和现有 dedupe；
- `max_new_tasks` 限制在 4。

### 风险 3: Repair Agent 行为变化影响现有测试

缓解：

- 保留 `queue_stall` 真实路径；
- 只对 `healthy_idle/cooldown_idle` 改成 observe。

### 回滚方式

如果 daemon 出现异常：

```bash
git diff
# 回滚本计划涉及文件
# 或使用 git restore 精准回滚：
git restore src/factor_lab/repair_runtime.py src/factor_lab/research_queue.py src/factor_lab/repair_playbooks.py src/factor_lab/repair_agent_engine.py
```

然后重启服务：

```bash
systemctl --user restart factor-lab-research-daemon factor-lab-web-ui
```

---

## 5. 推荐提交顺序

1. `test: add queue liveness classification coverage`
2. `fix: classify healthy idle separately from queue stall`
3. `fix: include queue liveness in repair runtime snapshot`
4. `fix: avoid reseed repair for healthy idle queues`
5. `test: cover baseline reseed diagnostics`
6. `fix: report baseline reseed skip reasons`
7. `fix: add refill fallback when baseline reseed is repeat-blocked`
8. `test: verify queue idle repair integration`

---

## 6. 完成后的预期状态

修复完成后，系统应该呈现：

- 如果只是任务跑完了：状态是 `healthy_idle`，不会报中等严重 `queue_stall`。
- 如果 baseline 被重复冷却挡住：状态是 `cooldown_idle`，action plan 能解释 repeat-blocked。
- 如果真的长时间没有注入/完成/heartbeat：状态是 `queue_stall`，Repair Agent 才执行 reseed/fallback。
- refill 不再只尝试固定 baseline；会在安全范围内尝试非重复扩展。
- Agent 运行观测更清楚：能区分“系统健康但暂时无任务”和“系统真的不推进”。
