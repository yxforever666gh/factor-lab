# Factor Lab 1.1 更新说明

发布日期：2026-05-16

## 重点

Factor Lab 1.1 把近期计划书中的安全约束继续收敛到“小机构化 / simulated paper research”主线：不恢复 broad daemon，不批量写入 workflow 队列，不开启实盘交易；优先把已有只读诊断、风险缺口和运行安全状态显式暴露出来，便于下一步人工复核和小步 TDD 迭代。

## 主要变化

### 1. 小机构化状态更透明

- 围绕 `small_institutional_value_sleeve_mvp` 继续固化 72 只持仓的纸面组合状态。
- 状态报告现在以只读方式呈现模拟组合风险约束诊断。
- 当前风险阻塞被明确表达为 `drawdown_risk_too_high` / `blocked_drawdown_gap`，避免把“没有安全候选”误判为可以继续自动扩张。

### 2. 模拟风险约束诊断

- 新增/完善最大回撤阈值差距诊断。
- 记录 best available drawdown、drawdown threshold、drawdown gap、candidate count 与 recommended safe next step。
- 当前基线：最佳可用最大回撤约 `-0.478256`，阈值 `-0.35`，缺口约 `0.128256`。
- 推荐下一步保持为 `tighten_simulation_risk_constraints_before_rerun`，而不是直接 rerun 或恢复自动化。

### 3. 运行时安全边界保持不变

- controlled runtime 仍保持 safe。
- controlled restart dry-run 目标状态仍是 `would_run_count=0`。
- 不恢复 broad daemon。
- 不批量 enqueue workflow。
- 不启用 live trading。
- 后续任何写队列或 daemon 恢复都需要新的计划书、dry-run 和明确验证。

### 4. de-OpenClaw 迁移与 agent 架构延续

- 继续保留旧 OpenClaw 风格的 agent-role 架构语义，而不是只替换 provider/model fallback。
- agent/provider/model/cache/cost 相关术语继续向精确化推进，避免把 agent role 与 provider profile 混为一谈。
- README、迁移完成文档、systemd、WebUI 与 LLM/provider 配置继续围绕 Hermes 接管后的运行形态整理。

### 5. 研究质量与纸面组合链路

- 继续把机制驱动、受控准入、bucket-aware OOS、paper portfolio、成本/换手、风险诊断等链路收敛到可审计工件。
- 当前方向仍是 simulated backtests 与 paper research，不进入交易/live-execution。

## 已知限制

- 当前 release 包含大量历史迁移、治理、WebUI、诊断和测试文件变更；`artifacts/`、本地 runtime 输出和知识库仍应谨慎区分，不应把运行垃圾重新纳入版本控制。
- 风险诊断显示尚无满足最大回撤阈值的安全候选，下一步应先收紧模拟风险约束或降低 sleeve aggressiveness，再考虑 bounded dry-run。
- full daemon restoration 仍不是本版本目标。

## 验证建议

推荐验证切片：

```bash
PYTHONPATH=src .venv/bin/python -m py_compile src/factor_lab/small_institutionalization_policy.py src/factor_lab/simulation_risk_constraint_diagnostics.py scripts/write_simulation_risk_constraint_diagnostics.py
PYTHONPATH=src .venv/bin/python -m pytest tests/test_small_institutionalization_policy.py tests/test_simulation_risk_constraint_diagnostics.py -q
PYTHONPATH=src .venv/bin/python scripts/write_simulation_risk_constraint_diagnostics.py
PYTHONPATH=src .venv/bin/python scripts/write_small_institutionalization_status.py
PYTHONPATH=src .venv/bin/python scripts/dry_run_controlled_restart.py
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_takeover.py
```

## 升级备注

- Python package version 更新到 `1.1.0`。
- Git release tag 使用 `1.1`。
