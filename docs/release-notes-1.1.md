# Factor Lab 1.1 更新说明

发布日期：2026-05-16
Git tag：`1.1`
Python package version：`1.1.0`

## 一句话总结

Factor Lab 1.1 不是一次“扩大自动化”的版本，而是一次把 Factor Lab 从旧 HermesNative 运行形态迁移到 Hermes 接管后、继续收紧为“小机构化 / simulated paper research”的安全版本：保留 agent-role 架构语义，明确 provider/model/cost/cache 边界；同时把 controlled runtime、bucket-aware OOS、paper portfolio、风险诊断和状态报告串成可审计链路，确保后续每一步都先计划、先 dry-run、先验证，再决定是否写队列或恢复运行。

## 发布背景

近期计划书的共同约束非常明确：

- 不恢复 broad daemon。
- 不批量 enqueue workflow。
- 不开启 live trading / 实盘交易。
- 不把“服务 active”误判为“研究运行安全”。
- 不把“没有安全候选”误判为“可以继续自动扩张”。
- 继续沿用 plan → tests/diagnostics → verification → plan-doc update 的节奏。

因此 1.1 的重点不是追求更多自动任务，而是把已经落地的治理、诊断、迁移和 paper research 能力整理成一个稳定基线，便于下一轮小步 TDD 继续推进。

## 主要变化

### 1. de-HermesNative 迁移进入可发布基线

1.1 延续并固化了 de-HermesNative takeover 的核心目标：Factor Lab 不只是替换 LLM provider 或 model fallback，而是保留旧 HermesNative 风格的 agent-role 架构语义。

本版本重点整理了以下边界：

- Hermes profile 与 provider/profile 分开表达，避免把“谁负责什么任务”和“调用哪个模型服务”混为一谈。
- base_url、endpoint path、provider profile、model label、pricing label 等术语继续精确化。
- LLM cost/cache 字段继续向 provider 真实返回能力对齐，避免用错误模型名或旧 HermesNative 假设污染成本统计。
- README、迁移完成文档、systemd、WebUI 与 LLM/provider 配置围绕 Hermes 接管后的运行形态继续收敛。

这意味着 1.1 的迁移完成标准不是“能调模型”这么简单，而是旧的多角色研究系统在 Hermes 语境下仍然能被清晰管理、诊断和审计。

### 2. controlled runtime 安全边界保持收紧

1.1 继续坚持 controlled-only 的运行模型：

`controlled admission feeder -> admitted workflow -> controlled-only executor`

但当前发布基线仍然不把 broad daemon 恢复作为目标。最近计划书和验证切片反复强调：

- controlled restart dry-run 的安全目标状态是 `would_run_count=0`。
- 如果没有 claimable admitted workflow，daemon 空闲不是故障。
- 如果 runtime audit 继续建议 `pause_daemon`，就不应为了“看起来在运行”而恢复 broad daemon。
- 后续任何写队列、systemd timer 扩频、daemon 恢复，都必须有新的计划书、dry-run 和明确验证。

这个版本把“运行安全”从服务存活提升为可解释状态：是否有 admitted workflow、是否有旧路径污染、是否有 missing mechanism/data blocker、是否需要继续 pause broad runtime。

### 3. bucket-aware OOS 与研究质量链路继续保留

1.1 保留并整理了前一阶段的重要研究质量治理成果：

- 机制驱动的 value route，而不是盲目复用 raw `book_yield` / `earnings_yield` 算术组合。
- bucket-aware OOS policy，用真实 bucket-pair 诊断替代 naive top-bottom split mismatch。
- controlled run ledger 与 route policy，把受控 workflow 输出反馈到后续 route selection。
- duplicate / finished-equivalent evidence suppression，避免重复跑同一类证据。
- research quality summary，把 mechanism lessons、data blockers、research waste 等信息沉淀到 artifacts 与 knowledge。

当前记忆中的稳定基线是：controlled-only daemon/feeder 安全；bucket-aware OOS policy 已经通过 `bucket_aware_oos_stable` 处理 raw split mismatch；industry、value_momentum、value_quality 路线曾分别达到较高 pass rate；完整测试套件曾达到 509 passed。1.1 以这些治理能力作为安全底座，而不是退回旧的 broad generated search。

### 4. 小机构化 / paper research 成为当前主线

1.1 把近期“小机构化”计划继续固化为默认研究方向：

- 当前策略模式：`long_only_equity_enhancement`。
- 当前纸面组合：`small_institutional_value_sleeve_mvp`。
- 当前持仓数量：72。
- 当前 benchmark 诊断：CSI1000 / 中证1000。
- 当前 turnover one-way 估计：`0.0`。
- 当前 estimated round-trip cost：`0.0`。
- 当前工作范围：simulated backtests 与 paper research，不进入 live execution。

这条线的意义是把 Factor Lab 从“自动找因子”推进到“小型机构研究流程雏形”：有组合、有 benchmark、有成本和换手诊断、有 weekly/paper monitoring report skeleton、有状态门禁、有风险 blocker，而不是只看单个因子 IC。

### 5. weekly / paper monitoring report 链路补齐

近期计划书要求在 benchmark/cost/turnover diagnostics 之后，生成面向人工复核的 weekly paper monitoring report skeleton。1.1 的发布说明把这部分纳入正式变更范围：

- paper monitoring report 不触发 queue write。
- weekly report present 后，状态可以进入当前安全 monitoring / simulation next step。
- 报告应覆盖 portfolio、change、cost、benchmark、blocker、observation window 等核心段落。
- 这类报告的目标是帮助人工判断，而不是自动授权 rerun 或 daemon 恢复。

换句话说，1.1 的“报告”不是装饰性文档，而是小机构化流程里的人工复核界面。

### 6. simulated portfolio construction repair 更保守

近期计划书中的一个核心 blocker 是：模拟组合构造修复仍然报告 `blocked_no_drawdown_safe_candidate`，同时 self-diagnosis 报告 `drawdown_risk_too_high`。

1.1 对这类修复逻辑的边界进行了明确：

- 只读读取已有 bounded matrix / simulation outputs。
- 候选排序优先看 max drawdown，再用 Sharpe / return 作为 tie-breaker。
- 如果没有候选满足 drawdown threshold，报告必须保持 blocked，而不是自动挑一个“相对最好”的候选继续跑。
- 即使未来找到 candidate，也应保持 `automation_allowed=false`，除非后续计划书明确批准 bounded dry-run。

这个设计避免了一个常见危险：把“修复算法找到了一个较不差的组合”误认为“风险已经可接受”。

### 7. 新增/完善模拟风险约束诊断

1.1 的近期计划重点落在 simulation risk constraint diagnostics：把 drawdown gap 从隐含问题变成显式状态。

当前诊断基线：

- diagnostic status：`blocked_drawdown_gap`
- best available max drawdown：约 `-0.478256`
- drawdown threshold：`-0.35`
- drawdown gap：约 `0.128256`
- repair report：`blocked_no_drawdown_safe_candidate`
- candidate_count：`0`
- self-diagnosis：`drawdown_risk_too_high`
- automation_allowed：`false`
- recommended safe next step：`tighten_simulation_risk_constraints_before_rerun`

这组字段的价值在于：它解释了为什么当前不应该直接 rerun，也不应该恢复自动化。最佳候选距离阈值仍有明显缺口，因此下一步应先收紧风险约束、降低 sleeve aggressiveness，或准备一个明确受限的人工 dry-run 计划。

### 8. 小机构化状态现在暴露风险 blocker

最新计划书要求把已有只读 risk constraint diagnostics 暴露到 small institutionalization status 中。1.1 将其作为发布说明的核心内容之一：

- 当 `artifacts/small_institutional_simulation/risk_constraint_diagnostics.json` 存在时，status 应展示 compact risk diagnostics section。
- 关键字段包括 diagnostic_status、best_available_max_drawdown、drawdown_threshold、drawdown_gap、recommended_safe_next_step、automation_allowed。
- missing artifact 行为保持兼容。
- status 不因为诊断存在就推进到 rerun。
- `next_action` 仍保持安全，继续偏向 manual review / risk tightening。

这样，用户打开状态报告时能直接看到“卡在哪里”和“为什么不能自动推进”，而不是只看到一个模糊的 next_action。

## 安全不变量

1.1 发布基线明确保留以下不变量：

- 不恢复 broad daemon。
- 不批量写入 workflow 队列。
- 不启用 live trading。
- 不把 artifacts/runtime 垃圾纳入版本控制。
- 不把旧 generated/recent/rolling 路径重新放进 controlled runtime。
- 不让 bucket-aware controlled workflow 自动生成 generic follow-up 污染。
- 不把 drawdown 仍不达标的组合推进到自动执行。
- 不把 provider/model fallback 问题描述成 agent-role 架构已经完成。

这些不变量比单次指标更重要，因为它们决定了 Factor Lab 之后能不能稳定地像“小研究机构流程”一样迭代。

## 已知限制

- 当前风险诊断仍显示没有满足最大回撤阈值的安全候选。
- `best_available_max_drawdown≈-0.478256` 与 `threshold=-0.35` 之间仍有约 `0.128256` 的缺口。
- full daemon restoration 仍不是 1.1 目标。
- paper portfolio 当前仍是 simulated / paper research，不代表可交易组合。
- 72 只持仓、CSI1000 benchmark、0.0 turnover/cost 当前更像状态基线；后续仍需继续验证真实调仓、成本、滑点、风险暴露和容量假设。
- `artifacts/`、本地 runtime 输出、knowledge 文件和 release 文档需要继续谨慎区分；提交时不要 broad `git add -A`。

## 建议验证切片

推荐在 1.1 tag 附近使用以下验证切片复核：

```bash
PYTHONPATH=src .venv/bin/python -m py_compile \
  src/factor_lab/small_institutionalization_policy.py \
  src/factor_lab/simulation_risk_constraint_diagnostics.py \
  src/factor_lab/simulated_portfolio_construction_repair.py \
  src/factor_lab/paper_monitoring_report.py \
  scripts/write_simulation_risk_constraint_diagnostics.py \
  scripts/write_simulated_portfolio_construction_repair.py \
  scripts/write_paper_monitoring_report.py

PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_small_institutionalization_policy.py \
  tests/test_simulation_risk_constraint_diagnostics.py \
  tests/test_simulated_portfolio_construction_repair.py \
  tests/test_paper_monitoring_report.py \
  tests/test_paper_portfolio_diagnostics.py \
  tests/test_paper_portfolio.py -q

PYTHONPATH=src .venv/bin/python scripts/write_simulation_risk_constraint_diagnostics.py
PYTHONPATH=src .venv/bin/python scripts/write_simulated_portfolio_construction_repair.py
PYTHONPATH=src .venv/bin/python scripts/write_paper_monitoring_report.py --next-observation-window weekly
PYTHONPATH=src .venv/bin/python scripts/write_small_institutionalization_status.py
PYTHONPATH=src .venv/bin/python scripts/dry_run_controlled_restart.py
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_takeover.py
```

期望结果：

- risk diagnostics JSON/MD 存在。
- small institutionalization status 能显示风险缺口。
- status 仍保持 runtime safe。
- dry-run 仍为 `would_run_count=0`。
- 没有 workflow queue write。
- 没有 broad daemon restore。
- 没有 live trading enablement。

## 升级备注

- Python package version 更新到 `1.1.0`。
- Git release tag 使用 `1.1`。
- 如果远端已有旧 `1.1` tag，应确认它指向本 release commit；必要时在本地重新指向 release commit 后再推送 tag。
- 当前本地存在 `knowledge/small_institutionalization.md` 生成时间戳变化和未跟踪 `artifacts/`，发布提交不应自动包含这些 runtime 输出，除非另有明确计划。

## 下一步建议

1. 先复核 1.1 tag 对应 commit 与 release notes 是否一致。
2. 如果继续推进，下一份计划书应围绕 `tighten_simulation_risk_constraints_before_rerun` 或 `reduce_sleeve_aggressiveness_before_rerun` 展开。
3. 若要做 bounded rerun，必须先写计划书，列出 dry-run、候选约束、风险阈值、预期 artifact 和回滚条件。
4. 在 drawdown blocker 消除前，不建议恢复 broad daemon、提高 feeder 频率或进入 live execution。
