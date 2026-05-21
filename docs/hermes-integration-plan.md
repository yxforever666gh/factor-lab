# Factor Lab × Hermes 集成计划书

> **项目状态**：2026-04-29  
> **目标**：将 Factor Lab 改造为 Hermes 驱动的自动化量化因子研究系统

---

## 执行摘要

**当前状态**：
- Factor Lab 是一个完整的自动化量化研究系统（~38,676 行 Python 代码，165 个模块）
- 已有完整的 agent-role 架构（planner、reviewer、diagnostician、data_steward）
- 已有 LLM provider 路由层（支持 direct_model、mock、hermes_native_agent 等）
- 已有 research daemon、WebUI、workflow 引擎、回测评估、因子生成器
- 正在进行 de-HermesNative 迁移和运行态加固

**核心发现**：
Factor Lab **不需要大规模重构**。它已经是一个成熟的自动化研究系统，有清晰的：
- 数据层（Tushare provider + cache + feature store）
- 因子层（expression evaluator + factor families）
- 回测层（workflow engine + evaluation + portfolio）
- 决策层（Hermes profiles + LLM provider router）
- 调度层（research daemon + task queue）

**Hermes 的定位**：
Hermes 不应该"替换"Factor Lab 的内部架构，而应该作为：
1. **交互式研究助手**：用自然语言驱动 Factor Lab 的各个模块
2. **并行任务编排器**：用 delegate_task 加速因子挖掘
3. **代码生成器**：生成新的因子表达式、配置文件
4. **结果分析器**：解读回测结果、提供优化建议
5. **运维助手**：监控 daemon、诊断问题、生成报告

---

## 架构定位

### Factor Lab 保持的职责

```
┌─────────────────────────────────────────────────────────────┐
│                      Factor Lab Core                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Data Layer   │  │ Factor Layer │  │ Backtest     │      │
│  │ - Tushare    │  │ - Evaluator  │  │ - Workflow   │      │
│  │ - Cache      │  │ - Families   │  │ - Portfolio  │      │
│  │ - Feature    │  │ - Generator  │  │ - Evaluation │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Agent Roles  │  │ LLM Router   │  │ Daemon       │      │
│  │ - Planner    │  │ - Providers  │  │ - Queue      │      │
│  │ - Reviewer   │  │ - Fallback   │  │ - Scheduler  │      │
│  │ - Analyst    │  │ - Pricing    │  │ - Heartbeat  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Hermes 新增的能力

```
┌─────────────────────────────────────────────────────────────┐
│                    Hermes Integration Layer                  │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Natural Language Interface                          │   │
│  │  "帮我测试动量因子，窗口 5-20 天"                    │   │
│  │  "分析一下为什么 mom_60 表现下降了"                  │   │
│  │  "生成一个结合价值和质量的新因子"                    │   │
│  └──────────────────────────────────────────────────────┘   │
│                           ↓                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Skill: factor-lab-mining                            │   │
│  │  - 解析用户意图                                      │   │
│  │  - 生成因子代码/配置                                 │   │
│  │  - 调用 Factor Lab CLI/API                           │   │
│  │  - 解析结果并给出建议                                │   │
│  └──────────────────────────────────────────────────────┘   │
│                           ↓                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Tools                                               │   │
│  │  - terminal: 运行 workflow                           │   │
│  │  - file: 读写配置/结果                               │   │
│  │  - execute_code: 数据分析/因子生成                   │   │
│  │  - delegate_task: 并行回测                           │   │
│  │  - memory: 记录好因子/坏因子经验                     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 集成策略

### 方案选择：轻量集成（推荐）

**不做**：
- ❌ 替换 Factor Lab 的 hermes_profile_settings 层
- ❌ 替换 hermes_decision_router
- ❌ 重写 workflow 引擎
- ❌ 改造 research daemon

**要做**：
- ✅ 创建 Hermes skill 作为"外部调度层"
- ✅ 提供简化的 CLI 入口（可选，现有脚本已够用）
- ✅ 用 Hermes 的工具能力补足 Factor Lab 的交互短板
- ✅ 保持 Factor Lab 独立运行能力

### 为什么选择轻量集成

1. **Factor Lab 已经很完整**：
   - 有完整的 agent 决策系统
   - 有自动化 daemon 和任务队列
   - 有 WebUI 监控和控制
   - 正在进行系统性加固（见 `2026-04-28-factor-lab-hardening-plan.md`）

2. **Hermes 的优势在交互和编排**：
   - 自然语言理解
   - 代码生成（因子表达式）
   - 并行任务（delegate_task）
   - 结果分析和建议

3. **避免重复建设**：
   - Factor Lab 的 planner agent 已经能生成研究计划
   - Factor Lab 的 candidate_generator 已经能生成因子
   - 不需要用 Hermes 重新实现这些

---

## 实施计划

### Phase 1: 创建 Hermes Skill（1-2 天）

#### Task 1.1: 编写 skill 文档

**文件**：`~/.hermes/skills/factor-lab/SKILL.md`

**内容结构**：
```yaml
---
name: factor-lab
description: 自动化量化因子研究与回测系统集成
triggers:
  - "因子"
  - "回测"
  - "量化"
  - "Factor Lab"
tools: [terminal, file, execute_code, delegate_task, memory]
---

# Factor Lab Integration

## 项目信息
- 路径: ~/factor-lab
- 配置: ~/factor-lab/configs/
- 结果: ~/factor-lab/artifacts/
- WebUI: http://127.0.0.1:8765

## 核心能力

### 1. 运行 Workflow
cd ~/factor-lab && python3 scripts/run_first_workflow.py
cd ~/factor-lab && python3 scripts/run_tushare_workflow.py

### 2. 查看结果
sqlite3 ~/factor-lab/artifacts/factor_lab.db "SELECT * FROM runs ORDER BY created_at DESC LIMIT 5"

### 3. 生成因子配置
创建 JSON 配置文件，定义 factors 数组

### 4. 启动/检查 Daemon
systemctl --user status factor-lab-research-daemon
journalctl --user -u factor-lab-research-daemon -n 50

### 5. WebUI 监控
curl http://127.0.0.1:8765/health
curl http://127.0.0.1:8765/runs

## 工作流模式

### 模式 A: 单因子快速测试
1. 生成因子表达式
2. 创建临时配置文件
3. 运行 workflow
4. 解析结果
5. 给出建议

### 模式 B: 批量并行测试
1. 生成多个因子变体
2. 用 delegate_task 并行运行
3. 汇总结果
4. 排序并推荐最优

### 模式 C: 研究计划执行
1. 读取 daemon 生成的计划
2. 检查任务队列状态
3. 必要时手动触发特定任务
4. 监控执行进度

### 模式 D: 结果分析与诊断
1. 读取 SQLite 数据库
2. 分析因子表现趋势
3. 识别失败原因
4. 提供优化建议

## 因子表达式语法

支持的字段（Tushare 数据）：
- momentum_20, momentum_60, momentum_120
- earnings_yield, book_yield, pb
- roe, turnover_shock_5_20
- 等（见 data.py 和 tushare_provider.py）

支持的操作：
- 加减乘除: +, -, *, /
- 示例: "roe - pb", "momentum_20 + earnings_yield"

## 注意事项

1. **Tushare 频率限制**：
   - 优先复用缓存
   - 检查 artifacts/tushare_cache/
   - 避免短时间大量请求

2. **Workflow 时间**：
   - 单个 workflow 可能需要几分钟
   - 并行任务建议 <= 4 个

3. **Daemon 状态**：
   - 检查 artifacts/research_daemon_heartbeat.json
   - 确认 daemon 正在运行且健康

4. **数据时间对齐**：
   - 注意未来函数问题
   - 确认数据可得性

## 记忆积累

用 memory 工具记录：
- 哪些因子类型效果好
- 哪些参数范围值得探索
- 哪些组合容易失败
- 数据/配置的坑
```

#### Task 1.2: 创建辅助脚本（可选）

如果现有脚本不够用，可以创建：

**文件**：`~/.hermes/skills/factor-lab/scripts/quick_backtest.py`

```python
#!/usr/bin/env python3
"""快速回测单个因子"""
import json
import sys
from pathlib import Path

def main():
    factor_name = sys.argv[1]
    expression = sys.argv[2]
    
    config = {
        "data_source": "tushare",
        "start_date": "2020-01-01",
        "end_date": "2023-12-31",
        "universe_limit": 80,
        "factors": [
            {"name": factor_name, "expression": expression}
        ]
    }
    
    config_path = Path(f"~/factor-lab/artifacts/temp_{factor_name}.json").expanduser()
    config_path.write_text(json.dumps(config, indent=2))
    
    print(f"Config written to {config_path}")
    print(f"Run: cd ~/factor-lab && python3 scripts/run_tushare_workflow.py")

if __name__ == "__main__":
    main()
```

#### Task 1.3: 验证 skill 加载

```bash
# 在 Hermes 中测试
hermes skill_view factor-lab
```

---

### Phase 2: 基础交互测试（半天）

#### Task 2.1: 单因子测试

**用户输入**：
```
帮我测试一个动量因子，窗口 10 天
```

**Hermes 执行流程**：
1. 加载 factor-lab skill
2. 检查数据缓存：
   ```python
   execute_code("""
   from pathlib import Path
   cache_dir = Path.home() / 'factor-lab/artifacts/tushare_cache'
   files = list(cache_dir.glob('*.csv'))
   print(f"Found {len(files)} cache files")
   for f in files[:3]:
       print(f"  {f.name}")
   """)
   ```
3. 生成配置文件：
   ```python
   write_file(
       path="~/factor-lab/artifacts/test_mom_10.json",
       content=json.dumps({
           "data_source": "tushare",
           "start_date": "2020-01-01",
           "end_date": "2023-12-31",
           "universe_limit": 80,
           "factors": [
               {"name": "mom_10", "expression": "momentum_10"}
           ]
       }, indent=2)
   )
   ```
4. 运行 workflow：
   ```bash
   terminal("cd ~/factor-lab && python3 -c \"
   import sys
   sys.path.insert(0, 'src')
   from factor_lab.workflow import run_workflow
   run_workflow(
       config_path='artifacts/test_mom_10.json',
       output_dir='artifacts/test_mom_10_output'
   )
   \"")
   ```
5. 读取结果：
   ```python
   result = read_file("~/factor-lab/artifacts/test_mom_10_output/results.json")
   ```
6. 分析并回复用户

**验收标准**：
- 能成功运行 workflow
- 能解析结果
- 给出有意义的建议

#### Task 2.2: 批量并行测试

**用户输入**：
```
帮我测试动量因子，窗口从 5 到 20 天，每 5 天一个
```

**Hermes 执行流程**：
1. 生成 4 个配置文件（mom_5, mom_10, mom_15, mom_20）
2. 使用 delegate_task 并行运行：
   ```python
   delegate_task(
       tasks=[
           {
               "goal": "回测 momentum_5d",
               "context": "配置文件: ~/factor-lab/artifacts/test_mom_5.json",
               "toolsets": ["terminal", "file"]
           },
           # ... 其他 3 个
       ]
   )
   ```
3. 汇总结果
4. 排序并推荐最优窗口

**验收标准**：
- 4 个任务并行执行
- 结果正确汇总
- 给出最优推荐

---

### Phase 3: 高级功能（1-2 天）

#### Task 3.1: 因子代码生成

**用户输入**：
```
帮我生成一个结合动量和价值的因子
```

**Hermes 执行流程**：
1. 分析需求
2. 生成因子表达式：
   ```python
   # 方案 1: 简单组合
   expression = "momentum_20 + earnings_yield"
   
   # 方案 2: 加权组合
   expression = "momentum_20 * 0.6 + earnings_yield * 0.4"
   
   # 方案 3: 差值
   expression = "momentum_20 - earnings_yield"
   ```
3. 创建配置并测试
4. 比较多个方案

#### Task 3.2: 结果深度分析

**用户输入**：
```
分析一下为什么 mom_60 最近表现下降了
```

**Hermes 执行流程**：
1. 读取历史数据：
   ```python
   execute_code("""
   import sqlite3
   conn = sqlite3.connect('~/factor-lab/artifacts/factor_lab.db')
   cursor = conn.cursor()
   cursor.execute('''
       SELECT created_at, factor_name, rank_ic_mean, sharpe_net
       FROM factor_evaluations
       WHERE factor_name LIKE '%mom_60%'
       ORDER BY created_at DESC
       LIMIT 20
   ''')
   for row in cursor.fetchall():
       print(row)
   """)
   ```
2. 分析趋势
3. 检查市场环境变化
4. 给出可能原因和建议

#### Task 3.3: Daemon 监控与诊断

**用户输入**：
```
检查一下 Factor Lab daemon 的状态
```

**Hermes 执行流程**：
1. 检查服务状态：
   ```bash
   terminal("systemctl --user status factor-lab-research-daemon")
   ```
2. 读取 heartbeat：
   ```python
   heartbeat = read_file("~/factor-lab/artifacts/research_daemon_heartbeat.json")
   ```
3. 检查任务队列：
   ```python
   execute_code("""
   import sqlite3
   conn = sqlite3.connect('~/factor-lab/artifacts/factor_lab.db')
   cursor = conn.cursor()
   cursor.execute('SELECT status, COUNT(*) FROM tasks GROUP BY status')
   for row in cursor.fetchall():
       print(f"{row[0]}: {row[1]}")
   """)
   ```
4. 给出健康报告

---

### Phase 4: 记忆与优化（持续）

#### Task 4.1: 建立因子知识库

用 memory 工具记录：

```python
memory(
    action="add",
    target="memory",
    content="动量因子在 A 股市场：最优窗口 10-15 天，Sharpe 通常 1.5-2.0，避免窗口 < 5 天（噪音大）"
)

memory(
    action="add",
    target="memory",
    content="价值因子：earnings_yield 比 book_yield 更稳定，但需要注意财报发布时间"
)

memory(
    action="add",
    target="memory",
    content="Factor Lab Tushare 缓存策略：优先复用大窗口缓存，避免重复拉取"
)
```

#### Task 4.2: 自动化报告生成

定期生成研究报告：

```python
execute_code("""
import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('~/factor-lab/artifacts/factor_lab.db')
cursor = conn.cursor()

# 最近 7 天的因子表现
week_ago = (datetime.now() - timedelta(days=7)).isoformat()
cursor.execute('''
    SELECT factor_name, AVG(rank_ic_mean) as avg_ic, AVG(sharpe_net) as avg_sharpe
    FROM factor_evaluations
    WHERE created_at > ?
    GROUP BY factor_name
    ORDER BY avg_sharpe DESC
    LIMIT 10
''', (week_ago,))

print("# 本周因子表现 Top 10\\n")
for row in cursor.fetchall():
    print(f"- {row[0]}: IC={row[1]:.3f}, Sharpe={row[2]:.2f}")
""")
```

---

## 验收标准

### 功能验收

- [ ] Hermes 能通过自然语言触发因子回测
- [ ] 支持单因子和批量并行测试
- [ ] 能生成因子表达式和配置文件
- [ ] 能解析回测结果并给出建议
- [ ] 能监控 daemon 状态
- [ ] 能分析历史数据和趋势
- [ ] 好因子/坏因子经验记录到 memory

### 性能验收

- [ ] 单因子回测 < 5 分钟（取决于数据量）
- [ ] 并行 4 个因子不超过单个的 1.5 倍时间
- [ ] Hermes 响应时间 < 10 秒（不含 workflow 执行）

### 质量验收

- [ ] 生成的配置文件格式正确
- [ ] 因子表达式语法正确
- [ ] 结果解析准确
- [ ] 建议有实际价值

---

## 风险与注意事项

### 1. Tushare 频率限制
- **风险**：短时间大量请求可能被限流
- **缓解**：优先复用缓存，检查 `artifacts/tushare_cache/`

### 2. Workflow 执行时间
- **风险**：单个 workflow 可能需要几分钟
- **缓解**：用 background=true 运行长任务，用 process() 监控进度

### 3. 数据质量
- **风险**：未来函数、缺失数据
- **缓解**：遵循 Factor Lab 的数据对齐策略（见 `docs/data-availability-policy.md`）

### 4. 并发控制
- **风险**：过多并行任务可能导致资源竞争
- **缓解**：限制并行数 <= 4

### 5. 与现有 Daemon 的协调
- **风险**：Hermes 手动触发的任务可能与 daemon 自动任务冲突
- **缓解**：
  - 手动任务使用独立的输出目录
  - 检查 daemon 状态再执行
  - 必要时暂停 daemon

---

## 后续扩展方向

### 短期（1-2 周）
- 支持更多因子类型（技术指标、基本面、另类数据）
- 自动化参数优化（网格搜索、贝叶斯优化）
- 因子组合优化

### 中期（1-2 月）
- 实时监控和告警
- 自动化研究报告生成
- 因子库管理和版本控制

### 长期（3-6 月）
- 多策略组合
- 风险管理和仓位控制
- 实盘模拟和回测对比

---

## 总结

这个计划的核心思想是：

1. **保持 Factor Lab 的完整性**：不破坏现有架构
2. **发挥 Hermes 的优势**：自然语言交互、代码生成、并行编排
3. **轻量集成**：通过 skill 和工具调用，而非深度耦合
4. **渐进式推进**：先基础功能，再高级特性
5. **持续优化**：通过 memory 积累经验

这样做的好处：
- Factor Lab 保持独立运行能力
- Hermes 提供更好的用户体验
- 两者各司其职，耦合度低
- 易于维护和扩展
