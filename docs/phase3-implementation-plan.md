# Phase 3: Factor Lab 高级功能实施计划

> **For Hermes:** 按顺序执行每个任务，每个任务完成后验证再继续。

**Goal:** 为 Factor Lab × Hermes 集成添加高级功能：因子代码生成、Daemon 监控、结果可视化

**Architecture:** 在现有 Hermes skill 基础上，添加辅助脚本和模板，增强交互能力和自动化程度

**Tech Stack:** Python, Hermes tools (execute_code, terminal, memory), Factor Lab workflow API

---

## Task 1: 因子代码生成器

**Objective:** 创建一个能根据自然语言描述生成因子表达式的工具

**Files:**
- Create: `~/.hermes/skills/factor-lab/scripts/generate_factor.py`
- Modify: `~/.hermes/skills/factor-lab/SKILL.md` (添加使用说明)

**Step 1: 创建因子生成脚本**

```python
#!/usr/bin/env python3
"""
根据描述生成因子表达式
支持的因子类型：
- 组合因子：将多个基础因子组合
- 变换因子：对基础因子进行数学变换
- 反转因子：对因子取负值
"""
import sys
import json

# 可用的基础字段
AVAILABLE_FIELDS = [
    'momentum_20', 'earnings_yield', 'book_yield', 'pb', 
    'turnover_shock_5_20', 'size_inv', 'pe_ttm', 'total_mv'
]

def generate_combination_factor(factors, weights=None):
    """生成组合因子"""
    if weights is None:
        weights = [1.0 / len(factors)] * len(factors)
    
    terms = [f"{w} * {f}" for w, f in zip(weights, factors)]
    return " + ".join(terms)

def generate_reversal_factor(factor):
    """生成反转因子"""
    return f"-({factor})"

def generate_ratio_factor(numerator, denominator):
    """生成比率因子"""
    return f"{numerator} / {denominator}"

def generate_difference_factor(factor1, factor2):
    """生成差值因子"""
    return f"{factor1} - {factor2}"

def main():
    if len(sys.argv) < 2:
        print("Usage: generate_factor.py <type> [args...]")
        print("Types:")
        print("  combine <factor1> <factor2> [factor3...] - 等权组合")
        print("  reverse <factor> - 反转因子")
        print("  ratio <numerator> <denominator> - 比率因子")
        print("  diff <factor1> <factor2> - 差值因子")
        sys.exit(1)
    
    factor_type = sys.argv[1]
    
    if factor_type == "combine":
        factors = sys.argv[2:]
        expression = generate_combination_factor(factors)
        name = "_".join(factors)
    elif factor_type == "reverse":
        factor = sys.argv[2]
        expression = generate_reversal_factor(factor)
        name = f"rev_{factor}"
    elif factor_type == "ratio":
        num, denom = sys.argv[2], sys.argv[3]
        expression = generate_ratio_factor(num, denom)
        name = f"{num}_over_{denom}"
    elif factor_type == "diff":
        f1, f2 = sys.argv[2], sys.argv[3]
        expression = generate_difference_factor(f1, f2)
        name = f"{f1}_minus_{f2}"
    else:
        print(f"Unknown type: {factor_type}")
        sys.exit(1)
    
    result = {
        "name": name,
        "expression": expression,
        "type": factor_type
    }
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
```

**Step 2: 测试因子生成器**

```bash
cd ~/.hermes/skills/factor-lab/scripts
chmod +x generate_factor.py

# 测试组合因子
python3 generate_factor.py combine earnings_yield book_yield

# 测试反转因子
python3 generate_factor.py reverse momentum_20

# 测试比率因子
python3 generate_factor.py ratio earnings_yield pb

# 测试差值因子
python3 generate_factor.py diff earnings_yield book_yield
```

**Expected Output:**
```json
{
  "name": "earnings_yield_book_yield",
  "expression": "0.5 * earnings_yield + 0.5 * book_yield",
  "type": "combine"
}
```

---

## Task 2: Daemon 监控脚本

**Objective:** 创建一个能检查 Factor Lab daemon 状态的脚本

**Files:**
- Create: `~/.hermes/skills/factor-lab/scripts/check_daemon.py`

**Step 1: 创建监控脚本**

```python
#!/usr/bin/env python3
"""检查 Factor Lab research daemon 状态"""
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

def check_systemd_status():
    """检查 systemd 服务状态"""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "factor-lab-research-daemon"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.stdout.strip()
    except Exception as e:
        return f"error: {e}"

def read_heartbeat():
    """读取 daemon heartbeat"""
    heartbeat_path = Path.home() / "factor-lab/artifacts/research_daemon_heartbeat.json"
    if not heartbeat_path.exists():
        return None
    
    try:
        with open(heartbeat_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}

def check_queue_status():
    """检查任务队列状态"""
    # 这里简化处理，实际应该查询数据库
    return {
        "pending": "N/A",
        "running": "N/A",
        "note": "需要查询 SQLite 数据库获取详细信息"
    }

def main():
    print("=" * 60)
    print("Factor Lab Daemon 状态检查")
    print("=" * 60)
    
    # 1. Systemd 状态
    status = check_systemd_status()
    print(f"\n【Systemd 服务】")
    print(f"  状态: {status}")
    
    # 2. Heartbeat
    heartbeat = read_heartbeat()
    print(f"\n【Heartbeat】")
    if heartbeat is None:
        print("  ⚠ 未找到 heartbeat 文件")
    elif "error" in heartbeat:
        print(f"  ✗ 读取失败: {heartbeat['error']}")
    else:
        print(f"  时间戳: {heartbeat.get('timestamp', 'N/A')}")
        print(f"  PID: {heartbeat.get('pid', 'N/A')}")
        print(f"  Provider: {heartbeat.get('provider', 'N/A')}")
        if 'queue' in heartbeat:
            q = heartbeat['queue']
            print(f"  队列:")
            print(f"    - Pending: {q.get('pending', 'N/A')}")
            print(f"    - Running: {q.get('running', 'N/A')}")
    
    # 3. 队列状态
    queue = check_queue_status()
    print(f"\n【任务队列】")
    print(f"  {queue.get('note', 'N/A')}")
    
    print("\n" + "=" * 60)
    
    # 返回状态码
    if status == "active":
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
```

**Step 2: 测试监控脚本**

```bash
cd ~/.hermes/skills/factor-lab/scripts
chmod +x check_daemon.py
python3 check_daemon.py
```

**Expected Output:**
```
============================================================
Factor Lab Daemon 状态检查
============================================================

【Systemd 服务】
  状态: active

【Heartbeat】
  时间戳: 2026-04-29T04:00:00+08:00
  PID: 12345
  Provider: real_llm
  队列:
    - Pending: 0
    - Running: 1

【任务队列】
  需要查询 SQLite 数据库获取详细信息

============================================================
```

---

## Task 3: 结果对比工具

**Objective:** 创建一个能对比多个因子回测结果的脚本

**Files:**
- Create: `~/.hermes/skills/factor-lab/scripts/compare_results.py`

**Step 1: 创建对比脚本**

```python
#!/usr/bin/env python3
"""对比多个因子的回测结果"""
import json
import sys
from pathlib import Path
from typing import List, Dict

def load_result(result_path: str) -> Dict:
    """加载单个结果文件"""
    path = Path(result_path).expanduser()
    if not path.exists():
        return {"error": f"文件不存在: {result_path}"}
    
    try:
        with open(path, 'r') as f:
            data = json.load(f)
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return data
    except Exception as e:
        return {"error": str(e)}

def compare_factors(result_paths: List[str]):
    """对比多个因子"""
    results = []
    for path in result_paths:
        result = load_result(path)
        if "error" not in result:
            results.append(result)
    
    if not results:
        print("没有有效的结果文件")
        return
    
    # 排序：按 sharpe_net 降序
    results.sort(key=lambda x: x.get('sharpe_net', -999), reverse=True)
    
    # 打印表格
    print("=" * 100)
    print(f"{'因子名称':<25} {'Rank IC':<12} {'IC IR':<12} {'Sharpe(净)':<12} {'年化收益':<12} {'通过':<8}")
    print("-" * 100)
    
    for r in results:
        name = r.get('factor_name', 'N/A')
        rank_ic = r.get('rank_ic_mean', 0)
        ic_ir = r.get('rank_ic_ir', 0)
        sharpe = r.get('sharpe_net', 0)
        annual_ret = r.get('net_return_annual', 0)
        passed = "✓" if r.get('pass_gate', False) else "✗"
        
        print(f"{name:<25} {rank_ic:<12.6f} {ic_ir:<12.6f} {sharpe:<12.4f} {annual_ret*100:<11.2f}% {passed:<8}")
    
    print("=" * 100)
    
    # 最佳因子
    best = results[0]
    print(f"\n🏆 最佳因子: {best.get('factor_name')}")
    print(f"   Sharpe (净): {best.get('sharpe_net', 0):.4f}")
    print(f"   年化收益: {best.get('net_return_annual', 0)*100:.2f}%")

def main():
    if len(sys.argv) < 2:
        print("Usage: compare_results.py <result1.json> <result2.json> ...")
        sys.exit(1)
    
    result_paths = sys.argv[1:]
    compare_factors(result_paths)

if __name__ == "__main__":
    main()
```

**Step 2: 测试对比工具**

```bash
cd ~/.hermes/skills/factor-lab/scripts
chmod +x compare_results.py

# 对比之前的测试结果
python3 compare_results.py \
  ~/factor-lab/artifacts/parallel_test_earnings_output/results.json \
  ~/factor-lab/artifacts/parallel_test_book_output/results.json \
  ~/factor-lab/artifacts/parallel_test_turnover_output/results.json
```

---

## Task 4: 更新 Skill 文档

**Objective:** 将新工具添加到 skill 文档中

**Files:**
- Modify: `~/.hermes/skills/factor-lab/SKILL.md`

**Step 1: 添加工具说明**

在 SKILL.md 的 "## 核心能力" 部分后添加：

```markdown
## 高级工具

### 1. 因子代码生成器
```bash
# 组合因子
python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py combine earnings_yield book_yield

# 反转因子
python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py reverse momentum_20

# 比率因子
python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py ratio earnings_yield pb

# 差值因子
python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py diff earnings_yield book_yield
```

### 2. Daemon 监控
```bash
python3 ~/.hermes/skills/factor-lab/scripts/check_daemon.py
```

### 3. 结果对比
```bash
python3 ~/.hermes/skills/factor-lab/scripts/compare_results.py \
  ~/factor-lab/artifacts/test1/results.json \
  ~/factor-lab/artifacts/test2/results.json
```
```

---

## Task 5: 集成测试

**Objective:** 测试所有新工具的端到端流程

**Step 1: 生成新因子并测试**

```bash
# 1. 生成组合因子
FACTOR_JSON=$(python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py combine earnings_yield book_yield)
echo $FACTOR_JSON

# 2. 提取因子名称和表达式
FACTOR_NAME=$(echo $FACTOR_JSON | jq -r '.name')
FACTOR_EXPR=$(echo $FACTOR_JSON | jq -r '.expression')

# 3. 创建配置并运行回测
# (使用 Hermes execute_code 或 terminal)

# 4. 对比结果
python3 ~/.hermes/skills/factor-lab/scripts/compare_results.py \
  ~/factor-lab/artifacts/test_*/results.json
```

**Step 2: 检查 Daemon 状态**

```bash
python3 ~/.hermes/skills/factor-lab/scripts/check_daemon.py
```

**Expected:** 显示完整的 daemon 状态信息

---

## 验收标准

- [ ] 因子生成器能生成 4 种类型的因子表达式
- [ ] Daemon 监控脚本能显示服务状态和 heartbeat
- [ ] 结果对比工具能正确排序和显示多个因子
- [ ] Skill 文档已更新，包含所有新工具的使用说明
- [ ] 端到端测试通过：生成因子 → 回测 → 对比结果

---

## 下一步

完成 Phase 3 后，进入 **Phase 4: Memory 系统集成**
- 记录因子表现历史
- 积累优化经验
- 自动推荐因子
