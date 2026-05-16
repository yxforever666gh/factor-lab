# Hermes × Factor Lab 集成 - 最终验收测试

**测试时间**: 2026-04-29  
**测试目标**: 验证完整工作流从因子生成到历史分析的端到端能力

---

## 测试场景

模拟真实使用场景：
1. 生成新因子
2. 执行回测
3. 对比结果
4. 分析历史趋势

---

## 测试步骤

### Step 1: 生成组合因子
```bash
python3 ~/.hermes/skills/factor-lab/scripts/generate_factor.py \
  --type combine \
  --name value_combo \
  --factors earnings_yield book_yield \
  --weights 0.6 0.4
```

**预期**: 生成 `value_combo.py` 文件

### Step 2: 执行回测
```bash
cd ~/factor-lab
python3 -m factor_lab.workflow configs/value_combo_test.json
```

配置文件:
```json
{
  "workflow_id": "value_combo_test",
  "mode": "single_factor",
  "factor_name": "value_combo",
  "universe_limit": 300,
  "start_date": "2020-01-01",
  "end_date": "2023-12-31",
  "evaluation": {
    "rank_ic_threshold": 0.02,
    "sharpe_net_threshold": 1.0
  }
}
```

**预期**: 生成 `artifacts/value_combo_test/results.json`

### Step 3: 对比多个因子
```bash
python3 ~/.hermes/skills/factor-lab/scripts/compare_results.py \
  ~/factor-lab/artifacts/earnings_yield_test/results.json \
  ~/factor-lab/artifacts/book_yield_test/results.json \
  ~/factor-lab/artifacts/value_combo_test/results.json
```

**预期**: 显示排序后的对比表格

### Step 4: 历史趋势分析
```bash
python3 ~/.hermes/skills/factor-lab/scripts/analyze_history.py --compare \
  earnings_yield book_yield value_combo 30
```

**预期**: 显示 30 天内的趋势对比

---

## 测试执行

