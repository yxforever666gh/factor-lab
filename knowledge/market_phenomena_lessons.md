# Market Phenomena Lessons

## quality_repair_delayed_repricing_v1: 盈利质量修复后的延迟重估
- latest_verdict: rejected_failed_verification
- times_reviewed: 1
- next_research_question: Is there a stricter regime condition or different participant constraint that would make this phenomenon testable again?

### What was learned
- Target group quality_repair_low_valuation did not beat controls in the minimal distribution check.
- Observed spread_vs_control=-0.002826358896685923.

### What failed
- minimal verification did not support the phenomenon

### Do not repeat
- 不要把这个现象简化成静态指标组合后继续生成策略
- do not repeat without a changed market mechanism or regime condition

## value_trap_escape_after_balance_sheet_repair_v1: 资产负债表修复后的价值陷阱脱离
- latest_verdict: supported_for_further_research
- times_reviewed: 1
- next_research_question: Does the phenomenon survive industry, size, regime, turnover, and drawdown-sensitivity splits?

### What was learned
- Target group balance_sheet_repair_low_valuation showed positive spread_vs_control=0.02751697794592494 in the minimal distribution check.
- This supports further research on the phenomenon, not immediate strategy generation.

### What failed
- none

### Do not repeat
- do not convert this directly into a strategy before regime/risk robustness review
