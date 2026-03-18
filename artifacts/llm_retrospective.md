# LLM 建议效果回溯

- 效果判断：positive
- 原因：核心候选在执行后的 generated batch 中继续存活。
- 保留下来的候选：mom_20, mom_plus_value
- 落入墓地：liquidity_turnover_shock, size_small
- 核心候选保留：mom_20, mom_plus_value
- 核心候选丢失：无
- 下一步提示：继续沿当前候选主线做小步扩展。