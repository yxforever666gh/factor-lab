from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class SingleLLMAgent:
    def __init__(self, provider: str | None = None) -> None:
        self.provider = provider or os.environ.get("FACTOR_LAB_LLM_PROVIDER", "mock")

    def generate_review(self, snapshot: dict[str, Any]) -> str:
        if self.provider == "mock":
            latest_run = snapshot.get("latest_run") or {}
            candidates = snapshot.get("latest_candidates", [])
            graveyard = snapshot.get("latest_graveyard", [])
            top_scores = snapshot.get("top_scores", [])[:3]
            best_strategy = (snapshot.get("portfolio_averages") or [{}])[0]
            lines = [
                "# LLM 研究评审（Mock）",
                "",
                "## 本轮核心结论",
                f"- 最新运行：{latest_run.get('run_id', 'n/a')}，配置 {latest_run.get('config_path', 'n/a')}。",
                f"- 当前候选池：{', '.join(candidates) if candidates else '无'}。",
                f"- 当前墓地：{', '.join(graveyard) if graveyard else '无'}。",
                "",
                "## 评分最高因子",
            ]
            for row in top_scores:
                lines.append(f"- {row['factor_name']}：平均分 {row['avg_score']}，出现 {row['runs']} 次。")
            lines.extend([
                "",
                "## 组合观察",
                f"- 当前平均表现最好的策略是 {best_strategy.get('strategy_name', 'n/a')}，平均夏普 {best_strategy.get('avg_sharpe', 'n/a')}。",
                "",
                "## 风险提示",
                "- 该评审由 mock provider 生成，内容基于结构化结果模板化总结，不代表真实外部大模型判断。",
                "",
                "## 下一轮建议",
                "- 继续跟踪稳定候选因子，并优先观察中性化后仍能存活的组合。",
            ])
            return "\n".join(lines)
        raise RuntimeError(f"Unsupported LLM provider for now: {self.provider}")

    def generate_plan(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        if self.provider == "mock":
            candidates = snapshot.get("latest_candidates", [])
            latest_run = snapshot.get("latest_run") or {}
            return {
                "provider": "mock",
                "summary": "优先围绕当前稳定候选池继续做小步扩展，不自动执行。",
                "recommended_jobs": [
                    {
                        "name": "candidate_focus_followup",
                        "reason": "围绕稳定候选池做进一步验证，避免一次扩太多。",
                        "based_on_candidates": candidates,
                        "window": {
                            "start_date": latest_run.get("start_date"),
                            "end_date": latest_run.get("end_date"),
                        },
                        "notes": [
                            "优先测试候选池相关的轻量组合变化",
                            "继续比较候选组合与中性化候选组合",
                        ],
                    }
                ],
            }
        raise RuntimeError(f"Unsupported LLM provider for now: {self.provider}")


def run_llm_cycle(snapshot_path: str | Path, review_output_path: str | Path, plan_output_path: str | Path) -> None:
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    agent = SingleLLMAgent()
    review = agent.generate_review(snapshot)
    plan = agent.generate_plan(snapshot)
    Path(review_output_path).write_text(review, encoding="utf-8")
    Path(plan_output_path).write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
