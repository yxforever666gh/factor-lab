from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.llm_recommendation_context import build_recommendation_context


if __name__ == "__main__":
    build_recommendation_context(
        weights_path="artifacts/llm_recommendation_weights.json",
        history_path="artifacts/llm_recommendation_history.json",
        output_path="artifacts/llm_recommendation_context.json",
    )
    print("recommendation context built")
