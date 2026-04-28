from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_portfolio_retrospective import build_portfolio_retrospective, build_portfolio_stability_score


if __name__ == "__main__":
    build_portfolio_retrospective(
        history_path="artifacts/paper_portfolio/portfolio_history.json",
        output_path="artifacts/paper_portfolio/portfolio_retrospective.json",
    )
    build_portfolio_stability_score(
        retro_path="artifacts/paper_portfolio/portfolio_retrospective.json",
        output_path="artifacts/paper_portfolio/portfolio_stability_score.json",
    )
    print("paper portfolio retrospective built")
