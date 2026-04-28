from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.decision_impact_report import build_decision_impact_report


if __name__ == "__main__":
    build_decision_impact_report()
    print("decision impact report built")
