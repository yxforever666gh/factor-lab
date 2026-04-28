from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_attribution import build_research_attribution


if __name__ == "__main__":
    build_research_attribution()
    print("research attribution built")
