from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_space_map import build_research_space_map


if __name__ == "__main__":
    build_research_space_map(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/research_space_map.json",
    )
    print("research space map built")
