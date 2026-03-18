from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.index_page import build_index_page


if __name__ == "__main__":
    build_index_page(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/index.html",
    )
