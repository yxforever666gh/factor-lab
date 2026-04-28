from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch


if __name__ == "__main__":
    run_batch(
        config_path="configs/tushare_batch.json",
        output_dir="artifacts/tushare_batch",
    )
