from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.workflow import run_workflow

if __name__ == "__main__":
    run_workflow(
        config_path="artifacts/parallel_test_turnover.json",
        output_dir="artifacts/parallel_test_turnover_output",
    )
