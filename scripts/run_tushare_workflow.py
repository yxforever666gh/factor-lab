from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.workflow import run_workflow


if __name__ == "__main__":
    run_workflow(
        config_path="configs/tushare_workflow.json",
        output_dir="artifacts/tushare_workflow",
    )
