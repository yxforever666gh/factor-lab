from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.workflow import run_workflow
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

if __name__ == "__main__":
    raise SystemExit(retired_legacy_entrypoint("scripts/run_turnover_test.py"))
