from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_strategy import apply_strategy_plan
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

__all__ = ["apply_strategy_plan"]


def main() -> int:
    # The legacy implementation is still importable for migration/regression
    # use, but its CLI previously injected tasks directly into SQLite.
    return retired_legacy_entrypoint("scripts/apply_strategy_plan.py")


if __name__ == "__main__":
    raise SystemExit(main())
