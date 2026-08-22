#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.gated_factor_feeder import write_gated_factor_configs
from factor_lab.research_gate import load_hypothesis
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

__all__ = ["load_hypothesis", "write_gated_factor_configs"]


def main() -> int:
    # Config generation can remain an internal compatibility helper, but the
    # old executable path bypassed Research OS preregistration and budgets.
    return retired_legacy_entrypoint("scripts/prepare_gated_research_task.py")


if __name__ == "__main__":
    raise SystemExit(main())
