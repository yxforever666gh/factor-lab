#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint


def main() -> int:
    return retired_legacy_entrypoint(
        "scripts/build_paper_portfolio.py",
        next_command="factor-lab shadow step",
    )


if __name__ == "__main__":
    raise SystemExit(main())
