#!/usr/bin/env python3
from __future__ import annotations

from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint


def main() -> int:
    return retired_legacy_entrypoint("scripts/run_harvest_cycle.py")


if __name__ == "__main__":
    raise SystemExit(main())
