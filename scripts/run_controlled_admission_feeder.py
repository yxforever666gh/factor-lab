#!/usr/bin/env python3
from __future__ import annotations

from factor_lab.controlled_admission_feeder import load_feeder_config, resolve_feeder_policy, run_controlled_admission_feeder
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

__all__ = [
    "load_feeder_config",
    "resolve_feeder_policy",
    "run_controlled_admission_feeder",
]


def main() -> int:
    # Configuration and numerical policy helpers remain available to import,
    # while CLI admission is closed in favor of the lifetime trial ledger.
    return retired_legacy_entrypoint("scripts/run_controlled_admission_feeder.py")


if __name__ == "__main__":
    raise SystemExit(main())
