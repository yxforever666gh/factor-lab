from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HarvestControllerPolicy:
    max_cycles: int = 3
    max_backtests: int = 300
    max_attempts_per_cycle: int = 5
    max_consecutive_branch_failures: int = 2
    max_route_failures: int = 3
    allow_controlled_execution: bool = False
    stop_on_data_request: bool = True
    stop_on_route_stop: bool = True
    stop_on_manual_review: bool = True
    no_timer: bool = True
    no_daemon: bool = True
    no_live_trading: bool = True
    no_automatic_promotion: bool = True

    def validate(self) -> None:
        for field in ("max_cycles", "max_backtests", "max_attempts_per_cycle", "max_consecutive_branch_failures", "max_route_failures"):
            if int(getattr(self, field)) < 0:
                raise ValueError(f"{field} must be >= 0")
        if not self.no_timer:
            raise ValueError("v4 controller cannot enable timers")
        if not self.no_daemon:
            raise ValueError("v4 controller cannot enable daemons")
        if not self.no_live_trading:
            raise ValueError("v4 controller cannot enable live trading")
        if not self.no_automatic_promotion:
            raise ValueError("v4 controller cannot enable automatic promotion")
