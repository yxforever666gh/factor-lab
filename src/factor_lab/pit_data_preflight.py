from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FieldPreflightDecision:
    field: str
    available: bool
    coverage: float | None
    ann_date_rate: float | None
    pit_safe: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "available": self.available,
            "coverage": self.coverage,
            "ann_date_rate": self.ann_date_rate,
            "pit_safe": self.pit_safe,
            "reasons": list(self.reasons),
        }


def decide_field_preflight(
    field: str,
    *,
    available: bool,
    coverage: float | None,
    ann_date_rate: float | None,
    requires_pit: bool = True,
    min_coverage: float = 0.70,
    min_ann_date_rate: float = 0.95,
) -> FieldPreflightDecision:
    reasons: list[str] = []
    if not available:
        reasons.append("field_unavailable")
    if coverage is None:
        reasons.append("coverage_unknown")
    elif coverage < min_coverage:
        reasons.append("coverage_below_threshold")
    if requires_pit:
        if ann_date_rate is None:
            reasons.append("ann_date_rate_unknown")
        elif ann_date_rate < min_ann_date_rate:
            reasons.append("ann_date_rate_below_threshold")
    return FieldPreflightDecision(
        field=field,
        available=available,
        coverage=coverage,
        ann_date_rate=ann_date_rate,
        pit_safe=not reasons,
        reasons=tuple(reasons),
    )


def summarize_table_preflight(table_summaries: dict[str, Any]) -> dict[str, Any]:
    decisions = []
    for table, summary in table_summaries.items():
        decisions.append(
            decide_field_preflight(
                table,
                available=bool(summary.get("rows", 0)),
                coverage=summary.get("decision", {}).get("coverage_vs_active_universe"),
                ann_date_rate=summary.get("ann_date_nonnull_rate"),
                requires_pit=True,
            ).to_dict()
        )
    return {
        "ready": all(d["pit_safe"] for d in decisions),
        "decisions": decisions,
    }


def preflight_report_ready(report: dict[str, Any]) -> dict[str, Any]:
    """Return whether a PIT preflight artifact permits gated config generation.

    The decision is intentionally conservative and checks the safety markers that
    the preflight script promises: explicit sample/full-market mode, no factor
    execution, no queue write, and no daemon start. Tushare must pass as the
    primary source; Diemeng may remain supplemental.
    """
    reasons: list[str] = []
    mode = report.get("mode")
    if mode not in {"sample", "full_market"}:
        reasons.append("missing_or_invalid_mode")
    if report.get("no_factor_run") is not True:
        reasons.append("factor_run_not_allowed_in_preflight")
    if report.get("no_queue_write") is not True:
        reasons.append("queue_write_not_allowed_in_preflight")
    if report.get("no_daemon_start") is not True:
        reasons.append("daemon_start_not_allowed_in_preflight")
    tushare_ready = bool(report.get("tushare", {}).get("summary", {}).get("ready_for_p0_value_trap_experiment"))
    if not tushare_ready:
        reasons.append("primary_tushare_pit_preflight_not_ready")
    return {
        "ready": not reasons,
        "reasons": reasons,
        "mode": mode,
        "primary_source": "tushare",
        "supplemental_source": "diemeng",
    }
