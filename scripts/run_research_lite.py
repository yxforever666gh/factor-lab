"""Run the historical-only Research Lite factor loop.

This entry point deliberately stays separate from production readiness, shadow
accounts, and forward-evidence activation.  It selects the repository's
existing canary or full feature input and delegates all research work to
``factor_lab.research_lite.run_research_lite``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from factor_lab.research_lite import run_research_lite


DEFAULT_FEATURE_PATHS = {
    "canary": (
        PROJECT_ROOT
        / "artifacts"
        / "expanded_long_only"
        / "feature_store"
        / "live_canary"
        / "expanded_top500_features.parquet"
    ),
    "full": (
        PROJECT_ROOT
        / "artifacts"
        / "expanded_long_only"
        / "feature_store"
        / "expanded_top500_features.parquet"
    ),
}
DEFAULT_EXECUTION_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "expanded_long_only"
    / "feature_store"
    / "expanded_execution_prices.parquet"
)
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "tushare_long_only_expanded.json"
DEFAULT_FACTORS_PATH = PROJECT_ROOT / "configs" / "factor_families_v1.json"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "runtime" / "artifacts" / "research-lite"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a historical-diagnostic factor loop without production, "
            "shadow, Champion, or forward-evidence gates."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("canary", "full"),
        default="canary",
        help="Use the small live-canary feature frame or the full feature frame.",
    )
    parser.add_argument(
        "--feature-path",
        type=Path,
        help="Override the mode-selected feature Parquet path.",
    )
    parser.add_argument(
        "--execution-path",
        type=Path,
        help=(
            "Execution-price Parquet path. Full mode uses the repository's "
            "expanded execution store by default; canary mode uses its feature frame."
        ),
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Long-only research configuration JSON path.",
    )
    parser.add_argument(
        "--factors-path",
        type=Path,
        default=DEFAULT_FACTORS_PATH,
        help="Factor/Sleeve definitions JSON path.",
    )
    parser.add_argument(
        "--factor",
        dest="factor_names",
        action="append",
        help="Run only this factor identifier; repeat to select several factors.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory; defaults to runtime/artifacts/research-lite/<mode>.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore reusable completed factor results and recompute them.",
    )
    return parser


def _portfolio_metric(row: Mapping[str, Any], key: str) -> Any:
    portfolio = row.get("portfolio")
    return portfolio.get(key) if isinstance(portfolio, Mapping) else None


def _compact_summary(result: Mapping[str, Any], *, mode: str) -> dict[str, Any]:
    rows_value = result.get("results")
    rows = (
        tuple(item for item in rows_value if isinstance(item, Mapping))
        if isinstance(rows_value, Sequence)
        and not isinstance(rows_value, (str, bytes))
        else ()
    )
    factor_rows: list[dict[str, Any]] = []
    for row in rows:
        portfolio = row.get("portfolio")
        portfolio_status = (
            portfolio.get("status") if isinstance(portfolio, Mapping) else None
        )
        compact = {
            "factor": row.get("name")
            or row.get("factor")
            or row.get("factor_name")
            or row.get("factor_id"),
            "status": row.get("status"),
            "portfolio_status": portfolio_status,
            "net_annual_return": _portfolio_metric(row, "net_annual_return"),
            "net_sharpe": _portfolio_metric(row, "net_sharpe"),
            "max_drawdown": _portfolio_metric(row, "max_drawdown"),
        }
        factor_rows.append({key: value for key, value in compact.items() if value is not None})

    summary: dict[str, Any] = {
        "status": result.get("status", "completed"),
        "mode": mode,
        "evidence_class": result.get("evidence_class"),
        "promotion_triggered": bool(result.get("promotion_triggered", False)),
        "candidate_written": bool(result.get("candidate_written", False)),
        "result_count": len(rows),
        "results": factor_rows,
    }
    for key in (
        "output_dir",
        "summary_path",
        "report_path",
        "manifest_path",
        "run_id",
        "fingerprint",
    ):
        if result.get(key) is not None:
            summary[key] = result[key]
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mode = str(args.mode)
    feature_path = (args.feature_path or DEFAULT_FEATURE_PATHS[mode]).resolve()
    execution_path = (
        args.execution_path.resolve()
        if args.execution_path is not None
        else (DEFAULT_EXECUTION_PATH.resolve() if mode == "full" else None)
    )
    config_path = args.config_path.resolve()
    factors_path = args.factors_path.resolve()
    output_dir = (args.output_dir or (DEFAULT_OUTPUT_ROOT / mode)).resolve()

    result = run_research_lite(
        feature_path,
        config_path,
        factors_path,
        output_dir,
        execution_path=execution_path,
        factor_names=(tuple(args.factor_names) if args.factor_names else None),
        resume=not args.no_resume,
    )
    if not isinstance(result, Mapping):
        raise TypeError("run_research_lite must return a mapping")
    summary = _compact_summary(result, mode=mode)
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":"), default=str))
    return 1 if str(summary["status"]).lower() in {"error", "failed"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
