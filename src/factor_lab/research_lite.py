"""A small, historical-only factor research loop.

``research_lite`` intentionally stops at diagnostics.  It reuses the real
long-only execution kernel, but it does not write to the Research OS catalog,
create a candidate, or activate a shadow account.  This makes the existing
expanded A-share Parquet stores useful while the production data plane is
still being completed.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.long_only_portfolio import (
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)


ENGINE_ID = "factor-lab/research-lite/v1"
EVIDENCE_CLASS = "historical_diagnostic"
_FORBIDDEN_SIGNAL_FIELDS = {
    "forward_return",
    "forward_return_5d",
    "forward_return_5d_open",
    "future_return",
    "label",
    "target",
}
_OMITTED_PORTFOLIO_DETAILS = {"periods", "trades"}


def _read_json_source(value: Mapping[str, Any] | Sequence[Mapping[str, Any]] | str | Path) -> Any:
    if isinstance(value, (str, Path)):
        return json.loads(Path(value).read_text(encoding="utf-8"))
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _implementation_sha256() -> str:
    root = Path(__file__).resolve().parent
    paths = (
        Path(__file__).resolve(),
        root / "factors.py",
        root / "long_only_portfolio.py",
        root / "research_os" / "execution_kernel.py",
    )
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _parquet_columns(path: Path) -> set[str]:
    return set(pq.ParquetFile(path).schema_arrow.names)


def _expression_fields(expression: str) -> set[str]:
    tree = ast.parse(expression, mode="eval")
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _normalize_factor_rows(
    factors: Mapping[str, Any] | Sequence[Mapping[str, Any]] | str | Path,
) -> list[dict[str, Any]]:
    payload = _read_json_source(factors)
    rows: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping) and isinstance(payload.get("families"), Sequence):
        for family in payload["families"]:
            if not isinstance(family, Mapping):
                continue
            family_name = str(family.get("family") or "other")
            for variant in family.get("variants") or []:
                if isinstance(variant, Mapping):
                    rows.append({"family": family_name, **variant})
    elif isinstance(payload, Mapping) and isinstance(payload.get("factors"), Sequence):
        rows = [row for row in payload["factors"] if isinstance(row, Mapping)]
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        rows = [row for row in payload if isinstance(row, Mapping)]
    else:
        raise ValueError("factors must be a list or a mapping containing families/factors")

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        name = str(row.get("name") or "").strip()
        expression = str(row.get("expression") or "").strip()
        if not name or not expression:
            raise ValueError("every factor needs a non-empty name and expression")
        if name in seen:
            raise ValueError(f"duplicate factor name: {name}")
        seen.add(name)
        try:
            parsed = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            raise ValueError(f"invalid expression for factor {name}: {exc.msg}") from exc
        referenced = {node.id for node in ast.walk(parsed) if isinstance(node, ast.Name)}
        forbidden = sorted(
            field
            for field in referenced
            if field.lower() in _FORBIDDEN_SIGNAL_FIELDS
            or field.lower().startswith("forward_")
            or field.lower().startswith("future_")
            or field.lower().startswith("label_")
            or field.lower().startswith("target_")
        )
        if forbidden:
            raise ValueError(f"factor {name} references forbidden future/label fields: {forbidden}")
        direction = -1 if float(row.get("direction") or 1) < 0 else 1
        normalized.append(
            {
                "name": name,
                "family": str(row.get("family") or "other"),
                "expression": expression,
                "direction": direction,
                "allow_in_long_only": bool(
                    row.get("allow_in_long_only", row.get("allow_in_portfolio", True))
                ),
                "role": str(row.get("role") or "research_probe"),
            }
        )
    if not normalized:
        raise ValueError("no factor definitions were selected")
    return normalized


def _rank_ic_diagnostics(frame: pd.DataFrame, signal: pd.Series) -> dict[str, Any]:
    label_column = next(
        (name for name in ("forward_return_5d_open", "forward_return_5d") if name in frame.columns),
        None,
    )
    if label_column is None:
        return {
            "status": "unavailable",
            "reason": "missing_forward_return_diagnostic_label",
            "rank_ic_mean": None,
            "rank_ic_std": None,
            "top_bottom_spread_mean": None,
            "date_count": 0,
        }

    rows = pd.DataFrame(
        {
            "date": frame["date"],
            "signal": pd.to_numeric(signal, errors="coerce"),
            "return": pd.to_numeric(frame[label_column], errors="coerce"),
        }
    ).replace([np.inf, -np.inf], np.nan).dropna()
    daily_ic: list[float] = []
    spreads: list[float] = []
    for _, group in rows.groupby("date", sort=True):
        if len(group) < 10 or group["signal"].nunique() < 5:
            continue
        correlation = group["signal"].corr(group["return"], method="spearman")
        if pd.notna(correlation):
            daily_ic.append(float(correlation))
        ranked = group.assign(
            bucket=pd.qcut(group["signal"].rank(method="first"), 5, labels=False)
        )
        spread = (
            ranked.loc[ranked["bucket"] == 4, "return"].mean()
            - ranked.loc[ranked["bucket"] == 0, "return"].mean()
        )
        if pd.notna(spread):
            spreads.append(float(spread))
    return {
        "status": "ok" if daily_ic else "insufficient_data",
        "label_column": label_column,
        "rank_ic_mean": round(float(np.mean(daily_ic)), 8) if daily_ic else None,
        "rank_ic_std": round(float(np.std(daily_ic)), 8) if daily_ic else None,
        "top_bottom_spread_mean": round(float(np.mean(spreads)), 8) if spreads else None,
        "date_count": len(daily_ic),
    }


def _compact_portfolio(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = {key: value for key, value in payload.items() if key not in _OMITTED_PORTFOLIO_DETAILS}
    compact["promotion_eligible"] = False
    compact["promotion_blockers"] = list(
        dict.fromkeys(
            [*(str(item) for item in compact.get("promotion_blockers") or []), "research_lite_diagnostic_only"]
        )
    )
    compact["details_omitted"] = sorted(_OMITTED_PORTFOLIO_DETAILS)
    return compact


def _safe_result_name(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._")
    suffix = _sha256_bytes(name.encode("utf-8"))[:10]
    return f"{(safe or 'factor')[:80]}-{suffix}"


def _manifest_valid(manifest_path: Path, output_dir: Path, run_fingerprint: str) -> bool:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = payload.get("files") or []
        if payload.get("run_fingerprint") != run_fingerprint or not rows:
            return False
        names = {Path(str(row.get("path"))).as_posix() for row in rows}
        if not {"summary.json", "report.md"}.issubset(names):
            return False
        for row in rows:
            path = Path(str(row["path"]))
            if not path.is_absolute():
                path = output_dir / path
            if not path.is_file() or _sha256_file(path) != row.get("sha256"):
                return False
        return True
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False


def _ranking_value(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return -999.0
    return numeric if np.isfinite(numeric) else -999.0


def _return_payload(summary: Mapping[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        **dict(summary),
        "output_dir": str(output_dir),
        "summary_path": str(output_dir / "summary.json"),
        "report_path": str(output_dir / "report.md"),
        "manifest_path": str(output_dir / "manifest.json"),
        "fingerprint": summary.get("run_fingerprint"),
    }


def _render_report(summary: Mapping[str, Any]) -> str:
    def metric(row: Mapping[str, Any], key: str) -> Any:
        portfolio = row.get("portfolio") or {}
        return portfolio.get(key) if isinstance(portfolio, Mapping) else None

    def number(value: Any, *, percent: bool = False) -> str:
        if value is None:
            return "—"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "—"
        return f"{numeric * 100:.2f}%" if percent else f"{numeric:.3f}"

    lines = [
        "# Factor Lab Research Lite 报告",
        "",
        f"- Evidence class: `{EVIDENCE_CLASS}`",
        "- Promotion: disabled（不会创建候选、Champion、Challenger 或实盘动作）",
        f"- 样本：{summary['data']['start_date']} 至 {summary['data']['end_date']}，"
        f"{summary['data']['row_count']:,} 行 / {summary['data']['ticker_count']:,} 只股票",
        f"- 组合：多头、每 {summary['portfolio']['rebalance_every_days']} 个交易日调仓、"
        f"目标 {summary['portfolio']['position_count']} 只",
        "",
    ]
    warnings = summary.get("data_warnings") or []
    if warnings:
        lines.extend(["## 数据提示", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    lines.extend(
        [
            "## 因子结果",
            "",
            "| 排名 | 因子 | Family | 状态 | 覆盖率 | Rank IC | 多头净年化 | 超额净年化 | Sharpe | 最大回撤 | 换手 |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    ranking = {name: index + 1 for index, name in enumerate(summary.get("ranking") or [])}
    for row in summary.get("results") or []:
        portfolio = row.get("portfolio") or {}
        status = portfolio.get("status") if isinstance(portfolio, Mapping) else row.get("status")
        lines.append(
            "| {rank} | {name} | {family} | {status} | {coverage} | {ic} | {annual} | {excess} | {sharpe} | {drawdown} | {turnover} |".format(
                rank=ranking.get(str(row["name"]), "—"),
                name=row["name"],
                family=row["family"],
                status=status,
                coverage=number(row.get("coverage_ratio"), percent=True),
                ic=number((row.get("diagnostics") or {}).get("rank_ic_mean")),
                annual=number(metric(row, "net_annual_return"), percent=True),
                excess=number(metric(row, "net_excess_annual_return"), percent=True),
                sharpe=number(metric(row, "net_sharpe")),
                drawdown=number(metric(row, "max_drawdown"), percent=True),
                turnover=number(metric(row, "actual_turnover"), percent=True),
            )
        )
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "本轮只证明历史数据上的因子计算与多头评估流程能够运行。结果属于已观察历史诊断，"
            "不能据此宣称未来盈利，也不会触发 promotion。",
            "",
        ]
    )
    return "\n".join(lines)


def run_research_lite(
    feature_path: str | Path,
    config: Mapping[str, Any] | str | Path,
    factors: Mapping[str, Any] | Sequence[Mapping[str, Any]] | str | Path,
    output_dir: str | Path,
    *,
    execution_path: str | Path | None = None,
    factor_names: Sequence[str] | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    """Run factors against a local historical feature store.

    The function is intentionally file-oriented and has no dependency on the
    production catalog.  A content fingerprint and per-factor files make an
    interrupted full-sample run resumable.
    """

    feature_file = Path(feature_path).resolve()
    execution_file = Path(execution_path).resolve() if execution_path is not None else None
    output_root = Path(output_dir).resolve()
    if not feature_file.is_file():
        raise FileNotFoundError(feature_file)
    if execution_file is not None and not execution_file.is_file():
        raise FileNotFoundError(execution_file)

    config_payload = _read_json_source(config)
    if not isinstance(config_payload, Mapping):
        raise ValueError("config must be a mapping or JSON object")
    factor_rows = _normalize_factor_rows(factors)
    if factor_names:
        requested = list(dict.fromkeys(str(name) for name in factor_names))
        lookup = {row["name"]: row for row in factor_rows}
        missing = [name for name in requested if name not in lookup]
        if missing:
            raise ValueError(f"unknown factor names: {missing}")
        factor_rows = [lookup[name] for name in requested]

    portfolio_config = LongOnlyPortfolioConfig.from_mapping(config_payload)
    feature_sha256 = _sha256_file(feature_file)
    execution_sha256 = _sha256_file(execution_file) if execution_file is not None else None
    implementation_sha256 = _implementation_sha256()
    fingerprint_payload = {
        "engine": ENGINE_ID,
        "implementation_sha256": implementation_sha256,
        "feature_sha256": feature_sha256,
        "execution_sha256": execution_sha256,
        "config": config_payload,
        "factors": factor_rows,
    }
    run_fingerprint = _sha256_bytes(_canonical_json(fingerprint_payload).encode("utf-8"))
    run_id = run_fingerprint[:16]
    summary_path = output_root / "summary.json"
    report_path = output_root / "report.md"
    manifest_path = output_root / "manifest.json"
    if resume and summary_path.is_file() and report_path.is_file() and manifest_path.is_file():
        cached = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
            cached.get("status") == "completed"
            and cached.get("run_fingerprint") == run_fingerprint
            and _manifest_valid(manifest_path, output_root, run_fingerprint)
        ):
            return _return_payload(cached, output_root)

    available_feature_columns = _parquet_columns(feature_file)
    feature_columns = {
        "date",
        "ticker",
        "forward_return_5d_open",
        "forward_return_5d",
        "st_filter_status",
    }
    for row in factor_rows:
        feature_columns.update(_expression_fields(row["expression"]))
    if execution_file is None:
        feature_columns.update(
            {
                portfolio_config.open_column,
                portfolio_config.adv_column,
                portfolio_config.volatility_column,
                *portfolio_config.eligible_columns,
                portfolio_config.limit_up_column,
                portfolio_config.limit_down_column,
                "is_suspended",
                "is_delisted",
                "split_ratio",
                "cash_dividend",
            }
        )
    frame = pd.read_parquet(
        feature_file,
        columns=sorted(feature_columns & available_feature_columns),
    )
    required = {"date", "ticker"}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise ValueError(f"feature store is missing required columns: {missing_columns}")
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["ticker"] = frame["ticker"].astype(str)
    if frame["date"].isna().any():
        raise ValueError("feature store contains invalid dates")
    if frame.duplicated(["date", "ticker"]).any():
        raise ValueError("feature store contains duplicate date/ticker rows")
    frame = frame.sort_values(["date", "ticker"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("feature store is empty")

    execution_frame: pd.DataFrame | None = None
    execution_calendar: dict[str, Any] | None = None
    if execution_file is not None:
        available_execution_columns = _parquet_columns(execution_file)

        def resolve(preferred: str, aliases: Sequence[str]) -> str | None:
            return next(
                (name for name in (preferred, *aliases) if name in available_execution_columns),
                None,
            )

        execution_date_column = resolve(portfolio_config.date_column, ("date", "trade_date"))
        execution_ticker_column = resolve(portfolio_config.ticker_column, ("ticker", "ts_code", "symbol"))
        execution_open_column = resolve(portfolio_config.open_column, ("open_price", "open_adj", "open"))
        execution_adv_column = resolve(
            portfolio_config.adv_column,
            ("amount_20d_avg", "adv", "average_daily_value"),
        )
        execution_volatility_column = resolve(
            portfolio_config.volatility_column,
            ("volatility", "vol_20"),
        )
        required_execution = {
            "date": execution_date_column,
            "ticker": execution_ticker_column,
            "open": execution_open_column,
            "adv": execution_adv_column,
            "volatility": execution_volatility_column,
        }
        missing_execution = [key for key, value in required_execution.items() if value is None]
        if missing_execution:
            raise ValueError(f"execution store is missing required fields: {missing_execution}")
        assert execution_date_column and execution_ticker_column

        raw_execution_dates = pd.read_parquet(
            execution_file,
            columns=[execution_date_column],
        )[execution_date_column]
        raw_execution_dates = pd.to_datetime(raw_execution_dates, errors="coerce")
        execution_dates = raw_execution_dates.dropna().drop_duplicates().sort_values().reset_index(drop=True)
        feature_dates = frame["date"].drop_duplicates().sort_values().reset_index(drop=True)
        missing_dates = sorted(set(feature_dates.tolist()) - set(execution_dates.tolist()))
        if missing_dates:
            preview = [pd.Timestamp(value).date().isoformat() for value in missing_dates[:5]]
            raise ValueError(
                f"execution store does not cover {len(missing_dates)} feature dates; first={preview}"
            )
        required_future_dates = portfolio_config.holding_days + 1
        future_dates = execution_dates[execution_dates > feature_dates.iloc[-1]].head(required_future_dates)
        if len(future_dates) < required_future_dates:
            raise ValueError(
                "execution store lacks the post-signal dates required for the final holding period: "
                f"need={required_future_dates}, found={len(future_dates)}"
            )
        maximum_date = future_dates.iloc[-1]
        optional_execution_columns = {
            *portfolio_config.eligible_columns,
            portfolio_config.limit_up_column,
            portfolio_config.limit_down_column,
            "is_suspended",
            "is_delisted",
            "split_ratio",
            "cash_dividend",
        }
        execution_columns = {
            value for value in required_execution.values() if value is not None
        } | (optional_execution_columns & available_execution_columns)
        execution_frame = pd.read_parquet(
            execution_file,
            columns=sorted(execution_columns),
            filters=[
                (execution_date_column, ">=", feature_dates.iloc[0].to_pydatetime()),
                (execution_date_column, "<=", maximum_date.to_pydatetime()),
            ],
        )
        execution_frame[execution_date_column] = pd.to_datetime(
            execution_frame[execution_date_column], errors="coerce"
        )
        missing_tickers = set(frame["ticker"].unique()) - set(
            execution_frame[execution_ticker_column].astype(str).unique()
        )
        if missing_tickers:
            raise ValueError(
                f"execution store is missing {len(missing_tickers)} feature tickers"
            )
        execution_calendar = {
            "start_date": pd.Timestamp(execution_frame[execution_date_column].min()).date().isoformat(),
            "end_date": pd.Timestamp(execution_frame[execution_date_column].max()).date().isoformat(),
            "trading_day_count": int(execution_frame[execution_date_column].nunique()),
            "post_signal_day_count": int(len(future_dates)),
        }

    output_root.mkdir(parents=True, exist_ok=True)
    strategy_dir = output_root / "strategies"
    strategy_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for row in factor_rows:
        result_path = strategy_dir / f"{_safe_result_name(row['name'])}.json"
        if resume and result_path.is_file():
            cached_result = json.loads(result_path.read_text(encoding="utf-8"))
            if (
                cached_result.get("run_fingerprint") == run_fingerprint
                and cached_result.get("factor") == row
            ):
                results.append(cached_result["result"])
                continue

        base_result: dict[str, Any] = {
            "name": row["name"],
            "family": row["family"],
            "expression": row["expression"],
            "direction": row["direction"],
            "role": row["role"],
            "allow_in_long_only": row["allow_in_long_only"],
        }
        try:
            signal = pd.to_numeric(
                apply_factor(frame, FactorDefinition(row["name"], row["expression"])),
                errors="coerce",
            ) * int(row["direction"])
            finite_signal = signal.replace([np.inf, -np.inf], np.nan)
            base_result["coverage_ratio"] = round(float(finite_signal.notna().mean()), 8)
            base_result["diagnostics"] = _rank_ic_diagnostics(frame, finite_signal)
            if row["allow_in_long_only"]:
                evaluation = evaluate_long_only_portfolio(
                    frame,
                    finite_signal,
                    portfolio_config,
                    pricing_frame=execution_frame,
                    promotion_blockers=("research_lite_diagnostic_only",),
                ).to_dict()
                base_result["portfolio"] = _compact_portfolio(evaluation)
                base_result["status"] = str(evaluation.get("status") or "unknown")
            else:
                base_result["portfolio"] = {
                    "status": "diagnostic_only",
                    "reason": "allow_in_long_only=false",
                }
                base_result["status"] = "diagnostic_only"
        except (KeyError, ValueError, TypeError, ZeroDivisionError) as exc:
            base_result.update(
                {
                    "status": "unavailable",
                    "coverage_ratio": 0.0,
                    "diagnostics": {
                        "status": "unavailable",
                        "reason": f"{type(exc).__name__}: {exc}",
                        "rank_ic_mean": None,
                        "top_bottom_spread_mean": None,
                    },
                    "portfolio": {
                        "status": "unavailable",
                        "reason": f"{type(exc).__name__}: {exc}",
                    },
                }
            )
        results.append(base_result)
        _write_json(
            result_path,
            {"run_fingerprint": run_fingerprint, "factor": row, "result": base_result},
        )

    ranked = sorted(
        (
            row
            for row in results
            if row.get("allow_in_long_only")
            and (row.get("portfolio") or {}).get("status") == "ok"
        ),
        key=lambda row: (
            _ranking_value((row.get("portfolio") or {}).get("net_excess_annual_return")),
            _ranking_value((row.get("portfolio") or {}).get("net_sharpe")),
            str(row.get("name")),
        ),
        reverse=True,
    )
    data_warnings: list[str] = []
    if "st_filter_status" not in frame.columns:
        data_warnings.append("样本未携带可验证的历史 ST 状态；允许研究计算，但不得据此晋级。")
    else:
        statuses = {str(value).strip().lower() for value in frame["st_filter_status"].dropna().unique()}
        if not statuses or any("unverified" in value or "degraded" in value for value in statuses):
            data_warnings.append("历史 ST 状态未完整验证；允许研究计算，但不得据此晋级。")

    generated_at = datetime.now(timezone.utc).isoformat()
    summary: dict[str, Any] = {
        "schema_version": 1,
        "engine": ENGINE_ID,
        "status": "completed",
        "run_id": run_id,
        "run_fingerprint": run_fingerprint,
        "implementation_sha256": implementation_sha256,
        "generated_at_utc": generated_at,
        "evidence_class": EVIDENCE_CLASS,
        "investment_claim_allowed": False,
        "promotion_triggered": False,
        "candidate_written": False,
        "shorting_used": False,
        "data": {
            "feature_path": str(feature_file),
            "feature_sha256": feature_sha256,
            "execution_path": str(execution_file) if execution_file is not None else None,
            "execution_sha256": execution_sha256,
            "start_date": frame["date"].min().date().isoformat(),
            "end_date": frame["date"].max().date().isoformat(),
            "row_count": int(len(frame)),
            "ticker_count": int(frame["ticker"].nunique()),
            "trading_day_count": int(frame["date"].nunique()),
            "execution_calendar": execution_calendar,
        },
        "portfolio": {
            "mode": "long_only",
            "capital": portfolio_config.capital,
            "position_count": portfolio_config.position_count,
            "holding_days": portfolio_config.holding_days,
            "rebalance_every_days": portfolio_config.rebalance_every_days,
            "max_adv_participation": portfolio_config.max_adv_participation,
        },
        "data_warnings": data_warnings,
        "factor_count": len(results),
        "tradable_factor_count": sum(bool(row.get("allow_in_long_only")) for row in results),
        "completed_portfolio_count": len(ranked),
        "ranking": [str(row["name"]) for row in ranked],
        "best_factor": str(ranked[0]["name"]) if ranked else None,
        "results": results,
    }
    _write_json(summary_path, summary)
    report_path.write_text(_render_report(summary), encoding="utf-8")
    artifact_files = [summary_path, report_path, *sorted(strategy_dir.glob("*.json"))]
    manifest = {
        "schema_version": 1,
        "algorithm": "sha256",
        "run_id": run_id,
        "run_fingerprint": run_fingerprint,
        "files": [
            {
                "path": str(path.relative_to(output_root)),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for path in artifact_files
        ],
    }
    _write_json(manifest_path, manifest)
    return _return_payload(summary, output_root)


__all__ = ["run_research_lite"]
