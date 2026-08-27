"""Resumable two-stage historical factor research runner."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import re
import subprocess
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import scipy

from factor_lab.data import audit_suspensions_snapshot
from factor_lab.data.pit_lineage import (
    PIT_CONTRACT_SCHEMA_VERSION,
    audit_pit_lineage,
    conservative_default_contract,
)
from factor_lab.portfolio import (
    ADJUSTED_TOTAL_RETURN_PRICE_BASIS,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)

from .contracts import FactorSpec, ValidationSpec
from .reporting import render_report
from .signals import directed_rank_blend, evaluate_factor_signal
from .walk_forward import WalkForwardSelectorSpec
from .walk_forward_runtime import run_walk_forward_sweep
from .validation import (
    FactorValidation,
    StageASelection,
    build_stage_a_selection,
    deterministic_block_bootstrap_mean,
    diagnose_train_similarity,
    evaluate_stage_a,
)


ENGINE_ID = "factor-lab/research/v7"
EVIDENCE_CLASS = "historical_diagnostic"
RESULTS_FIRST_SUITE = "results-first"
WALK_FORWARD_SUITE = "walk-forward"
ADAPTIVE_SUITE = "adaptive"
_DETAIL_FIELDS = {"periods", "trades", "optimization_audit", "account_nav_path"}
_REQUIRED_PROMOTION_GATE_KEYS = {
    "validation_net_excess_annual_return_min",
    "validation_net_sharpe_min",
    "validation_information_ratio_min",
    "validation_max_drawdown_min",
    "positive_half_year_ratio_min",
    "average_holding_count_min",
    "capacity_violation_count_max",
    "validation_excess_mean_bootstrap_lower_min",
    "benchmark_return_coverage_min",
    "execution_input_policy_match_ratio_min",
    "execution_input_future_violation_count_max",
    "execution_input_coverage_min",
    "validation_observations_min",
    "execution_period_coverage_min",
    "signal_evaluable_date_ratio_min",
    "signal_median_cross_section_coverage_min",
}
_ROBUSTNESS_ABSOLUTE_BLOCKERS = {
    "average_holding_count_below_threshold",
    "capacity_violation",
    "benchmark_return_coverage_below_threshold",
    "execution_input_policy_mismatch",
    "future_execution_input_detected",
    "execution_input_coverage_below_threshold",
    "validation_observations_below_threshold",
    "execution_period_coverage_below_threshold",
    "validation_signal_evaluable_ratio_below_threshold",
    "validation_signal_cross_section_coverage_below_threshold",
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _manifest_payload_sha256(manifest: Mapping[str, Any]) -> str:
    """Detect manifest-envelope damage; this is not an external trust root."""

    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    return _sha256_value(payload)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _completed_run_valid(summary_path: Path, output_dir: Path, run_fingerprint: str) -> bool:
    """Verify the immutable outputs before accepting a completed checkpoint."""

    manifest_path = output_dir / "manifest.json"
    if not summary_path.is_file() or not manifest_path.is_file():
        return False
    try:
        summary = _read_json(summary_path)
        manifest = _read_json(manifest_path)
        expected_run_id = run_fingerprint[:16]
        if summary.get("status") != "completed":
            return False
        if summary.get("run_fingerprint") != run_fingerprint:
            return False
        if manifest.get("run_fingerprint") != run_fingerprint:
            return False
        if (
            manifest.get("schema_version") != 2
            or manifest.get("algorithm") != "sha256"
            or manifest.get("run_id") != expected_run_id
            or summary.get("run_id") != expected_run_id
            or output_dir.resolve().name != expected_run_id
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(manifest.get("manifest_sha256") or "")
            )
            or manifest.get("manifest_sha256")
            != _manifest_payload_sha256(manifest)
        ):
            return False
        inputs = manifest.get("inputs") or []
        if not isinstance(inputs, list):
            return False
        suspension_inputs = [
            row
            for row in inputs
            if isinstance(row, Mapping)
            and row.get("role") == "tushare_suspend_d"
        ]
        if len(suspension_inputs) != 1:
            return False
        suspension_input = suspension_inputs[0]
        suspension_status = suspension_input.get("status")
        suspension_path = Path(str(suspension_input.get("path") or ""))
        suspension_metadata_path = Path(
            str(suspension_input.get("metadata_path") or "")
        )
        summary_data = summary.get("data") or {}
        summary_suspension_hash = summary_data.get("suspension_sha256")
        summary_suspension_metadata_hash = summary_data.get(
            "suspension_metadata_sha256"
        )
        summary_suspension_audit = summary_data.get("suspension_snapshot_audit")
        if (
            not suspension_path.is_absolute()
            or not suspension_metadata_path.is_absolute()
            or summary_data.get("suspension_status") != suspension_status
            or summary_data.get("suspension_path") != str(suspension_path)
            or summary_data.get("suspension_metadata_path")
            != str(suspension_metadata_path)
        ):
            return False
        if suspension_status == "available":
            if (
                not suspension_path.is_absolute()
                or not suspension_path.is_file()
                or not suspension_metadata_path.is_absolute()
                or not suspension_metadata_path.is_file()
                or suspension_input.get("size_bytes")
                != suspension_path.stat().st_size
                or suspension_input.get("metadata_size_bytes")
                != suspension_metadata_path.stat().st_size
                or suspension_input.get("sha256") != summary_suspension_hash
                or suspension_input.get("metadata_sha256")
                != summary_suspension_metadata_hash
                or _sha256_file(suspension_path) != summary_suspension_hash
                or _sha256_file(suspension_metadata_path)
                != summary_suspension_metadata_hash
            ):
                return False
            actual_suspension_audit = audit_suspensions_snapshot(
                suspension_path,
                metadata_path=suspension_metadata_path,
            )
            if (
                suspension_input.get("audit") != summary_suspension_audit
                or suspension_input.get("audit") != actual_suspension_audit
            ):
                return False
        elif suspension_status == "unavailable":
            if (
                suspension_input.get("sha256") is not None
                or suspension_input.get("metadata_sha256") is not None
                or suspension_input.get("size_bytes") is not None
                or suspension_input.get("metadata_size_bytes") is not None
                or suspension_input.get("audit") is not None
                or summary_suspension_hash is not None
                or summary_suspension_metadata_hash is not None
                or summary_suspension_audit is not None
                or suspension_path.exists()
                or suspension_metadata_path.exists()
            ):
                return False
        else:
            return False
        protocol_inputs = [
            row
            for row in inputs
            if isinstance(row, Mapping)
            and row.get("role") == "adaptive_protocol"
        ]
        if summary.get("suite") == ADAPTIVE_SUITE:
            if len(protocol_inputs) != 1:
                return False
            protocol_input = protocol_inputs[0]
            protocol_file = Path(str(protocol_input.get("path") or ""))
            adaptive_summary = summary.get("adaptive") or {}
            expected_protocol_hash = (
                adaptive_summary.get("protocol_sha256")
                or (adaptive_summary.get("protocol") or {}).get("sha256")
            )
            if (
                not protocol_file.is_absolute()
                or not protocol_file.is_file()
                or protocol_input.get("size_bytes")
                != protocol_file.stat().st_size
                or protocol_input.get("sha256") != expected_protocol_hash
                or _sha256_file(protocol_file) != expected_protocol_hash
            ):
                return False
        elif protocol_inputs:
            return False
        rows = manifest.get("files") or []
        if not isinstance(rows, list) or not rows:
            return False
        root = output_dir.resolve()
        names: set[str] = set()
        for row in rows:
            relative = Path(str(row["path"]))
            if relative.is_absolute():
                return False
            path = (output_dir / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                return False
            normalized = relative.as_posix()
            if (
                normalized in names
                or row.get("size_bytes") != path.stat().st_size
                or _sha256_file(path) != row.get("sha256")
            ):
                return False
            names.add(normalized)
        required = {"summary.json", "report.md", "pit-lineage.json"}
        required.update(
            f"factors/{_safe_name(str(name))}.json"
            for name in summary.get("stage_b_selected") or []
        )
        walk_forward = summary.get("walk_forward") or {}
        if walk_forward.get("enabled") and not walk_forward.get("canary_smoke_only"):
            required.add("walk-forward/walk-forward-summary.json")
            candidate_registry = tuple(walk_forward.get("candidate_registry") or ())
            for offset in walk_forward.get("rebalance_offsets") or ():
                offset_root = f"walk-forward/offset-{int(offset):02d}"
                required.add(f"{offset_root}/decisions.json")
                required.add(f"{offset_root}/dynamic.json")
                if (walk_forward.get("fixed_comparator") or {}).get("factor_name"):
                    required.add(f"{offset_root}/fixed-comparator.json")
                required.update(
                    f"{offset_root}/static/{_safe_name(str(name))}.json"
                    for name in candidate_registry
                )
                required.update(
                    f"{offset_root}/scoring/static/{_safe_name(str(name))}.json"
                    for name in candidate_registry
                )
        adaptive = summary.get("adaptive") or {}
        if adaptive.get("enabled") and not adaptive.get("canary_smoke_only"):
            required.add("adaptive/adaptive-summary.json")
            expert_registry = tuple(adaptive.get("expert_registry") or ())
            account_registry = tuple(adaptive.get("account_registry") or ())
            for offset in adaptive.get("rebalance_offsets") or ():
                offset_root = f"adaptive/offset-{int(offset):02d}"
                required.add(f"{offset_root}/decisions.json")
                required.update(
                    f"{offset_root}/shadows/{_safe_name(str(name))}.json"
                    for name in expert_registry
                )
                required.update(
                    f"{offset_root}/accounts/{_safe_name(str(name))}.json"
                    for name in account_registry
                )
        return required.issubset(names)
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        json.JSONDecodeError,
    ):
        return False


def _git_state(project_root: Path) -> dict[str, Any]:
    def command(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args], cwd=project_root, check=True, capture_output=True, text=True
            )
            return result.stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "commit": command("rev-parse", "HEAD"),
        "branch": command("branch", "--show-current"),
        "dirty": bool(command("status", "--porcelain")),
    }


def _implementation_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    paths = [
        *sorted((package_root / "research").glob("*.py")),
        *sorted((package_root / "portfolio").glob("*.py")),
        package_root / "cli.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        if not path.is_file():
            continue
        digest.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _data_builder_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted((package_root / "data").glob("*.py")):
        digest.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _runtime_identity() -> dict[str, Any]:
    """Return the exact numerical runtime bound into resumable artifacts."""

    return {
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
        },
        "packages": {
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
            "scipy": scipy.__version__,
        },
    }


def _suspension_input_identity(
    path: Path,
    metadata_path: Path,
    *,
    requested_start: str,
    requested_end: str,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    if not path.exists() and not metadata_path.exists():
        return None, None, None
    audit = audit_suspensions_snapshot(
        path,
        metadata_path=metadata_path,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    return audit, str(audit["hash"]), _sha256_file(metadata_path)


def _verify_loaded_input_snapshot(
    *,
    feature_path: Path,
    feature_sha256: str,
    execution_path: Path,
    execution_sha256: str,
    suspension_path: Path,
    suspension_metadata_path: Path,
    suspension_audit: Mapping[str, Any] | None,
    suspension_sha256: str | None,
    suspension_metadata_sha256: str | None,
    suspension_requested_start: str,
    suspension_requested_end: str,
    adaptive_protocol_path: Path | None = None,
    adaptive_protocol_sha256: str | None = None,
) -> tuple[str, str, dict[str, Any] | None, str | None, str | None]:
    """Fail if a frozen input changed after it was fingerprinted."""

    if (adaptive_protocol_path is None) != (adaptive_protocol_sha256 is None):
        raise ValueError(
            "adaptive protocol path and SHA-256 must either both be set or both be absent"
        )

    try:
        current_feature_hash = _sha256_file(feature_path)
        current_execution_hash = _sha256_file(execution_path)
        current_protocol_hash = (
            _sha256_file(adaptive_protocol_path)
            if adaptive_protocol_path is not None
            else None
        )
        (
            current_suspension_audit,
            current_suspension_hash,
            current_suspension_metadata_hash,
        ) = _suspension_input_identity(
            suspension_path,
            suspension_metadata_path,
            requested_start=suspension_requested_start,
            requested_end=suspension_requested_end,
        )
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "research input changed or became invalid during execution"
        ) from exc

    changed: list[str] = []
    for role, expected, actual in (
        ("features", feature_sha256, current_feature_hash),
        ("execution", execution_sha256, current_execution_hash),
        ("suspensions", suspension_sha256, current_suspension_hash),
        (
            "suspensions_metadata",
            suspension_metadata_sha256,
            current_suspension_metadata_hash,
        ),
        ("suspensions_audit", suspension_audit, current_suspension_audit),
        ("adaptive_protocol", adaptive_protocol_sha256, current_protocol_hash),
    ):
        if expected != actual:
            changed.append(role)
    if changed:
        raise RuntimeError(
            "research input changed during execution: " + ", ".join(changed)
        )
    return (
        current_feature_hash,
        current_execution_hash,
        current_suspension_audit,
        current_suspension_hash,
        current_suspension_metadata_hash,
    )


def _frame_identity_sha256(
    frame: pd.DataFrame, columns: Sequence[str]
) -> str:
    present = [column for column in columns if column in frame.columns]
    digest = hashlib.sha256()
    digest.update(_canonical_json(present).encode("utf-8"))
    if present:
        normalized = frame[present].copy()
        for column in present:
            if pd.api.types.is_datetime64_any_dtype(normalized[column]):
                normalized[column] = normalized[column].dt.strftime(
                    "%Y-%m-%dT%H:%M:%S.%f"
                )
        normalized = normalized.sort_values(present).reset_index(drop=True)
        digest.update(
            pd.util.hash_pandas_object(normalized, index=False)
            .to_numpy(dtype=np.uint64, copy=False)
            .tobytes()
        )
    return digest.hexdigest()


def _project_root(value: str | Path | None = None) -> Path:
    if value is not None:
        return Path(value).resolve()
    return Path(__file__).resolve().parents[3]


def _default_data_paths(root: Path) -> tuple[Path, Path]:
    canonical = root / "runtime" / "data" / "top500"
    features = canonical / "features.parquet"
    execution = canonical / "execution.parquet"
    if features.is_file() and execution.is_file():
        return features, execution
    legacy = root / "artifacts" / "expanded_long_only" / "feature_store"
    return legacy / "expanded_top500_features.parquet", legacy / "expanded_execution_prices.parquet"


def load_factor_suite(path: str | Path, suite: str) -> tuple[FactorSpec, list[FactorSpec]]:
    payload = _read_json(Path(path))
    control = FactorSpec.from_mapping(payload.get("control") or {})
    suite_rows = (payload.get("suites") or {}).get(suite)
    if not isinstance(suite_rows, list):
        raise ValueError(f"unknown factor suite: {suite}")
    rows = [FactorSpec.from_mapping(row) for row in suite_rows]
    # Some legacy suites repeat the separately declared control row.  Remove
    # that one redundant declaration, then reject every remaining ambiguity;
    # silently collapsing duplicate challengers can bind one signal to another
    # factor's artifact.
    rows = [row for row in rows if row.name != control.name]
    _assert_unique_artifact_names(
        [control.name, *(row.name for row in rows)],
        context=f"{suite} factor registry",
    )
    return control, rows


def _validation_spec(config: Mapping[str, Any]) -> ValidationSpec:
    values = dict(config.get("validation") or {})
    return ValidationSpec(**values)


def _portfolio_config(
    config: Mapping[str, Any],
    *,
    suite: str | None = None,
    adaptive_protocol: Mapping[str, Any] | None = None,
) -> LongOnlyPortfolioConfig:
    if suite not in {RESULTS_FIRST_SUITE, WALK_FORWARD_SUITE, ADAPTIVE_SUITE}:
        result = LongOnlyPortfolioConfig.from_mapping(config)
    else:
        merged = dict(config)
        portfolio = dict(config.get("portfolio") or {})
        if suite == ADAPTIVE_SUITE:
            if adaptive_protocol is None:
                raise ValueError("adaptive suite requires a frozen protocol")
            frozen = dict(adaptive_protocol.get("portfolio") or {})
            portfolio.update(
                {
                    "holding_days": frozen["holding_days"],
                    "rebalance_every_days": frozen["rebalance_every_days"],
                    "position_count": frozen["position_count_per_expert"],
                    "target_weight": 1.0
                    / int(frozen["position_count_per_expert"]),
                    "retention_buffer": frozen["retention_buffer"],
                    "periods_per_year": frozen["periods_per_year"],
                }
            )
        else:
            suite_settings = (
                config.get("results_first")
                if suite == RESULTS_FIRST_SUITE
                else config.get("walk_forward")
            ) or {}
            portfolio.update(dict(suite_settings.get("portfolio") or {}))
        merged["portfolio"] = portfolio
        result = LongOnlyPortfolioConfig.from_mapping(merged)
    if result.price_basis != ADJUSTED_TOTAL_RETURN_PRICE_BASIS:
        raise ValueError(
            "research runner only accepts price_basis=adjusted_total_return; "
            "raw_with_actions is not backed by an attested production artifact"
        )
    if result.open_column != "open_adj":
        raise ValueError(
            "research runner adjusted_total_return contract requires open_column=open_adj"
        )
    if result.lot_size != 0:
        raise ValueError(
            "research runner adjusted_total_return contract requires lot_size=0"
        )
    return result


def _feature_columns(
    path: Path,
    factors: Sequence[FactorSpec],
    validation: ValidationSpec,
    *,
    extra_required_fields: Sequence[str] = (),
) -> list[str]:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    wanted = {
        validation.date_column,
        "ticker",
        "eligible",
        "universe_member",
        "st_filter_status",
        "label_exit_date",
        "financial_available_date",
        *validation.label_columns,
        *extra_required_fields,
    }
    for factor in factors:
        wanted.update(factor.required_fields)
    missing = sorted(
        {validation.date_column, "ticker", *map(str, extra_required_fields)}
        - available
    )
    if missing:
        raise ValueError(f"feature store missing required columns: {missing}")
    return sorted(wanted & available)


def _load_features(
    path: Path,
    factors: Sequence[FactorSpec],
    validation: ValidationSpec,
    *,
    extra_required_fields: Sequence[str] = (),
) -> pd.DataFrame:
    frame = pd.read_parquet(
        path,
        columns=_feature_columns(
            path,
            factors,
            validation,
            extra_required_fields=extra_required_fields,
        ),
    )
    frame[validation.date_column] = pd.to_datetime(frame[validation.date_column], errors="coerce")
    frame["ticker"] = frame["ticker"].astype(str)
    if frame[validation.date_column].isna().any():
        raise ValueError("feature store contains invalid dates")
    if frame.duplicated([validation.date_column, "ticker"]).any():
        raise ValueError("feature store contains duplicate date/ticker rows")
    input_rows = len(frame)
    applied: list[str] = []
    research_mask = pd.Series(True, index=frame.index)
    status_counts = (
        {
            str(key): int(value)
            for key, value in frame["st_filter_status"].fillna("missing").value_counts().items()
        }
        if "st_filter_status" in frame.columns
        else {}
    )
    for column in ("eligible", "universe_member"):
        if column not in frame.columns:
            continue
        applied.append(column)
        values = frame[column]
        if values.dtype == bool:
            accepted = values.fillna(False)
        else:
            accepted = values.astype(str).str.strip().str.casefold().isin(
                {"1", "true", "yes", "y"}
            )
        research_mask &= accepted
    excluded_status_counts = (
        {
            str(key): int(value)
            for key, value in frame.loc[~research_mask, "st_filter_status"]
            .fillna("missing")
            .value_counts()
            .items()
        }
        if "st_filter_status" in frame.columns
        else {}
    )
    frame = frame.loc[research_mask].copy()
    if frame.empty:
        raise ValueError("feature store has no eligible universe rows for research")
    result = frame.sort_values([validation.date_column, "ticker"]).reset_index(drop=True)
    result.attrs["research_universe_filter"] = {
        "columns_applied": applied,
        "input_row_count": int(input_rows),
        "included_row_count": int(len(result)),
        "excluded_row_count": int(input_rows - len(result)),
        "included_ratio": round(float(len(result) / input_rows), 8) if input_rows else 0.0,
        "st_filter_status_counts": status_counts,
        "excluded_st_filter_status_counts": excluded_status_counts,
    }
    return result


def _resolve_column(available: set[str], preferred: str, aliases: Sequence[str]) -> str | None:
    return next((name for name in (preferred, *aliases) if name in available), None)


_SUSPENSION_INTERVAL_PATTERN = re.compile(
    r"(?P<start_hour>\d{1,2}):(?P<start_minute>\d{2})\s*"
    r"(?:-|~|—|–|至)\s*"
    r"(?P<end_hour>\d{1,2}):(?P<end_minute>\d{2})"
)
_MARKET_OPEN_MINUTE = 9 * 60 + 30


def _event_flag_series(values: pd.Series) -> pd.Series:
    """Normalize vendor/event booleans without treating arbitrary strings as true."""

    if pd.api.types.is_bool_dtype(values.dtype):
        return values.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(values.dtype):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    return (
        values.astype("string")
        .str.strip()
        .str.casefold()
        .isin({"1", "true", "yes", "y", "on"})
    )


def _suspension_timing_class(value: Any) -> str:
    """Classify whether an authoritative S event blocks that session's open."""

    if value is None or pd.isna(value):
        return "full_day"
    timing = str(value).strip()
    if not timing:
        return "full_day"
    for match in _SUSPENSION_INTERVAL_PATTERN.finditer(timing):
        start_hour = int(match.group("start_hour"))
        start_minute = int(match.group("start_minute"))
        end_hour = int(match.group("end_hour"))
        end_minute = int(match.group("end_minute"))
        if not (
            0 <= start_hour <= 23
            and 0 <= end_hour <= 23
            and 0 <= start_minute <= 59
            and 0 <= end_minute <= 59
        ):
            continue
        start = start_hour * 60 + start_minute
        end = end_hour * 60 + end_minute
        if start <= _MARKET_OPEN_MINUTE <= end:
            return "open_intraday"
    return "ignored_after_open"


def _inject_delist_events(
    frame: pd.DataFrame,
    *,
    feature_path: Path,
    date_column: str,
    ticker_column: str,
) -> pd.DataFrame:
    """Project a known delist onto every session at/after its event date.

    The flag is never visible before the event date.  Event-only rows carry no
    executable price or liquidity, so the accounting kernel can write down an
    existing holding without fabricating a sale or cash recovery.
    """

    available = set(pq.ParquetFile(feature_path).schema_arrow.names)
    if not {"ticker", "delist_date"}.issubset(available):
        frame.attrs["security_event_injection"] = {
            "status": "unavailable",
            "reason": "feature_store_missing_ticker_or_delist_date",
            "delist_security_count": 0,
            "delist_flagged_session_count": 0,
            "event_only_row_count": 0,
        }
        return frame
    reference = pd.read_parquet(
        feature_path, columns=["ticker", "delist_date"]
    )
    reference["ticker"] = reference["ticker"].astype(str)
    reference["delist_date"] = pd.to_datetime(
        reference["delist_date"], errors="coerce"
    ).dt.normalize()
    reference = (
        reference.dropna(subset=["delist_date"])
        .groupby("ticker", as_index=False, sort=True)["delist_date"]
        .min()
    )
    calendar = pd.DatetimeIndex(
        sorted(pd.to_datetime(frame[date_column], errors="coerce").dropna().unique())
    )
    if reference.empty or calendar.empty:
        frame.attrs["security_event_injection"] = {
            "status": "available",
            "availability_rule": "every_execution_session_on_or_after_delist_date",
            "delist_security_count": 0,
            "delist_flagged_session_count": 0,
            "event_only_row_count": 0,
        }
        return frame
    events: list[dict[str, Any]] = []
    delist_security_count = 0
    for row in reference.itertuples(index=False):
        event_date = pd.Timestamp(row.delist_date)
        position = int(calendar.searchsorted(event_date, side="left"))
        if position >= len(calendar):
            continue
        delist_security_count += 1
        events.extend(
            {
                date_column: pd.Timestamp(session),
                ticker_column: str(row.ticker),
                "is_delisted": True,
            }
            for session in calendar[position:]
        )
    if not events:
        frame.attrs["security_event_injection"] = {
            "status": "available",
            "availability_rule": "every_execution_session_on_or_after_delist_date",
            "delist_security_count": 0,
            "delist_flagged_session_count": 0,
            "event_only_row_count": 0,
        }
        return frame

    output = frame.copy()
    if "is_delisted" not in output.columns:
        output["is_delisted"] = False
    else:
        output["is_delisted"] = output["is_delisted"].fillna(False).astype(bool)
    event_frame = pd.DataFrame(events).drop_duplicates(
        [date_column, ticker_column], keep="last"
    )
    event_flags = event_frame[[date_column, ticker_column]].copy()
    event_flags["_factor_lab_delist_event"] = True
    output = output.merge(
        event_flags,
        on=[date_column, ticker_column],
        how="outer",
        validate="many_to_one",
        indicator="_factor_lab_security_event_merge",
    )
    event_only = output["_factor_lab_security_event_merge"].eq("right_only")
    event_only_count = int(event_only.sum())
    output["is_delisted"] = (
        output["is_delisted"].astype("boolean").fillna(False).astype(bool)
        | output["_factor_lab_delist_event"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )
    for column in ("eligible", "universe_member"):
        if column in output.columns:
            output.loc[event_only, column] = False
    output = output.drop(
        columns=[
            "_factor_lab_delist_event",
            "_factor_lab_security_event_merge",
        ]
    )
    output = output.sort_values([date_column, ticker_column]).reset_index(drop=True)
    output.attrs["security_event_injection"] = {
        "status": "available",
        "availability_rule": "every_execution_session_on_or_after_delist_date",
        "delist_security_count": int(delist_security_count),
        "delist_flagged_session_count": int(len(event_frame)),
        "event_only_row_count": int(event_only_count),
        "cash_recovery_policy": "zero_unless_explicit_event_terms_exist",
    }
    return output


def _inject_suspension_events(
    frame: pd.DataFrame,
    *,
    suspension_path: Path | None,
    date_column: str,
    ticker_column: str,
    snapshot_audit: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Merge authoritative Tushare ``suspend_d`` S events into execution.

    A blank ``suspend_timing`` means a full-session suspension.  A timed event
    blocks the opening auction only when an interval covers 09:30.  Later
    intraday suspensions and R markers never rewrite opening tradability.
    """

    metadata = dict(frame.attrs.get("security_event_injection") or {})
    resolved_path = suspension_path.resolve() if suspension_path is not None else None
    unavailable = {
        "suspension_status": "unavailable",
        "suspension_source": "tushare_suspend_d",
        "suspension_artifact_path": (
            str(resolved_path) if resolved_path is not None else None
        ),
        "suspension_artifact_sha256": None,
        "suspension_unavailable_reason": (
            "artifact_not_configured"
            if resolved_path is None
            else "artifact_missing"
        ),
        "suspension_timing_policy": (
            "S_with_blank_timing_or_interval_covering_09:30_blocks_open;"
            "R_and_S_after_open_do_not_override_open"
        ),
        "suspension_source_row_count": 0,
        "suspension_full_day_session_count": 0,
        "suspension_open_intraday_session_count": 0,
        "suspension_ignored_after_open_session_count": 0,
        "suspension_resume_marker_count": 0,
        "suspension_flagged_session_count": 0,
        "suspension_security_count": 0,
        "suspension_event_only_row_count": 0,
        "suspension_ignored_delisted_session_count": 0,
    }
    if resolved_path is None:
        metadata.update(unavailable)
        frame.attrs["security_event_injection"] = metadata
        return frame

    resolved_metadata_path = resolved_path.with_name("suspensions.meta.json")
    if not resolved_path.exists() and not resolved_metadata_path.exists():
        metadata.update(unavailable)
        frame.attrs["security_event_injection"] = metadata
        return frame
    audited = dict(
        snapshot_audit
        or audit_suspensions_snapshot(
            resolved_path,
            metadata_path=resolved_metadata_path,
        )
    )
    if (
        audited.get("status") != "complete"
        or Path(str(audited.get("path") or "")).resolve() != resolved_path
        or Path(str(audited.get("metadata_path") or "")).resolve()
        != resolved_metadata_path.resolve()
        or not re.fullmatch(r"[0-9a-f]{64}", str(audited.get("hash") or ""))
    ):
        raise ValueError("invalid audited suspension snapshot identity")

    available = set(pq.ParquetFile(resolved_path).schema_arrow.names)
    event_ticker_column = _resolve_column(available, "ticker", ("ts_code",))
    event_date_column = _resolve_column(available, "date", ("trade_date",))
    missing = [
        name
        for name, resolved in (
            ("ticker/ts_code", event_ticker_column),
            ("date/trade_date", event_date_column),
            ("suspend_type", "suspend_type" if "suspend_type" in available else None),
            (
                "suspend_timing",
                "suspend_timing" if "suspend_timing" in available else None,
            ),
        )
        if resolved is None
    ]
    if missing:
        raise ValueError(
            "suspension artifact missing required fields: " + ", ".join(missing)
        )
    assert event_ticker_column and event_date_column
    events = pd.read_parquet(
        resolved_path,
        columns=[
            event_ticker_column,
            event_date_column,
            "suspend_type",
            "suspend_timing",
        ],
    ).rename(
        columns={
            event_ticker_column: "_event_ticker",
            event_date_column: "_event_date",
            "suspend_type": "_event_type",
            "suspend_timing": "_event_timing",
        }
    )
    source_row_count = int(len(events))
    events["_event_ticker"] = events["_event_ticker"].astype("string").str.strip()
    events["_event_date"] = pd.to_datetime(
        events["_event_date"], errors="coerce"
    ).dt.normalize()
    events["_event_type"] = (
        events["_event_type"].astype("string").str.strip().str.upper()
    )
    invalid_identity = (
        events["_event_ticker"].isna()
        | events["_event_ticker"].eq("")
        | events["_event_date"].isna()
    )
    if bool(invalid_identity.any()):
        raise ValueError("suspension artifact contains invalid ticker/date identities")
    unknown_types = sorted(
        set(events.loc[~events["_event_type"].isin({"S", "R"}), "_event_type"])
    )
    if unknown_types:
        raise ValueError(
            "suspension artifact contains unsupported suspend_type values: "
            + ", ".join(map(str, unknown_types))
        )
    calendar = set(pd.to_datetime(frame[date_column], errors="coerce").dt.normalize())
    events = events.loc[events["_event_date"].isin(calendar)].copy()
    events["_row_timing_class"] = "resume"
    suspension_mask = events["_event_type"].eq("S")
    events.loc[suspension_mask, "_row_timing_class"] = events.loc[
        suspension_mask, "_event_timing"
    ].map(_suspension_timing_class)
    timing_priority = {"ignored_after_open": 0, "open_intraday": 1, "full_day": 2}
    events["_timing_priority"] = events["_row_timing_class"].map(
        timing_priority
    ).fillna(-1)
    resume_marker_count = int(
        events.loc[events["_event_type"].eq("R"), ["_event_date", "_event_ticker"]]
        .drop_duplicates()
        .shape[0]
    )
    # Tushare legitimately reports S and R for the same ticker/session when a
    # security halts and resumes intraday.  R is evidence only: it cannot
    # cancel an S interval that blocks the 09:30 open.
    session_events = (
        events.sort_values(
            ["_event_date", "_event_ticker", "_timing_priority"],
            kind="mergesort",
        )
        .drop_duplicates(["_event_date", "_event_ticker"], keep="last")
        .rename(columns={"_row_timing_class": "_timing_class"})
        .reset_index(drop=True)
    )
    full_day_count = int(session_events["_timing_class"].eq("full_day").sum())
    open_intraday_count = int(
        session_events["_timing_class"].eq("open_intraday").sum()
    )
    ignored_after_open_count = int(
        session_events["_timing_class"].eq("ignored_after_open").sum()
    )
    blocking = session_events.loc[
        session_events["_timing_class"].isin({"full_day", "open_intraday"}),
        ["_event_date", "_event_ticker", "_timing_class"],
    ].rename(
        columns={
            "_event_date": date_column,
            "_event_ticker": ticker_column,
            "_timing_class": "_factor_lab_suspension_class",
        }
    )
    blocking["_factor_lab_suspension_event"] = True

    output = frame.copy()
    suspension_column = _resolve_column(
        set(output.columns), "is_suspended", ("suspended", "is_pause", "paused")
    )
    if suspension_column is None:
        suspension_column = "is_suspended"
        output[suspension_column] = False
    else:
        output[suspension_column] = _event_flag_series(output[suspension_column])
    if blocking.empty:
        event_only = pd.Series(False, index=output.index)
        event_mask = pd.Series(False, index=output.index)
        output["_factor_lab_suspension_class"] = pd.NA
    else:
        output = output.merge(
            blocking,
            on=[date_column, ticker_column],
            how="outer",
            validate="one_to_one",
            indicator="_factor_lab_suspension_merge",
        )
        event_only = output["_factor_lab_suspension_merge"].eq("right_only")
        event_mask = _event_flag_series(output["_factor_lab_suspension_event"])
    delisted_column = _resolve_column(
        set(output.columns), "is_delisted", ("delisted", "delist_flag")
    )
    delisted_mask = (
        _event_flag_series(output[delisted_column])
        if delisted_column is not None
        else pd.Series(False, index=output.index)
    )
    effective_event = event_mask & ~delisted_mask
    output[suspension_column] = (
        _event_flag_series(output[suspension_column]) | effective_event
    )
    output.loc[delisted_mask, suspension_column] = False
    for column in ("eligible", "universe_member"):
        if column in output.columns:
            output.loc[event_only & effective_event, column] = False

    effective_rows = output.loc[effective_event]
    effective_full_day_count = int(
        effective_rows["_factor_lab_suspension_class"].eq("full_day").sum()
    )
    effective_open_intraday_count = int(
        effective_rows["_factor_lab_suspension_class"].eq("open_intraday").sum()
    )
    event_only_count = int((event_only & effective_event).sum())
    ignored_delisted_count = int((event_mask & delisted_mask).sum())
    drop_columns = [
        "_factor_lab_suspension_class",
        "_factor_lab_suspension_event",
        "_factor_lab_suspension_merge",
    ]
    output = output.drop(
        columns=[column for column in drop_columns if column in output.columns]
    )
    output = output.sort_values([date_column, ticker_column]).reset_index(drop=True)
    metadata.update(
        {
            **unavailable,
            "suspension_status": "available",
            "suspension_artifact_sha256": (
                str(audited["hash"])
            ),
            "suspension_unavailable_reason": None,
            "suspension_source_row_count": source_row_count,
            "suspension_full_day_session_count": effective_full_day_count,
            "suspension_open_intraday_session_count": (
                effective_open_intraday_count
            ),
            "suspension_ignored_after_open_session_count": (
                ignored_after_open_count
            ),
            "suspension_resume_marker_count": resume_marker_count,
            "suspension_flagged_session_count": int(effective_event.sum()),
            "suspension_security_count": int(
                effective_rows[ticker_column].astype(str).nunique()
            ),
            "suspension_event_only_row_count": event_only_count,
            "suspension_ignored_delisted_session_count": ignored_delisted_count,
            "suspension_source_full_day_session_count": full_day_count,
            "suspension_source_open_intraday_session_count": open_intraday_count,
        }
    )
    output.attrs["security_event_injection"] = metadata
    return output


def _load_execution(
    path: Path,
    config: LongOnlyPortfolioConfig,
    *,
    feature_path: Path | None = None,
    suspension_path: Path | None = None,
    suspension_snapshot_audit: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    date_column = _resolve_column(available, config.date_column, ("date", "trade_date"))
    ticker_column = _resolve_column(available, config.ticker_column, ("ticker", "ts_code", "symbol"))
    open_column = _resolve_column(available, config.open_column, ("open_price", "open_adj", "open"))
    adv_column = _resolve_column(available, config.adv_column, ("amount_20d_avg", "adv", "average_daily_value"))
    volatility_column = _resolve_column(available, config.volatility_column, ("volatility", "vol_20"))
    required = {
        "date": date_column,
        "ticker": ticker_column,
        "open": open_column,
        "adv": adv_column,
        "volatility": volatility_column,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ValueError(f"execution store missing required fields: {missing}")
    optional_resolved = {
        *(
            column
            for column in config.eligible_columns
            if column in available
        ),
        *(
            value
            for value in (
                _resolve_column(
                    available,
                    config.limit_up_column,
                    (
                        "one_price_limit_up",
                        "limit_up",
                        "is_limit_up",
                        "up_limit_locked",
                    ),
                ),
                _resolve_column(
                    available,
                    config.limit_down_column,
                    (
                        "one_price_limit_down",
                        "limit_down",
                        "is_limit_down",
                        "down_limit_locked",
                    ),
                ),
                _resolve_column(
                    available,
                    "is_suspended",
                    ("suspended", "is_pause", "paused"),
                ),
                _resolve_column(
                    available,
                    "is_delisted",
                    ("delisted", "delist_flag"),
                ),
                _resolve_column(
                    available,
                    "split_ratio",
                    ("share_split_ratio",),
                ),
                _resolve_column(
                    available,
                    "cash_dividend",
                    ("cash_dividend_per_share",),
                ),
            )
            if value is not None
        ),
    }
    columns = {value for value in required.values() if value} | optional_resolved
    frame = pd.read_parquet(path, columns=sorted(columns))
    assert date_column and ticker_column
    frame[date_column] = pd.to_datetime(
        frame[date_column], errors="coerce"
    ).dt.normalize()
    frame[ticker_column] = frame[ticker_column].astype(str)
    frame = frame.dropna(subset=[date_column, ticker_column]).sort_values(
        [date_column, ticker_column]
    )
    if feature_path is not None:
        frame = _inject_delist_events(
            frame,
            feature_path=feature_path,
            date_column=date_column,
            ticker_column=ticker_column,
        )
    frame = _inject_suspension_events(
        frame,
        suspension_path=suspension_path,
        date_column=date_column,
        ticker_column=ticker_column,
        snapshot_audit=suspension_snapshot_audit,
    )
    return frame


def _execution_lineage_fields(
    frame: pd.DataFrame,
    config: LongOnlyPortfolioConfig,
) -> tuple[str, ...]:
    """Return the actual execution columns consumed by the portfolio kernel."""

    available = set(frame.columns)
    resolved = [
        _resolve_column(
            available,
            config.open_column,
            ("open_price", "open_adj", "open"),
        ),
        _resolve_column(
            available,
            config.adv_column,
            ("amount_20d_avg", "adv", "average_daily_value"),
        ),
        _resolve_column(
            available,
            config.volatility_column,
            ("volatility", "vol_20"),
        ),
        _resolve_column(
            available,
            config.limit_up_column,
            (
                "one_price_limit_up",
                "limit_up",
                "is_limit_up",
                "up_limit_locked",
            ),
        ),
        _resolve_column(
            available,
            config.limit_down_column,
            (
                "one_price_limit_down",
                "limit_down",
                "is_limit_down",
                "down_limit_locked",
            ),
        ),
        _resolve_column(
            available,
            "is_suspended",
            ("suspended", "is_pause", "paused"),
        ),
        _resolve_column(
            available,
            "is_delisted",
            ("delisted", "delist_flag"),
        ),
        _resolve_column(
            available,
            "split_ratio",
            ("share_split_ratio",),
        ),
        _resolve_column(
            available,
            "cash_dividend",
            ("cash_dividend_per_share",),
        ),
    ]
    resolved.extend(
        column for column in config.eligible_columns if column in available
    )
    return tuple(dict.fromkeys(value for value in resolved if value is not None))


def _compound(values: Iterable[float]) -> float:
    array = np.asarray(list(values), dtype=float)
    return float(np.prod(1.0 + array) - 1.0) if len(array) else 0.0


def _annualized(values: Sequence[float], periods_per_year: float) -> float:
    if not values:
        return 0.0
    total = _compound(values)
    if total <= -1.0:
        return -1.0
    return float((1.0 + total) ** (periods_per_year / len(values)) - 1.0)


def _ratio(values: Sequence[float], periods_per_year: float) -> float:
    if len(values) < 2:
        return 0.0
    standard_deviation = float(np.std(values, ddof=1))
    return float(np.mean(values) / standard_deviation * math.sqrt(periods_per_year)) if standard_deviation else 0.0


def _account_nav_path_for_periods(
    account_nav_path: Sequence[Mapping[str, Any]] | None,
    periods: Sequence[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], bool]:
    """Select only the daily NAV observations linked to ``periods``.

    Sequence ranges are written by the account simulator.  Selecting by those
    immutable links avoids an as-of lookup that could pull a mark from before
    the requested window or from a later period sharing the same calendar
    boundary.
    """

    rows = list(periods)
    if not rows:
        return [], True
    by_sequence: dict[int, Mapping[str, Any]] = {}
    duplicate_sequences: set[int] = set()
    for entry in account_nav_path or ():
        try:
            sequence = int(entry.get("sequence"))
        except (TypeError, ValueError):
            continue
        if sequence in by_sequence:
            duplicate_sequences.add(sequence)
        else:
            by_sequence[sequence] = entry

    selected_sequences: set[int] = set()
    complete = True
    for row in rows:
        try:
            first_sequence = int(row.get("account_nav_path_start_sequence"))
            last_sequence = int(row.get("account_nav_path_end_sequence"))
        except (TypeError, ValueError):
            complete = False
            continue
        if first_sequence > last_sequence:
            complete = False
            continue
        matching = [
            sequence
            for sequence in by_sequence
            if first_sequence <= sequence <= last_sequence
        ]
        if (
            first_sequence not in by_sequence
            or last_sequence not in by_sequence
            or not matching
        ):
            complete = False
            continue
        try:
            expected_daily_observations = int(
                row.get("daily_nav_observation_count")
            )
        except (TypeError, ValueError):
            complete = False
            continue
        matching.sort()
        phases = [str(by_sequence[value].get("phase")) for value in matching]
        lower = pd.Timestamp(
            row.get("accounting_boundary_date")
            or row.get("signal_date")
            or row.get("start_date")
        )
        start_date = pd.Timestamp(row.get("start_date"))
        upper = pd.Timestamp(row.get("end_date"))
        try:
            matching_dates = [
                pd.Timestamp(by_sequence[value].get("date"))
                for value in matching
            ]
        except (TypeError, ValueError):
            complete = False
            continue
        daily_dates = matching_dates[2:]
        if (
            expected_daily_observations <= 0
            or len(matching) != expected_daily_observations + 2
            or phases[:2] != ["accounting_boundary", "posttrade"]
            or phases[2:] != ["daily_end"] * expected_daily_observations
            or any(value in duplicate_sequences for value in matching)
            or matching_dates != sorted(matching_dates)
            or matching_dates[0] != lower
            or matching_dates[1] != start_date
            or not daily_dates
            or daily_dates[0] <= start_date
            or daily_dates[-1] != upper
            or len(set(daily_dates)) != expected_daily_observations
        ):
            complete = False
            continue
        for sequence in matching:
            try:
                observed_date = pd.Timestamp(by_sequence[sequence].get("date"))
            except (TypeError, ValueError):
                complete = False
                continue
            if observed_date < lower or observed_date > upper:
                complete = False
                continue
            selected_sequences.add(sequence)
    selected = [by_sequence[value] for value in sorted(selected_sequences)]
    return selected, complete


def _daily_nav_max_drawdown(
    account_nav_path: Sequence[Mapping[str, Any]],
) -> float | None:
    if not account_nav_path:
        return 0.0
    values: list[float] = []
    for row in account_nav_path:
        try:
            value = float(row.get("nav"))
        except (TypeError, ValueError):
            return None
        if not np.isfinite(value) or value < 0.0:
            return None
        values.append(value)
    curve = np.asarray(values, dtype=float)
    peaks = np.maximum.accumulate(curve)
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdowns = np.where(peaks > 0.0, curve / peaks - 1.0, 0.0)
    return float(np.min(drawdowns)) if len(drawdowns) else 0.0


def _window_metrics(
    periods: Sequence[Mapping[str, Any]],
    *,
    account_nav_path: Sequence[Mapping[str, Any]] | None = None,
    start: str,
    end: str | None,
    periods_per_year: float,
    bootstrap_spec: ValidationSpec | None = None,
    bootstrap_key: str = "portfolio_active_return",
    expected_observations: int | None = None,
) -> dict[str, Any]:
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end else None
    rows = [
        row
        for row in periods
        if pd.Timestamp(row["signal_date"]) >= lower
        and (upper is None or pd.Timestamp(row["signal_date"]) <= upper)
        and (upper is None or pd.Timestamp(row["end_date"]) <= upper)
    ]
    net = [float(row.get("net_return") or 0.0) for row in rows]
    gross = [float(row.get("gross_return") or 0.0) for row in rows]
    benchmark = [float(row.get("benchmark_return") or 0.0) for row in rows]
    excess = [left - right for left, right in zip(net, benchmark)]
    half_year: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        date = pd.Timestamp(row["signal_date"])
        half_year.setdefault(f"{date.year}-H{1 if date.month <= 6 else 2}", []).append(row)
    half_year_excess = [
        _compound(float(item.get("net_return") or 0.0) for item in group)
        - _compound(float(item.get("benchmark_return") or 0.0) for item in group)
        for group in half_year.values()
    ]
    net_annual = _annualized(net, periods_per_year)
    benchmark_annual = _annualized(benchmark, periods_per_year)
    benchmark_expected = int(
        sum(int(row.get("benchmark_expected_endpoint_count") or 0) for row in rows)
    )
    benchmark_observed = int(
        sum(int(row.get("benchmark_observed_endpoint_count") or 0) for row in rows)
    )
    benchmark_complete = int(
        sum(int(row.get("benchmark_complete_return_count") or 0) for row in rows)
    )
    benchmark_constituents = benchmark_expected // 2
    bootstrap = (
        deterministic_block_bootstrap_mean(
            excess,
            samples=bootstrap_spec.bootstrap_samples,
            block_size=bootstrap_spec.bootstrap_block_size,
            confidence=bootstrap_spec.bootstrap_confidence,
            seed=bootstrap_spec.bootstrap_seed,
            key=bootstrap_key,
        ).to_dict()
        if bootstrap_spec is not None
        else None
    )
    input_dates = [
        pd.Timestamp(value)
        for row in rows
        for value in (row.get("execution_input_min_date"), row.get("execution_input_max_date"))
        if value
    ]
    future_input_violations = sum(
        int(row.get("execution_input_future_violation_count") or 0)
        for row in rows
    )
    execution_policy_matches = sum(
        str(row.get("execution_input_policy"))
        == "previous_valid_ticker_observation"
        for row in rows
    )
    execution_input_required = int(
        sum(int(row.get("execution_input_required_count") or 0) for row in rows)
    )
    execution_input_observed = int(
        sum(int(row.get("execution_input_observed_count") or 0) for row in rows)
    )
    expected_periods = int(expected_observations) if expected_observations is not None else len(rows)
    window_nav_path, daily_nav_path_complete = _account_nav_path_for_periods(
        account_nav_path, rows
    )
    daily_max_drawdown = (
        _daily_nav_max_drawdown(window_nav_path)
        if daily_nav_path_complete
        else None
    )
    return {
        "start": start,
        "end": end,
        "observations": len(rows),
        "expected_observations": expected_periods,
        "execution_period_coverage": round(
            len(rows) / expected_periods if expected_periods else 0.0, 8
        ),
        "net_return": round(_compound(net), 8),
        "gross_return": round(_compound(gross), 8),
        "benchmark_return": round(_compound(benchmark), 8),
        "net_annual_return": round(net_annual, 8),
        "benchmark_annual_return": round(benchmark_annual, 8),
        "net_excess_annual_return": round(net_annual - benchmark_annual, 8),
        "net_sharpe": round(_ratio(net, periods_per_year), 8),
        "information_ratio": round(_ratio(excess, periods_per_year), 8),
        "max_drawdown": round(daily_max_drawdown, 8)
        if daily_max_drawdown is not None
        else None,
        "max_drawdown_basis": "daily_account_nav",
        "daily_nav_path_complete": daily_nav_path_complete,
        "daily_nav_observations": len(window_nav_path),
        "account_nav_path_start_sequence": (
            int(window_nav_path[0]["sequence"]) if window_nav_path else None
        ),
        "account_nav_path_end_sequence": (
            int(window_nav_path[-1]["sequence"]) if window_nav_path else None
        ),
        "positive_half_year_ratio": round(float(np.mean(np.asarray(half_year_excess) > 0.0)), 8)
        if half_year_excess
        else 0.0,
        "average_holding_count": round(float(np.mean([row.get("holding_count", 0) for row in rows])), 6)
        if rows
        else 0.0,
        "actual_turnover": round(float(np.mean([row.get("turnover", 0.0) for row in rows])), 8)
        if rows
        else 0.0,
        "annualized_turnover": round(
            float(np.mean([row.get("turnover", 0.0) for row in rows])) * periods_per_year,
            8,
        )
        if rows
        else 0.0,
        "benchmark_expected_endpoint_count": benchmark_expected,
        "benchmark_observed_endpoint_count": benchmark_observed,
        "benchmark_complete_return_count": benchmark_complete,
        "benchmark_missing_start_count": int(
            sum(int(row.get("benchmark_missing_start_count") or 0) for row in rows)
        ),
        "benchmark_missing_end_count": int(
            sum(int(row.get("benchmark_missing_end_count") or 0) for row in rows)
        ),
        "benchmark_endpoint_coverage": round(
            benchmark_observed / benchmark_expected if benchmark_expected else 0.0, 8
        ),
        "benchmark_return_coverage": round(
            benchmark_complete / benchmark_constituents if benchmark_constituents else 0.0,
            8,
        ),
        "excess_return_mean_bootstrap": bootstrap,
        "excess_return_mean_bootstrap_lower": (
            bootstrap.get("lower") if bootstrap is not None else None
        ),
        "execution_input_policy": "previous_valid_ticker_observation",
        "execution_input_policy_match_ratio": round(
            execution_policy_matches / len(rows) if rows else 0.0, 8
        ),
        "execution_input_future_violation_count": int(future_input_violations),
        "execution_input_required_count": execution_input_required,
        "execution_input_observed_count": execution_input_observed,
        "execution_input_coverage": round(
            execution_input_observed / execution_input_required
            if execution_input_required
            else 1.0,
            8,
        ),
        "max_execution_input_age_days": max(
            (int(row.get("max_execution_input_age_days") or 0) for row in rows),
            default=0,
        ),
        "execution_input_min_date": str(min(input_dates).date()) if input_dates else None,
        "execution_input_max_date": str(max(input_dates).date()) if input_dates else None,
        "capacity_violation_count": int(sum(int(row.get("capacity_violation_count") or 0) for row in rows)),
        "blocked_trade_count": int(sum(int(row.get("blocked_trade_count") or 0) for row in rows)),
        "total_cost": round(float(sum(float((row.get("costs") or {}).get("total") or 0.0) for row in rows)), 4),
    }


def _gate(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> tuple[bool, list[str]]:
    gate = config.get("promotion_gate") or {}
    missing_gate_keys = sorted(_REQUIRED_PROMOTION_GATE_KEYS - set(gate))
    checks = [
        (float(metrics.get("net_excess_annual_return") or 0.0) > float(gate.get("validation_net_excess_annual_return_min", 0.0)), "non_positive_validation_net_excess"),
        (float(metrics.get("net_sharpe") or 0.0) >= float(gate.get("validation_net_sharpe_min", 0.8)), "validation_sharpe_below_threshold"),
        (float(metrics.get("information_ratio") or 0.0) >= float(gate.get("validation_information_ratio_min", 0.5)), "validation_information_ratio_below_threshold"),
        (
            metrics.get("max_drawdown") is not None
            and float(metrics["max_drawdown"])
            >= float(gate.get("validation_max_drawdown_min", -0.25)),
            "validation_drawdown_exceeds_limit",
        ),
        (float(metrics.get("positive_half_year_ratio") or 0.0) >= float(gate.get("positive_half_year_ratio_min", 0.6)), "positive_half_year_ratio_below_threshold"),
        (float(metrics.get("average_holding_count") or 0.0) >= float(gate.get("average_holding_count_min", 40)), "average_holding_count_below_threshold"),
        (int(metrics.get("capacity_violation_count") or 0) <= int(gate.get("capacity_violation_count_max", 0)), "capacity_violation"),
    ]
    if "validation_excess_mean_bootstrap_lower_min" in gate:
        lower = metrics.get("excess_return_mean_bootstrap_lower")
        checks.append(
            (
                lower is not None
                and float(lower)
                > float(gate["validation_excess_mean_bootstrap_lower_min"]),
                "validation_excess_bootstrap_lower_below_threshold",
            )
        )
    if "benchmark_return_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("benchmark_return_coverage") or 0.0)
                >= float(gate["benchmark_return_coverage_min"]),
                "benchmark_return_coverage_below_threshold",
            )
        )
    if "execution_input_policy_match_ratio_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_input_policy_match_ratio") or 0.0)
                >= float(gate["execution_input_policy_match_ratio_min"]),
                "execution_input_policy_mismatch",
            )
        )
    if "execution_input_future_violation_count_max" in gate:
        checks.append(
            (
                int(metrics.get("execution_input_future_violation_count") or 0)
                <= int(gate["execution_input_future_violation_count_max"]),
                "future_execution_input_detected",
            )
        )
    if "execution_input_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_input_coverage") or 0.0)
                >= float(gate["execution_input_coverage_min"]),
                "execution_input_coverage_below_threshold",
            )
        )
    if "validation_observations_min" in gate:
        minimum_observations = int(
            metrics.get("minimum_required_observations")
            or gate["validation_observations_min"]
        )
        checks.append(
            (
                int(metrics.get("observations") or 0)
                >= minimum_observations,
                "validation_observations_below_threshold",
            )
        )
    if "execution_period_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_period_coverage") or 0.0)
                >= float(gate["execution_period_coverage_min"]),
                "execution_period_coverage_below_threshold",
            )
        )
    if "signal_evaluable_date_ratio_min" in gate:
        checks.append(
            (
                float(metrics.get("signal_evaluable_date_ratio") or 0.0)
                >= float(gate["signal_evaluable_date_ratio_min"]),
                "validation_signal_evaluable_ratio_below_threshold",
            )
        )
    if "signal_median_cross_section_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("signal_median_cross_section_coverage") or 0.0)
                >= float(gate["signal_median_cross_section_coverage_min"]),
                "validation_signal_cross_section_coverage_below_threshold",
            )
        )
    blockers = [
        *(f"missing_promotion_gate_config:{key}" for key in missing_gate_keys),
        *(reason for passed, reason in checks if not passed),
    ]
    return not blockers, blockers


def _audit_falsification(
    validation: FactorValidation,
    metrics: Mapping[str, Any],
    policy: ValidationSpec,
) -> tuple[bool, list[str]]:
    """Use audit evidence as a veto only, never as a ranking input."""

    signal_observations = validation.audit.evaluable_date_count
    portfolio_observations = int(metrics.get("observations") or 0)
    if min(signal_observations, portfolio_observations) < policy.audit_min_observations:
        return False, ["audit_insufficient_observations"]
    failures = list(validation.audit_signal_failures)
    active_interval = metrics.get("excess_return_mean_bootstrap") or {}
    if (
        active_interval.get("upper") is not None
        and float(active_interval["upper"]) < 0.0
    ):
        failures.append("audit_active_return_bootstrap_upper_negative")
    falsified = len(failures) >= policy.audit_min_failed_metrics
    return falsified, failures


def _compact_portfolio(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = {key: value for key, value in payload.items() if key not in _DETAIL_FIELDS}
    compact["details_omitted"] = sorted(_DETAIL_FIELDS)
    return compact


def _safe_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "factor"
    return value[:100]


def _assert_unique_artifact_names(
    names: Sequence[str], *, context: str
) -> None:
    logical_names: set[str] = set()
    normalized: dict[str, str] = {}
    for raw_name in names:
        name = str(raw_name)
        if name in logical_names:
            raise ValueError(f"{context} contains duplicate name: {name!r}")
        logical_names.add(name)
        safe = _safe_name(name)
        # Artifact uniqueness must be portable to the case-insensitive Windows
        # workspace used for production runs.
        artifact_key = safe.casefold()
        previous = normalized.get(artifact_key)
        if previous is not None:
            raise ValueError(
                f"{context} names collide after artifact normalization: "
                f"{previous!r} and {name!r} -> {safe!r}"
            )
        normalized[artifact_key] = name


def _expected_portfolio_observations(
    feature_frame: pd.DataFrame,
    execution_frame: pd.DataFrame,
    portfolio_config: LongOnlyPortfolioConfig,
    validation: ValidationSpec,
) -> dict[str, int]:
    """Count scheduled, fully-contained periods from the explicit signal anchor."""

    feature_dates = pd.to_datetime(
        feature_frame[portfolio_config.date_column], errors="coerce"
    ).dropna()
    execution_date_column = _resolve_column(
        set(execution_frame.columns),
        portfolio_config.date_column,
        ("date", "trade_date"),
    )
    if feature_dates.empty or execution_date_column is None:
        return {"train": 0, "validation": 0, "audit": 0}
    execution_dates = [
        pd.Timestamp(value)
        for value in sorted(
            pd.to_datetime(execution_frame[execution_date_column], errors="coerce")
            .dropna()
            .unique()
        )
    ]
    signal_start = feature_dates.min()
    signal_end = feature_dates.max()
    first_index = next(
        (index for index, value in enumerate(execution_dates) if value >= signal_start),
        len(execution_dates),
    )
    scheduled = [
        (
            execution_dates[index],
            execution_dates[index + portfolio_config.holding_days + 1],
        )
        for index in range(
            first_index + portfolio_config.rebalance_offset_days,
            len(execution_dates) - portfolio_config.holding_days - 1,
            portfolio_config.rebalance_every_days,
        )
        if execution_dates[index] <= signal_end
        if portfolio_config.evaluation_start_date is None
        or execution_dates[index]
        >= pd.Timestamp(portfolio_config.evaluation_start_date)
    ]
    boundaries = {
        "train": (pd.Timestamp(validation.train_start), pd.Timestamp(validation.train_end)),
        "validation": (
            pd.Timestamp(validation.validation_start),
            pd.Timestamp(validation.validation_end),
        ),
        "audit": (pd.Timestamp(validation.audit_start), None),
    }
    return {
        split: sum(
            signal_date >= lower
            and (upper is None or signal_date <= upper)
            and (upper is None or end_date <= upper)
            for signal_date, end_date in scheduled
        )
        for split, (lower, upper) in boundaries.items()
    }


def _portfolio_result(
    factor: FactorSpec,
    validation: FactorValidation,
    signal: pd.Series,
    feature_frame: pd.DataFrame,
    execution_frame: pd.DataFrame,
    portfolio_config: LongOnlyPortfolioConfig,
    research_config: Mapping[str, Any],
    *,
    target_weights_by_date: Mapping[Any, Mapping[str, float]] | None = None,
    optimization_audit_by_date: Mapping[Any, Mapping[str, Any]] | None = None,
    require_optimized_targets: bool = False,
    include_period_target_weights: bool = False,
) -> dict[str, Any]:
    directed = pd.to_numeric(signal, errors="coerce") * validation.frozen_direction
    evaluation = evaluate_long_only_portfolio(
        feature_frame,
        directed,
        portfolio_config,
        pricing_frame=execution_frame,
        target_weights_by_date=target_weights_by_date,
        optimization_audit_by_date=optimization_audit_by_date,
        promotion_blockers=("historical_diagnostic_only",),
        require_optimized_targets=require_optimized_targets,
    ).to_dict()
    periods = list(evaluation.get("periods") or [])
    account_nav_path = list(evaluation.get("account_nav_path") or [])
    spec = _validation_spec(research_config)
    expected = _expected_portfolio_observations(
        feature_frame,
        execution_frame,
        portfolio_config,
        spec,
    )
    windows = {
        "train": _window_metrics(
            periods,
            account_nav_path=account_nav_path,
            start=spec.train_start,
            end=spec.train_end,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:train:active_return",
            expected_observations=expected["train"],
        ),
        "validation": _window_metrics(
            periods,
            account_nav_path=account_nav_path,
            start=spec.validation_start,
            end=spec.validation_end,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:validation:active_return",
            expected_observations=expected["validation"],
        ),
        "audit": _window_metrics(
            periods,
            account_nav_path=account_nav_path,
            start=spec.audit_start,
            end=None,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:audit:active_return",
            expected_observations=expected["audit"],
        ),
    }

    for split, diagnostics in (
        ("train", validation.train),
        ("validation", validation.validation),
        ("audit", validation.audit),
    ):
        windows[split]["signal_evaluable_date_ratio"] = diagnostics.evaluable_date_ratio
        windows[split]["signal_median_cross_section_coverage"] = (
            diagnostics.median_cross_section_coverage
        )
        base_minimum = int(
            (research_config.get("promotion_gate") or {}).get(
                "validation_observations_min", 0
            )
        )
        windows[split]["minimum_required_observations"] = int(
            math.ceil(
                base_minimum
                * spec.holding_days
                / portfolio_config.rebalance_every_days
            )
        )
    gate_passed, gate_blockers = _gate(windows["validation"], research_config)
    audit_falsified, audit_reasons = _audit_falsification(
        validation, windows["audit"], spec
    )
    if "audit_insufficient_observations" in audit_reasons:
        audit_status = "insufficient_evidence"
    elif audit_falsified:
        audit_status = "falsified"
    else:
        audit_status = "not_falsified"
    return {
        "factor_name": factor.name,
        "family": factor.family,
        "factor": factor.to_dict(),
        "frozen_direction": validation.frozen_direction,
        "stage_a": validation.to_dict(),
        "portfolio": _compact_portfolio(evaluation),
        "account_nav_path": account_nav_path,
        "windows": windows,
        "gate_passed": gate_passed,
        "gate_blockers": gate_blockers,
        "audit_role": "falsification_only",
        "audit_status": audit_status,
        "audit_falsified": audit_falsified,
        "audit_falsification_reasons": audit_reasons,
        "period_active_returns": [
            {
                "signal_date": row.get("signal_date"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "accounting_boundary_date": row.get(
                    "accounting_boundary_date"
                ),
                "net_return": float(row.get("net_return") or 0.0),
                "benchmark_return": float(row.get("benchmark_return") or 0.0),
                "active_return": float(row.get("net_return") or 0.0)
                - float(row.get("benchmark_return") or 0.0),
                "benchmark_return_coverage": float(
                    row.get("benchmark_return_coverage") or 0.0
                ),
                "benchmark_endpoint_coverage": float(
                    row.get("benchmark_endpoint_coverage") or 0.0
                ),
                "account_nav_path_start_sequence": row.get(
                    "account_nav_path_start_sequence"
                ),
                "account_nav_path_end_sequence": row.get(
                    "account_nav_path_end_sequence"
                ),
                "daily_nav_observation_count": row.get(
                    "daily_nav_observation_count"
                ),
                "max_drawdown": row.get("max_drawdown"),
            }
            for row in periods
        ],
        "period_target_weights": (
            [
                {
                    "signal_date": row.get("signal_date"),
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "target_weight_mode": row.get("target_weight_mode"),
                    "target_weights": dict(row.get("target_weights") or {}),
                    "optimization_audit": dict(
                        row.get("optimization_audit") or {}
                    ),
                }
                for row in periods
            ]
            if include_period_target_weights
            else []
        ),
        "beats_control": False,
        "control_comparison": None,
        "validated": False,
    }


def _results_first_metrics(
    result: Mapping[str, Any],
    research_config: Mapping[str, Any],
    *,
    periods_per_year: float,
    reference_periods: Sequence[Mapping[str, Any]] | None = None,
    optimization_scope: str = "all_observed_history",
) -> dict[str, Any]:
    """Score one strategy over every observed historical period.

    This is deliberately an in-sample leaderboard. It optimizes the result the
    user asked for and never labels the winner as independently validated.
    """

    settings = dict(research_config.get("results_first") or {})
    incomplete_policy = str(
        settings.get("incomplete_period_policy", "exclude_from_ranking")
    )
    if incomplete_policy != "exclude_from_ranking":
        raise ValueError(
            "results_first incomplete_period_policy must be 'exclude_from_ranking'"
        )

    observed_rows = list(result.get("period_active_returns") or [])
    missing_periods = 0
    if reference_periods is None:
        rows = observed_rows
        observed_periods = len(rows)
        comparison_basis = "strategy_observed_periods"
    else:
        observed_by_date = {
            str(row.get("signal_date")): row
            for row in observed_rows
            if row.get("signal_date") is not None
        }
        rows = []
        observed_periods = 0
        for reference in reference_periods:
            signal_date = str(reference.get("signal_date"))
            candidate = observed_by_date.get(signal_date)
            if candidate is None:
                missing_periods += 1
                net_return = 0.0
            else:
                observed_periods += 1
                net_return = float(candidate.get("net_return") or 0.0)
            benchmark_return = float(reference.get("benchmark_return") or 0.0)
            rows.append(
                {
                    "signal_date": reference.get("signal_date"),
                    "net_return": net_return,
                    "benchmark_return": benchmark_return,
                    "active_return": net_return - benchmark_return,
                    "benchmark_return_coverage": float(
                        reference.get("benchmark_return_coverage") or 0.0
                    ),
                    "benchmark_endpoint_coverage": float(
                        reference.get("benchmark_endpoint_coverage") or 0.0
                    ),
                    "start_date": candidate.get("start_date")
                    if candidate is not None
                    else None,
                    "end_date": candidate.get("end_date")
                    if candidate is not None
                    else None,
                    "accounting_boundary_date": candidate.get(
                        "accounting_boundary_date"
                    )
                    if candidate is not None
                    else None,
                    "account_nav_path_start_sequence": candidate.get(
                        "account_nav_path_start_sequence"
                    )
                    if candidate is not None
                    else None,
                    "account_nav_path_end_sequence": candidate.get(
                        "account_nav_path_end_sequence"
                    )
                    if candidate is not None
                    else None,
                    "daily_nav_observation_count": candidate.get(
                        "daily_nav_observation_count"
                    )
                    if candidate is not None
                    else None,
                }
            )
        comparison_basis = "control_signal_dates"
    net = np.asarray([float(row.get("net_return") or 0.0) for row in rows], dtype=float)
    active = np.asarray(
        [float(row.get("active_return") or 0.0) for row in rows], dtype=float
    )
    benchmark_coverage = np.asarray(
        [
            float(row.get("benchmark_return_coverage") or 0.0)
            for row in rows
        ],
        dtype=float,
    )
    finite = np.isfinite(net) & np.isfinite(active)
    net = net[finite]
    active = active[finite]
    benchmark_coverage = benchmark_coverage[finite]
    selected_nav_path, daily_nav_path_complete = _account_nav_path_for_periods(
        list(result.get("account_nav_path") or []), rows
    )
    daily_nav_path_complete = bool(
        daily_nav_path_complete and missing_periods == 0
    )
    daily_max_drawdown = (
        _daily_nav_max_drawdown(selected_nav_path)
        if daily_nav_path_complete
        else None
    )
    if not len(net):
        return {
            "observations": 0,
            "historical_score": None,
            "net_annual_return": None,
            "net_sharpe": None,
            "information_ratio": None,
            "max_drawdown": None,
            "max_drawdown_basis": "daily_account_nav",
            "daily_nav_path_complete": daily_nav_path_complete,
            "daily_nav_observations": len(selected_nav_path),
            "optimization_scope": optimization_scope,
            "comparison_period_basis": comparison_basis,
            "observed_strategy_periods": observed_periods,
            "missing_strategy_periods": missing_periods,
            "period_coverage": 0.0,
            "benchmark_return_coverage_min": 0.0,
            "benchmark_return_coverage_mean": 0.0,
            "missing_period_score_policy": "cash_return_zero_diagnostic_only",
            "incomplete_period_ranking_policy": incomplete_policy,
        }

    growth = float(np.prod(1.0 + net))
    annual_return = (
        growth ** (float(periods_per_year) / len(net)) - 1.0
        if growth > 0.0
        else -1.0
    )
    net_std = float(np.std(net, ddof=1)) if len(net) > 1 else 0.0
    active_std = float(np.std(active, ddof=1)) if len(active) > 1 else 0.0
    scale = math.sqrt(float(periods_per_year))
    sharpe = float(np.mean(net) / net_std * scale) if net_std > 0.0 else 0.0
    information_ratio = (
        float(np.mean(active) / active_std * scale) if active_std > 0.0 else 0.0
    )
    default_score_weights = {
        "net_sharpe": 1.0,
        "information_ratio": 0.35,
        "net_annual_return": 0.50,
        "max_drawdown": 0.35,
    }
    configured_weights = dict(settings.get("score_weights") or {})
    unknown_weights = sorted(set(configured_weights) - set(default_score_weights))
    if unknown_weights:
        raise ValueError(
            "unsupported results_first score_weights: " + ", ".join(unknown_weights)
        )
    score_weights: dict[str, float] = {}
    for key, raw_value in {**default_score_weights, **configured_weights}.items():
        if isinstance(raw_value, (bool, np.bool_)):
            raise ValueError("results_first score_weights must be finite non-negative numbers")
        try:
            parsed_weight = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "results_first score_weights must be finite non-negative numbers"
            ) from exc
        if not np.isfinite(parsed_weight) or parsed_weight < 0.0:
            raise ValueError("results_first score_weights must be finite non-negative numbers")
        score_weights[key] = parsed_weight
    if not any(score_weights.values()):
        raise ValueError("at least one results_first score weight must be positive")
    return {
        "observations": int(len(net)),
        "optimization_scope": optimization_scope,
        "comparison_period_basis": comparison_basis,
        "observed_strategy_periods": observed_periods,
        "missing_strategy_periods": missing_periods,
        "period_coverage": round(observed_periods / len(rows), 8),
        "benchmark_return_coverage_min": round(
            float(np.min(benchmark_coverage)), 8
        ),
        "benchmark_return_coverage_mean": round(
            float(np.mean(benchmark_coverage)), 8
        ),
        "missing_period_score_policy": "cash_return_zero_diagnostic_only",
        "incomplete_period_ranking_policy": incomplete_policy,
        "historical_score": None,
        "score_method": "cross_strategy_percentile_weighted",
        "net_annual_return": round(annual_return, 8),
        "net_sharpe": round(sharpe, 8),
        "information_ratio": round(information_ratio, 8),
        "max_drawdown": round(daily_max_drawdown, 8)
        if daily_max_drawdown is not None
        else None,
        "max_drawdown_basis": "daily_account_nav",
        "daily_nav_path_complete": daily_nav_path_complete,
        "daily_nav_observations": len(selected_nav_path),
        "account_nav_path_start_sequence": (
            int(selected_nav_path[0]["sequence"]) if selected_nav_path else None
        ),
        "account_nav_path_end_sequence": (
            int(selected_nav_path[-1]["sequence"])
            if selected_nav_path
            else None
        ),
        "active_return_annual_mean": round(float(np.mean(active)) * periods_per_year, 8),
        "score_weights": score_weights,
    }


def _build_results_first_ensembles(
    frame: pd.DataFrame,
    control: FactorSpec,
    challengers: Sequence[FactorSpec],
    signals: Mapping[str, pd.Series],
    validations: Mapping[str, FactorValidation],
    validation_spec: ValidationSpec,
    research_config: Mapping[str, Any],
) -> tuple[list[FactorSpec], dict[str, pd.Series], dict[str, FactorValidation]]:
    settings = dict(research_config.get("results_first") or {})
    if str(settings.get("optimization_scope", "all_observed_history")) != (
        "all_observed_history"
    ):
        raise ValueError(
            "results_first optimization_scope must be 'all_observed_history'"
        )
    if str(settings.get("missing_challenger_policy", "fallback_control")) != (
        "fallback_control"
    ):
        raise ValueError(
            "results_first missing_challenger_policy must be 'fallback_control'"
        )
    raw_weights = tuple(settings.get("challenger_weights", (0.2, 0.4, 0.6)))
    if not raw_weights or any(isinstance(value, (bool, np.bool_)) for value in raw_weights):
        raise ValueError("results_first challenger_weights must be in (0, 1]")
    try:
        weights = tuple(float(value) for value in raw_weights)
    except (TypeError, ValueError) as exc:
        raise ValueError("results_first challenger_weights must be in (0, 1]") from exc
    if any(not np.isfinite(value) or value <= 0.0 or value > 1.0 for value in weights):
        raise ValueError("results_first challenger_weights must be in (0, 1]")
    if len(set(weights)) != len(weights):
        raise ValueError("results_first challenger_weights must be unique")
    weight_labels = tuple(
        format(value, ".12g").replace(".", "p") for value in weights
    )
    if len(set(weight_labels)) != len(weight_labels):
        raise ValueError("results_first challenger_weights must have unique labels")

    ensemble_factors: list[FactorSpec] = []
    ensemble_signals: dict[str, pd.Series] = {}
    ensemble_validations: dict[str, FactorValidation] = {}
    control_validation = validations[control.name]
    # Daily ranks are invariant across the weight grid.  Compute the control
    # once and each challenger's fallback-adjusted rank once instead of doing
    # two full groupby/rank passes for every candidate weight.
    control_rank = directed_rank_blend(
        frame,
        signals[control.name],
        signals[control.name],
        control_direction=control_validation.frozen_direction,
        challenger_direction=control_validation.frozen_direction,
        challenger_weight=0.0,
        date_column=validation_spec.date_column,
    )
    for challenger in challengers:
        challenger_validation = validations[challenger.name]
        effective_challenger_rank = directed_rank_blend(
            frame,
            signals[control.name],
            signals[challenger.name],
            control_direction=control_validation.frozen_direction,
            challenger_direction=challenger_validation.frozen_direction,
            challenger_weight=1.0,
            date_column=validation_spec.date_column,
        )
        for weight, weight_label in zip(weights, weight_labels, strict=True):
            name = f"blend__{control.name}__{challenger.name}__w{weight_label}"
            factor = FactorSpec(
                name=name,
                family="results_first_ensemble",
                kind="ensemble",
                direction_policy="pre_directed",
                params={
                    "control_factor": control.name,
                    "challenger_factor": challenger.name,
                    "control_direction": control_validation.frozen_direction,
                    "challenger_direction": challenger_validation.frozen_direction,
                    "challenger_weight": weight,
                    "missing_challenger_policy": "fallback_control",
                    "optimization_scope": "all_observed_history",
                },
                role="results_first_candidate",
            )
            signal = (
                (1.0 - weight) * control_rank + weight * effective_challenger_rank
            ).where(control_rank.notna()).rename(name)
            validation = evaluate_stage_a(
                frame,
                factor,
                validation_spec,
                signal=signal,
            )
            ensemble_factors.append(factor)
            ensemble_signals[name] = signal
            ensemble_validations[name] = validation
    return ensemble_factors, ensemble_signals, ensemble_validations


def _walk_forward_weights(settings: Mapping[str, Any]) -> tuple[float, ...]:
    raw = tuple(settings.get("candidate_weights") or (0.3, 0.7))
    if not raw or any(isinstance(value, (bool, np.bool_)) for value in raw):
        raise ValueError("walk_forward candidate_weights must be unique values in (0, 1]")
    try:
        weights = tuple(float(value) for value in raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "walk_forward candidate_weights must be unique values in (0, 1]"
        ) from exc
    if (
        len(set(weights)) != len(weights)
        or any(not np.isfinite(value) or value <= 0.0 or value > 1.0 for value in weights)
    ):
        raise ValueError("walk_forward candidate_weights must be unique values in (0, 1]")
    return weights


def _walk_forward_fixed_comparator(settings: Mapping[str, Any]) -> dict[str, str]:
    """Validate the single return-blind comparator protocol used by v5."""

    raw = settings.get("fixed_comparator") or {}
    if not isinstance(raw, Mapping):
        raise ValueError("walk_forward fixed_comparator must be an object")
    protocol = {
        "name": str(raw.get("name", "fixed_registry_equal_weight")),
        "weighting": str(raw.get("weighting", "equal")),
        "missing_signal_policy": str(
            raw.get("missing_signal_policy", "fallback_control")
        ),
    }
    if set(raw) != set(protocol) and raw:
        raise ValueError("walk_forward fixed_comparator contains unknown settings")
    if protocol != {
        "name": "fixed_registry_equal_weight",
        "weighting": "equal",
        "missing_signal_policy": "fallback_control",
    }:
        raise ValueError(
            "walk_forward fixed_comparator must use the frozen v6 protocol"
        )
    return protocol


def _build_walk_forward_candidates(
    frame: pd.DataFrame,
    control: FactorSpec,
    challengers: Sequence[FactorSpec],
    signals: Mapping[str, pd.Series],
    validations: Mapping[str, FactorValidation],
    validation_spec: ValidationSpec,
    research_config: Mapping[str, Any],
) -> tuple[list[FactorSpec], dict[str, pd.Series], dict[str, FactorValidation]]:
    """Build the fixed, small candidate registry used by the causal selector."""

    settings = dict(research_config.get("walk_forward") or {})
    configured_evidence = str(
        settings.get("evidence_class", "post_selection_causal_simulation")
    )
    if configured_evidence != "post_selection_causal_simulation":
        raise ValueError(
            "walk_forward evidence_class must be "
            "'post_selection_causal_simulation'"
        )
    _walk_forward_fixed_comparator(settings)
    allowed = tuple(str(value) for value in settings.get("candidate_factors") or ())
    if not allowed or len(set(allowed)) != len(allowed):
        raise ValueError("walk_forward candidate_factors must be a unique non-empty list")
    by_name = {factor.name: factor for factor in challengers}
    missing = [name for name in allowed if name not in by_name]
    if missing:
        raise ValueError(
            "walk_forward candidate_factors are not registered: " + ", ".join(missing)
        )
    weights = _walk_forward_weights(settings)
    labels = tuple(format(value, ".12g").replace(".", "p") for value in weights)
    if len(set(labels)) != len(labels):
        raise ValueError("walk_forward candidate_weights must have unique labels")

    control_validation = validations[control.name]
    if control.direction_policy != "fixed" or control_validation.frozen_direction not in {-1, 1}:
        raise ValueError("walk_forward control must have a fixed direction")
    control_rank = directed_rank_blend(
        frame,
        signals[control.name],
        signals[control.name],
        control_direction=control_validation.frozen_direction,
        challenger_direction=control_validation.frozen_direction,
        challenger_weight=0.0,
        date_column=validation_spec.date_column,
    ).rename(control.name)
    candidate_factors = [control]
    candidate_signals = {control.name: control_rank}
    candidate_validations = {control.name: control_validation}

    for challenger_name in allowed:
        challenger = by_name[challenger_name]
        challenger_validation = validations[challenger.name]
        if challenger.direction_policy != "fixed":
            raise ValueError(
                f"walk_forward candidate must have a fixed direction: {challenger.name}"
            )
        challenger_rank = directed_rank_blend(
            frame,
            signals[control.name],
            signals[challenger.name],
            control_direction=control_validation.frozen_direction,
            challenger_direction=challenger_validation.frozen_direction,
            challenger_weight=1.0,
            date_column=validation_spec.date_column,
        )
        for weight, label in zip(weights, labels, strict=True):
            name = f"causal_blend__{control.name}__{challenger.name}__w{label}"
            factor = FactorSpec(
                name=name,
                family="walk_forward_candidate",
                kind="ensemble",
                direction_policy="pre_directed",
                params={
                    "control_factor": control.name,
                    "challenger_factor": challenger.name,
                    "control_direction": control_validation.frozen_direction,
                    "challenger_direction": challenger_validation.frozen_direction,
                    "challenger_weight": weight,
                    "missing_challenger_policy": "fallback_control",
                    "selection_role": "causal_walk_forward_static_candidate",
                },
                role="walk_forward_candidate",
            )
            signal = (
                (1.0 - weight) * control_rank + weight * challenger_rank
            ).where(control_rank.notna()).rename(name)
            validation = evaluate_stage_a(
                frame, factor, validation_spec, signal=signal
            )
            candidate_factors.append(factor)
            candidate_signals[name] = signal
            candidate_validations[name] = validation
    return candidate_factors, candidate_signals, candidate_validations


def _control_comparison(
    candidate: Mapping[str, Any],
    control: Mapping[str, Any],
    config: Mapping[str, Any],
    validation: ValidationSpec,
    *,
    correction_factor: int,
) -> dict[str, Any]:
    lower = pd.Timestamp(validation.validation_start)
    upper = pd.Timestamp(validation.validation_end)

    def window(payload: Mapping[str, Any]) -> dict[str, float]:
        return {
            str(row["signal_date"]): float(row.get("net_return") or 0.0)
            for row in payload.get("period_active_returns") or []
            if lower <= pd.Timestamp(str(row["signal_date"])) <= upper
            and pd.Timestamp(str(row["end_date"])) <= upper
        }

    candidate_returns = window(candidate)
    control_returns = window(control)
    common_dates = sorted(set(candidate_returns) & set(control_returns))
    differences = [
        candidate_returns[date_value] - control_returns[date_value]
        for date_value in common_dates
    ]
    familywise_alpha = 1.0 - validation.bootstrap_confidence
    adjusted_confidence = 1.0 - familywise_alpha / max(1, int(correction_factor))
    interval = deterministic_block_bootstrap_mean(
        differences,
        samples=validation.bootstrap_samples,
        block_size=validation.bootstrap_block_size,
        confidence=adjusted_confidence,
        seed=validation.bootstrap_seed,
        key="portfolio:validation:paired_control_improvement",
    ).to_dict()
    minimum_observations = int(
        (config.get("promotion_gate") or {}).get("validation_observations_min", 0)
    )
    blockers: list[str] = []
    if len(common_dates) < minimum_observations:
        blockers.append("control_comparison_observations_below_threshold")
    if interval.get("lower") is None or float(interval["lower"]) <= 0.0:
        blockers.append("control_improvement_bootstrap_lower_not_positive")
    return {
        "control_factor": control.get("factor_name"),
        "split": "validation",
        "common_observations": len(common_dates),
        "mean_net_return_difference": round(float(np.mean(differences)), 8)
        if differences
        else None,
        "bootstrap": interval,
        "simultaneous_confidence_method": "bonferroni_fwer",
        "correction_factor": max(1, int(correction_factor)),
        "passed": not blockers,
        "blockers": blockers,
    }


def _beats_control(
    candidate: Mapping[str, Any],
    control: Mapping[str, Any],
    config: Mapping[str, Any],
    comparison: Mapping[str, Any] | None,
) -> bool:
    left = (candidate.get("windows") or {}).get("validation") or {}
    right = (control.get("windows") or {}).get("validation") or {}
    tolerance = float((config.get("control_comparison") or {}).get("max_drawdown_worsening_tolerance", 0.02))
    return bool(
        candidate.get("gate_passed")
        and float(left.get("net_sharpe") or 0.0) > float(right.get("net_sharpe") or 0.0)
        and float(left.get("net_excess_annual_return") or 0.0) > float(right.get("net_excess_annual_return") or 0.0)
        and float(left.get("max_drawdown") or 0.0) >= float(right.get("max_drawdown") or 0.0) - tolerance
        and bool((comparison or {}).get("passed", False))
    )


def _canary_frames(
    features: pd.DataFrame,
    execution: pd.DataFrame,
    config: LongOnlyPortfolioConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.DatetimeIndex(sorted(features[config.date_column].dropna().unique()))[-20:]
    recent = features[features[config.date_column].isin(dates)]
    tickers = recent.groupby(config.ticker_column).size().sort_values(ascending=False).head(50).index.astype(str)
    feature_sample = recent[recent[config.ticker_column].astype(str).isin(tickers)].copy()
    execution_date_column = _resolve_column(set(execution.columns), config.date_column, ("date", "trade_date"))
    execution_ticker_column = _resolve_column(set(execution.columns), config.ticker_column, ("ticker", "ts_code", "symbol"))
    assert execution_date_column and execution_ticker_column
    all_execution_dates = pd.DatetimeIndex(sorted(execution[execution_date_column].dropna().unique()))
    future = all_execution_dates[all_execution_dates > dates[-1]][: config.holding_days + 1]
    allowed_dates = set(dates.tolist()) | set(future.tolist())
    execution_sample = execution[
        execution[execution_date_column].isin(allowed_dates)
        & execution[execution_ticker_column].astype(str).isin(tickers)
    ].copy()
    return feature_sample, execution_sample


def _anchor_window_aggregate(
    anchors: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return per-split min/median/max and a median synthetic window."""

    aggregate: dict[str, Any] = {}
    medians: dict[str, Any] = {}
    for split in ("train", "validation", "audit"):
        windows = [
            (row.get("windows") or {}).get(split) or {}
            for row in anchors
        ]
        numeric_keys = sorted(
            {
                key
                for window in windows
                for key, value in window.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            }
        )
        split_aggregate: dict[str, Any] = {}
        split_median: dict[str, Any] = {}
        for key in numeric_keys:
            values = np.asarray(
                [float(window[key]) for window in windows if key in window],
                dtype=float,
            )
            values = values[np.isfinite(values)]
            if not len(values):
                continue
            median = float(np.median(values))
            split_aggregate[key] = {
                "min": round(float(np.min(values)), 8),
                "median": round(median, 8),
                "max": round(float(np.max(values)), 8),
            }
            split_median[key] = round(median, 8)
        aggregate[split] = split_aggregate
        medians[split] = split_median
    return aggregate, medians


def _robustness_integrity_blockers(
    anchors: Sequence[Mapping[str, Any]],
) -> list[str]:
    blockers = {
        str(blocker)
        for anchor in anchors
        for blocker in anchor.get("gate_blockers") or []
        if str(blocker) in _ROBUSTNESS_ABSOLUTE_BLOCKERS
        or str(blocker).startswith("missing_promotion_gate_config:")
    }
    return sorted(blockers)


def _run_robustness(
    factors: Sequence[FactorSpec],
    validations: Mapping[str, FactorValidation],
    signals: Mapping[str, pd.Series],
    features: pd.DataFrame,
    execution: pd.DataFrame,
    base_config: LongOnlyPortfolioConfig,
    research_config: Mapping[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    matrix = research_config.get("robustness") or {}
    offsets_by_days = matrix.get("anchor_offsets_by_rebalance_days") or {}
    minimum_pass_ratio = float(matrix.get("minimum_anchor_pass_ratio", 0.75))
    if not 0.0 <= minimum_pass_ratio <= 1.0:
        raise ValueError("robustness minimum_anchor_pass_ratio must be between 0 and 1")
    for factor in factors:
        for positions in matrix.get("position_counts") or (50, 75, 100):
            for rebalance in matrix.get("rebalance_every_days") or (5, 20):
                offsets = offsets_by_days.get(str(rebalance), [0])
                if not isinstance(offsets, list) or not offsets:
                    raise ValueError(f"robustness offsets missing for {rebalance} days")
                normalized_offsets = [int(value) for value in offsets]
                if len(normalized_offsets) != len(set(normalized_offsets)):
                    raise ValueError(f"duplicate robustness offsets for {rebalance} days")
                if any(value < 0 or value >= int(rebalance) for value in normalized_offsets):
                    raise ValueError(f"invalid robustness offset for {rebalance} days")
                anchors: list[dict[str, Any]] = []
                for offset in normalized_offsets:
                    config = replace(
                        base_config,
                        position_count=int(positions),
                        holding_days=int(rebalance),
                        rebalance_every_days=int(rebalance),
                        rebalance_offset_days=offset,
                        periods_per_year=252.0 / int(rebalance),
                    )
                    result = _portfolio_result(
                        factor,
                        validations[factor.name],
                        signals[factor.name],
                        features,
                        execution,
                        config,
                        research_config,
                    )
                    anchors.append(
                        {
                            "rebalance_offset_days": offset,
                            "gate_passed": result["gate_passed"],
                            "gate_blockers": result["gate_blockers"],
                            "windows": result["windows"],
                            "portfolio": result["portfolio"],
                        }
                    )
                statistics, median_windows = _anchor_window_aggregate(anchors)
                median_gate_passed, median_gate_blockers = _gate(
                    median_windows.get("validation") or {}, research_config
                )
                pass_ratio = float(
                    np.mean([bool(row["gate_passed"]) for row in anchors])
                )
                integrity_blockers = _robustness_integrity_blockers(anchors)
                robust = bool(
                    pass_ratio >= minimum_pass_ratio and median_gate_passed
                    and not integrity_blockers
                )
                blockers = [*median_gate_blockers, *integrity_blockers]
                if pass_ratio < minimum_pass_ratio:
                    blockers.append("anchor_pass_ratio_below_threshold")
                rows.append(
                    {
                        "factor_name": factor.name,
                        "position_count": int(positions),
                        "rebalance_every_days": int(rebalance),
                        "anchor_offsets": normalized_offsets,
                        "anchor_count": len(anchors),
                        "anchor_pass_ratio": round(pass_ratio, 8),
                        "minimum_anchor_pass_ratio": minimum_pass_ratio,
                        "median_gate_passed": median_gate_passed,
                        "robust": robust,
                        "robustness_blockers": list(dict.fromkeys(blockers)),
                        "window_statistics": statistics,
                        "median_windows": median_windows,
                        "anchors": anchors,
                        "exploratory_only": True,
                        "promotion_eligible": False,
                    }
                )
                _write_json(output_path, {"status": "running", "results": rows})
    payload = {
        "status": "completed",
        "search_stopped": True,
        "selection_basis": "train_shortlist_order",
        "audit_used_for_selection": False,
        "exploratory_only": True,
        "results": rows,
    }
    _write_json(output_path, payload)
    return payload


def _load_adaptive_protocol(path: Path) -> tuple[dict[str, Any], str]:
    payload = _read_json(path)
    if payload.get("schema_version") != 1:
        raise ValueError("adaptive protocol schema_version must be 1")
    if payload.get("protocol_id") != "factor-lab/5.0/adaptive-core-overlay":
        raise ValueError("unexpected adaptive protocol_id")
    if payload.get("release") != "5.0":
        raise ValueError("adaptive protocol release must be 5.0")
    if payload.get("status") != "frozen_before_historical_execution":
        raise ValueError("adaptive protocol is not frozen")
    if payload.get("evidence_class") != "post_selection_adaptive_simulation":
        raise ValueError("unexpected adaptive evidence class")
    if payload.get("investment_claim_allowed") is not False:
        raise ValueError("adaptive protocol must forbid investment claims")
    routing = payload.get("routing") or {}
    if (
        routing.get("allow_post_run_threshold_changes") is not False
        or routing.get("allow_historical_rerun_to_change_route_after_release")
        is not False
    ):
        raise ValueError("adaptive protocol routing is not immutable")
    experts = payload.get("experts") or {}
    registry = experts.get("ordered_registry") or []
    if len(registry) != 4 or len(set(map(str, registry))) != 4:
        raise ValueError("adaptive protocol requires four unique ordered experts")
    accounts = (payload.get("scoring_accounts") or {}).get("per_offset") or []
    if len(accounts) != 5 or len(set(map(str, accounts))) != 5:
        raise ValueError("adaptive protocol requires five unique scoring accounts")
    return payload, _sha256_file(path)


def run_research(
    *,
    project_root: str | Path | None = None,
    suite: str = ADAPTIVE_SUITE,
    mode: str = "canary",
    resume: bool = True,
    feature_path: str | Path | None = None,
    execution_path: str | Path | None = None,
    factors_path: str | Path | None = None,
    research_config_path: str | Path | None = None,
    protocol_path: str | Path | None = None,
    run_robustness: bool = True,
) -> dict[str, Any]:
    """Run a deterministic historical research suite."""

    if mode not in {"canary", "full"}:
        raise ValueError("mode must be canary or full")
    root = _project_root(project_root)
    default_features, default_execution = _default_data_paths(root)
    feature_file = Path(feature_path or default_features).resolve()
    execution_file = Path(execution_path or default_execution).resolve()
    suspension_file = (
        root / "runtime" / "data" / "top500" / "suspensions.parquet"
    ).resolve()
    suspension_metadata_file = suspension_file.with_name("suspensions.meta.json")
    factors_file = Path(factors_path or root / "configs" / "factors.json").resolve()
    research_file = Path(research_config_path or root / "configs" / "research.json").resolve()
    adaptive_protocol_file = (
        Path(protocol_path or root / "protocols" / "5.0.json").resolve()
        if suite == ADAPTIVE_SUITE
        else None
    )
    required_paths = [feature_file, execution_file, factors_file, research_file]
    if adaptive_protocol_file is not None:
        required_paths.append(adaptive_protocol_file)
    for path in required_paths:
        if not path.is_file():
            raise FileNotFoundError(path)

    research_config = _read_json(research_file)
    adaptive_protocol: dict[str, Any] | None = None
    adaptive_protocol_hash: str | None = None
    if adaptive_protocol_file is not None:
        adaptive_protocol, adaptive_protocol_hash = _load_adaptive_protocol(
            adaptive_protocol_file
        )
    validation_spec = _validation_spec(research_config)
    control, challengers = load_factor_suite(factors_file, suite)
    if suite == RESULTS_FIRST_SUITE:
        control = replace(control, direction_policy="all_history_ic")
        challengers = [
            replace(factor, direction_policy="all_history_ic")
            for factor in challengers
        ]
    elif suite in {WALK_FORWARD_SUITE, ADAPTIVE_SUITE}:
        control = replace(
            control,
            direction_policy="fixed",
            params={**dict(control.params), "fixed_direction": 1},
        )
    all_factors = [control, *challengers]
    portfolio_config = _portfolio_config(
        research_config,
        suite=suite,
        adaptive_protocol=adaptive_protocol,
    )
    feature_hash = _sha256_file(feature_file)
    execution_hash = _sha256_file(execution_file)
    execution_available = set(pq.ParquetFile(execution_file).schema_arrow.names)
    execution_date_for_suspensions = _resolve_column(
        execution_available,
        portfolio_config.date_column,
        ("date", "trade_date"),
    )
    if execution_date_for_suspensions is None:
        raise ValueError("execution store missing date field for suspension audit")
    execution_dates_for_suspensions = pd.to_datetime(
        pd.read_parquet(
            execution_file, columns=[execution_date_for_suspensions]
        )[execution_date_for_suspensions],
        errors="coerce",
    ).dropna()
    if execution_dates_for_suspensions.empty:
        raise ValueError("execution store has no valid dates for suspension audit")
    suspension_requested_start = (
        execution_dates_for_suspensions.min().date().isoformat()
    )
    suspension_requested_end = (
        execution_dates_for_suspensions.max().date().isoformat()
    )
    (
        suspension_snapshot_audit,
        suspension_hash,
        suspension_metadata_hash,
    ) = _suspension_input_identity(
        suspension_file,
        suspension_metadata_file,
        requested_start=suspension_requested_start,
        requested_end=suspension_requested_end,
    )
    implementation_hash = _implementation_sha256()
    data_builder_hash = _data_builder_sha256()
    runtime_identity = _runtime_identity()
    robustness_affects_run = bool(
        mode == "full" and suite in {"next", "recovery"}
    )
    fingerprint = _sha256_value(
        {
            "engine": ENGINE_ID,
            "mode": mode,
            "suite": suite,
            "implementation": implementation_hash,
            "data_builder": data_builder_hash,
            "runtime": runtime_identity,
            "features": feature_hash,
            "execution": execution_hash,
            "suspensions": {
                "artifact_sha256": suspension_hash,
                "metadata_sha256": suspension_metadata_hash,
            },
            "factors": [row.to_dict() for row in all_factors],
            "research": research_config,
            "adaptive_protocol": (
                {
                    "sha256": adaptive_protocol_hash,
                    "payload": adaptive_protocol,
                }
                if adaptive_protocol is not None
                else None
            ),
            "run_robustness": (
                bool(run_robustness) if robustness_affects_run else None
            ),
        }
    )
    run_id = fingerprint[:16]
    output_dir = root / "runtime" / "runs" / run_id
    summary_path = output_dir / "summary.json"
    existing_summary = summary_path.is_file()
    if resume and _completed_run_valid(summary_path, output_dir, fingerprint):
        cached = _read_json(summary_path)
        _write_json(
            root / "runtime" / "runs" / "latest.json",
            {"run_id": run_id, "output_dir": str(output_dir), "summary_path": str(summary_path)},
        )
        return cached
    # A published summary with a bad/missing manifest is a corruption signal,
    # not an incomplete checkpoint.  Recompute every cached result so modified
    # bodies cannot be re-hashed into a newly trusted manifest.
    cache_resume_allowed = bool(resume and not existing_summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    factor_dir = output_dir / "factors"
    factor_dir.mkdir(parents=True, exist_ok=True)

    adaptive_feature_fields = (
        ("return_1d", "momentum_120") if suite == ADAPTIVE_SUITE else ()
    )
    features = _load_features(
        feature_file,
        all_factors,
        validation_spec,
        extra_required_fields=adaptive_feature_fields,
    )
    execution = _load_execution(
        execution_file,
        portfolio_config,
        feature_path=feature_file,
        suspension_path=suspension_file,
        suspension_snapshot_audit=suspension_snapshot_audit,
    )
    (
        feature_hash,
        execution_hash,
        suspension_snapshot_audit,
        suspension_hash,
        suspension_metadata_hash,
    ) = _verify_loaded_input_snapshot(
        feature_path=feature_file,
        feature_sha256=feature_hash,
        execution_path=execution_file,
        execution_sha256=execution_hash,
        suspension_path=suspension_file,
        suspension_metadata_path=suspension_metadata_file,
        suspension_audit=suspension_snapshot_audit,
        suspension_sha256=suspension_hash,
        suspension_metadata_sha256=suspension_metadata_hash,
        suspension_requested_start=suspension_requested_start,
        suspension_requested_end=suspension_requested_end,
        adaptive_protocol_path=adaptive_protocol_file,
        adaptive_protocol_sha256=adaptive_protocol_hash,
    )
    signals = {
        factor.name: evaluate_factor_signal(features, factor, date_column=validation_spec.date_column)
        for factor in all_factors
    }
    stage_a_rows = [
        evaluate_stage_a(features, factor, validation_spec, signal=signals[factor.name])
        for factor in all_factors
    ]
    validations = {row.factor_name: row for row in stage_a_rows}
    similarities = diagnose_train_similarity(
        features, signals, stage_a_rows, validation_spec
    )
    stage_a_selection: StageASelection | None = None
    if suite in {
        "legacy-regression",
        RESULTS_FIRST_SUITE,
        WALK_FORWARD_SUITE,
        ADAPTIVE_SUITE,
    }:
        selected = [factor for factor in all_factors if factor.role != "diagnostic_only"]
    else:
        stage_a_selection = build_stage_a_selection(
            stage_a_rows,
            similarities,
            validation_spec,
            excluded_names={control.name},
        )
        factor_by_name = {factor.name: factor for factor in all_factors}
        selected = [
            control,
            *[
                factor_by_name[row.factor_name]
                for row in stage_a_selection.selected
            ],
        ]

    if suite == RESULTS_FIRST_SUITE:
        ensemble_factors, ensemble_signals, ensemble_validations = (
            _build_results_first_ensembles(
                features,
                control,
                [factor for factor in challengers if factor.role != "diagnostic_only"],
                signals,
                validations,
                validation_spec,
                research_config,
            )
        )
        signals.update(ensemble_signals)
        validations.update(ensemble_validations)
        stage_a_rows.extend(ensemble_validations[factor.name] for factor in ensemble_factors)
        # Standalone challengers provide component diagnostics and an optimized
        # direction, but only fallback-control ensembles are exposure-comparable
        # enough to enter the expensive portfolio leaderboard.
        selected = [control, *ensemble_factors]
    elif suite in {WALK_FORWARD_SUITE, ADAPTIVE_SUITE}:
        candidate_factors, candidate_signals, candidate_validations = (
            _build_walk_forward_candidates(
                features,
                control,
                challengers,
                signals,
                validations,
                validation_spec,
                research_config,
            )
        )
        signals.update(candidate_signals)
        validations.update(candidate_validations)
        stage_a_rows.extend(
            candidate_validations[factor.name]
            for factor in candidate_factors
            if factor.name != control.name
        )
        if suite == ADAPTIVE_SUITE:
            assert adaptive_protocol is not None
            frozen_registry = list(
                (adaptive_protocol.get("experts") or {}).get(
                    "ordered_registry"
                )
                or []
            )
            candidate_by_name = {
                factor.name: factor for factor in candidate_factors
            }
            missing_frozen = [
                name for name in frozen_registry if name not in candidate_by_name
            ]
            if missing_frozen:
                raise ValueError(
                    "adaptive protocol experts are absent from runtime candidates: "
                    + ", ".join(map(str, missing_frozen))
                )
            selected = [candidate_by_name[name] for name in frozen_registry]
        else:
            selected = candidate_factors

    _assert_unique_artifact_names(
        [factor.name for factor in selected],
        context=f"{suite} factor artifact",
    )
    if suite == ADAPTIVE_SUITE:
        assert adaptive_protocol is not None
        assert adaptive_protocol_hash is not None
        from .adaptive_runtime import validate_adaptive_protocol

        # Canary and full mode must reject the same protocol drift.  The full
        # sweep validates again at its artifact boundary, but waiting until
        # that expensive path would let the default smoke test bless modified
        # gates, allocator parameters, or overlay semantics.
        validate_adaptive_protocol(
            adaptive_protocol,
            protocol_sha256=adaptive_protocol_hash,
            factors=selected,
            control=control,
            base_config=portfolio_config,
        )
    execution_date_for_lineage = _resolve_column(
        set(execution.columns), portfolio_config.date_column, ("date", "trade_date")
    )
    execution_ticker_for_lineage = _resolve_column(
        set(execution.columns),
        portfolio_config.ticker_column,
        ("ticker", "ts_code", "symbol"),
    )
    assert execution_date_for_lineage is not None
    assert execution_ticker_for_lineage is not None
    membership_path = feature_file.parent / "membership.parquet"
    universe_hash = (
        _sha256_file(membership_path)
        if membership_path.is_file()
        else _frame_identity_sha256(
            features,
            [
                validation_spec.date_column,
                "ticker",
                "universe_member",
                "eligible",
            ],
        )
    )
    calendar_hash = _frame_identity_sha256(
        execution[[execution_date_for_lineage]].drop_duplicates(),
        [execution_date_for_lineage],
    )
    feature_lineage_fields = {
        field
        for factor in all_factors
        for field in factor.required_fields
    }
    feature_lineage_fields.update(
        features.attrs.get("research_universe_filter", {}).get(
            "columns_applied", ()
        )
    )
    feature_lineage_fields.update(adaptive_feature_fields)
    if any(
        factor.kind == "builtin" and factor.builtin == "pit_cashflow_quality"
        for factor in all_factors
    ):
        # This availability guard is consumed inside the builtin in addition
        # to its declared numeric inputs, so bind it to the feature artifact
        # even though legacy factor registries did not list it explicitly.
        feature_lineage_fields.add("financial_available_date")
    lineage_required_fields = tuple(
        sorted(
            {
                *feature_lineage_fields,
                *_execution_lineage_fields(execution, portfolio_config),
            }
        )
    )
    lineage_contract = conservative_default_contract(
        artifact_sha256=feature_hash,
        execution_artifact_sha256=execution_hash,
        suspension_artifact_sha256=suspension_hash,
        builder_sha256=data_builder_hash,
        calendar_sha256=calendar_hash,
        universe_sha256=universe_hash,
    )
    lineage_audit = audit_pit_lineage(
        lineage_contract,
        lineage_required_fields,
    )
    lineage_payload = {
        "schema_version": PIT_CONTRACT_SCHEMA_VERSION,
        "scope": "registered_factor_and_execution_dependencies",
        "strict_fail_closed": True,
        "historical_ingested_at_accepted_as_available_at": False,
        "builder_hash_semantics": (
            "current_data_code_identity_not_artifact_build_attestation"
        ),
        "contract": lineage_contract.to_dict(),
        "audit": lineage_audit,
    }
    lineage_path = output_dir / "pit-lineage.json"
    _write_json(lineage_path, lineage_payload)
    execution_date_column = _resolve_column(
        set(execution.columns), portfolio_config.date_column, ("date", "trade_date")
    )
    assert execution_date_column is not None
    last_decision_date = features[validation_spec.date_column].max()
    execution_tail = execution[execution[execution_date_column] > last_decision_date]
    evaluation_features = features
    evaluation_execution = execution
    if mode == "canary":
        evaluation_features, evaluation_execution = _canary_frames(features, execution, portfolio_config)

    stage_b: list[dict[str, Any]] = []
    for factor in selected:
        result_path = factor_dir / f"{_safe_name(factor.name)}.json"
        cached_result: dict[str, Any] | None = None
        if cache_resume_allowed and result_path.is_file():
            payload = _read_json(result_path)
            if (
                payload.get("run_fingerprint") == fingerprint
                and (payload.get("result") or {}).get("factor_name") == factor.name
                and (payload.get("result") or {}).get("account_nav_path")
                and payload.get("result_sha256")
                == _sha256_value(payload.get("result") or {})
            ):
                cached_result = payload.get("result")
        if cached_result is None:
            signal = signals[factor.name]
            if mode == "canary":
                signal = signal.loc[evaluation_features.index]
            cached_result = _portfolio_result(
                factor,
                validations[factor.name],
                signal,
                evaluation_features,
                evaluation_execution,
                portfolio_config,
                research_config,
                include_period_target_weights=suite == ADAPTIVE_SUITE,
            )
            _write_json(
                result_path,
                {
                    "run_fingerprint": fingerprint,
                    "result_sha256": _sha256_value(cached_result),
                    "result": cached_result,
                },
            )
        stage_b.append(cached_result)

    control_result = next(row for row in stage_b if row["factor_name"] == control.name)
    validated: list[str] = []
    pre_audit_confirmed: list[str] = []
    results_first_rankings: list[dict[str, Any]] = []
    results_first_excluded: list[dict[str, Any]] = []
    best_historical_strategy: str | None = None
    walk_forward_summary: dict[str, Any] | None = None
    adaptive_summary: dict[str, Any] | None = None
    if suite == RESULTS_FIRST_SUITE:
        comparison_periods = list(control_result.get("period_active_returns") or [])
        for row in stage_b:
            metrics = _results_first_metrics(
                row,
                research_config,
                periods_per_year=portfolio_config.periods_per_year,
                reference_periods=comparison_periods,
                optimization_scope=(
                    "all_observed_history"
                    if mode == "full"
                    else "canary_recent_window_smoke_only"
                ),
            )
            row["results_first_metrics"] = metrics
            row["strategy_kind"] = (
                "control"
                if row["factor_name"] == control.name
                else "ensemble"
                if ((row.get("factor") or {}).get("kind") == "ensemble")
                else "standalone_diagnostic"
            )
            row["results_first_ranking_eligible"] = bool(
                row["strategy_kind"] in {"control", "ensemble"}
                and metrics.get("observations")
                and float(metrics.get("period_coverage") or 0.0) >= 1.0
                and metrics.get("daily_nav_path_complete") is True
                and metrics.get("max_drawdown") is not None
            )
            row["results_first_ranking_exclusion_reason"] = (
                None
                if row["results_first_ranking_eligible"]
                else "standalone_diagnostic_only"
                if row["strategy_kind"] == "standalone_diagnostic"
                else "daily_account_nav_path_incomplete"
                if metrics.get("daily_nav_path_complete") is not True
                or metrics.get("max_drawdown") is None
                else "incomplete_control_period_coverage"
            )
            row["pre_audit_confirmed"] = False
            row["validated"] = False

        if mode == "full":
            def ranking_value(row: Mapping[str, Any], key: str) -> float:
                value = (row.get("results_first_metrics") or {}).get(key)
                if value is None:
                    return -np.inf
                parsed = float(value)
                return parsed if np.isfinite(parsed) else -np.inf

            score_rows = [
                row for row in stage_b if row["results_first_ranking_eligible"]
            ]
            if score_rows:
                score_weights = dict(
                    (score_rows[0].get("results_first_metrics") or {}).get(
                        "score_weights"
                    )
                    or {}
                )
                weight_total = float(sum(score_weights.values()))
                for row in score_rows:
                    (row.get("results_first_metrics") or {})[
                        "score_percentiles"
                    ] = {}
                for metric_name, weight in score_weights.items():
                    metric_ranks = pd.Series(
                        [ranking_value(row, metric_name) for row in score_rows],
                        dtype=float,
                    ).rank(method="average", pct=True)
                    for row, percentile in zip(
                        score_rows, metric_ranks.tolist(), strict=True
                    ):
                        (row.get("results_first_metrics") or {})[
                            "score_percentiles"
                        ][metric_name] = round(float(percentile), 8)
                for row in score_rows:
                    metrics = row.get("results_first_metrics") or {}
                    percentiles = metrics["score_percentiles"]
                    metrics["historical_score"] = round(
                        sum(
                            float(score_weights[key]) * float(percentiles[key])
                            for key in score_weights
                        )
                        / weight_total,
                        8,
                    )

            ranked_rows = sorted(
                score_rows,
                key=lambda row: (
                    -ranking_value(row, "historical_score"),
                    -ranking_value(row, "net_annual_return"),
                    str(row.get("factor_name") or ""),
                ),
            )
            control_score = ranking_value(control_result, "historical_score")
            if not np.isfinite(control_score):
                control_score = 0.0
            for rank, row in enumerate(ranked_rows, start=1):
                metrics = dict(row.get("results_first_metrics") or {})
                score = metrics.get("historical_score")
                row["historical_rank"] = rank
                row["historical_score_delta_vs_control"] = (
                    round(float(score) - control_score, 8) if score is not None else None
                )
                results_first_rankings.append(
                    {
                        "rank": rank,
                        "factor_name": row.get("factor_name"),
                        "strategy_kind": row.get("strategy_kind"),
                        "historical_score_delta_vs_control": row.get(
                            "historical_score_delta_vs_control"
                        ),
                        **metrics,
                    }
                )
            if results_first_rankings:
                best_historical_strategy = str(results_first_rankings[0]["factor_name"])
            results_first_excluded = [
                {
                    "factor_name": row.get("factor_name"),
                    "strategy_kind": row.get("strategy_kind"),
                    "reason": row.get("results_first_ranking_exclusion_reason"),
                    "period_coverage": (row.get("results_first_metrics") or {}).get(
                        "period_coverage"
                    ),
                    "observed_strategy_periods": (
                        row.get("results_first_metrics") or {}
                    ).get("observed_strategy_periods"),
                    "observations": (row.get("results_first_metrics") or {}).get(
                        "observations"
                    ),
                }
                for row in stage_b
                if not row["results_first_ranking_eligible"]
            ]
    elif suite == ADAPTIVE_SUITE:
        assert adaptive_protocol is not None
        assert adaptive_protocol_hash is not None
        if mode == "full":
            from .adaptive_runtime import run_adaptive_sweep

            adaptive_summary, _adaptive_base_accounts = run_adaptive_sweep(
                factors=selected,
                validations=validations,
                signals=signals,
                features=features,
                execution=execution,
                base_config=portfolio_config,
                research_config=research_config,
                base_results=stage_b,
                control=control,
                protocol=adaptive_protocol,
                protocol_sha256=adaptive_protocol_hash,
                output_dir=output_dir,
                run_fingerprint=fingerprint,
                resume=cache_resume_allowed,
                portfolio_result=_portfolio_result,
                historical_metrics=_results_first_metrics,
            )
        else:
            adaptive_summary = {
                "enabled": True,
                "protocol_id": adaptive_protocol["protocol_id"],
                "protocol_sha256": adaptive_protocol_hash,
                "protocol_status": adaptive_protocol["status"],
                "evidence_class": "engineering_smoke",
                "canary_smoke_only": True,
                "expert_registry": list(
                    (adaptive_protocol.get("experts") or {})[
                        "ordered_registry"
                    ]
                ),
                "account_registry": list(
                    (adaptive_protocol.get("scoring_accounts") or {})[
                        "per_offset"
                    ]
                ),
                "rebalance_offsets": [],
                "common_evaluation_start": None,
                "shadow_accounts_valid": False,
                "scoring_accounts_valid": False,
                "future_feedback_violation_count": 0,
                "future_overlay_violation_count": 0,
                "integrity_valid": False,
                "gate_results": {},
                "frozen_route": None,
                "prospective_status": "not_activated",
                "offsets": [],
            }
        for row in stage_b:
            row["strategy_kind"] = "adaptive_expert_shadow"
            row["account_role"] = (
                "adaptive_shadow_full_history"
                if mode == "full"
                else "engineering_smoke_account"
            )
            row["cross_strategy_comparison_eligible"] = False
            row["authoritative_comparison_artifact"] = (
                "adaptive/adaptive-summary.json" if mode == "full" else None
            )
            row["pre_audit_confirmed"] = False
            row["validated"] = False
    elif suite == WALK_FORWARD_SUITE:
        if mode == "full":
            walk_forward_summary, _dynamic_base_result = run_walk_forward_sweep(
                factors=selected,
                validations=validations,
                signals=signals,
                features=features,
                execution=execution,
                base_config=portfolio_config,
                research_config=research_config,
                base_results=stage_b,
                control=control,
                output_dir=output_dir,
                run_fingerprint=fingerprint,
                resume=cache_resume_allowed,
                portfolio_result=_portfolio_result,
                historical_metrics=_results_first_metrics,
            )
        else:
            walk_forward_settings = dict(research_config.get("walk_forward") or {})
            selector = WalkForwardSelectorSpec.from_mapping(
                walk_forward_settings.get("selector") or {}
            )
            fixed_comparator = _walk_forward_fixed_comparator(
                walk_forward_settings
            )
            walk_forward_summary = {
                "enabled": True,
                "protocol": "causal_walk_forward",
                "evidence_class": "engineering_smoke",
                "canary_smoke_only": True,
                "ranking_available": False,
                "selector_executed": False,
                "selector": selector.to_dict(),
                "candidate_registry": [factor.name for factor in selected],
                "fixed_comparator_factor": fixed_comparator["name"],
                "fixed_comparator": {
                    "factor_name": fixed_comparator["name"],
                    "protocol": fixed_comparator,
                    "candidate_registry": [factor.name for factor in selected],
                    "uses_realized_returns": False,
                    "independent_cost_account_per_offset": False,
                    "phase_ranking_eligible": False,
                    "phase_rank": None,
                    "dynamic_phase_deltas": {},
                    "dynamic_positive_annual_return_delta_ratio": None,
                },
                "dynamic_factor": "causal_walk_forward_dynamic",
                "dynamic_status": "experimental_account",
                "rebalance_offsets": [],
                "phase_quantile": float(
                    walk_forward_settings.get("phase_quantile", 0.20)
                ),
                "common_evaluation_start": None,
                "future_selection_violation_count": 0,
                "full_dynamic_period_coverage": False,
                "causal_history_valid": False,
                "control_phase_ranking_eligible": False,
                "dynamic_phase_ranking_eligible": False,
                "dynamic_control_common_offset_count": 0,
                "historical_diagnostic_passed": False,
                "best_phase_strategy": None,
                "dynamic_phase_rank": None,
                "phase_rankings": [],
                "offsets": [],
            }
        for row in stage_b:
            row["strategy_kind"] = (
                "walk_forward_dynamic"
                if row["factor_name"] == "causal_walk_forward_dynamic"
                else "control"
                if row["factor_name"] == control.name
                else "static_candidate"
            )
            row["account_role"] = (
                "selector_shadow_full_history"
                if mode == "full"
                else "engineering_smoke_account"
            )
            row["cross_strategy_comparison_eligible"] = False
            row["authoritative_comparison_artifact"] = (
                "walk-forward/walk-forward-summary.json"
                if mode == "full"
                else None
            )
            row["pre_audit_confirmed"] = False
            row["validated"] = False
    else:
        challenger_count = max(
            1, sum(row["factor_name"] != control.name for row in stage_b)
        )
        for row in stage_b:
            if row["factor_name"] == control.name:
                continue
            comparison = _control_comparison(
                row,
                control_result,
                research_config,
                validation_spec,
                correction_factor=challenger_count,
            )
            row["control_comparison"] = comparison
            row["beats_control"] = _beats_control(
                row, control_result, research_config, comparison
            )
            row["pre_audit_confirmed"] = bool(
                suite != "legacy-regression"
                and row["gate_passed"]
                and row["beats_control"]
            )
            if row["pre_audit_confirmed"]:
                pre_audit_confirmed.append(str(row["factor_name"]))
            row["validated"] = bool(
                mode == "full"
                and row["pre_audit_confirmed"]
                and not row.get("audit_falsified", False)
            )
            if row["validated"]:
                validated.append(str(row["factor_name"]))
    # Walk-forward per-factor artifacts are selector shadows (or canary smoke
    # accounts), never cross-strategy comparison evidence.  The equal-AUM
    # comparison lives only under walk-forward/ and in its phase summary.
    for row in stage_b:
        result_path = factor_dir / f"{_safe_name(str(row['factor_name']))}.json"
        _write_json(
            result_path,
            {
                "run_fingerprint": fingerprint,
                "result_sha256": _sha256_value(row),
                "result": row,
            },
        )

    robustness: dict[str, Any] | None = None
    if (
        suite != RESULTS_FIRST_SUITE
        and mode == "full"
        and suite in {"next", "recovery"}
        and not pre_audit_confirmed
        and run_robustness
    ):
        # The finite matrix covers every train-admitted challenger.  It must not
        # pick a "best" subject from validation or audit results, because doing
        # so would turn the diagnostic matrix into an unregistered search.
        challenger_by_name = {factor.name: factor for factor in challengers}
        robustness_factors = [
            control,
            *[
                challenger_by_name[str(row["factor_name"])]
                for row in stage_b
                if row["factor_name"] != control.name
            ],
        ]
        robustness = _run_robustness(
            robustness_factors,
            validations,
            signals,
            features,
            execution,
            portfolio_config,
            research_config,
            output_dir / "robustness.json",
        )

    if suite == ADAPTIVE_SUITE and mode == "full":
        search_status = "adaptive_protocol_evaluated"
    elif suite == ADAPTIVE_SUITE:
        search_status = "adaptive_canary_smoke"
    elif suite == WALK_FORWARD_SUITE and mode == "full":
        search_status = "causal_walk_forward_sweep_completed"
    elif suite == WALK_FORWARD_SUITE:
        search_status = "causal_walk_forward_canary_smoke"
    elif suite == RESULTS_FIRST_SUITE and mode == "full":
        search_status = "results_first_historical_ranking_completed"
    elif suite == RESULTS_FIRST_SUITE:
        search_status = "results_first_canary_smoke"
    elif mode == "canary":
        search_status = "canary_smoke"
    elif suite == "legacy-regression":
        search_status = "legacy_regression_completed"
    elif validated:
        search_status = "confirmed_candidate_found"
    elif pre_audit_confirmed:
        search_status = "audit_falsified_stop"
    elif robustness is not None:
        search_status = "robustness_completed_exhausted"
    elif not run_robustness:
        search_status = "robustness_skipped"
    else:
        search_status = "completed_no_candidate"

    data_warning = research_config.get("data_warning")
    if not data_warning and ("st_filter_status" not in features.columns or any(
        "unverified" in str(value).casefold() or "degraded" in str(value).casefold()
        for value in features.get("st_filter_status", pd.Series(dtype=str)).dropna().unique()
    )):
        data_warning = "st_history_unverified"
    summary: dict[str, Any] = {
        "schema_version": 5,
        "engine": ENGINE_ID,
        "status": "completed",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "suite": suite,
        "mode": mode,
        "evidence_class": (
            "post_selection_adaptive_simulation"
            if suite == ADAPTIVE_SUITE and mode == "full"
            else "post_selection_causal_simulation"
            if suite == WALK_FORWARD_SUITE and mode == "full"
            else "engineering_smoke"
            if suite in {WALK_FORWARD_SUITE, ADAPTIVE_SUITE}
            else EVIDENCE_CLASS
        ),
        "investment_claim_allowed": False,
        "investment_claim_blockers": [
            "historical_or_post_selection_evidence_is_not_forward_confirmation",
            *(
                []
                if lineage_audit["investment_claim_allowed"]
                else ["point_in_time_lineage_not_verified"]
            ),
        ],
        "promotion_triggered": False,
        "canary_smoke_only": mode == "canary",
        "gate_results_interpretable": (
            mode == "full"
            and suite
            not in {RESULTS_FIRST_SUITE, WALK_FORWARD_SUITE, ADAPTIVE_SUITE}
        ),
        "ranking_results_interpretable": (
            mode == "full" and suite == RESULTS_FIRST_SUITE
        ),
        "walk_forward_results_interpretable": bool(
            mode == "full"
            and suite == WALK_FORWARD_SUITE
            and (walk_forward_summary or {}).get("causal_history_valid")
        ),
        "adaptive_results_interpretable": bool(
            mode == "full"
            and suite == ADAPTIVE_SUITE
            and (adaptive_summary or {}).get("integrity_valid") is True
        ),
        "git": _git_state(root),
        "implementation_sha256": implementation_hash,
        "data_builder_sha256": data_builder_hash,
        "runtime_identity": runtime_identity,
        "pit_lineage": {
            "artifact_path": "pit-lineage.json",
            "contract_sha256": _sha256_value(lineage_contract.to_dict()),
            **lineage_audit,
        },
        "data": {
            "feature_path": str(feature_file),
            "feature_sha256": feature_hash,
            "execution_path": str(execution_file),
            "execution_sha256": execution_hash,
            "suspension_path": str(suspension_file),
            "suspension_sha256": suspension_hash,
            "suspension_metadata_path": str(suspension_metadata_file),
            "suspension_metadata_sha256": suspension_metadata_hash,
            "suspension_status": (
                "available" if suspension_hash is not None else "unavailable"
            ),
            "suspension_snapshot_audit": suspension_snapshot_audit,
            "start_date": features[validation_spec.date_column].min().date().isoformat(),
            "end_date": features[validation_spec.date_column].max().date().isoformat(),
            "row_count": int(len(features)),
            "ticker_count": int(features["ticker"].nunique()),
            "warning": data_warning,
            "execution_start_date": execution[execution_date_column].min().date().isoformat(),
            "execution_end_date": execution[execution_date_column].max().date().isoformat(),
            "execution_tail_policy": "pricing_only_after_last_decision_date",
            "execution_tail_row_count": int(len(execution_tail)),
            "execution_tail_date_count": int(execution_tail[execution_date_column].nunique()),
            "research_universe_filter": features.attrs.get(
                "research_universe_filter", {}
            ),
            "security_event_injection": execution.attrs.get(
                "security_event_injection", {}
            ),
        },
        "control_factor": control.name,
        "portfolio_config": asdict(portfolio_config),
        "price_accounting": {
            "price_basis": portfolio_config.price_basis,
            "execution_price_column": portfolio_config.open_column,
            "price_source": portfolio_config.price_source
            or f"input_column:{portfolio_config.open_column}",
            "corporate_action_mode": "embedded_in_adjusted_prices",
            "lot_size": int(portfolio_config.lot_size),
            "explicit_split_dividend_events_enabled": False,
        },
        "stage_a": [row.to_dict() for row in stage_a_rows],
        "stage_a_selection": stage_a_selection.to_dict()
        if stage_a_selection is not None
        else {
            "basis": (
                "all_registered_plus_runtime_ensembles"
                if suite == RESULTS_FIRST_SUITE
                else "fixed_direction_causal_candidate_registry"
                if suite in {WALK_FORWARD_SUITE, ADAPTIVE_SUITE}
                else "legacy_regression_all_registered"
            ),
            "selected": [row.name for row in selected if row.name != control.name],
            "decisions": [],
            "similarities": [row.to_dict() for row in similarities],
        },
        "stage_b_selected": [str(row["factor_name"]) for row in stage_b],
        "stage_b": stage_b,
        "validated_factors": validated,
        "validated_count": len(validated),
        "pre_audit_confirmed_factors": pre_audit_confirmed,
        "results_first": {
            "enabled": suite == RESULTS_FIRST_SUITE,
            "ranking_available": suite == RESULTS_FIRST_SUITE and mode == "full",
            "optimization_scope": (
                "all_observed_history"
                if suite == RESULTS_FIRST_SUITE and mode == "full"
                else "canary_recent_window_smoke_only"
                if suite == RESULTS_FIRST_SUITE
                else None
            ),
            "comparison_period_basis": "control_signal_dates"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "missing_period_score_policy": "cash_return_zero_diagnostic_only"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "incomplete_period_ranking_policy": "exclude_from_ranking"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "best_historical_strategy": best_historical_strategy,
            "rankings": results_first_rankings,
            "excluded_from_ranking": results_first_excluded,
        },
        "walk_forward": walk_forward_summary,
        "adaptive": adaptive_summary,
        "robustness": robustness,
        "search_status": search_status,
        "search_stopped": bool(
            mode == "full"
            and suite not in {
                "legacy-regression",
                RESULTS_FIRST_SUITE,
                WALK_FORWARD_SUITE,
            }
        ),
    }
    report_path = output_dir / "report.md"
    report_path.write_text(render_report(summary), encoding="utf-8")
    # ``summary.json`` is the completed marker.  Build and hash it under a
    # pending name, publish the manifest, then atomically expose the summary.
    pending_summary_path = output_dir / "summary.pending.json"
    _write_json(pending_summary_path, summary)
    artifact_paths: list[tuple[str, Path]] = [
        ("summary.json", pending_summary_path),
        ("report.md", report_path),
        ("pit-lineage.json", lineage_path),
        *[
            (path.relative_to(output_dir).as_posix(), path)
            for path in sorted(factor_dir.glob("*.json"))
        ],
    ]
    if (output_dir / "robustness.json").is_file():
        artifact_paths.append(("robustness.json", output_dir / "robustness.json"))
    walk_forward_dir = output_dir / "walk-forward"
    if walk_forward_dir.is_dir():
        artifact_paths.extend(
            (
                path.relative_to(output_dir).as_posix(),
                path,
            )
            for path in sorted(walk_forward_dir.rglob("*.json"))
        )
    adaptive_dir = output_dir / "adaptive"
    if adaptive_dir.is_dir():
        artifact_paths.extend(
            (
                path.relative_to(output_dir).as_posix(),
                path,
            )
            for path in sorted(adaptive_dir.rglob("*.json"))
        )
    # Recheck every external byte-bound input immediately before publication.
    # Full adaptive runs are long enough for a protocol or data file to change
    # after the post-load check; no completed summary/manifest may be exposed
    # for such a mixed snapshot.
    (
        feature_hash,
        execution_hash,
        suspension_snapshot_audit,
        suspension_hash,
        suspension_metadata_hash,
    ) = _verify_loaded_input_snapshot(
        feature_path=feature_file,
        feature_sha256=feature_hash,
        execution_path=execution_file,
        execution_sha256=execution_hash,
        suspension_path=suspension_file,
        suspension_metadata_path=suspension_metadata_file,
        suspension_audit=suspension_snapshot_audit,
        suspension_sha256=suspension_hash,
        suspension_metadata_sha256=suspension_metadata_hash,
        suspension_requested_start=suspension_requested_start,
        suspension_requested_end=suspension_requested_end,
        adaptive_protocol_path=adaptive_protocol_file,
        adaptive_protocol_sha256=adaptive_protocol_hash,
    )
    manifest = {
        "schema_version": 2,
        "algorithm": "sha256",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "inputs": [
            {
                "role": "tushare_suspend_d",
                "path": str(suspension_file),
                "status": (
                    "available" if suspension_hash is not None else "unavailable"
                ),
                "size_bytes": (
                    suspension_file.stat().st_size
                    if suspension_hash is not None
                    else None
                ),
                "sha256": suspension_hash,
                "metadata_path": str(suspension_metadata_file),
                "metadata_size_bytes": (
                    suspension_metadata_file.stat().st_size
                    if suspension_metadata_hash is not None
                    else None
                ),
                "metadata_sha256": suspension_metadata_hash,
                "audit": suspension_snapshot_audit,
            },
            *(
                [
                    {
                        "role": "adaptive_protocol",
                        "path": str(adaptive_protocol_file),
                        "size_bytes": adaptive_protocol_file.stat().st_size,
                        "sha256": adaptive_protocol_hash,
                    }
                ]
                if adaptive_protocol_file is not None
                else []
            ),
        ],
        "files": [
            {
                "path": logical_path,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for logical_path, path in artifact_paths
        ],
    }
    manifest["manifest_sha256"] = _manifest_payload_sha256(manifest)
    _write_json(output_dir / "manifest.json", manifest)
    pending_summary_path.replace(summary_path)
    _write_json(
        root / "runtime" / "runs" / "latest.json",
        {"run_id": run_id, "output_dir": str(output_dir), "summary_path": str(summary_path)},
    )
    return summary


def latest_run(project_root: str | Path | None = None) -> dict[str, Any] | None:
    root = _project_root(project_root)
    path = root / "runtime" / "runs" / "latest.json"
    return _read_json(path) if path.is_file() else None


__all__ = [
    "ADAPTIVE_SUITE",
    "ENGINE_ID",
    "latest_run",
    "load_factor_suite",
    "run_research",
]
