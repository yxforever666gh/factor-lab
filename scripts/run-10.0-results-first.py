#!/usr/bin/env python
"""Run the fully exposed 10.0 results-first quarterly momentum diagnostic."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Mapping

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.etf_assets import MultiAssetStage, load_multi_asset_stage  # noqa: E402
from factor_lab.release_integrity import canonical_payload_sha256, file_sha256  # noqa: E402
from factor_lab.research.multi_asset import (  # noqa: E402
    BASE_COST_BPS_PER_SIDE,
    CASH_CODE,
    CASH_ONLY_ID,
    CONTROL_ID,
    INITIAL_CAPITAL_RMB,
    LOT_SIZE,
    MAX_SIGNAL_ADV_PARTICIPATION,
    QUARTERLY_BORDA_ID,
    QUARTERLY_BORDA_END_LAG,
    QUARTERLY_BORDA_START_LAG,
    RISK_CODES,
    SimulationConfig,
    STRESS_COST_BPS_PER_SIDE,
    VOLATILITY_BALANCED_ID,
    build_monthly_targets,
    phase_metrics,
    simulate_targets,
)


RELEASE = "10.0"
ROUTE = "quarterly_12_1_dual_momentum_rank_budget"
PROTOCOL_ID = "factor-lab/10.0/results-first-quarterly-borda-v1"
PROTOCOL_PATH = Path("protocols/10.0-results-first-quarterly-borda.json")
PROTOCOL_PAYLOAD = "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b"
PROTOCOL_FILE_SHA256 = "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd"
RUNNER_PATH = Path("scripts/run-10.0-results-first.py")
CORE_PATH = Path("src/factor_lab/research/multi_asset.py")
SOURCE_ROOT = ROOT / "runtime" / "data" / "multi-asset-9.0" / "sources"
SOURCE_STAGE = "audit"
SOURCE_MANIFEST_PAYLOAD = "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
SOURCE_MANIFEST_FILE_SHA256 = "cdbf8ba498142adff04216b476522f47ee18df6f0fa02f3395d0e141191adbfa"
SOURCE_START = "2014-01-15"
SOURCE_END = "2026-08-28"
EVIDENCE_PATH = Path("protocols/evidence/10.0/results-first-diagnostic.json")
EVALUATION_ROLES = (
    "candidate",
    "candidate_stress",
    "static",
    "static_stress",
    "v9",
    "v9_stress",
    "cash",
    "cash_stress",
)
GATE_ROLES = (
    "candidate",
    "candidate_stress",
    "static",
    "static_stress",
    "cash",
    "cash_stress",
)
ARTIFACTS = ("targets", "orders", "daily_nav", "holdings", "trades")
PERIODS: dict[str, tuple[str, str]] = {
    "D1": ("2015-03-02", "2019-12-31"),
    "D2": ("2020-01-02", "2022-12-30"),
    "D3": ("2023-01-03", "2026-08-28"),
    "full": ("2015-03-02", "2026-08-28"),
}
BASE_COST_BPS = 8.0
STRESS_COST_BPS = 16.0
MINIMUM_POSITIVE_YEAR_RATIO = 0.5
MINIMUM_FILL_RATIO = 0.98
MAXIMUM_CAPACITY_LIMITED_RATIO = 0.02
MAXIMUM_ACCOUNTING_ERROR = 1e-8
ALLOWED_STATUSES = {
    "executed",
    "partial_cash",
    "blocked_cash",
    "blocked_missing_open",
    "blocked_capacity",
}
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    value = completed.stdout.decode("ascii").strip()
    if _COMMIT_RE.fullmatch(value) is None:
        raise ValueError("invalid Git HEAD")
    return value


def _git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _implementation_identity(*, commit_bound: bool) -> dict[str, Any]:
    head = _git_head()
    paths = (RUNNER_PATH, CORE_PATH, PROTOCOL_PATH)
    files: dict[str, Any] = {}
    for relative in paths:
        path = ROOT / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"implementation file is absent or indirect: {relative}")
        working = path.read_bytes()
        files[relative.as_posix()] = {
            "path": relative.as_posix(),
            "file_sha256": hashlib.sha256(working).hexdigest(),
        }
    if commit_bound:
        branch = _git("branch", "--show-current").decode("utf-8").strip()
        if branch != "main":
            raise RuntimeError(f"formal 10.0 evidence requires main, found {branch!r}")
        if _git("status", "--porcelain").strip():
            raise RuntimeError("formal 10.0 evidence requires a clean worktree")
        for relative in paths:
            working = (ROOT / relative).read_bytes()
            committed = _git("show", f"HEAD:{relative.as_posix()}")
            if working != committed:
                raise RuntimeError(
                    f"formal implementation differs from HEAD blob: {relative}"
                )
    return {
        "git_head": head,
        "commit_bound": commit_bound,
        "files": files,
    }


def _create_only(path: Path, payload: Mapping[str, Any]) -> None:
    target = path if path.is_absolute() else ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        target.unlink(missing_ok=True)
        raise


def _stage_through(stage: MultiAssetStage, end: str) -> MultiAssetStage:
    cutoff = pd.Timestamp(end).normalize()

    def through(frame: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
        selected = frame.loc[dates.le(cutoff)].copy().reset_index(drop=True)
        if selected.empty or pd.to_datetime(selected["trade_date"]).max().normalize() > cutoff:
            raise ValueError(f"could not enforce causal source cutoff {end}")
        return selected

    manifest = dict(stage.manifest)
    manifest["price_end_date"] = cutoff.date().isoformat()
    return MultiAssetStage(
        path=stage.path,
        manifest=manifest,
        calendar=through(stage.calendar),
        assets={code: through(frame) for code, frame in stage.assets.items()},
    )


def _filter_targets(
    targets: pd.DataFrame, *, start: str, end: str
) -> pd.DataFrame:
    executions = pd.to_datetime(targets["execution_date"], errors="raise").dt.normalize()
    selected = targets.loc[
        executions.between(pd.Timestamp(start), pd.Timestamp(end))
    ].copy()
    if selected.empty:
        raise ValueError(f"no target executes between {start} and {end}")
    return selected.reset_index(drop=True)


def _assert_frame_exact(actual: pd.DataFrame, expected: pd.DataFrame, *, role: str) -> None:
    try:
        pd.testing.assert_frame_equal(
            actual.reset_index(drop=True),
            expected.reset_index(drop=True),
            check_dtype=True,
            check_exact=True,
        )
    except AssertionError as exc:
        raise ValueError(f"{role} differs") from exc


def _prefix_replay_count(
    stage: MultiAssetStage,
    targets: pd.DataFrame,
    sessions: tuple[pd.Timestamp, ...],
) -> int:
    count = 0
    for signal_date, expected in targets.groupby("signal_date", sort=True):
        signal = pd.Timestamp(signal_date).normalize()
        execution = pd.Timestamp(expected["execution_date"].iloc[0]).normalize()
        prefix = MultiAssetStage(
            path=stage.path,
            manifest=dict(stage.manifest),
            calendar=stage.calendar.loc[
                pd.to_datetime(stage.calendar["trade_date"]).dt.normalize().le(execution)
            ].copy(),
            assets={
                code: frame.loc[
                    pd.to_datetime(frame["trade_date"]).dt.normalize().le(signal)
                ].copy()
                for code, frame in stage.assets.items()
            },
        )
        prefix_sessions = tuple(value for value in sessions if value <= execution)
        rebuilt = build_monthly_targets(
            prefix.assets, prefix_sessions, QUARTERLY_BORDA_ID
        )
        actual = rebuilt.loc[
            pd.to_datetime(rebuilt["signal_date"]).dt.normalize().eq(signal)
        ]
        if actual.empty:
            raise ValueError(f"prefix replay omitted {signal.date()}")
        _assert_frame_exact(
            actual.sort_values("code", kind="mergesort"),
            expected.sort_values("code", kind="mergesort"),
            role=f"prefix target {signal.date()}",
        )
        count += 1
    return count


def _exact_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _execution_validity(result: Mapping[str, Any]) -> dict[str, Any]:
    if any(not isinstance(result.get(name), pd.DataFrame) for name in ARTIFACTS):
        raise RuntimeError("result lacks a complete artifact set")
    trades = result["trades"]
    daily = result["daily_nav"]
    holdings = result["holdings"]
    if "status" not in trades or trades["status"].isna().any():
        raise RuntimeError("trade status is incomplete")
    statuses = trades["status"].astype(str)
    status_values_allowed = set(statuses).issubset(ALLOWED_STATUSES)
    if not status_values_allowed:
        raise RuntimeError("unknown trade status")
    numeric_trade_columns = (
        "requested_execution_notional",
        "actual_executed_notional",
        "capacity_limited_execution_notional",
        "planned_signal_notional",
        "capacity_rmb",
    )
    numeric: dict[str, pd.Series] = {}
    for column in numeric_trade_columns:
        if column not in trades:
            raise RuntimeError(f"trades lack {column}")
        values = pd.to_numeric(trades[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all() or values.lt(0.0).any():
            raise RuntimeError(f"invalid trade field {column}")
        numeric[column] = values
    requested = math.fsum(map(float, numeric["requested_execution_notional"]))
    executed = math.fsum(map(float, numeric["actual_executed_notional"]))
    capacity_limited = math.fsum(
        map(float, numeric["capacity_limited_execution_notional"])
    )
    status_execution_identity = all(
        (
            status == "executed"
            and actual > 1e-6
            and math.isclose(
                actual,
                request - capacity_rejected,
                rel_tol=1e-12,
                abs_tol=1e-6,
            )
        )
        or (
            status == "partial_cash"
            and 1e-6 < actual < request - capacity_rejected - 1e-6
        )
        or (
            status not in {"executed", "partial_cash"} and actual <= 1e-6
        )
        for status, request, actual, capacity_rejected in zip(
            statuses,
            numeric["requested_execution_notional"],
            numeric["actual_executed_notional"],
            numeric["capacity_limited_execution_notional"],
            strict=True,
        )
    )
    capacity_violations = int(
        (
            numeric["planned_signal_notional"]
            > numeric["capacity_rmb"] + 1e-8
        ).sum()
    )
    required_daily = {
        "trade_date",
        "cash",
        "nav",
        "accounting_error",
        "requested_notional",
        "executed_notional",
        "capacity_limited_requested_notional",
    }
    if not required_daily.issubset(daily.columns):
        raise RuntimeError("daily NAV lacks accounting fields")
    cash = pd.to_numeric(daily["cash"], errors="coerce")
    nav = pd.to_numeric(daily["nav"], errors="coerce")
    accounting = pd.to_numeric(daily["accounting_error"], errors="coerce")
    daily_requested = pd.to_numeric(daily["requested_notional"], errors="coerce")
    daily_executed = pd.to_numeric(daily["executed_notional"], errors="coerce")
    daily_capacity = pd.to_numeric(
        daily["capacity_limited_requested_notional"], errors="coerce"
    )
    if (
        cash.isna().any()
        or nav.isna().any()
        or accounting.isna().any()
        or daily_requested.isna().any()
        or daily_executed.isna().any()
        or daily_capacity.isna().any()
        or not np.isfinite(cash.to_numpy()).all()
        or not np.isfinite(nav.to_numpy()).all()
        or not np.isfinite(accounting.to_numpy()).all()
        or not np.isfinite(daily_requested.to_numpy()).all()
        or not np.isfinite(daily_executed.to_numpy()).all()
        or not np.isfinite(daily_capacity.to_numpy()).all()
        or nav.le(0.0).any()
        or daily_requested.lt(0.0).any()
        or daily_executed.lt(0.0).any()
        or daily_capacity.lt(0.0).any()
    ):
        raise RuntimeError("daily NAV is invalid")
    if not {"trade_date", "market_value"}.issubset(holdings.columns):
        raise RuntimeError("holdings lack market value")
    market_value = pd.to_numeric(holdings["market_value"], errors="coerce")
    if market_value.isna().any() or not np.isfinite(market_value.to_numpy()).all():
        raise RuntimeError("holdings market value is invalid")
    gross = (
        holdings.assign(_market_value=market_value.abs())
        .groupby("trade_date", sort=False)["_market_value"]
        .sum()
    )
    daily_indexed = daily.assign(
        trade_date=pd.to_datetime(daily["trade_date"], errors="raise").dt.normalize()
    ).set_index("trade_date")
    gross.index = pd.to_datetime(gross.index, errors="raise").normalize()
    aligned = gross.reindex(daily_indexed.index)
    if aligned.isna().any():
        raise RuntimeError("holdings do not cover daily NAV")
    gross_ratio = aligned.to_numpy(dtype=float) / nav.to_numpy(dtype=float)
    if not np.isfinite(gross_ratio).all() or bool(np.any(gross_ratio < 0.0)):
        raise RuntimeError("gross exposure is invalid")
    capacity = result.get("capacity")
    capacity_identity = bool(
        isinstance(capacity, Mapping)
        and math.isclose(float(capacity["requested_notional_total"]), requested, rel_tol=1e-12, abs_tol=1e-6)
        and math.isclose(float(capacity["executed_notional_total"]), executed, rel_tol=1e-12, abs_tol=1e-6)
        and math.isclose(
            float(capacity["capacity_limited_requested_notional"]),
            capacity_limited,
            rel_tol=1e-12,
            abs_tol=1e-6,
        )
        and _exact_nonnegative_int(capacity.get("capacity_violation_count"))
        and int(capacity["capacity_violation_count"]) == capacity_violations
    )
    daily_trade_identity = bool(
        math.isclose(
            math.fsum(map(float, daily_requested)),
            requested,
            rel_tol=1e-12,
            abs_tol=1e-6,
        )
        and math.isclose(
            math.fsum(map(float, daily_executed)),
            executed,
            rel_tol=1e-12,
            abs_tol=1e-6,
        )
        and math.isclose(
            math.fsum(map(float, daily_capacity)),
            capacity_limited,
            rel_tol=1e-12,
            abs_tol=1e-6,
        )
    )
    valid = bool(
        status_execution_identity
        and capacity_violations == 0
        and int(cash.lt(-1e-8).sum()) == 0
        and int((gross_ratio > 1.0 + 1e-8).sum()) == 0
        and capacity_identity
        and daily_trade_identity
        and executed <= requested + 1e-6
        and capacity_limited <= requested + 1e-6
    )
    return {
        "passed": valid,
        "artifact_set_complete": True,
        "status_values_allowed": status_values_allowed,
        "status_execution_identity_exact": status_execution_identity,
        "capacity_violation_count": capacity_violations,
        "negative_cash_observation_count": int(cash.lt(-1e-8).sum()),
        "leverage_observation_count": int((gross_ratio > 1.0 + 1e-8).sum()),
        "maximum_gross_exposure_ratio": float(np.max(gross_ratio)),
        "maximum_nav_reconciliation_error": float(accounting.abs().max()),
        "requested_notional_total": requested,
        "executed_notional_total": executed,
        "capacity_limited_requested_notional": capacity_limited,
        "capacity_aggregation_identity_exact": capacity_identity,
        "daily_trade_notional_identity_exact": daily_trade_identity,
    }


def _metric_view(result: Mapping[str, Any], *, start: str, end: str) -> dict[str, Any]:
    value = phase_metrics(result, start=start, end=end)
    value["execution_validity"] = _execution_validity(result)
    return value


def _absolute_gate(
    candidate: Mapping[str, Any],
    static: Mapping[str, Any],
    cash: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "cagr_strictly_above_cash": {
            "metric": float(candidate["cagr"]) - float(cash["cagr"]),
            "threshold": 0.0,
        },
        "cagr_strictly_above_static": {
            "metric": float(candidate["cagr"]) - float(static["cagr"]),
            "threshold": 0.0,
        },
        "positive_complete_year_ratio_at_least": {
            "metric": float(candidate["positive_complete_year_ratio"]),
            "threshold": MINIMUM_POSITIVE_YEAR_RATIO,
        },
        "requested_notional_fill_ratio_at_least": {
            "metric": float(candidate["requested_notional_fill_ratio"]),
            "threshold": MINIMUM_FILL_RATIO,
        },
        "capacity_limited_requested_notional_ratio_at_most": {
            "metric": float(candidate["capacity_limited_requested_notional_ratio"]),
            "threshold": MAXIMUM_CAPACITY_LIMITED_RATIO,
        },
        "nav_reconciliation_error_at_most": {
            "metric": float(candidate["nav_reconciliation_error"]),
            "threshold": MAXIMUM_ACCOUNTING_ERROR,
        },
    }
    checks = {
        key: item["metric"] > item["threshold"]
        if key.startswith("cagr_strictly")
        else item["metric"] <= item["threshold"]
        if key.endswith("_at_most")
        else item["metric"] >= item["threshold"]
        for key, item in values.items()
    }
    checks["hard_execution_and_accounting"] = bool(
        candidate["execution_validity"]["passed"]
    )
    return {"passed": all(checks.values()), "checks": checks, "values": values}


def _run_period(stage: MultiAssetStage, name: str, start: str, end: str) -> dict[str, Any]:
    phase_stage = _stage_through(stage, end)
    sessions = tuple(
        pd.to_datetime(phase_stage.calendar["trade_date"], errors="raise").dt.normalize()
    )
    targets = {
        "candidate": _filter_targets(
            build_monthly_targets(
                phase_stage.assets, sessions, QUARTERLY_BORDA_ID
            ),
            start=start,
            end=end,
        ),
        "static": _filter_targets(
            build_monthly_targets(phase_stage.assets, sessions, CONTROL_ID),
            start=start,
            end=end,
        ),
        "v9": _filter_targets(
            build_monthly_targets(
                phase_stage.assets, sessions, VOLATILITY_BALANCED_ID
            ),
            start=start,
            end=end,
        ),
        "cash": _filter_targets(
            build_monthly_targets(phase_stage.assets, sessions, CASH_ONLY_ID),
            start=start,
            end=end,
        ),
    }
    prefix_count = _prefix_replay_count(
        phase_stage, targets["candidate"], sessions
    )
    specs = {
        "candidate": (targets["candidate"], BASE_COST_BPS),
        "candidate_stress": (targets["candidate"], STRESS_COST_BPS),
        "static": (targets["static"], BASE_COST_BPS),
        "static_stress": (targets["static"], STRESS_COST_BPS),
        "v9": (targets["v9"], BASE_COST_BPS),
        "v9_stress": (targets["v9"], STRESS_COST_BPS),
        "cash": (targets["cash"], BASE_COST_BPS),
        "cash_stress": (targets["cash"], STRESS_COST_BPS),
    }
    results = {
        role: simulate_targets(
            phase_stage.assets,
            role_targets,
            sessions,
            SimulationConfig(cost_bps_per_side=cost),
        )
        for role, (role_targets, cost) in specs.items()
    }
    for base, stress in (
        ("candidate", "candidate_stress"),
        ("static", "static_stress"),
        ("v9", "v9_stress"),
        ("cash", "cash_stress"),
    ):
        _assert_frame_exact(
            results[base]["targets"],
            results[stress]["targets"],
            role=f"{name} {base}/{stress} targets",
        )
    metrics = {
        role: _metric_view(result, start=start, end=end)
        for role, result in results.items()
    }
    all_eight_roles_valid = all(
        metrics[role]["execution_validity"]["passed"] for role in EVALUATION_ROLES
    )
    all_six_gate_roles_valid = all(
        metrics[role]["execution_validity"]["passed"] for role in GATE_ROLES
    )
    base_gate = _absolute_gate(
        metrics["candidate"], metrics["static"], metrics["cash"]
    )
    stress_gate = _absolute_gate(
        metrics["candidate_stress"],
        metrics["static_stress"],
        metrics["cash_stress"],
    )
    passed = (
        all_six_gate_roles_valid and base_gate["passed"] and stress_gate["passed"]
    )
    return {
        "period": name,
        "start_date": start,
        "end_date": end,
        "fresh_cash_and_empty_holdings": True,
        "candidate_prefix_replay_signal_count": prefix_count,
        "candidate_base_stress_targets_exact": True,
        "metrics": metrics,
        "base_gate": base_gate,
        "stress_gate": stress_gate,
        "all_six_gate_roles_hard_valid": all_six_gate_roles_valid,
        "all_eight_roles_hard_valid_disclosure": all_eight_roles_valid,
        "passed": passed,
    }


def _verify_source(stage: MultiAssetStage) -> None:
    manifest_path = stage.path / "manifest.json"
    if (
        stage.manifest.get("stage") != SOURCE_STAGE
        or stage.manifest.get("price_start_date") != SOURCE_START
        or stage.manifest.get("price_end_date") != SOURCE_END
        or stage.manifest.get("payload_sha256") != SOURCE_MANIFEST_PAYLOAD
        or file_sha256(manifest_path) != SOURCE_MANIFEST_FILE_SHA256
    ):
        raise ValueError("retained 9.0 audit source identity differs")
    for code, frame in stage.assets.items():
        maximum = pd.to_datetime(frame["trade_date"], errors="raise").max().normalize()
        if maximum > pd.Timestamp(SOURCE_END):
            raise ValueError(f"source exceeds cutoff for {code}: {maximum}")


def _read_protocol() -> dict[str, Any]:
    value = json.loads((ROOT / PROTOCOL_PATH).read_text(encoding="utf-8"))
    strategy = value.get("frozen_strategy") or {}
    execution = value.get("inherited_execution_contract") or {}
    gate = value.get("results_first_gate") or {}
    assets = value.get("assets") or {}
    frozen_periods = value.get("fully_exposed_periods") or {}
    expected_periods = {
        name: {
            "performance_start": start,
            "performance_end": end,
        }
        for name, (start, end) in PERIODS.items()
    }
    if (
        not isinstance(value, dict)
        or value.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD
        or file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("direction_change") is not True
        or value.get("route") != ROUTE
        or strategy.get("strategy_id") != QUARTERLY_BORDA_ID
        or assets.get("risk_codes_in_frozen_tie_order") != list(RISK_CODES)
        or assets.get("cash_code") != CASH_CODE
        or QUARTERLY_BORDA_START_LAG != 252
        or QUARTERLY_BORDA_END_LAG != 21
        or strategy.get("signal_schedule")
        != "last official SSE session of each natural calendar quarter after close"
        or strategy.get("top_k") is not None
        or strategy.get("volatility_weighting") is not False
        or strategy.get("parameter_grid") is not False
        or strategy.get("stress_reuses_base_targets_exactly") is not True
        or execution.get("initial_capital_rmb") != INITIAL_CAPITAL_RMB
        or execution.get("lot_size_units") != LOT_SIZE
        or BASE_COST_BPS != BASE_COST_BPS_PER_SIDE
        or STRESS_COST_BPS != STRESS_COST_BPS_PER_SIDE
        or execution.get("base_cost_bps_per_side") != BASE_COST_BPS
        or execution.get("stress_cost_bps_per_side") != STRESS_COST_BPS
        or execution.get("capacity_limit_fraction_of_signal_date_adv20")
        != MAX_SIGNAL_ADV_PARTICIPATION
        or value.get("selection_contract", {}).get("selected_strategy_id")
        != QUARTERLY_BORDA_ID
        or value.get("evidence", {}).get("path") != EVIDENCE_PATH.as_posix()
        or set(frozen_periods) != set(PERIODS)
        or any(
            any(frozen_periods[name].get(key) != expected for key, expected in fields.items())
            for name, fields in expected_periods.items()
        )
        or gate.get("applies_to_each")
        != [
            "D1.base_8bp",
            "D1.stress_16bp",
            "D2.base_8bp",
            "D2.stress_16bp",
            "D3.base_8bp",
            "D3.stress_16bp",
        ]
        or gate.get("positive_complete_year_ratio_at_least")
        != MINIMUM_POSITIVE_YEAR_RATIO
        or gate.get("candidate_cagr_strictly_above_matching_investable_cash_cagr")
        is not True
        or gate.get("candidate_cagr_strictly_above_matching_static_risk_budget_cagr")
        is not True
        or gate.get("requested_notional_fill_ratio_at_least") != MINIMUM_FILL_RATIO
        or gate.get("capacity_limited_requested_notional_ratio_at_most")
        != MAXIMUM_CAPACITY_LIMITED_RATIO
        or gate.get("nav_reconciliation_error_at_most") != MAXIMUM_ACCOUNTING_ERROR
        or gate.get("capacity_violation_count_at_most") != 0
        or gate.get("negative_cash_observation_count_at_most") != 0
        or gate.get("leverage_observation_count_at_most") != 0
        or gate.get("sharpe_is_disclosure_only") is not True
        or gate.get("max_drawdown_is_disclosure_only") is not True
        or gate.get("annualized_turnover_is_disclosure_only") is not True
        or gate.get("full_period_is_disclosure_only") is not True
        or gate.get("all_six_D1_D2_D3_roles_must_pass") is not True
        or gate.get("pooled_period_rescue") is not False
        or value.get("claim_contract", {}).get("evidence_class")
        != "fully_exposed_results_first_causal_historical_diagnostic"
    ):
        raise ValueError("10.0 protocol identity differs")
    return value


def _build_evidence(
    stage: MultiAssetStage,
    protocol: Mapping[str, Any],
    implementation: Mapping[str, Any],
) -> dict[str, Any]:
    periods = {
        name: _run_period(stage, name, start, end)
        for name, (start, end) in PERIODS.items()
    }
    selection_periods = ("D1", "D2", "D3")
    passed = all(periods[name]["passed"] for name in selection_periods)
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_results_first_diagnostic",
        "release": RELEASE,
        "route": ROUTE,
        "status": (
            "candidate_passed_all_results_first_gates"
            if passed
            else "candidate_failed_results_first_gates"
        ),
        "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": PROTOCOL_ID,
            "file_sha256": PROTOCOL_FILE_SHA256,
            "payload_sha256": PROTOCOL_PAYLOAD,
        },
        "source": {
            "path": "runtime/data/multi-asset-9.0/sources/stage=audit",
            "manifest_file_sha256": SOURCE_MANIFEST_FILE_SHA256,
            "manifest_payload_sha256": SOURCE_MANIFEST_PAYLOAD,
            "price_start_date": SOURCE_START,
            "price_end_date": SOURCE_END,
        },
        "implementation": dict(implementation),
        "candidate": {
            "strategy_id": QUARTERLY_BORDA_ID,
            "selected_after_observing_all_reported_results": True,
            "candidate_definition_was_not_preregistered_before_market_results": True,
            "candidate_base_stress_target_reuse_required": True,
        },
        "comparators": {
            "matched_static_strategy_id": CONTROL_ID,
            "v9_strategy_id": VOLATILITY_BALANCED_ID,
            "investable_cash_strategy_id": CASH_ONLY_ID,
        },
        "gate_contract": dict(protocol["results_first_gate"]),
        "periods": periods,
        "selection": {
            "gate_passed": passed,
            "selected_candidate_id": QUARTERLY_BORDA_ID if passed else None,
            "runner_up_fallback": False,
            "parameter_or_candidate_change_after_results_allowed": False,
        },
        "claim_contract": dict(protocol["claim_contract"]),
    }
    evidence["payload_sha256"] = canonical_payload_sha256(evidence)
    return evidence


def _stored_validity_passes(metrics: Mapping[str, Any]) -> bool:
    validity = metrics.get("execution_validity")
    return bool(
        isinstance(validity, Mapping)
        and validity.get("passed") is True
        and validity.get("artifact_set_complete") is True
        and validity.get("status_values_allowed") is True
        and validity.get("status_execution_identity_exact") is True
        and validity.get("capacity_aggregation_identity_exact") is True
        and validity.get("daily_trade_notional_identity_exact") is True
        and validity.get("capacity_violation_count") == 0
        and validity.get("negative_cash_observation_count") == 0
        and validity.get("leverage_observation_count") == 0
        and math.isfinite(float(validity.get("maximum_gross_exposure_ratio", math.nan)))
        and 0.0 <= float(validity["maximum_gross_exposure_ratio"]) <= 1.0 + 1e-8
        and math.isfinite(float(validity.get("maximum_nav_reconciliation_error", math.nan)))
        and float(validity["maximum_nav_reconciliation_error"])
        == float(metrics.get("nav_reconciliation_error", math.nan))
        and float(validity["maximum_nav_reconciliation_error"])
        <= MAXIMUM_ACCOUNTING_ERROR
    )


def _verify_implementation_binding(implementation: Any) -> None:
    expected_paths = {path.as_posix() for path in (RUNNER_PATH, CORE_PATH, PROTOCOL_PATH)}
    if (
        not isinstance(implementation, Mapping)
        or set(implementation) != {"git_head", "commit_bound", "files"}
        or implementation.get("commit_bound") is not True
        or _COMMIT_RE.fullmatch(str(implementation.get("git_head") or "")) is None
        or not isinstance(implementation.get("files"), Mapping)
        or set(implementation["files"]) != expected_paths
    ):
        raise ValueError("10.0 evidence implementation identity differs")
    commit = str(implementation["git_head"])
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("10.0 evidence implementation is not an ancestor of HEAD")
    for relative in (RUNNER_PATH, CORE_PATH, PROTOCOL_PATH):
        key = relative.as_posix()
        binding = implementation["files"][key]
        current = (ROOT / relative).read_bytes()
        committed = _git("show", f"{commit}:{key}")
        if (
            not isinstance(binding, Mapping)
            or set(binding) != {"path", "file_sha256"}
            or binding.get("path") != key
            or binding.get("file_sha256") != hashlib.sha256(current).hexdigest()
            or binding.get("file_sha256") != hashlib.sha256(committed).hexdigest()
        ):
            raise ValueError(f"10.0 evidence implementation differs: {key}")


def verify_evidence(evidence: Mapping[str, Any], *, verify_data: bool = False) -> None:
    protocol = _read_protocol()
    if (
        not isinstance(evidence, Mapping)
        or set(evidence)
        != {
            "schema_version",
            "kind",
            "release",
            "route",
            "status",
            "evidence_class",
            "protocol",
            "source",
            "implementation",
            "candidate",
            "comparators",
            "gate_contract",
            "periods",
            "selection",
            "claim_contract",
            "payload_sha256",
        }
        or evidence.get("payload_sha256") != canonical_payload_sha256(evidence)
        or evidence.get("schema_version") != 1
        or evidence.get("kind") != "factor_lab_results_first_diagnostic"
        or evidence.get("release") != RELEASE
        or evidence.get("route") != ROUTE
        or evidence.get("evidence_class")
        != "fully_exposed_results_first_causal_historical_diagnostic"
        or evidence.get("protocol")
        != {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": PROTOCOL_ID,
            "file_sha256": PROTOCOL_FILE_SHA256,
            "payload_sha256": PROTOCOL_PAYLOAD,
        }
        or evidence.get("source")
        != {
            "path": "runtime/data/multi-asset-9.0/sources/stage=audit",
            "manifest_file_sha256": SOURCE_MANIFEST_FILE_SHA256,
            "manifest_payload_sha256": SOURCE_MANIFEST_PAYLOAD,
            "price_start_date": SOURCE_START,
            "price_end_date": SOURCE_END,
        }
        or evidence.get("candidate")
        != {
            "strategy_id": QUARTERLY_BORDA_ID,
            "selected_after_observing_all_reported_results": True,
            "candidate_definition_was_not_preregistered_before_market_results": True,
            "candidate_base_stress_target_reuse_required": True,
        }
        or evidence.get("comparators")
        != {
            "matched_static_strategy_id": CONTROL_ID,
            "v9_strategy_id": VOLATILITY_BALANCED_ID,
            "investable_cash_strategy_id": CASH_ONLY_ID,
        }
        or evidence.get("gate_contract") != protocol.get("results_first_gate")
        or evidence.get("claim_contract") != protocol.get("claim_contract")
    ):
        raise ValueError("10.0 evidence top-level contract differs")
    _verify_implementation_binding(evidence.get("implementation"))
    periods = evidence.get("periods")
    if not isinstance(periods, Mapping) or set(periods) != set(PERIODS):
        raise ValueError("10.0 evidence period set differs")
    for name, (start, end) in PERIODS.items():
        period = periods[name]
        metrics = period.get("metrics") if isinstance(period, Mapping) else None
        if (
            not isinstance(metrics, Mapping)
            or set(metrics) != set(EVALUATION_ROLES)
            or set(period)
            != {
                "period",
                "start_date",
                "end_date",
                "fresh_cash_and_empty_holdings",
                "candidate_prefix_replay_signal_count",
                "candidate_base_stress_targets_exact",
                "metrics",
                "base_gate",
                "stress_gate",
                "all_six_gate_roles_hard_valid",
                "all_eight_roles_hard_valid_disclosure",
                "passed",
            }
            or period.get("period") != name
            or period.get("start_date") != start
            or period.get("end_date") != end
            or period.get("fresh_cash_and_empty_holdings") is not True
            or period.get("candidate_base_stress_targets_exact") is not True
            or not _exact_nonnegative_int(period.get("candidate_prefix_replay_signal_count"))
            or int(period["candidate_prefix_replay_signal_count"]) <= 0
        ):
            raise ValueError(f"10.0 {name} evidence shape differs")
        all_eight = all(_stored_validity_passes(metrics[role]) for role in EVALUATION_ROLES)
        all_six = all(_stored_validity_passes(metrics[role]) for role in GATE_ROLES)
        base_gate = _absolute_gate(metrics["candidate"], metrics["static"], metrics["cash"])
        stress_gate = _absolute_gate(
            metrics["candidate_stress"], metrics["static_stress"], metrics["cash_stress"]
        )
        passed = all_six and base_gate["passed"] and stress_gate["passed"]
        if (
            period.get("base_gate") != base_gate
            or period.get("stress_gate") != stress_gate
            or period.get("all_six_gate_roles_hard_valid") is not all_six
            or period.get("all_eight_roles_hard_valid_disclosure") is not all_eight
            or period.get("passed") is not passed
        ):
            raise ValueError(f"10.0 {name} gate does not replay")
    passed = all(bool(periods[name]["passed"]) for name in ("D1", "D2", "D3"))
    expected_selection = {
        "gate_passed": passed,
        "selected_candidate_id": QUARTERLY_BORDA_ID if passed else None,
        "runner_up_fallback": False,
        "parameter_or_candidate_change_after_results_allowed": False,
    }
    expected_status = (
        "candidate_passed_all_results_first_gates"
        if passed
        else "candidate_failed_results_first_gates"
    )
    if evidence.get("selection") != expected_selection or evidence.get("status") != expected_status:
        raise ValueError("10.0 evidence selection differs")
    if verify_data:
        stage = load_multi_asset_stage(SOURCE_ROOT, SOURCE_STAGE)
        _verify_source(stage)
        expected = _build_evidence(stage, protocol, evidence["implementation"])
        if expected != evidence:
            raise ValueError("10.0 evidence does not replay from retained data")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=EVIDENCE_PATH)
    args = parser.parse_args(argv)
    stage = load_multi_asset_stage(SOURCE_ROOT, SOURCE_STAGE)
    _verify_source(stage)
    protocol = _read_protocol()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    formal_output = output.resolve() == (ROOT / EVIDENCE_PATH).resolve()
    implementation = _implementation_identity(commit_bound=formal_output)
    evidence = _build_evidence(stage, protocol, implementation)
    if formal_output:
        verify_evidence(evidence, verify_data=False)
    if formal_output and _implementation_identity(commit_bound=True) != implementation:
        raise RuntimeError("formal implementation changed during 10.0 evaluation")
    _create_only(args.output, evidence)
    print(f"status={evidence['status']}")
    print(f"payload_sha256={evidence['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
