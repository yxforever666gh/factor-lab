#!/usr/bin/env python
"""Verify the 10.1 quarterly cycle against physical historical prefixes."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.etf_assets import MultiAssetStage, load_multi_asset_stage  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)
from factor_lab.research.multi_asset import (  # noqa: E402
    BASE_COST_BPS_PER_SIDE,
    QUARTERLY_BORDA_ID,
    SimulationConfig,
    build_monthly_targets,
    simulate_targets,
)


RELEASE = "10.1"
PROTOCOL_ID = "factor-lab/10.1/quarterly-prospective-cycle-v1"
PROTOCOL_PATH = Path("protocols/10.1-quarterly-prospective-cycle.json")
PROTOCOL_PAYLOAD = "0c3f2240cc404c1084230f1efbfe3f9fd3f0fa73dbbdc69ec63e5465ef7610ca"
PROTOCOL_FILE_SHA256 = "81240134127de2fedde6e231f8a3a02dd74950ff9da67e5298e71834c61843b5"
DRY_RUN_PATH = Path("scripts/run-10.1-historical-dry-run.py")
CYCLE_PATH = Path("scripts/run-10.1-quarterly-cycle.py")
CORE_PATH = Path("src/factor_lab/research/multi_asset.py")
CAPTURE_PATH = Path("src/factor_lab/data/etf_live.py")
CLI_PATH = Path("src/factor_lab/cli.py")
FORMAL_ROOT = ROOT / "runtime" / "prospective" / RELEASE
LABEL = "historical_asof_dry_run"
PLAN_FIELDS = (
    "code",
    "side",
    "target_weight",
    "signal_price",
    "signal_adv20_rmb",
    "capacity_rmb",
    "pre_signal_shares",
    "desired_shares",
    "requested_delta_shares",
    "requested_shares",
    "capacity_capped_shares",
    "planned_delta_shares",
    "planned_shares",
    "requested_signal_notional",
    "planned_signal_notional",
    "capacity_limited_shares",
    "capacity_limited_signal_notional",
    "capacity_limited",
)
UNIT_SCALE_PLAN_FIELDS = {
    "pre_signal_shares",
    "desired_shares",
    "requested_delta_shares",
    "requested_shares",
    "capacity_capped_shares",
    "planned_delta_shares",
    "planned_shares",
    "capacity_limited_shares",
}


def _read_protocol() -> dict[str, Any]:
    value = json.loads((ROOT / PROTOCOL_PATH).read_text(encoding="utf-8"))
    dry = value.get("historical_asof_dry_run") or {}
    if (
        value.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD
        or file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("frozen_strategy", {}).get("strategy_id")
        != QUARTERLY_BORDA_ID
        or dry.get("label") != LABEL
        or dry.get("prospective_label_allowed") is not False
        or dry.get("formal_artifact_write_allowed") is not False
    ):
        raise ValueError("10.1 historical dry-run protocol identity differs")
    return value


def _dates(frame: pd.DataFrame) -> pd.Series:
    values = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    if values.isna().any():
        raise ValueError("source contains an invalid trade_date")
    return values


def _stage_prefix(
    stage: MultiAssetStage,
    *,
    market_end: pd.Timestamp,
    calendar_end: pd.Timestamp,
) -> MultiAssetStage:
    def through(frame: pd.DataFrame, end: pd.Timestamp) -> pd.DataFrame:
        value = frame.loc[_dates(frame).le(end)].copy().reset_index(drop=True)
        if value.empty:
            raise ValueError(f"empty physical prefix through {end.date()}")
        return value

    manifest = dict(stage.manifest)
    manifest["price_end_date"] = market_end.date().isoformat()
    return MultiAssetStage(
        path=stage.path,
        manifest=manifest,
        calendar=through(stage.calendar, calendar_end),
        assets={code: through(frame, market_end) for code, frame in stage.assets.items()},
    )


def _sorted(frame: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.reset_index(drop=True)
    return frame.sort_values(list(keys), kind="mergesort").reset_index(drop=True)


def _assert_frame_exact(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    *,
    role: str,
    keys: Sequence[str],
) -> None:
    try:
        pd.testing.assert_frame_equal(
            _sorted(actual, keys),
            _sorted(expected, keys),
            check_exact=True,
            check_dtype=True,
            check_index_type=True,
            check_column_type=True,
            check_frame_type=True,
            check_names=True,
            check_like=False,
        )
    except AssertionError as exc:
        raise ValueError(f"{role} differs") from exc


def _through(frame: pd.DataFrame, column: str, end: pd.Timestamp) -> pd.DataFrame:
    dates = pd.to_datetime(frame[column], errors="coerce").dt.normalize()
    if dates.isna().any():
        raise ValueError(f"artifact contains invalid {column}")
    return frame.loc[dates.le(end)].copy()


def _at(frame: pd.DataFrame, column: str, date: pd.Timestamp) -> pd.DataFrame:
    dates = pd.to_datetime(frame[column], errors="coerce").dt.normalize()
    return frame.loc[dates.eq(date)].copy()


def _orders_at(result: Mapping[str, Any], signal: pd.Timestamp) -> pd.DataFrame:
    orders = result["orders"]
    if not isinstance(orders, pd.DataFrame):
        raise TypeError("simulation lacks orders DataFrame")
    current = _at(orders, "signal_date", signal)
    missing = sorted(set(PLAN_FIELDS) - set(orders.columns))
    if missing and not current.empty:
        raise ValueError(f"sealed plan lacks columns: {missing}")
    return current


def _plan(records: pd.DataFrame) -> list[dict[str, Any]]:
    if records.empty:
        return []
    return records.loc[:, list(PLAN_FIELDS)].sort_values(
        "code", kind="mergesort"
    ).to_dict(orient="records")


def _execution_plan_matches_seal(
    records: pd.DataFrame, sealed_records: pd.DataFrame
) -> bool:
    if records.empty:
        return sealed_records.empty
    actual = _plan(records)
    sealed = _plan(sealed_records)
    expected_by_code = {str(row["code"]): dict(row) for row in sealed}
    if (
        len(expected_by_code) != len(sealed)
        or {str(row["code"]) for row in actual} != set(expected_by_code)
    ):
        return False
    adjusted: list[dict[str, Any]] = []
    for row in records.sort_values("code", kind="mergesort").itertuples(
        index=False
    ):
        expected = expected_by_code[str(row.code)]
        multiplier = float(getattr(row, "execution_unit_multiplier", 1.0))
        if not math.isfinite(multiplier) or multiplier <= 0.0:
            return False
        transformed = dict(expected)
        for field in UNIT_SCALE_PLAN_FIELDS:
            scaled = int(expected[field]) * multiplier
            if not float(scaled).is_integer():
                return False
            transformed[field] = int(scaled)
        adjusted.append(transformed)
    return actual == adjusted


def _target_signals(targets: pd.DataFrame) -> list[pd.Timestamp]:
    signals = sorted(
        pd.Timestamp(value).normalize() for value in targets["signal_date"].unique()
    )
    if not signals:
        raise ValueError("historical dry-run has no quarterly signals")
    return signals


def _canonical_targets(targets: pd.DataFrame) -> pd.DataFrame:
    value = targets.copy()
    value["borda_rank"] = pd.to_numeric(value["borda_rank"], errors="coerce").astype(
        "float64"
    )
    return value


def _quarter(value: pd.Timestamp) -> tuple[int, int]:
    return value.year, (value.month - 1) // 3 + 1


def _next_quarter_end(
    sessions: Sequence[pd.Timestamp], signal: pd.Timestamp
) -> pd.Timestamp | None:
    for index, value in enumerate(sessions[:-1]):
        if value > signal and _quarter(value) != _quarter(sessions[index + 1]):
            return value
    return None


def _repo_relative(path: Path) -> str:
    return Path(os.path.relpath(path.resolve(), ROOT.resolve())).as_posix()


def _implementation_identity() -> dict[str, Any]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    paths = (
        DRY_RUN_PATH,
        CYCLE_PATH,
        CORE_PATH,
        CAPTURE_PATH,
        CLI_PATH,
        PROTOCOL_PATH,
    )
    return {
        "git_head": head,
        "files": {
            path.as_posix(): {
                "path": path.as_posix(),
                "file_sha256": file_sha256(ROOT / path),
            }
            for path in paths
        },
    }


def _tree_snapshot(root: Path) -> dict[str, Any]:
    if not root.exists():
        return {"directories": [], "files": {}, "symlinks": {}}
    directories: list[str] = []
    files: dict[str, str] = {}
    symlinks: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            symlinks[relative] = os.readlink(path)
        elif path.is_dir():
            directories.append(relative)
        elif path.is_file():
            files[relative] = file_sha256(path)
    return {"directories": directories, "files": files, "symlinks": symlinks}


def _build_report(stage: MultiAssetStage, protocol: Mapping[str, Any]) -> dict[str, Any]:
    sessions = tuple(_dates(stage.calendar))
    full_targets = build_monthly_targets(
        stage.assets, sessions, QUARTERLY_BORDA_ID
    )
    signals = _target_signals(full_targets)
    full_result = simulate_targets(
        stage.assets,
        full_targets,
        sessions,
        SimulationConfig(cost_bps_per_side=BASE_COST_BPS_PER_SIDE),
    )
    market_end = max(_dates(frame).max() for frame in stage.assets.values())
    cycles: list[dict[str, Any]] = []
    by_signal: dict[pd.Timestamp, dict[str, Any]] = {}
    by_outcome: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    for index, signal in enumerate(signals):
        current_full_targets = full_targets.loc[
            pd.to_datetime(full_targets["signal_date"])
            .dt.normalize()
            .eq(signal)
        ]
        execution_values = pd.to_datetime(
            current_full_targets["execution_date"]
        ).dt.normalize().unique()
        if len(execution_values) != 1:
            raise ValueError("signal lacks one frozen next official session")
        execution = pd.Timestamp(execution_values[0]).normalize()
        outcome = _next_quarter_end(sessions, signal)
        if outcome is not None and outcome > market_end:
            outcome = None
        cycle = {
            "cycle_id": f"{signal.year}Q{(signal.month - 1) // 3 + 1}",
            "signal_date": signal.date().isoformat(),
            "execution_date": execution.date().isoformat(),
            "cumulative_target_signal_count": index + 1,
            "signal_close_nav": None,
            "sealed_order_count": None,
            "target_prefix_exact": None,
            "sealed_plan_prefix_exact": None,
            "outcome_date": None,
            "outcome_prefix_exact": None,
            "terminal_nav": None,
            "terminal_trade_count": None,
        }
        cycles.append(cycle)
        by_signal[signal] = cycle
        if outcome is not None:
            by_outcome.setdefault(outcome, []).append(cycle)

    events = sorted(set(by_signal) | set(by_outcome))
    for event in events:
        signal_cycle = by_signal.get(event)
        calendar_end = (
            pd.Timestamp(signal_cycle["execution_date"])
            if signal_cycle is not None
            else event
        )
        prefix = _stage_prefix(
            stage, market_end=event, calendar_end=calendar_end
        )
        prefix_sessions = tuple(_dates(prefix.calendar))
        prefix_targets = build_monthly_targets(
            prefix.assets, prefix_sessions, QUARTERLY_BORDA_ID
        )
        current_full_targets = full_targets.loc[
            pd.to_datetime(full_targets["signal_date"]).dt.normalize().le(event)
        ].reset_index(drop=True)
        _assert_frame_exact(
            _canonical_targets(prefix_targets),
            _canonical_targets(current_full_targets),
            role=f"target prefix {event.date()}",
            keys=("signal_date", "code"),
        )
        prefix_result = simulate_targets(
            prefix.assets,
            prefix_targets,
            prefix_sessions,
            SimulationConfig(cost_bps_per_side=BASE_COST_BPS_PER_SIDE),
        )

        for completed in by_outcome.get(event, []):
            for artifact, date_column, keys in (
                ("daily_nav", "trade_date", ("trade_date",)),
                ("holdings", "trade_date", ("trade_date", "code")),
                ("trades", "execution_date", ("execution_date", "code", "side")),
            ):
                _assert_frame_exact(
                    prefix_result[artifact],
                    _through(full_result[artifact], date_column, event),
                    role=f"outcome prefix {completed['cycle_id']} {artifact}",
                    keys=keys,
                )
            terminal = _at(prefix_result["daily_nav"], "trade_date", event)
            if len(terminal) != 1:
                raise ValueError("outcome lacks one terminal NAV row")
            completed.update(
                {
                    "outcome_date": event.date().isoformat(),
                    "outcome_prefix_exact": True,
                    "terminal_nav": float(terminal.iloc[0]["nav"]),
                    "terminal_trade_count": int(
                        len(_through(prefix_result["trades"], "execution_date", event))
                    ),
                }
            )

        if signal_cycle is not None:
            sealed_orders = _orders_at(prefix_result, event)
            execution_orders = _orders_at(full_result, event)
            if not _execution_plan_matches_seal(execution_orders, sealed_orders):
                raise ValueError(f"sealed plan prefix {event.date()} differs")
            _assert_frame_exact(
                _at(prefix_result["daily_nav"], "trade_date", event),
                _at(full_result["daily_nav"], "trade_date", event),
                role=f"signal-close NAV {event.date()}",
                keys=("trade_date",),
            )
            _assert_frame_exact(
                _at(prefix_result["holdings"], "trade_date", event),
                _at(full_result["holdings"], "trade_date", event),
                role=f"signal-close holdings {event.date()}",
                keys=("code",),
            )
            nav = _at(prefix_result["daily_nav"], "trade_date", event)
            signal_cycle.update(
                {
                    "signal_close_nav": float(nav.iloc[0]["nav"]),
                    "sealed_order_count": len(sealed_orders),
                    "target_prefix_exact": True,
                    "sealed_plan_prefix_exact": True,
                }
            )

    source_manifest = stage.path / "manifest.json"
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_10_1_historical_asof_dry_run",
        "release": RELEASE,
        "label": LABEL,
        "prospective": False,
        "strategy_id": QUARTERLY_BORDA_ID,
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "payload_sha256": protocol["payload_sha256"],
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
        },
        "source": {
            "path": _repo_relative(stage.path),
            "stage": stage.manifest.get("stage"),
            "price_start_date": stage.manifest.get("price_start_date"),
            "price_end_date": stage.manifest.get("price_end_date"),
            "manifest_payload_sha256": stage.manifest.get("payload_sha256"),
            "manifest_file_sha256": (
                file_sha256(source_manifest) if source_manifest.is_file() else None
            ),
        },
        "execution": {
            "cost_bps_per_side": BASE_COST_BPS_PER_SIDE,
            "continuous_account": True,
            "fresh_cash_reset_after_genesis": False,
            "physical_market_prefix_per_signal": True,
        },
        "implementation": _implementation_identity(),
        "summary": {
            "signal_count": len(signals),
            "signal_count_strictly_positive": len(signals) > 0,
            "confirmed_outcome_count": sum(
                cycle["outcome_prefix_exact"] is True for cycle in cycles
            ),
            "target_prefix_mismatch_count": 0,
            "sealed_plan_prefix_mismatch_count": 0,
            "signal_close_state_prefix_mismatch_count": 0,
            "outcome_prefix_mismatch_count": 0,
            "formal_path_write_count": 0,
        },
        "cycles": cycles,
        "claim_contract": {
            "evidence_class": LABEL,
            "prospective_label_allowed": False,
            "may_be_promoted_to_prospective": False,
            "profit_claim_allowed": False,
        },
    }
    result["payload_sha256"] = canonical_payload_sha256(result)
    return result


def _outside_formal_root(path: Path) -> Path:
    target = path.expanduser().resolve()
    formal = FORMAL_ROOT.resolve()
    if target == formal or formal in target.parents:
        raise ValueError("historical dry-run output cannot enter formal paths")
    return target


def _create_only(path: Path, value: Mapping[str, Any]) -> None:
    target = _outside_formal_root(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        target.unlink(missing_ok=True)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _outside_formal_root(args.output)
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"historical dry-run output is create-only: {output}")
    formal_before = _tree_snapshot(FORMAL_ROOT)
    protocol = _read_protocol()
    stage = load_multi_asset_stage(args.source_root, args.stage)
    result = _build_report(stage, protocol)
    if _tree_snapshot(FORMAL_ROOT) != formal_before:
        raise RuntimeError("historical dry-run changed the formal runtime tree")
    _create_only(output, result)
    if _tree_snapshot(FORMAL_ROOT) != formal_before:
        output.unlink(missing_ok=True)
        raise RuntimeError("historical dry-run changed the formal runtime tree")
    print(
        json.dumps(
            {"status": "historical_asof_dry_run_complete", **result["summary"]},
            ensure_ascii=False,
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
