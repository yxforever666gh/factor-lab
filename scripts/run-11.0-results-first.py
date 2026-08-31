#!/usr/bin/env python
"""Run the fully exposed 11.0 results-first dual-confirm diagnostic."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Mapping

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256  # noqa: E402
from factor_lab.research.multi_asset import (  # noqa: E402
    CASH_ONLY_ID,
    CONTROL_ID,
    QUARTERLY_BORDA_ID,
    QUARTERLY_DUAL_CONFIRM_BLEND_ID,
    SimulationConfig,
    build_monthly_targets,
    simulate_targets,
)


def _load_v10() -> Any:
    path = ROOT / "scripts" / "run-10.0-results-first.py"
    spec = importlib.util.spec_from_file_location("factor_lab_v10_results_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the 10.0 results-first helpers")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


V10 = _load_v10()
RELEASE = "11.0"
ROUTE = QUARTERLY_DUAL_CONFIRM_BLEND_ID
PROTOCOL_ID = "factor-lab/11.0/results-first-dual-confirm-blend-v1"
PROTOCOL_PATH = Path("protocols/11.0-results-first-dual-confirm-blend.json")
PROTOCOL_PAYLOAD = "d23739b85fa02d0cfeca977ba5f60fe003ae5753a387f7b10fa611a6688ae0bf"
PROTOCOL_FILE_SHA256 = "8c6b20996e1e735a020fd71a31b0401570948549a041c5f3848a3dd19ae8fc7c"
RUNNER_PATH = Path("scripts/run-11.0-results-first.py")
CORE_PATH = Path("src/factor_lab/research/multi_asset.py")
V10_RUNNER_PATH = Path("scripts/run-10.0-results-first.py")
SOURCE_ROOT = V10.SOURCE_ROOT
SOURCE_STAGE = V10.SOURCE_STAGE
SOURCE_MANIFEST_PAYLOAD = V10.SOURCE_MANIFEST_PAYLOAD
SOURCE_MANIFEST_FILE_SHA256 = V10.SOURCE_MANIFEST_FILE_SHA256
SOURCE_START = V10.SOURCE_START
SOURCE_END = V10.SOURCE_END
EVIDENCE_PATH = Path("protocols/evidence/11.0/results-first-diagnostic.json")
PERIODS = dict(V10.PERIODS)
BASE_COST_BPS = 8.0
STRESS_COST_BPS = 16.0
MINIMUM_CAGR_MARGIN = 0.005
MINIMUM_POSITIVE_YEAR_RATIO = 0.5
MINIMUM_FILL_RATIO = 0.98
MAXIMUM_CAPACITY_LIMITED_RATIO = 0.02
MAXIMUM_ACCOUNTING_ERROR = 1e-8
ROLES = (
    "candidate", "candidate_stress", "v10", "v10_stress",
    "static", "static_stress", "cash", "cash_stress",
)
SCRATCH_SELECTED_ID = "dual75_borda25"
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")


def _hashes_pending() -> bool:
    return PROTOCOL_PAYLOAD.startswith("PENDING_") or PROTOCOL_FILE_SHA256.startswith("PENDING_")


def _read_protocol() -> dict[str, Any]:
    path = ROOT / PROTOCOL_PATH
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("route") != ROUTE
        or value.get("frozen_strategy", {}).get("strategy_id") != ROUTE
        or value.get("evidence", {}).get("path") != EVIDENCE_PATH.as_posix()
        or value.get("results_first_gate") != _gate_contract()
        or value.get("claim_contract") != _claim_contract()
    ):
        raise ValueError("11.0 protocol identity differs")
    if not _hashes_pending() and (
        value.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD
        or file_sha256(path) != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("11.0 protocol hash differs")
    return value


def _implementation_identity(*, commit_bound: bool) -> dict[str, Any]:
    head = V10._git_head()
    paths = (RUNNER_PATH, CORE_PATH, PROTOCOL_PATH, V10_RUNNER_PATH)
    files = {
        path.as_posix(): {
            "path": path.as_posix(),
            "file_sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest(),
        }
        for path in paths
    }
    if commit_bound:
        if V10._git("branch", "--show-current").decode().strip() != "main":
            raise RuntimeError("formal 11.0 evidence requires main")
        if V10._git("status", "--porcelain").strip():
            raise RuntimeError("formal 11.0 evidence requires a clean worktree")
        for path in paths:
            if (ROOT / path).read_bytes() != V10._git("show", f"HEAD:{path.as_posix()}"):
                raise RuntimeError(f"formal implementation differs from HEAD: {path}")
    return {"git_head": head, "commit_bound": commit_bound, "files": files}


def _prefix_replay_count(stage: Any, targets: pd.DataFrame, sessions: tuple[pd.Timestamp, ...]) -> int:
    count = 0
    for signal_date, expected in targets.groupby("signal_date", sort=True):
        signal = pd.Timestamp(signal_date).normalize()
        execution = pd.Timestamp(expected["execution_date"].iloc[0]).normalize()
        prefix = V10.MultiAssetStage(
            path=stage.path,
            manifest=dict(stage.manifest),
            calendar=stage.calendar.loc[pd.to_datetime(stage.calendar["trade_date"]).dt.normalize().le(execution)].copy(),
            assets={
                code: frame.loc[pd.to_datetime(frame["trade_date"]).dt.normalize().le(signal)].copy()
                for code, frame in stage.assets.items()
            },
        )
        rebuilt = build_monthly_targets(
            prefix.assets,
            tuple(value for value in sessions if value <= execution),
            QUARTERLY_DUAL_CONFIRM_BLEND_ID,
        )
        actual = rebuilt.loc[pd.to_datetime(rebuilt["signal_date"]).dt.normalize().eq(signal)]
        if actual.empty:
            raise ValueError(f"candidate prefix omitted {signal.date()}")
        V10._assert_frame_exact(
            actual.sort_values("code", kind="mergesort"),
            expected.sort_values("code", kind="mergesort"),
            role=f"11.0 prefix target {signal.date()}",
        )
        count += 1
    return count


def _gate(
    candidate: Mapping[str, Any],
    v10: Mapping[str, Any],
    static: Mapping[str, Any],
    cash: Mapping[str, Any],
    *,
    v10_base: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark = max(float(v10["cagr"]), float(static["cagr"]), float(cash["cagr"]))
    values = {
        "cagr_margin_over_best_comparator_at_least": {
            "metric": float(candidate["cagr"]) - benchmark,
            "threshold": MINIMUM_CAGR_MARGIN,
        },
        "positive_complete_year_ratio_at_least": {
            "metric": float(candidate["positive_complete_year_ratio"]),
            "threshold": MINIMUM_POSITIVE_YEAR_RATIO,
        },
        "positive_complete_year_count_at_least_comparators": {
            "metric": float(candidate["positive_complete_year_count"])
            - max(
                float(v10["positive_complete_year_count"]),
                float(static["positive_complete_year_count"]),
            ),
            "threshold": 0.0,
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
        "cagr_margin_over_best_comparator_at_least": (
            values["cagr_margin_over_best_comparator_at_least"]["metric"]
            > MINIMUM_CAGR_MARGIN
            or math.isclose(
                values["cagr_margin_over_best_comparator_at_least"]["metric"],
                MINIMUM_CAGR_MARGIN,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ),
        "positive_complete_year_ratio_at_least": values["positive_complete_year_ratio_at_least"]["metric"] >= MINIMUM_POSITIVE_YEAR_RATIO,
        "positive_complete_year_count_at_least_comparators": values["positive_complete_year_count_at_least_comparators"]["metric"] >= 0.0,
        "requested_notional_fill_ratio_at_least": values["requested_notional_fill_ratio_at_least"]["metric"] >= MINIMUM_FILL_RATIO,
        "capacity_limited_requested_notional_ratio_at_most": values["capacity_limited_requested_notional_ratio_at_most"]["metric"] <= MAXIMUM_CAPACITY_LIMITED_RATIO,
        "nav_reconciliation_error_at_most": values["nav_reconciliation_error_at_most"]["metric"] <= MAXIMUM_ACCOUNTING_ERROR,
        "hard_execution_and_accounting": V10._stored_validity_passes(candidate),
    }
    if v10_base is not None:
        values["stress_cagr_strictly_above_v10_base"] = {
            "metric": float(candidate["cagr"]) - float(v10_base["cagr"]),
            "threshold": 0.0,
        }
        checks["stress_cagr_strictly_above_v10_base"] = values["stress_cagr_strictly_above_v10_base"]["metric"] > 0.0
    return {"passed": all(checks.values()), "checks": checks, "values": values}


def _run_period(stage: Any, name: str, start: str, end: str) -> dict[str, Any]:
    phase = V10._stage_through(stage, end)
    sessions = tuple(pd.to_datetime(phase.calendar["trade_date"], errors="raise").dt.normalize())
    target_ids = {
        "candidate": QUARTERLY_DUAL_CONFIRM_BLEND_ID,
        "v10": QUARTERLY_BORDA_ID,
        "static": CONTROL_ID,
        "cash": CASH_ONLY_ID,
    }
    targets = {
        role: V10._filter_targets(
            build_monthly_targets(phase.assets, sessions, strategy_id),
            start=start,
            end=end,
        )
        for role, strategy_id in target_ids.items()
    }
    prefix_count = _prefix_replay_count(phase, targets["candidate"], sessions)
    specs = {
        "candidate": (targets["candidate"], BASE_COST_BPS),
        "candidate_stress": (targets["candidate"], STRESS_COST_BPS),
        "v10": (targets["v10"], BASE_COST_BPS),
        "v10_stress": (targets["v10"], STRESS_COST_BPS),
        "static": (targets["static"], BASE_COST_BPS),
        "static_stress": (targets["static"], STRESS_COST_BPS),
        "cash": (targets["cash"], BASE_COST_BPS),
        "cash_stress": (targets["cash"], STRESS_COST_BPS),
    }
    results = {
        role: simulate_targets(phase.assets, target, sessions, SimulationConfig(cost_bps_per_side=cost))
        for role, (target, cost) in specs.items()
    }
    for role in ("candidate", "v10", "static", "cash"):
        V10._assert_frame_exact(results[role]["targets"], results[f"{role}_stress"]["targets"], role=f"{name} {role} base/stress targets")
    metrics = {role: V10._metric_view(result, start=start, end=end) for role, result in results.items()}
    all_valid = all(V10._stored_validity_passes(metrics[role]) for role in ROLES)
    base_gate = _gate(metrics["candidate"], metrics["v10"], metrics["static"], metrics["cash"])
    stress_gate = _gate(
        metrics["candidate_stress"],
        metrics["v10_stress"],
        metrics["static_stress"],
        metrics["cash_stress"],
        v10_base=metrics["v10"],
    )
    return {
        "period": name,
        "start_date": start,
        "end_date": end,
        "fresh_cash_and_empty_holdings": True,
        "candidate_prefix_replay_signal_count": prefix_count,
        "candidate_target_prefix_mismatch_count": 0,
        "all_base_stress_targets_exact": True,
        "metrics": metrics,
        "base_gate": base_gate,
        "stress_gate": stress_gate,
        "all_eight_roles_hard_valid": all_valid,
        "passed": bool(prefix_count > 0 and all_valid and base_gate["passed"] and stress_gate["passed"]),
    }


def _gate_contract() -> dict[str, Any]:
    return {
        "applies_to_each": [
            "D1.base_8bp", "D1.stress_16bp", "D2.base_8bp", "D2.stress_16bp",
            "D3.base_8bp", "D3.stress_16bp", "full.base_8bp", "full.stress_16bp",
        ],
        "candidate_cagr_at_least_matching_cash_static_and_published_10_0_plus": MINIMUM_CAGR_MARGIN,
        "stress_cagr_strictly_above_matching_published_10_0_base_cagr": True,
        "positive_complete_year_ratio_at_least": MINIMUM_POSITIVE_YEAR_RATIO,
        "positive_complete_year_count_at_least_matching_10_0_and_static": True,
        "requested_notional_fill_ratio_at_least": MINIMUM_FILL_RATIO,
        "capacity_limited_requested_notional_ratio_at_most": MAXIMUM_CAPACITY_LIMITED_RATIO,
        "capacity_violation_count_at_most": 0,
        "negative_cash_observation_count_at_most": 0,
        "leverage_observation_count_at_most": 0,
        "nav_reconciliation_error_at_most": MAXIMUM_ACCOUNTING_ERROR,
        "base_stress_targets_exact": True,
        "target_prefix_mismatch_count_at_most": 0,
        "sharpe_is_disclosure_only": True,
        "max_drawdown_is_disclosure_only": True,
        "annualized_turnover_is_disclosure_only": True,
        "all_eight_period_cost_roles_must_pass": True,
        "pooled_period_rescue": False,
    }


def _claim_contract() -> dict[str, Any]:
    return {
        "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
        "independent_oos": False,
        "alpha_claim_allowed": False,
        "profit_claim_allowed": False,
        "stable_future_profit_claim_allowed": False,
        "investment_recommendation_allowed": False,
        "fresh_future_evidence_required": True,
        "historical_pass_interpretation": "post-selection fully exposed public-history diagnostic only",
    }


def _scratch_projection(periods: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    projected: dict[str, Any] = {}
    edges: list[float] = []
    prefix_mismatches = 0
    for name in PERIODS:
        period = periods[name]
        metrics = period["metrics"]

        def candidate(role: str) -> dict[str, float]:
            value = metrics[role]
            return {
                "cagr": float(value["cagr"]),
                "sharpe": float(value["sharpe"]),
                "max_drawdown": float(value["max_drawdown"]),
                "annualized_turnover": float(value["annualized_turnover"]),
                "fill_ratio": float(value["requested_notional_fill_ratio"]),
                "capacity_limited_ratio": float(
                    value["capacity_limited_requested_notional_ratio"]
                ),
                "max_abs_accounting_error": float(
                    value["nav_reconciliation_error"]
                ),
            }

        base_comparators = tuple(metrics[role] for role in ("v10", "static", "cash"))
        stress_comparators = tuple(
            metrics[role] for role in ("v10_stress", "static_stress", "cash_stress")
        )
        edges.extend(
            (
                float(metrics["candidate"]["cagr"])
                - max(float(value["cagr"]) for value in base_comparators),
                float(metrics["candidate_stress"]["cagr"])
                - max(float(value["cagr"]) for value in stress_comparators),
            )
        )
        prefix_mismatches += int(period["candidate_target_prefix_mismatch_count"])
        projected[name] = {
            "base": candidate("candidate"),
            "stress": candidate("candidate_stress"),
            "published_10_0_base_cagr": float(metrics["v10"]["cagr"]),
            "published_10_0_stress_cagr": float(metrics["v10_stress"]["cagr"]),
            "static_base_cagr": float(metrics["static"]["cagr"]),
            "static_stress_cagr": float(metrics["static_stress"]["cagr"]),
            "cash_base_cagr": float(metrics["cash"]["cagr"]),
            "cash_stress_cagr": float(metrics["cash_stress"]["cagr"]),
            "passed": bool(period["passed"]),
        }
    return {
        "candidate_id": SCRATCH_SELECTED_ID,
        "minimum_cagr_edge": min(edges),
        "target_prefix_mismatch_count": prefix_mismatches,
        "periods": projected,
        "all_return_and_execution_gates_passed": all(
            bool(periods[name]["passed"]) for name in PERIODS
        ),
    }


def _scratch_replay_summary(
    periods: Mapping[str, Mapping[str, Any]], protocol: Mapping[str, Any]
) -> dict[str, Any]:
    projection = _scratch_projection(periods)
    return {
        "matched": projection == protocol.get("selected_scratch_evidence"),
        "projection_payload_sha256": canonical_payload_sha256(projection),
        "minimum_cagr_edge": projection["minimum_cagr_edge"],
        "target_prefix_mismatch_count": projection[
            "target_prefix_mismatch_count"
        ],
        "all_return_and_execution_gates_passed": projection[
            "all_return_and_execution_gates_passed"
        ],
    }


def _build_evidence(stage: Any, protocol: Mapping[str, Any], implementation: Mapping[str, Any]) -> dict[str, Any]:
    periods = {name: _run_period(stage, name, start, end) for name, (start, end) in PERIODS.items()}
    hard_gates_passed = all(value["passed"] for value in periods.values())
    scratch_replay = _scratch_replay_summary(periods, protocol)
    passed = hard_gates_passed and scratch_replay["matched"]
    status = (
        "formal_exact_replay_mismatch"
        if not scratch_replay["matched"]
        else "candidate_passed_all_results_first_gates"
        if hard_gates_passed
        else "candidate_failed_results_first_gates"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_11_0_results_first_diagnostic",
        "release": RELEASE,
        "route": ROUTE,
        "status": status,
        "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": PROTOCOL_ID,
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
            "payload_sha256": protocol["payload_sha256"],
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
            "strategy_id": ROUTE,
            "selected_after_observing_all_reported_results": True,
            "candidate_definition_was_not_preregistered_before_market_results": True,
            "candidate_base_stress_targets_exact": True,
        },
        "comparators": {
            "published_10_0_strategy_id": QUARTERLY_BORDA_ID,
            "matched_static_strategy_id": CONTROL_ID,
            "investable_cash_strategy_id": CASH_ONLY_ID,
        },
        "gate_contract": _gate_contract(),
        "periods": periods,
        "scratch_replay": scratch_replay,
        "selection": {
            "gate_passed": passed,
            "selected_candidate_id": ROUTE if passed else None,
            "runner_up_fallback": False,
            "parameter_or_candidate_change_after_results_allowed": False,
        },
        "claim_contract": _claim_contract(),
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    return value


def _verify_implementation_binding(implementation: Any) -> None:
    paths = (RUNNER_PATH, CORE_PATH, PROTOCOL_PATH, V10_RUNNER_PATH)
    expected = {path.as_posix() for path in paths}
    if (
        not isinstance(implementation, Mapping)
        or set(implementation) != {"git_head", "commit_bound", "files"}
        or implementation.get("commit_bound") is not True
        or _COMMIT_RE.fullmatch(str(implementation.get("git_head") or "")) is None
        or not isinstance(implementation.get("files"), Mapping)
        or set(implementation["files"]) != expected
    ):
        raise ValueError("11.0 evidence implementation identity differs")
    commit = str(implementation["git_head"])
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("11.0 implementation is not an ancestor of HEAD")
    for path in paths:
        key = path.as_posix()
        binding = implementation["files"][key]
        current = (ROOT / path).read_bytes()
        committed = V10._git("show", f"{commit}:{key}")
        if (
            not isinstance(binding, Mapping)
            or binding.get("path") != key
            or binding.get("file_sha256") != hashlib.sha256(current).hexdigest()
            or binding.get("file_sha256") != hashlib.sha256(committed).hexdigest()
        ):
            raise ValueError(f"11.0 implementation differs: {key}")


def verify_evidence(evidence: Mapping[str, Any], *, verify_data: bool = False) -> None:
    protocol = _read_protocol()
    if evidence.get("payload_sha256") != canonical_payload_sha256(evidence):
        raise ValueError("11.0 evidence payload differs")
    if (
        evidence.get("kind") != "factor_lab_11_0_results_first_diagnostic"
        or evidence.get("release") != RELEASE
        or evidence.get("route") != ROUTE
        or evidence.get("evidence_class") != "fully_exposed_results_first_causal_historical_diagnostic"
        or evidence.get("protocol") != {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": PROTOCOL_ID,
            "file_sha256": PROTOCOL_FILE_SHA256,
            "payload_sha256": PROTOCOL_PAYLOAD,
        }
        or evidence.get("source") != {
            "path": "runtime/data/multi-asset-9.0/sources/stage=audit",
            "manifest_file_sha256": SOURCE_MANIFEST_FILE_SHA256,
            "manifest_payload_sha256": SOURCE_MANIFEST_PAYLOAD,
            "price_start_date": SOURCE_START,
            "price_end_date": SOURCE_END,
        }
        or evidence.get("gate_contract") != _gate_contract()
        or evidence.get("claim_contract") != _claim_contract()
        or set(evidence.get("periods") or {}) != set(PERIODS)
    ):
        raise ValueError("11.0 evidence identity differs")
    _verify_implementation_binding(evidence.get("implementation"))
    for name, period in evidence["periods"].items():
        metrics = period["metrics"]
        base_gate = _gate(metrics["candidate"], metrics["v10"], metrics["static"], metrics["cash"])
        stress_gate = _gate(metrics["candidate_stress"], metrics["v10_stress"], metrics["static_stress"], metrics["cash_stress"], v10_base=metrics["v10"])
        all_valid = all(V10._stored_validity_passes(metrics[role]) for role in ROLES)
        passed = bool(period["candidate_target_prefix_mismatch_count"] == 0 and period["candidate_prefix_replay_signal_count"] > 0 and all_valid and base_gate["passed"] and stress_gate["passed"])
        if period["base_gate"] != base_gate or period["stress_gate"] != stress_gate or period["all_eight_roles_hard_valid"] is not all_valid or period["passed"] is not passed:
            raise ValueError(f"11.0 {name} gate does not replay")
    scratch_replay = _scratch_replay_summary(evidence["periods"], protocol)
    if evidence.get("scratch_replay") != scratch_replay:
        raise ValueError("11.0 formal scratch replay differs")
    hard_gates_passed = all(
        period["passed"] for period in evidence["periods"].values()
    )
    passed = hard_gates_passed and scratch_replay["matched"]
    if evidence.get("selection") != {
        "gate_passed": passed,
        "selected_candidate_id": ROUTE if passed else None,
        "runner_up_fallback": False,
        "parameter_or_candidate_change_after_results_allowed": False,
    }:
        raise ValueError("11.0 evidence selection differs")
    expected_status = (
        "formal_exact_replay_mismatch"
        if not scratch_replay["matched"]
        else "candidate_passed_all_results_first_gates"
        if hard_gates_passed
        else "candidate_failed_results_first_gates"
    )
    if evidence.get("status") != expected_status:
        raise ValueError("11.0 evidence status differs")
    if verify_data:
        stage = V10.load_multi_asset_stage(SOURCE_ROOT, SOURCE_STAGE)
        V10._verify_source(stage)
        expected = _build_evidence(stage, protocol, evidence["implementation"])
        if expected != evidence:
            raise ValueError("11.0 evidence does not replay from retained data")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=EVIDENCE_PATH)
    args = parser.parse_args(argv)
    stage = V10.load_multi_asset_stage(SOURCE_ROOT, SOURCE_STAGE)
    V10._verify_source(stage)
    protocol = _read_protocol()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    formal = output.resolve() == (ROOT / EVIDENCE_PATH).resolve()
    implementation = _implementation_identity(commit_bound=formal)
    evidence = _build_evidence(stage, protocol, implementation)
    if formal:
        verify_evidence(evidence, verify_data=False)
    V10._create_only(args.output, evidence)
    print(f"status={evidence['status']}")
    print(f"payload_sha256={evidence['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
