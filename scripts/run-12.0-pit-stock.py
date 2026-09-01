#!/usr/bin/env python
"""Build the sealed 12.0 development PIT stock panel and targets."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.pit_stock import PITStockRawStore, build_quarterly_panel  # noqa: E402
from factor_lab.release_integrity import file_sha256  # noqa: E402
from factor_lab.research.pit_stock import (  # noqa: E402
    PITStockStrategyConfig,
    STRATEGY_ID,
    canonical_sha256,
    select_quarterly_targets,
)
from factor_lab.research.pit_stock_account import (  # noqa: E402
    PITStockScreeningAccountConfig,
    simulate_screening_accounts,
    summarize_screening_account,
)


RELEASE = "12.0"
STAGE = "development"
FIRST_SIGNAL = "2017-09-29"
LAST_SENTINEL_SIGNAL = "2022-12-30"
LAST_COMPLETE_OUTCOME_SIGNAL = "2022-09-30"
MAXIMUM_READ_DATE = "2023-01-03"
DEFAULT_OUTPUT = ROOT / "runtime" / "data" / "pit-stock-12.0" / STAGE
DEFAULT_SCREENING_OUTPUT = (
    ROOT / "runtime" / "data" / "pit-stock-12.0" / "development-screening"
)
PROTOCOL_PATH = Path("protocols/12.0-quarterly-pit-stock.json")
PROTOCOL_ID = "factor-lab/12.0/quarterly-pit-stock-v1"
PROTOCOL_PAYLOAD_SHA256 = "493e7fa32e93e1f96add0cc7c873c5f1991def48533f82e0fce02eaa4cd9a1e7"
PROTOCOL_FILE_SHA256 = "0ba5356d99befe02dd2c8053c6ef360ade823f1d8ddc65a937095acedcc675ee"
SCOUT_DISCLOSURE = {
    "stage1": {
        "candidate_count": 6,
        "script_sha256": "be36733c1e93740fe1e927caa0dbe9ee4553c94845d067c289198a2bab4c5b8e",
        "result_file_sha256": "2ac04e05b05c77d223cd4694e06a9036d250e456a577cb31df35bcb131553e4d",
        "result_payload_sha256": "7d4b0b6aac052f1fd6c9ca3ee07431ab44250caf1ca0516312b803cc0cb76eab",
    },
    "stage2": {
        "added_candidate_count": 4,
        "script_sha256": "b00fae54ba2de5f37a2e3a938f16df9dbf9fc6d37ba7ae2750a3411c56cca3eb",
        "result_file_sha256": "6087f89af5a1e2ef537cc491d0d40b891f25116f73f4e818a2d964476abd8cd6",
        "result_payload_sha256": "dbc13beb21d6d55ad6f2afc96bea932352dabbba47edceb3f043efa958ddc689",
    },
    "stage3": {
        "added_candidate_count": 3,
        "script_sha256": "fab225c07f1b1f4af672945f0a25f5aaaa2b3e2603d712d9121f0083eb1b441f",
        "result_file_sha256": "128dde3b23e856c9f19f813ae0eb82f6ab2b232021b174ee24bd380e0df8a9f5",
        "result_payload_sha256": "a541ab1f9f4aa1df2bfa83af70f83376daff30392e63b00eb8fd1499b4cfec55",
    },
    "stage4_strict_panel_replay": {
        "candidate_count": 9,
        "new_candidate_count": 0,
        "script_sha256": "d6d23afd6f682b22f1685848f71fe679e44b280237b2ca850975167ad1770b24",
        "result_file_sha256": "c49fefb162557fdc48759966851b24b442bd4c0a11bdde3f5e49473af53bc3ff",
        "result_payload_sha256": "d61021c0d244f21371622b9f4b93c440e009afe83522b26fbc6f355ebd70e3cd",
        "winner_unchanged": True,
    },
}


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, stdout=subprocess.PIPE
    ).stdout.decode("utf-8", errors="strict").strip()


def _read_protocol() -> dict[str, Any]:
    path = ROOT / PROTOCOL_PATH
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("frozen_strategy", {}).get("strategy_id") != STRATEGY_ID
        or value.get("payload_sha256") != PROTOCOL_PAYLOAD_SHA256
        or canonical_sha256(
            {key: item for key, item in value.items() if key != "payload_sha256"}
        )
        != PROTOCOL_PAYLOAD_SHA256
        or file_sha256(path) != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("12.0 protocol identity/hash differs")
    return value


def _implementation_identity(*, formal: bool) -> dict[str, Any]:
    head = _git("rev-parse", "HEAD")
    branch = _git("branch", "--show-current")
    dirty = bool(_git("status", "--porcelain"))
    if formal:
        if branch != "main" or dirty:
            raise RuntimeError("formal 12.0 run requires clean main")
        remote = _git("rev-parse", "origin/main")
        live_rows = _git("ls-remote", "--refs", "origin", "refs/heads/main").splitlines()
        live = live_rows[0].split()[0] if len(live_rows) == 1 else ""
        if remote != head or live != head:
            raise RuntimeError(
                "formal 12.0 run requires HEAD == origin/main == live remote main"
            )
    return {
        "git_head": head,
        "branch": branch,
        "git_dirty": dirty,
        "commit_bound": bool(formal),
    }


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
            + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            if os.name != "nt":
                raise
    finally:
        os.close(descriptor)


def _target_identity(frame: pd.DataFrame) -> str:
    value = frame[
        ["strategy_id", "signal_date", "ticker", "target_weight"]
    ].copy()
    value["signal_date"] = pd.to_datetime(value["signal_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    value = value.sort_values(["signal_date", "ticker"], kind="mergesort")
    return canonical_sha256(value.to_dict("records"))


def verify_output(output: Path, *, require_formal: bool = False) -> dict[str, Any]:
    output = output.resolve()
    manifest_path = output / "manifest.json"
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    claimed_payload = value.get("payload_sha256")
    unsigned = dict(value)
    unsigned.pop("payload_sha256", None)
    if claimed_payload != canonical_sha256(unsigned):
        raise ValueError("development manifest payload hash differs")
    for role, name in (
        ("panel", "quarterly-snapshots.parquet"),
        ("targets", "targets.parquet"),
        ("decisions", "decisions.json"),
        ("source_allowlist", "source-allowlist.json"),
    ):
        path = output / name
        if not path.is_file() or file_sha256(path) != value[role]["file_sha256"]:
            raise ValueError(f"development {role} file hash differs")
    allowlist = json.loads(
        (output / "source-allowlist.json").read_text(encoding="utf-8")
    )
    unsigned_allowlist = dict(allowlist)
    allowlist_payload = unsigned_allowlist.pop("payload_sha256", None)
    if (
        allowlist_payload != canonical_sha256(unsigned_allowlist)
        or allowlist_payload != value["source_allowlist"]["payload_sha256"]
    ):
        raise ValueError("development source allowlist payload differs")
    panel = pd.read_parquet(output / "quarterly-snapshots.parquet")
    targets = pd.read_parquet(output / "targets.parquet")
    decisions = json.loads((output / "decisions.json").read_text(encoding="utf-8"))
    if len(panel) != value["panel"]["row_count"] or len(targets) != value["targets"]["row_count"]:
        raise ValueError("development Parquet row count differs")
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    if not panel.groupby("signal_date")["universe_member"].sum().eq(1000).all():
        raise ValueError("development panel does not have exactly 1000 members per signal")
    targets["signal_date"] = pd.to_datetime(targets["signal_date"]).dt.normalize()
    by_signal = {
        str(signal.date()): group.copy()
        for signal, group in targets.groupby("signal_date", sort=True)
    }
    if len(decisions) != value["decisions"]["row_count"]:
        raise ValueError("development decision count differs")
    for decision in decisions:
        signal = str(decision["signal_date"])
        group = by_signal.get(signal, targets.iloc[0:0])
        payload = [
            {"ticker": str(row.ticker), "target_weight": float(row.target_weight)}
            for row in group.sort_values("ticker", kind="mergesort").itertuples()
        ]
        if (
            len(group) != int(decision["selected_count"])
            or canonical_sha256(payload) != decision["target_sha256"]
            or abs(float(group["target_weight"].sum()) - float(decision["target_weight_sum"])) > 1e-12
        ):
            raise ValueError(f"development target/decision mismatch for {signal}")
    if _target_identity(targets) != value["targets"]["payload_sha256"]:
        raise ValueError("development target payload differs")
    if canonical_sha256(decisions) != value["decisions"]["payload_sha256"]:
        raise ValueError("development decision payload differs")
    implementation = value.get("implementation") or {}
    if require_formal and (
        implementation.get("commit_bound") is not True
        or implementation.get("git_dirty") is not False
        or implementation.get("branch") != "main"
        or implementation.get("git_head") != _git("rev-parse", "HEAD")
    ):
        raise ValueError("development artifact is not formal/current-commit bound")
    return value


def _screening_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    base = metrics["candidate_base"]
    stress = metrics["candidate_stress"]
    benchmark = metrics["adv500_base"]
    checks = {
        "base_full_cagr_strictly_positive": base["cagr"] > 0.0,
        "base_train_cagr_strictly_positive": base["train_quarterly"]["cagr"] > 0.0,
        "base_validation_cagr_strictly_positive": base["validation_quarterly"]["cagr"] > 0.0,
        "stress_full_cagr_strictly_positive": stress["cagr"] > 0.0,
        "base_full_sharpe_at_least_0_4": base["sharpe"] >= 0.4,
        "stress_full_sharpe_at_least_0_35": stress["sharpe"] >= 0.35,
        "base_max_drawdown_at_least_negative_0_35": base["max_drawdown"] >= -0.35,
        "base_train_cagr_above_adv500": (
            base["train_quarterly"]["cagr"]
            > benchmark["train_quarterly"]["cagr"]
        ),
        "base_validation_cagr_above_adv500": (
            base["validation_quarterly"]["cagr"]
            > benchmark["validation_quarterly"]["cagr"]
        ),
        "base_fill_at_least_0_98": base["requested_notional_fill_ratio"] >= 0.98,
        "base_capacity_limited_at_most_0_02": (
            base["capacity_limited_requested_notional_ratio"] <= 0.02
        ),
        "base_no_capacity_negative_cash_or_leverage_violation": (
            base["capacity_violation_count"] == 0
            and base["negative_cash_observation_count"] == 0
            and base["leverage_observation_count"] == 0
        ),
        "base_account_reconciliation_at_most_1e_8": (
            base["max_nav_reconciliation_error"] <= 1e-8
        ),
    }
    pre_attribution = all(checks.values())
    return {
        "checks": checks,
        "pre_attribution_passed": pre_attribution,
        "group_pnl_reconciliation_pending": True,
        "real_share_100_lot_gate_pending": True,
        "winner_freeze_allowed": False,
    }


def verify_screening_output(
    output: Path, *, require_formal: bool = False
) -> dict[str, Any]:
    output = output.resolve()
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    unsigned = dict(manifest)
    claimed = unsigned.pop("payload_sha256", None)
    if claimed != canonical_sha256(unsigned):
        raise ValueError("screening manifest payload hash differs")
    for role, name in (
        ("daily_nav", "daily-nav.parquet"),
        ("boundaries", "boundaries.parquet"),
        ("orders", "orders.parquet"),
        ("holdings", "holdings.parquet"),
        ("result", "result.json"),
    ):
        path = output / name
        if not path.is_file() or file_sha256(path) != manifest["artifacts"][role]["file_sha256"]:
            raise ValueError(f"screening {role} hash differs")
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    if canonical_sha256({key: value for key, value in result.items() if key != "payload_sha256"}) != result["payload_sha256"]:
        raise ValueError("screening result payload hash differs")
    if (
        manifest.get("result_payload_sha256") != result["payload_sha256"]
        or manifest.get("status") != result.get("status")
    ):
        raise ValueError("screening manifest/result binding differs")
    daily = pd.read_parquet(output / "daily-nav.parquet")
    boundaries = pd.read_parquet(output / "boundaries.parquet")
    orders = pd.read_parquet(output / "orders.parquet")
    holdings = pd.read_parquet(output / "holdings.parquet")
    if len(daily) != manifest["artifacts"]["daily_nav"]["row_count"]:
        raise ValueError("screening daily NAV row count differs")
    if len(boundaries) != manifest["artifacts"]["boundaries"]["row_count"]:
        raise ValueError("screening boundary row count differs")
    if len(orders) != manifest["artifacts"]["orders"]["row_count"]:
        raise ValueError("screening order row count differs")
    if len(holdings) != manifest["artifacts"]["holdings"]["row_count"]:
        raise ValueError("screening holding row count differs")
    if set(daily["role"]) != {
        "candidate_base",
        "candidate_stress",
        "adv500_base",
        "adv500_stress",
    }:
        raise ValueError("screening roles differ")
    candidate_hashes = boundaries.loc[
        boundaries["role"].isin(["candidate_base", "candidate_stress"])
    ].pivot(index="execution_date", columns="role", values="target_sha256")
    benchmark_hashes = boundaries.loc[
        boundaries["role"].isin(["adv500_base", "adv500_stress"])
    ].pivot(index="execution_date", columns="role", values="target_sha256")
    if not candidate_hashes["candidate_base"].eq(candidate_hashes["candidate_stress"]).all():
        raise ValueError("candidate base/stress targets differ")
    if not benchmark_hashes["adv500_base"].eq(benchmark_hashes["adv500_stress"]).all():
        raise ValueError("ADV500 base/stress targets differ")
    initial = float(result["source_receipt"]["config"]["initial_capital_rmb"])
    recalculated = {
        role: summarize_screening_account(
            daily.loc[daily["role"].eq(role)].reset_index(drop=True),
            boundaries.loc[boundaries["role"].eq(role)].reset_index(drop=True),
            orders.loc[orders["role"].eq(role)].reset_index(drop=True),
            initial_capital=initial,
        )
        for role in sorted(set(daily["role"]))
    }
    if recalculated != result["metrics"]:
        raise ValueError("screening persisted metrics do not exact-replay")
    if _screening_gate(recalculated) != result["gate"]:
        raise ValueError("screening persisted gate does not exact-replay")
    development_root = Path(manifest["development_input"]["path"])
    development_manifest = verify_output(
        development_root, require_formal=require_formal
    )
    if (
        manifest["development_input"]["manifest_payload_sha256"]
        != development_manifest["payload_sha256"]
        or manifest["development_input"]["manifest_file_sha256"]
        != file_sha256(development_root / "manifest.json")
        or result["source_receipt"]["market"]["market_partition_contract_sha256"]
        != development_manifest["source_receipt"][
            "market_partition_contract_sha256"
        ]
        or result["source_receipt"]["market"]["security_master"][
            "snapshot_sha256"
        ]
        != development_manifest["source_receipt"]["security_master"][
            "snapshot_sha256"
        ]
    ):
        raise ValueError("screening source/development binding differs")
    implementation = manifest.get("implementation") or {}
    if require_formal and (
        implementation.get("commit_bound") is not True
        or implementation.get("git_dirty") is not False
        or implementation.get("branch") != "main"
        or implementation.get("git_head") != _git("rev-parse", "HEAD")
    ):
        raise ValueError("screening artifact is not formal/current-commit bound")
    return manifest


def build(output: Path, *, formal: bool = False) -> dict[str, Any]:
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"create-only development output already exists: {output}")
    protocol = _read_protocol()
    config = PITStockStrategyConfig()
    store = PITStockRawStore(ROOT, maximum_read_date=MAXIMUM_READ_DATE)
    built = build_quarterly_panel(
        store,
        first_signal=FIRST_SIGNAL,
        last_signal=LAST_SENTINEL_SIGNAL,
        config=config,
    )
    decisions = []
    targets = []
    for signal_date, snapshot in built.panel.groupby("signal_date", sort=True):
        selected, decision = select_quarterly_targets(snapshot, config)
        decisions.append(decision.to_dict())
        targets.append(selected)
        print(
            f"target {pd.Timestamp(signal_date).date()} gate={decision.market_gate_open} "
            f"selected={decision.selected_count}",
            flush=True,
        )
    target_frame = pd.concat(targets, ignore_index=True) if targets else pd.DataFrame()
    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    try:
        panel_path = transaction / "quarterly-snapshots.parquet"
        target_path = transaction / "targets.parquet"
        decisions_path = transaction / "decisions.json"
        allowlist_path = transaction / "source-allowlist.json"
        manifest_path = transaction / "manifest.json"
        built.panel.to_parquet(panel_path, index=False)
        target_frame.to_parquet(target_path, index=False)
        _fsync_file(panel_path)
        _fsync_file(target_path)
        _write_json(decisions_path, decisions)
        source_allowlist = store.source_allowlist()
        _write_json(allowlist_path, source_allowlist)
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_12_0_pit_stock_development_manifest",
            "release": RELEASE,
            "stage": STAGE,
            "status": "development_panel_and_targets_built_holdout_sealed",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "strategy_id": STRATEGY_ID,
            "protocol": {
                "path": PROTOCOL_PATH.as_posix(),
                "protocol_id": PROTOCOL_ID,
                "payload_sha256": protocol["payload_sha256"],
                "file_sha256": PROTOCOL_FILE_SHA256,
            },
            "config": asdict(config),
            "phase": {
                "first_signal": FIRST_SIGNAL,
                "last_sentinel_signal": LAST_SENTINEL_SIGNAL,
                "last_complete_outcome_signal": LAST_COMPLETE_OUTCOME_SIGNAL,
                "maximum_read_date": MAXIMUM_READ_DATE,
                "market_partition_after_cutoff_read": False,
                "raw_root_contains_later_partitions": True,
                "isolation": "checkpointed logical allowlist, not a physical copy",
            },
            "source_receipt": built.source_receipt,
            "source_allowlist": {
                "path": allowlist_path.name,
                "payload_sha256": source_allowlist["payload_sha256"],
                "file_sha256": file_sha256(allowlist_path),
            },
            "panel": {
                "path": panel_path.name,
                "row_count": int(len(built.panel)),
                "signal_count": len(built.signal_dates),
                "signal_dates": list(built.signal_dates),
                "payload_sha256": built.panel_payload_sha256,
                "file_sha256": file_sha256(panel_path),
            },
            "targets": {
                "path": target_path.name,
                "row_count": int(len(target_frame)),
                "signal_count": int(target_frame["signal_date"].nunique()),
                "payload_sha256": _target_identity(target_frame),
                "file_sha256": file_sha256(target_path),
            },
            "decisions": {
                "path": decisions_path.name,
                "row_count": len(decisions),
                "payload_sha256": canonical_sha256(decisions),
                "file_sha256": file_sha256(decisions_path),
            },
            "scout_disclosure": SCOUT_DISCLOSURE,
            "implementation": _implementation_identity(formal=formal),
            "claim_contract": {
                "evidence_class": "fully_exposed_causal_development_build",
                "return_evaluation_included": False,
                "independent_oos": False,
                "profit_claim_allowed": False,
                "selection_or_holdout_rows_read_by_this_run": False,
            },
        }
        manifest["payload_sha256"] = canonical_sha256(manifest)
        _write_json(manifest_path, manifest)
        _fsync_directory(transaction)
        os.replace(transaction, output)
        _fsync_directory(output.parent)
    except BaseException:
        if transaction.exists():
            for path in sorted(transaction.glob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
            transaction.rmdir()
        raise
    return verify_output(output, require_formal=formal)


def screening(output: Path, *, formal: bool = False) -> dict[str, Any]:
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"create-only screening output already exists: {output}")
    protocol = _read_protocol()
    implementation = _implementation_identity(formal=formal)
    development_root = DEFAULT_OUTPUT.resolve()
    development_manifest = verify_output(
        development_root, require_formal=formal
    )
    if formal and (
        development_manifest.get("implementation", {}).get("commit_bound") is not True
        or development_manifest["implementation"].get("git_head")
        != implementation["git_head"]
    ):
        raise RuntimeError("formal screening requires a same-commit formal panel")
    panel = pd.read_parquet(development_root / "quarterly-snapshots.parquet")
    targets = pd.read_parquet(development_root / "targets.parquet")
    account_config = PITStockScreeningAccountConfig()
    store = PITStockRawStore(ROOT, maximum_read_date=MAXIMUM_READ_DATE)
    built = simulate_screening_accounts(
        store, panel, targets, config=account_config
    )
    gate = _screening_gate(built.metrics)
    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    try:
        paths = {
            "daily_nav": transaction / "daily-nav.parquet",
            "boundaries": transaction / "boundaries.parquet",
            "orders": transaction / "orders.parquet",
            "holdings": transaction / "holdings.parquet",
            "result": transaction / "result.json",
        }
        built.daily_nav.to_parquet(paths["daily_nav"], index=False)
        built.boundaries.to_parquet(paths["boundaries"], index=False)
        built.orders.to_parquet(paths["orders"], index=False)
        built.holdings.to_parquet(paths["holdings"], index=False)
        for role in ("daily_nav", "boundaries", "orders", "holdings"):
            _fsync_file(paths[role])
        result: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_12_0_development_screening_result",
            "release": RELEASE,
            "status": (
                "pre_attribution_screening_passed"
                if gate["pre_attribution_passed"]
                else "screening_failed"
            ),
            "price_basis": "adjusted_total_return_synthetic_units",
            "real_100_share_claim_allowed": False,
            "metrics": built.metrics,
            "gate": gate,
            "target_payloads": built.target_payloads,
            "source_receipt": built.source_receipt,
        }
        result["payload_sha256"] = canonical_sha256(result)
        _write_json(paths["result"], result)
        artifacts = {
            role: {
                "path": path.name,
                "row_count": (
                    len(getattr(built, role))
                    if role != "result"
                    else 1
                ),
                "file_sha256": file_sha256(path),
            }
            for role, path in paths.items()
        }
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_12_0_development_screening_manifest",
            "release": RELEASE,
            "status": result["status"],
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "development_input": {
                "path": str(development_root),
                "manifest_payload_sha256": development_manifest["payload_sha256"],
                "manifest_file_sha256": file_sha256(
                    development_root / "manifest.json"
                ),
            },
            "protocol": {
                "path": PROTOCOL_PATH.as_posix(),
                "protocol_id": PROTOCOL_ID,
                "payload_sha256": protocol["payload_sha256"],
                "file_sha256": PROTOCOL_FILE_SHA256,
            },
            "artifacts": artifacts,
            "result_payload_sha256": result["payload_sha256"],
            "implementation": implementation,
            "winner_freeze_allowed": False,
        }
        manifest["payload_sha256"] = canonical_sha256(manifest)
        _write_json(transaction / "manifest.json", manifest)
        _fsync_directory(transaction)
        os.replace(transaction, output)
        _fsync_directory(output.parent)
    except BaseException:
        if transaction.exists():
            for path in sorted(transaction.glob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
            transaction.rmdir()
        raise
    return verify_screening_output(output, require_formal=formal)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=("panel", "screening"), default="panel"
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--formal", action="store_true")
    args = parser.parse_args(argv)
    output = args.output or (
        DEFAULT_OUTPUT if args.mode == "panel" else DEFAULT_SCREENING_OUTPUT
    )
    result = (
        build(output, formal=args.formal)
        if args.mode == "panel"
        else screening(output, formal=args.formal)
    )
    print(
        json.dumps(
            {
                "output": str(output.resolve()),
                "payload_sha256": result["payload_sha256"],
                "status": result["status"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
