#!/usr/bin/env python
"""Run the candidate-only 13.0 sequential-minute development stage."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
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

from factor_lab.data.diemeng_minute_store import CandidateMinuteStore  # noqa: E402
from factor_lab.data.pit_stock import PITStockRawStore  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)
from factor_lab.research.pit_stock_real_simulation import (  # noqa: E402
    simulate_candidate_real_share_accounts,
)


RELEASE = "13.0"
PROTOCOL_PATH = ROOT / "protocols" / "13.0-profit-first-real-share-closure.json"
PROTOCOL_PAYLOAD_SHA256 = (
    "7989478d24cc4597a7066eab5a737e698d3647d5a70704f85854a744ec3fa9d8"
)
PROTOCOL_FILE_SHA256 = (
    "9973cbdeb0e2d421e227afe7d22ffa9074e0f847041a8025b294cdb5f642fef2"
)
PANEL_PATH = (
    ROOT
    / "runtime"
    / "data"
    / "pit-stock-12.0"
    / "development"
    / "quarterly-snapshots.parquet"
)
TARGETS_PATH = PANEL_PATH.with_name("targets.parquet")
PANEL_FILE_SHA256 = (
    "d51cad0de60484292ca24e4909d2c7617e0d83d3b8df58b29358f438eb5a48ca"
)
TARGETS_FILE_SHA256 = (
    "ce3cd6c37dd1b04e77c170055e1bb7351cd99b6e2a201e7745ac1cf29a9ea06c"
)
SUSPENSIONS_PATH = ROOT / "runtime" / "data" / "top500" / "suspensions.parquet"
SUSPENSIONS_META_PATH = SUSPENSIONS_PATH.with_name("suspensions.meta.json")
SUSPENSIONS_FILE_SHA256 = (
    "5fc3c971b1a0376f5da3243a15a0bf4817f11f0339ef825daae298715b44689d"
)
SUSPENSIONS_ROW_COUNT = 170_703
DEFAULT_OUTPUT = Path(
    r"H:\Download\FactorLabPytest\factor-lab-13.0-minute-stage1-result"
)
IMPLEMENTATION_FILES = (
    Path("src/factor_lab/data/corporate_actions.py"),
    Path("src/factor_lab/data/diemeng_minute_store.py"),
    Path("src/factor_lab/data/diemeng_minutes.py"),
    Path("src/factor_lab/data/pit_stock.py"),
    Path("src/factor_lab/portfolio/execution.py"),
    Path("src/factor_lab/release_integrity.py"),
    Path("src/factor_lab/research/pit_stock.py"),
    Path("src/factor_lab/research/pit_stock_account.py"),
    Path("src/factor_lab/research/pit_stock_minute_execution.py"),
    Path("src/factor_lab/research/pit_stock_minute_scope.py"),
    Path("src/factor_lab/research/pit_stock_minute_stages.py"),
    Path("src/factor_lab/research/pit_stock_real_account.py"),
    Path("src/factor_lab/research/pit_stock_real_simulation.py"),
    Path("scripts/capture-13.0-candidate-minutes.py"),
    Path("scripts/run-13.0-minute-stage1.py"),
    Path("scripts/run-13.0-profit-first.py"),
)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.decode("utf-8", errors="strict").strip()


def _json_ready(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            default=str,
        )
    )


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(
            _json_ready(value),
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _read_protocol() -> dict[str, Any]:
    value = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
    if (
        value.get("payload_sha256") != PROTOCOL_PAYLOAD_SHA256
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD_SHA256
        or file_sha256(PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("13.0 protocol binding differs")
    return value


def _actions_module():
    path = ROOT / "scripts" / "run-13.0-profit-first.py"
    spec = importlib.util.spec_from_file_location("factor_lab_13_actions", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("13.0 action runner cannot be imported")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _minutes_module():
    path = ROOT / "scripts" / "capture-13.0-candidate-minutes.py"
    spec = importlib.util.spec_from_file_location("factor_lab_13_minutes", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("13.0 minute capture runner cannot be imported")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_actions(
    root: Path,
    *,
    expected_payload_sha256: str,
    expected_file_sha256: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    root = root.resolve()
    manifest_path = root / "manifest.json"
    if (
        len(expected_payload_sha256) != 64
        or len(expected_file_sha256) != 64
        or not manifest_path.is_file()
        or file_sha256(manifest_path) != expected_file_sha256
    ):
        raise ValueError("candidate action external manifest anchor differs")
    manifest = _actions_module().verify_capture(root)
    if (
        manifest.get("payload_sha256") != expected_payload_sha256
        or manifest.get("scope", {}).get("name") != "candidate"
        or manifest.get("scope", {}).get("ticker_count") != 501
        or manifest.get("protocol", {}).get("payload_sha256")
        != PROTOCOL_PAYLOAD_SHA256
        or manifest.get("protocol", {}).get("file_sha256")
        != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("candidate action manifest identity differs")
    return (
        pd.read_parquet(root / "resolved-actions.parquet"),
        pd.read_parquet(root / "tushare-reference-diagnostics.parquet"),
        manifest,
    )


def _load_suspensions() -> pd.DataFrame:
    if (
        not SUSPENSIONS_PATH.is_file()
        or not SUSPENSIONS_META_PATH.is_file()
        or file_sha256(SUSPENSIONS_PATH) != SUSPENSIONS_FILE_SHA256
    ):
        raise ValueError("13.0 suspension artifact differs")
    meta = json.loads(SUSPENSIONS_META_PATH.read_text(encoding="utf-8"))
    if (
        meta.get("status") != "complete"
        or meta.get("endpoint") != "suspend_d"
        or meta.get("rows") != SUSPENSIONS_ROW_COUNT
        or meta.get("file", {}).get("sha256") != SUSPENSIONS_FILE_SHA256
    ):
        raise ValueError("13.0 suspension metadata differs")
    frame = pd.read_parquet(SUSPENSIONS_PATH)
    expected = ("ticker", "date", "suspend_type", "suspend_timing")
    if tuple(map(str, frame.columns)) != expected or len(frame) != SUSPENSIONS_ROW_COUNT:
        raise ValueError("13.0 suspension frame differs")
    return frame


def _artifact(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "path": path.name,
        "file_sha256": file_sha256(path),
        "size_bytes": path.stat().st_size,
        "row_count": len(frame),
    }


def _implementation_files() -> dict[str, str]:
    result = {
        path.as_posix(): file_sha256(ROOT / path)
        for path in IMPLEMENTATION_FILES
    }
    if len(result) != len(IMPLEMENTATION_FILES) or any(
        len(value) != 64 for value in result.values()
    ):
        raise ValueError("stage-1 implementation file binding differs")
    return result


def run_stage1(
    output: Path,
    *,
    minute_root: Path,
    minute_manifest_payload_sha256: str,
    minute_manifest_file_sha256: str,
    actions_root: Path,
    actions_manifest_payload_sha256: str,
    actions_manifest_file_sha256: str,
) -> dict[str, Any]:
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"create-only stage-1 output exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    protocol = _read_protocol()
    if file_sha256(PANEL_PATH) != PANEL_FILE_SHA256:
        raise ValueError("12.0 development panel bytes differ")
    if file_sha256(TARGETS_PATH) != TARGETS_FILE_SHA256:
        raise ValueError("12.0 development target bytes differ")
    if (
        len(minute_manifest_payload_sha256) != 64
        or len(minute_manifest_file_sha256) != 64
    ):
        raise ValueError("minute external manifest anchors are required")
    minute_manifest_path = Path(minute_root).resolve() / "manifest.json"
    if (
        not minute_manifest_path.is_file()
        or file_sha256(minute_manifest_path) != minute_manifest_file_sha256
    ):
        raise ValueError("minute external manifest file anchor differs")
    minute_manifest = _minutes_module().verify_capture(Path(minute_root))
    if (
        minute_manifest.get("payload_sha256")
        != minute_manifest_payload_sha256
        or minute_manifest.get("protocol", {}).get("payload_sha256")
        != PROTOCOL_PAYLOAD_SHA256
        or minute_manifest.get("protocol", {}).get("file_sha256")
        != PROTOCOL_FILE_SHA256
        or minute_manifest.get("pair_count") != 4_729
    ):
        raise ValueError("formal candidate minute bundle differs")
    minutes = CandidateMinuteStore(
        minute_root,
        expected_manifest_payload_sha256=minute_manifest_payload_sha256,
        expected_manifest_file_sha256=minute_manifest_file_sha256,
    )
    actions, action_diagnostics, action_manifest = _load_actions(
        actions_root,
        expected_payload_sha256=actions_manifest_payload_sha256,
        expected_file_sha256=actions_manifest_file_sha256,
    )
    panel = pd.read_parquet(PANEL_PATH)
    targets = pd.read_parquet(TARGETS_PATH)
    suspensions = _load_suspensions()
    raw_store = PITStockRawStore(ROOT, maximum_read_date="2023-01-03")
    raw_source_receipt = raw_store.source_receipt()
    raw_source_allowlist = raw_store.source_allowlist()
    implementation_files = _implementation_files()
    built = simulate_candidate_real_share_accounts(
        store=raw_store,
        panel=panel,
        stored_targets=targets,
        resolved_actions=actions,
        suspension_events=suspensions,
        include_benchmark=False,
        tushare_reference_diagnostics=action_diagnostics,
        execution_mode="sequential_minute",
        minute_market_provider=minutes,
    )
    gate = built.metrics["phase_gate"]
    if gate.get("complete") is not False:
        raise ValueError("stage-1 gate incorrectly claims all-role completion")
    stage_1_passed = gate.get("stage_1_passed") is True
    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    frames = {
        "daily_nav": built.daily_nav,
        "boundaries": built.boundaries,
        "orders": built.orders,
        "postings": built.postings,
        "periods": built.periods,
        "period_ticker_pnl": built.period_ticker_pnl,
        "group_pnl": built.group_pnl,
    }
    try:
        artifacts: dict[str, Any] = {}
        for role, frame in frames.items():
            path = transaction / f"{role.replace('_', '-')}.parquet"
            frame.to_parquet(path, index=False)
            with path.open("r+b") as handle:
                os.fsync(handle.fileno())
            artifacts[role] = _artifact(path, frame)
        result: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_minute_stage1_result",
            "release": RELEASE,
            "status": (
                "stage_1_passed_stage_2_implementation_not_frozen"
                if stage_1_passed
                else "stage_1_failed_stage_2_forbidden"
            ),
            "stage": "stage_1_candidate_only",
            "target_payload_sha256": built.target_payload_sha256,
            "metrics": _json_ready(built.metrics),
            "stage_1_passed": stage_1_passed,
            "stage_2_gate_satisfied": stage_1_passed,
            "stage_2_permitted": False,
            "stage_2_block_reason": (
                "formal_stage_2_runner_not_yet_frozen"
                if stage_1_passed
                else "stage_1_gate_failed"
            ),
            "stage_2_dispatched": False,
            "implementation_file_sha256": implementation_files,
            "claim_contract": {
                "selection_opened": False,
                "profit_or_stable_future_claim_allowed": False,
                "real_capital_enablement_allowed": False,
            },
        }
        result["payload_sha256"] = canonical_payload_sha256(result)
        result_path = transaction / "result.json"
        _write_json(result_path, result)
        artifacts["result"] = {
            "path": result_path.name,
            "file_sha256": file_sha256(result_path),
            "size_bytes": result_path.stat().st_size,
        }
        source_receipt_path = transaction / "raw-source-receipt.json"
        source_allowlist_path = transaction / "raw-source-allowlist.json"
        _write_json(source_receipt_path, raw_source_receipt)
        _write_json(source_allowlist_path, raw_source_allowlist)
        artifacts["raw_source_receipt"] = {
            "path": source_receipt_path.name,
            "file_sha256": file_sha256(source_receipt_path),
            "size_bytes": source_receipt_path.stat().st_size,
            "payload_sha256": raw_source_receipt.get("payload_sha256"),
        }
        artifacts["raw_source_allowlist"] = {
            "path": source_allowlist_path.name,
            "file_sha256": file_sha256(source_allowlist_path),
            "size_bytes": source_allowlist_path.stat().st_size,
            "payload_sha256": raw_source_allowlist.get("payload_sha256"),
        }
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_minute_stage1_manifest",
            "release": RELEASE,
            "status": result["status"],
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "protocol": {
                "path": "protocols/13.0-profit-first-real-share-closure.json",
                "payload_sha256": protocol["payload_sha256"],
                "file_sha256": PROTOCOL_FILE_SHA256,
            },
            "minute_input": {
                "path": str(Path(minute_root).resolve()),
                "manifest_payload_sha256": minute_manifest_payload_sha256,
                "manifest_file_sha256": minute_manifest_file_sha256,
            },
            "action_input": {
                "path": str(Path(actions_root).resolve()),
                "manifest_payload_sha256": action_manifest["payload_sha256"],
                "manifest_file_sha256": actions_manifest_file_sha256,
            },
            "suspension_input": {
                "path": str(SUSPENSIONS_PATH.resolve()),
                "file_sha256": SUSPENSIONS_FILE_SHA256,
                "row_count": SUSPENSIONS_ROW_COUNT,
            },
            "artifacts": artifacts,
            "implementation": {
                "git_head": _git("rev-parse", "HEAD"),
                "branch": _git("branch", "--show-current"),
                "git_dirty": bool(_git("status", "--porcelain")),
                "commit_bound": False,
                "file_sha256": implementation_files,
            },
            "stage_2_dispatch_rule": (
                "forbidden_until_a_formal_stage_2_runner_is_added_to_the_implementation_freeze_and_stage_1_is_replayed_under_those_exact_hashes"
            ),
        }
        manifest["payload_sha256"] = canonical_payload_sha256(manifest)
        _write_json(transaction / "manifest.json", manifest)
        os.replace(transaction, output)
    except BaseException:
        if transaction.exists():
            shutil.rmtree(transaction)
        raise
    return verify_stage1(
        output,
        expected_manifest_payload_sha256=manifest["payload_sha256"],
        expected_manifest_file_sha256=file_sha256(output / "manifest.json"),
    )


def verify_stage1(
    output: Path,
    *,
    expected_manifest_payload_sha256: str,
    expected_manifest_file_sha256: str,
) -> dict[str, Any]:
    root = Path(output).resolve()
    path = root / "manifest.json"
    if (
        not path.is_file()
        or file_sha256(path) != expected_manifest_file_sha256
    ):
        raise ValueError("stage-1 external manifest file anchor differs")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if (
        manifest.get("payload_sha256") != expected_manifest_payload_sha256
        or canonical_payload_sha256(manifest) != expected_manifest_payload_sha256
        or manifest.get("protocol", {}).get("payload_sha256")
        != PROTOCOL_PAYLOAD_SHA256
        or manifest.get("implementation", {}).get("file_sha256")
        != _implementation_files()
    ):
        raise ValueError("stage-1 manifest identity differs")
    expected_artifact_roles = {
        "daily_nav",
        "boundaries",
        "orders",
        "postings",
        "periods",
        "period_ticker_pnl",
        "group_pnl",
        "result",
        "raw_source_receipt",
        "raw_source_allowlist",
    }
    if (
        set(manifest.get("artifacts", {})) != expected_artifact_roles
        or manifest.get("suspension_input")
        != {
            "path": str(SUSPENSIONS_PATH.resolve()),
            "file_sha256": SUSPENSIONS_FILE_SHA256,
            "row_count": SUSPENSIONS_ROW_COUNT,
        }
        or manifest.get("stage_2_dispatch_rule")
        != "forbidden_until_a_formal_stage_2_runner_is_added_to_the_implementation_freeze_and_stage_1_is_replayed_under_those_exact_hashes"
    ):
        raise ValueError("stage-1 manifest contract differs")
    minute_input = manifest.get("minute_input") or {}
    minute_path = Path(str(minute_input.get("path"))).resolve()
    minute_manifest_path = minute_path / "manifest.json"
    if (
        not minute_manifest_path.is_file()
        or file_sha256(minute_manifest_path)
        != minute_input.get("manifest_file_sha256")
    ):
        raise ValueError("stage-1 minute provenance differs")
    minute_manifest = _minutes_module().verify_capture(minute_path)
    if minute_manifest.get("payload_sha256") != minute_input.get(
        "manifest_payload_sha256"
    ):
        raise ValueError("stage-1 minute provenance differs")
    action_input = manifest.get("action_input") or {}
    action_path = Path(str(action_input.get("path"))).resolve()
    action_manifest_path = action_path / "manifest.json"
    if (
        not action_manifest_path.is_file()
        or file_sha256(action_manifest_path)
        != action_input.get("manifest_file_sha256")
    ):
        raise ValueError("stage-1 action provenance differs")
    action_manifest = _actions_module().verify_capture(action_path)
    if action_manifest.get("payload_sha256") != action_input.get(
        "manifest_payload_sha256"
    ):
        raise ValueError("stage-1 action provenance differs")
    _load_suspensions()
    for value in manifest.get("artifacts", {}).values():
        artifact = root / str(value["path"])
        if (
            not artifact.is_file()
            or file_sha256(artifact) != value["file_sha256"]
            or artifact.stat().st_size != int(value["size_bytes"])
        ):
            raise ValueError("stage-1 artifact differs")
        if artifact.suffix == ".parquet" and len(pd.read_parquet(artifact)) != int(
            value["row_count"]
        ):
            raise ValueError("stage-1 artifact row count differs")
    result_path = root / manifest["artifacts"]["result"]["path"]
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if (
        result.get("payload_sha256") != canonical_payload_sha256(result)
        or result.get("stage_2_gate_satisfied") != result.get("stage_1_passed")
        or result.get("stage_2_permitted") is not False
        or result.get("stage_2_dispatched") is not False
        or result.get("stage_2_block_reason")
        != (
            "formal_stage_2_runner_not_yet_frozen"
            if result.get("stage_1_passed") is True
            else "stage_1_gate_failed"
        )
        or result.get("implementation_file_sha256")
        != manifest.get("implementation", {}).get("file_sha256")
        or manifest.get("status") != result.get("status")
        or result.get("status")
        not in {
            "stage_1_passed_stage_2_implementation_not_frozen",
            "stage_1_failed_stage_2_forbidden",
        }
    ):
        raise ValueError("stage-1 result differs")
    for role in ("raw_source_receipt", "raw_source_allowlist"):
        entry = manifest["artifacts"][role]
        value = json.loads((root / entry["path"]).read_text(encoding="utf-8"))
        if (
            value.get("payload_sha256") != entry.get("payload_sha256")
            or canonical_payload_sha256(value) != entry.get("payload_sha256")
        ):
            raise ValueError("stage-1 raw source binding differs")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--minute-root", type=Path, required=True)
    parser.add_argument("--minute-manifest-payload", required=True)
    parser.add_argument("--minute-manifest-file", required=True)
    parser.add_argument("--actions-root", type=Path, required=True)
    parser.add_argument("--actions-manifest-payload", required=True)
    parser.add_argument("--actions-manifest-file", required=True)
    args = parser.parse_args(argv)
    manifest = run_stage1(
        args.output,
        minute_root=args.minute_root,
        minute_manifest_payload_sha256=args.minute_manifest_payload,
        minute_manifest_file_sha256=args.minute_manifest_file,
        actions_root=args.actions_root,
        actions_manifest_payload_sha256=args.actions_manifest_payload,
        actions_manifest_file_sha256=args.actions_manifest_file,
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "status": manifest["status"],
                "payload_sha256": manifest["payload_sha256"],
                "file_sha256": file_sha256(args.output.resolve() / "manifest.json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
