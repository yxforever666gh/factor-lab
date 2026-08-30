#!/usr/bin/env python
"""Freeze the clean 7.0 implementation before any formal ETF returns open."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256  # noqa: E402


PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
ASSET_SELECTION_PATH = Path("protocols/7.0-asset-selection.json")
CLOSURE_PATH = Path("protocols/7.0-release.json")
PRIOR_RESULT_PATH = Path("protocols/evidence/6.3/result.json")
PRECLOSURE_TRAIN_PATH = Path("protocols/evidence/7.0/preclosure-train.json")
PRIOR_TAG = "6.3"
PRIOR_TAG_OBJECT = "bf923c3f757be13a8fdef566d3404c3625861721"
PRIOR_COMMIT = "9ec3f9dd6941ae797a1407f85e00ff770e8d1c60"
PRIOR_RESULT_PAYLOAD = "5ce9e7e92a0908f2e0fb1554801b900d746cc67fd27600fbf4fc82850323cadf"
IMPLEMENTATION_PATHS = (
    ".github/workflows/ci.yml",
    "configs/data.json",
    "pyproject.toml",
    "scripts/build-7.0-asset-selection.py",
    "scripts/build-7.0-preselection-closure.py",
    "scripts/publish-tag.ps1",
    "scripts/run-multi-asset-evidence.py",
    "src/factor_lab/__init__.py",
    "src/factor_lab/cli.py",
    "src/factor_lab/data/__init__.py",
    "src/factor_lab/data/build.py",
    "src/factor_lab/data/catalog.py",
    "src/factor_lab/data/enrich.py",
    "src/factor_lab/data/etf_assets.py",
    "src/factor_lab/data/opportunity_set.py",
    "src/factor_lab/data/pit_lineage.py",
    "src/factor_lab/data/security_master.py",
    "src/factor_lab/data/sources.py",
    "src/factor_lab/data/suspensions.py",
    "src/factor_lab/data/wide_pricing.py",
    "src/factor_lab/release_integrity.py",
    "src/factor_lab/research/__init__.py",
    "src/factor_lab/research/contracts.py",
    "src/factor_lab/research/multi_asset.py",
    "src/factor_lab/research/signals.py",
    "src/factor_lab/research/validation.py",
    "src/factor_lab/research/wide_universe.py",
    "src/factor_lab/strategy.py",
)
FORBIDDEN_BEFORE_CLOSURE = (
    Path("runtime/data/multi-asset-7.0"),
    Path("protocols/evidence/7.0/winner-freeze.json"),
    Path("protocols/evidence/7.0/historical-audit.json"),
    Path("protocols/evidence/7.0/result.json"),
)


def _git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _runner_helpers() -> Any:
    path = ROOT / "scripts" / "run-multi-asset-evidence.py"
    spec = importlib.util.spec_from_file_location("factor_lab_v7_closure_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the frozen 7.0 runner helpers")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _json(path: Path) -> dict[str, Any]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    actual = canonical_payload_sha256(value)
    if value.get("payload_sha256") != actual:
        raise ValueError(f"invalid canonical payload: {path}")
    return value


def _create_only(path: Path, payload: Mapping[str, Any]) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
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


def _verify_prior_release() -> dict[str, str]:
    tag_type = _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
    tag_object = _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
    peeled = _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
    if tag_type != "tag" or tag_object != PRIOR_TAG_OBJECT or peeled != PRIOR_COMMIT:
        raise ValueError("published 6.3 annotated tag binding differs")
    _git("merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD")
    result = _json(PRIOR_RESULT_PATH)
    if result.get("payload_sha256") != PRIOR_RESULT_PAYLOAD:
        raise ValueError("published 6.3 terminal result differs")
    committed = _git("show", f"{PRIOR_COMMIT}:{PRIOR_RESULT_PATH.as_posix()}")
    current = (ROOT / PRIOR_RESULT_PATH).read_bytes()
    if committed != current:
        raise ValueError("current 6.3 terminal result is not the published blob")
    return {
        "tag": PRIOR_TAG,
        "annotated_tag_object": PRIOR_TAG_OBJECT,
        "peeled_commit": PRIOR_COMMIT,
        "terminal_result_path": PRIOR_RESULT_PATH.as_posix(),
        "terminal_result_file_sha256": hashlib.sha256(current).hexdigest(),
        "terminal_result_payload_sha256": PRIOR_RESULT_PAYLOAD,
    }


def main() -> int:
    if (ROOT / CLOSURE_PATH).exists():
        raise FileExistsError("7.0 preselection closure is create-only")
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"7.0 closure requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("7.0 closure requires a clean implementation commit")
    commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    helpers = _runner_helpers()
    helpers._require_source_imports()
    helpers._require_head_pushed_and_ci_success(commit)
    runtime = helpers._runtime_identity()
    for path in FORBIDDEN_BEFORE_CLOSURE:
        if (ROOT / path).exists() or (ROOT / path).is_symlink():
            raise RuntimeError(f"formal 7.0 returns or evidence already exist: {path}")

    protocol = _json(PROTOCOL_PATH)
    asset_selection = _json(ASSET_SELECTION_PATH)
    preclosure_train = _json(PRECLOSURE_TRAIN_PATH)
    helpers._verify_disclosed_outcome_boundary(preclosure_train)
    if (
        protocol.get("protocol_id")
        != "factor-lab/7.0/fixed-multi-asset-trend-budget-v1"
        or protocol.get("release") != "7.0"
        or protocol.get("direction_change") is not True
        or len(protocol.get("candidate_registry", [])) != 1
    ):
        raise ValueError("unexpected 7.0 protocol contract")
    if (
        asset_selection.get("cutoff_date") != "2015-02-27"
        or asset_selection.get("selected_codes")
        != [
            "510300.SH",
            "159920.SZ",
            "513100.SH",
            "518880.SH",
            "511010.SH",
            "511880.SH",
        ]
    ):
        raise ValueError("unexpected causal ETF asset selection")
    asset_contract = protocol.get("assets", {})
    if (
        asset_contract.get("selection_evidence_file_sha256")
        != file_sha256(ROOT / ASSET_SELECTION_PATH)
        or asset_contract.get("selection_evidence_payload_sha256")
        != asset_selection.get("payload_sha256")
    ):
        raise ValueError("protocol does not bind the causal ETF selection bytes")
    disclosure_contract = protocol.get("preclosure_train_disclosure", {})
    if (
        disclosure_contract.get("path") != PRECLOSURE_TRAIN_PATH.as_posix()
        or disclosure_contract.get("file_sha256")
        != file_sha256(ROOT / PRECLOSURE_TRAIN_PATH)
        or disclosure_contract.get("payload_sha256")
        != preclosure_train.get("payload_sha256")
        or preclosure_train.get("status")
        != "train_falsified_before_preselection_closure"
        or preclosure_train.get("selection", {}).get("validation_opened") is not False
        or preclosure_train.get("selection", {}).get("audit_opened") is not False
        or preclosure_train.get("disclosure", {}).get(
            "validation_market_outcomes_opened"
        )
        is not False
        or preclosure_train.get("disclosure", {}).get(
            "audit_market_outcomes_opened"
        )
        is not False
    ):
        raise ValueError("protocol does not bind the disclosed preclosure train failure")

    tree = _git("rev-parse", "HEAD^{tree}").decode("ascii").strip()
    implementation: dict[str, dict[str, str]] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = ROOT / relative
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"implementation path is absent or indirect: {relative}")
        working = path.read_bytes()
        committed = _git("show", f"{commit}:{relative}")
        if working != committed:
            raise ValueError(f"working bytes differ from commit: {relative}")
        implementation[relative] = {
            "path": relative,
            "sha256": hashlib.sha256(working).hexdigest(),
        }

    protocol_bytes = (ROOT / PROTOCOL_PATH).read_bytes()
    if _git("show", f"{commit}:{PROTOCOL_PATH.as_posix()}") != protocol_bytes:
        raise ValueError("implementation commit does not contain the protocol bytes")
    selection_bytes = (ROOT / ASSET_SELECTION_PATH).read_bytes()
    if _git("show", f"{commit}:{ASSET_SELECTION_PATH.as_posix()}") != selection_bytes:
        raise ValueError("implementation commit does not contain asset-selection bytes")
    disclosure_bytes = (ROOT / PRECLOSURE_TRAIN_PATH).read_bytes()
    if _git("show", f"{commit}:{PRECLOSURE_TRAIN_PATH.as_posix()}") != disclosure_bytes:
        raise ValueError("implementation commit does not contain preclosure-train evidence")
    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": "7.0",
        "closure_role": "post_exposure_failure_replay_root",
        "direction_change": True,
        "route": "fixed_multi_asset_causal_trend_budget",
        "status": "implementation_frozen_after_disclosed_train_failure",
        "selection_returns_opened": True,
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
            "payload_sha256": protocol["payload_sha256"],
            "protocol_id": protocol["protocol_id"],
        },
        "asset_selection": {
            "path": ASSET_SELECTION_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / ASSET_SELECTION_PATH),
            "payload_sha256": asset_selection["payload_sha256"],
            "selection_cutoff": asset_selection["cutoff_date"],
            "selected_codes": asset_selection["selected_codes"],
        },
        "prior_release": _verify_prior_release(),
        "implementation_commit": commit,
        "implementation_tree": tree,
        "implementation": implementation,
        "runtime": runtime,
        "formal_data": {
            "preclosure_train_disclosure": {
                "path": PRECLOSURE_TRAIN_PATH.as_posix(),
                "file_sha256": file_sha256(ROOT / PRECLOSURE_TRAIN_PATH),
                "payload_sha256": preclosure_train["payload_sha256"],
                "status": preclosure_train["status"],
                "validation_market_outcomes_opened": False,
                "audit_market_outcomes_opened": False
            }
        },
        "claim_contract": protocol["claim_contract"],
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if _git("rev-parse", "HEAD").decode("ascii").strip() != commit:
        raise RuntimeError("HEAD changed while building the 7.0 closure")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("worktree changed while building the 7.0 closure")
    helpers._require_head_pushed_and_ci_success(commit)
    _create_only(CLOSURE_PATH, closure)
    print(f"implementation_commit={commit}")
    print(f"payload_sha256={closure['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
