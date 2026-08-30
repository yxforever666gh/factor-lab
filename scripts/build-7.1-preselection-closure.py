#!/usr/bin/env python
"""Freeze the clean 7.1 corrective implementation before its fresh train replay."""

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


RELEASE = "7.1"
PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
ASSET_SELECTION_PATH = Path("protocols/7.0-asset-selection.json")
AMENDMENT_PATH = Path("protocols/7.1-corrective-amendment-1.json")
PRECLOSURE_TRAIN_PATH = Path("protocols/evidence/7.0/preclosure-train.json")
PRIOR_CLOSURE_PATH = Path("protocols/7.0-release.json")
PRIOR_FAILURE_PATH = Path("protocols/evidence/7.0/execution-failure.json")
CLOSURE_PATH = Path("protocols/7.1-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/7.1")
WORK_ROOT = Path("runtime/data/multi-asset-7.1")
PRIOR_TAG = "7.0"
PRIOR_TAG_OBJECT = "25bbc306e8842feab923380416f8329e0dd81100"
PRIOR_COMMIT = "412026ca0370d53ca704adfd1122a811e768842e"
PRIOR_CLOSURE_PAYLOAD = "d0b6072234d45363144a47517c8c4c535e4c9550ea36925a4b7cc54216110009"
PRIOR_FAILURE_PAYLOAD = "04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9"
AMENDMENT_PAYLOAD = "7335cdbb61cd0d7b9c3e6f6896ec576c7e403b87d83cfa3d6679965691984c86"
IMPLEMENTATION_PATHS = (
    ".github/workflows/ci.yml",
    "configs/data.json",
    "pyproject.toml",
    "scripts/build-7.1-preselection-closure.py",
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
    WORK_ROOT,
    CLOSURE_PATH,
    EVIDENCE_ROOT,
)


def _git(*args: str, check: bool = True) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            f"git command failed: {args!r}: "
            + completed.stderr.decode("utf-8", errors="replace").strip()
        )
    return completed.stdout


def _runner_helpers() -> Any:
    path = ROOT / "scripts" / "run-multi-asset-evidence.py"
    spec = importlib.util.spec_from_file_location("factor_lab_v71_closure_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the frozen 7.1 runner helpers")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _json(path: Path) -> dict[str, Any]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    if value.get("payload_sha256") != canonical_payload_sha256(value):
        raise ValueError(f"invalid canonical payload: {path}")
    return value


def _create_only(path: Path, payload: Mapping[str, Any]) -> None:
    target = ROOT / path
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


def _tag_has_no_normal_7_0_result() -> None:
    for path in (
        "protocols/evidence/7.0/winner-freeze.json",
        "protocols/evidence/7.0/historical-audit.json",
        "protocols/evidence/7.0/result.json",
    ):
        completed = subprocess.run(
            ["git", "cat-file", "-e", f"{PRIOR_COMMIT}:{path}"],
            cwd=ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode == 0:
            raise ValueError(f"published 7.0 unexpectedly contains normal evidence: {path}")


def _verify_prior_release() -> dict[str, Any]:
    if (
        _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
    ):
        raise ValueError("published 7.0 annotated tag binding differs")
    _git("merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD")
    remote = _git(
        "ls-remote",
        "--exit-code",
        "origin",
        f"refs/tags/{PRIOR_TAG}",
        f"refs/tags/{PRIOR_TAG}^{{}}",
    ).decode("ascii")
    refs = {
        ref: object_id
        for object_id, ref in (line.split() for line in remote.splitlines())
    }
    if refs != {
        f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
        f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
    }:
        raise ValueError("remote 7.0 annotated tag binding differs")
    _tag_has_no_normal_7_0_result()
    closure = _json(PRIOR_CLOSURE_PATH)
    failure = _json(PRIOR_FAILURE_PATH)
    if (
        closure.get("payload_sha256") != PRIOR_CLOSURE_PAYLOAD
        or failure.get("payload_sha256") != PRIOR_FAILURE_PAYLOAD
        or failure.get("status") != "selection_inconclusive_software_failure"
        or failure.get("classification") != "target_order_replay_false_negative"
    ):
        raise ValueError("published 7.0 failure boundary differs")
    current_closure = (ROOT / PRIOR_CLOSURE_PATH).read_bytes()
    current_failure = (ROOT / PRIOR_FAILURE_PATH).read_bytes()
    if (
        _git("show", f"{PRIOR_COMMIT}:{PRIOR_CLOSURE_PATH.as_posix()}")
        != current_closure
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_FAILURE_PATH.as_posix()}")
        != current_failure
    ):
        raise ValueError("current 7.0 failure files are not the published blobs")
    return {
        "release": "7.0",
        "tag": PRIOR_TAG,
        "annotated_tag_object": PRIOR_TAG_OBJECT,
        "peeled_commit": PRIOR_COMMIT,
        "preselection_closure": {
            "path": PRIOR_CLOSURE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(current_closure).hexdigest(),
            "payload_sha256": PRIOR_CLOSURE_PAYLOAD,
        },
        "execution_failure": {
            "path": PRIOR_FAILURE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(current_failure).hexdigest(),
            "payload_sha256": PRIOR_FAILURE_PAYLOAD,
            "status": failure["status"],
            "classification": failure["classification"],
        },
    }


def main() -> int:
    if (ROOT / CLOSURE_PATH).exists():
        raise FileExistsError("7.1 preselection closure is create-only")
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"7.1 closure requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("7.1 closure requires a clean implementation commit")
    for path in FORBIDDEN_BEFORE_CLOSURE:
        if (ROOT / path).exists() or (ROOT / path).is_symlink():
            raise RuntimeError(f"formal 7.1 returns or evidence already exist: {path}")

    commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    helpers = _runner_helpers()
    if set(IMPLEMENTATION_PATHS) != helpers.EXPECTED_IMPLEMENTATION_PATHS:
        raise ValueError("builder and runner implementation path sets differ")
    helpers._require_source_imports()
    helpers._require_head_pushed_and_ci_success(commit)
    runtime = helpers._runtime_identity()

    protocol = _json(PROTOCOL_PATH)
    asset_selection = _json(ASSET_SELECTION_PATH)
    amendment = _json(AMENDMENT_PATH)
    disclosure = _json(PRECLOSURE_TRAIN_PATH)
    failure = _json(PRIOR_FAILURE_PATH)
    helpers._verify_corrective_amendment(amendment)
    helpers._verify_disclosed_outcome_boundary(disclosure)
    if (
        protocol.get("protocol_id")
        != "factor-lab/7.0/fixed-multi-asset-trend-budget-v1"
        or protocol.get("release") != "7.0"
        or asset_selection.get("selected_codes")
        != [
            "510300.SH",
            "159920.SZ",
            "513100.SH",
            "518880.SH",
            "511010.SH",
            "511880.SH",
        ]
        or amendment.get("payload_sha256") != AMENDMENT_PAYLOAD
        or failure.get("payload_sha256") != PRIOR_FAILURE_PAYLOAD
    ):
        raise ValueError("unexpected 7.1 corrective contract")
    boundary = failure.get("evidence_boundary") or {}
    if any(
        boundary.get(key) is not False
        for key in (
            "validation_market_outcomes_opened",
            "winner_freeze_created",
            "audit_market_outcomes_opened",
            "terminal_result_created",
        )
    ):
        raise ValueError("7.0 failure receipt opened a forbidden downstream phase")
    prior_release = _verify_prior_release()

    tree = _git("rev-parse", "HEAD^{tree}").decode("ascii").strip()
    implementation: dict[str, dict[str, str]] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = ROOT / relative
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"implementation path is absent or indirect: {relative}")
        working = path.read_bytes()
        if _git("show", f"{commit}:{relative}") != working:
            raise ValueError(f"working bytes differ from commit: {relative}")
        implementation[relative] = {
            "path": relative,
            "sha256": hashlib.sha256(working).hexdigest(),
        }
    for relative in (
        PROTOCOL_PATH,
        ASSET_SELECTION_PATH,
        AMENDMENT_PATH,
        PRECLOSURE_TRAIN_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_FAILURE_PATH,
    ):
        if _git("show", f"{commit}:{relative.as_posix()}") != (ROOT / relative).read_bytes():
            raise ValueError(f"implementation commit lacks contract bytes: {relative}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": RELEASE,
        "closure_role": "corrective_train_replay_after_7_0_execution_failure",
        "direction_change": False,
        "route": "fixed_multi_asset_causal_trend_budget",
        "status": "corrective_implementation_frozen_for_exact_failed_train_replay",
        "prior_train_returns_opened": True,
        "corrective_train_returns_opened": False,
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
        "corrective_amendment": {
            "path": AMENDMENT_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / AMENDMENT_PATH),
            "payload_sha256": amendment["payload_sha256"],
            "amendment_id": amendment["amendment_id"],
        },
        "prior_release": prior_release,
        "implementation_commit": commit,
        "implementation_tree": tree,
        "implementation": implementation,
        "runtime": runtime,
        "formal_data": {
            "preclosure_train_disclosure": {
                "path": PRECLOSURE_TRAIN_PATH.as_posix(),
                "file_sha256": file_sha256(ROOT / PRECLOSURE_TRAIN_PATH),
                "payload_sha256": disclosure["payload_sha256"],
                "status": disclosure["status"],
                "validation_market_outcomes_opened": False,
                "audit_market_outcomes_opened": False,
            },
            "prior_execution_failure": {
                "path": PRIOR_FAILURE_PATH.as_posix(),
                "file_sha256": file_sha256(ROOT / PRIOR_FAILURE_PATH),
                "payload_sha256": failure["payload_sha256"],
                "status": failure["status"],
                "classification": failure["classification"],
                "validation_market_outcomes_opened": False,
                "winner_freeze_created": False,
                "audit_market_outcomes_opened": False,
                "terminal_result_created": False,
            },
        },
        "claim_contract": protocol["claim_contract"],
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if _git("rev-parse", "HEAD").decode("ascii").strip() != commit:
        raise RuntimeError("HEAD changed while building the 7.1 closure")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("worktree changed while building the 7.1 closure")
    helpers._require_head_pushed_and_ci_success(commit)
    _create_only(CLOSURE_PATH, closure)
    print(f"implementation_commit={commit}")
    print(f"payload_sha256={closure['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
