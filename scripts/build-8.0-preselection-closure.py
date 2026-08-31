#!/usr/bin/env python
"""Freeze the clean 8.0 static-allocation implementation before replay."""

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


RELEASE = "8.0"
PROTOCOL_PATH = Path("protocols/8.0-static-capital-budget.json")
INHERITED_PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
CLOSURE_PATH = Path("protocols/8.0-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/8.0")
WORK_ROOT = Path("runtime/data/multi-asset-8.0")
RUNNER_PATH = Path("scripts/run-multi-asset-evidence.py")

PRIOR_TAG = "7.1"
PRIOR_TAG_OBJECT = "15ea8e8de95638fdc0786ff0f35177b0ecba878d"
PRIOR_COMMIT = "e7f09e17646cc44d78a49f6ddc41acc471f205d4"
PRIOR_CLOSURE_PATH = Path("protocols/7.1-release.json")
PRIOR_FREEZE_PATH = Path("protocols/evidence/7.1/winner-freeze.json")
PRIOR_RESULT_PATH = Path("protocols/evidence/7.1/result.json")
PRIOR_CLOSURE_FILE_SHA256 = (
    "794b11d55cfbdf1f33e5e15c917691b76f244a9fd5f8f400a5f862d7830f11cd"
)
PRIOR_CLOSURE_PAYLOAD = (
    "8cd80c7c770477cf29c2fa04348e9ed16f637f7d5ee61f31232d6f1f81ff2e55"
)
PRIOR_FREEZE_FILE_SHA256 = (
    "2b239ac699d80db0965d87f1fb96a366b7a2f820c173fa08988fb4801323fa77"
)
PRIOR_FREEZE_PAYLOAD = (
    "451b7de8bbcba9372731b7dd7236e16a46467bdf5499eeff5e17e8e946ffabfd"
)
PRIOR_RESULT_FILE_SHA256 = (
    "ff0278104d1e7fd5f940671322e1987ea416bb4eeb7b3a343ec814393053449a"
)
PRIOR_RESULT_PAYLOAD = (
    "869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9"
)
PROTOCOL_PAYLOAD = (
    "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
)
INHERITED_PROTOCOL_FILE_SHA256 = (
    "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
)
INHERITED_PROTOCOL_PAYLOAD = (
    "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
)

FORBIDDEN_BEFORE_CLOSURE = (WORK_ROOT, EVIDENCE_ROOT, CLOSURE_PATH)
CLOSURE_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "closure_role",
    "direction_change",
    "route",
    "status",
    "prior_train_returns_opened",
    "validation_market_outcomes_opened",
    "audit_status",
    "protocol",
    "prior_release",
    "prior_train_exposure",
    "implementation_commit",
    "implementation_tree",
    "implementation",
    "runtime",
    "formal_data",
    "claim_contract",
    "payload_sha256",
}


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
    path = ROOT / RUNNER_PATH
    spec = importlib.util.spec_from_file_location(
        "factor_lab_v8_closure_helpers", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the frozen 8.0 runner helpers")
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


def _verify_runner_contract(helpers: Any) -> tuple[str, ...]:
    expected_paths = getattr(helpers, "EXPECTED_IMPLEMENTATION_PATHS", None)
    if not isinstance(expected_paths, (set, frozenset)) or not expected_paths:
        raise ValueError("8.0 runner lacks an exact implementation path set")
    paths = tuple(sorted(map(str, expected_paths)))
    required = {RUNNER_PATH.as_posix(), "scripts/build-8.0-preselection-closure.py"}
    if (
        getattr(helpers, "RELEASE", None) != RELEASE
        or Path(getattr(helpers, "PROTOCOL_PATH", "")) != PROTOCOL_PATH
        or Path(getattr(helpers, "INHERITED_PROTOCOL_PATH", ""))
        != INHERITED_PROTOCOL_PATH
        or getattr(helpers, "INHERITED_PROTOCOL_FILE_SHA256", None)
        != INHERITED_PROTOCOL_FILE_SHA256
        or getattr(helpers, "INHERITED_PROTOCOL_PAYLOAD", None)
        != INHERITED_PROTOCOL_PAYLOAD
        or Path(getattr(helpers, "CLOSURE_PATH", "")) != CLOSURE_PATH
        or Path(getattr(helpers, "EVIDENCE_ROOT", "")) != EVIDENCE_ROOT
        or Path(getattr(helpers, "WORK_ROOT", "")).resolve()
        != (ROOT / WORK_ROOT).resolve()
        or not required.issubset(set(paths))
    ):
        raise ValueError("runner has not migrated to the exact 8.0 namespace")
    return paths


def _remote_tag_refs() -> dict[str, str]:
    remote = _git(
        "ls-remote",
        "--exit-code",
        "origin",
        f"refs/tags/{PRIOR_TAG}",
        f"refs/tags/{PRIOR_TAG}^{{}}",
    ).decode("ascii")
    try:
        return {
            ref: object_id
            for object_id, ref in (line.split() for line in remote.splitlines())
        }
    except ValueError as exc:
        raise ValueError("remote 7.1 tag response is malformed") from exc


def _tag_blob(path: Path) -> bytes:
    current = (ROOT / path).read_bytes()
    tagged = _git("show", f"{PRIOR_COMMIT}:{path.as_posix()}")
    if current != tagged:
        raise ValueError(f"current prior-release file differs from 7.1 tag: {path}")
    return current


def _verify_prior_release() -> tuple[dict[str, Any], dict[str, Any]]:
    if (
        _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
    ):
        raise ValueError("published 7.1 annotated tag binding differs")
    _git("merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD")
    if _remote_tag_refs() != {
        f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
        f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
    }:
        raise ValueError("remote 7.1 annotated tag binding differs")

    closure_bytes = _tag_blob(PRIOR_CLOSURE_PATH)
    freeze_bytes = _tag_blob(PRIOR_FREEZE_PATH)
    result_bytes = _tag_blob(PRIOR_RESULT_PATH)
    closure = json.loads(closure_bytes.decode("utf-8"))
    freeze = json.loads(freeze_bytes.decode("utf-8"))
    result = json.loads(result_bytes.decode("utf-8"))
    for role, value in (
        ("7.1 closure", closure),
        ("7.1 winner freeze", freeze),
        ("7.1 terminal result", result),
    ):
        if value.get("payload_sha256") != canonical_payload_sha256(value):
            raise ValueError(f"published {role} has an invalid canonical payload")
    if (
        hashlib.sha256(closure_bytes).hexdigest() != PRIOR_CLOSURE_FILE_SHA256
        or closure.get("payload_sha256") != PRIOR_CLOSURE_PAYLOAD
        or closure.get("release") != "7.1"
        or hashlib.sha256(freeze_bytes).hexdigest() != PRIOR_FREEZE_FILE_SHA256
        or freeze.get("payload_sha256") != PRIOR_FREEZE_PAYLOAD
        or freeze.get("release") != "7.1"
        or freeze.get("status") != "selected_null_frozen_train_failed"
        or freeze.get("selected_candidate_id") is not None
        or freeze.get("validation") is not None
        or freeze.get("validation_market_outcomes_opened") is not False
        or freeze.get("audit_market_outcomes_opened") is not False
        or freeze.get("train", {}).get("gate", {}).get("passed") is not False
        or hashlib.sha256(result_bytes).hexdigest() != PRIOR_RESULT_FILE_SHA256
        or result.get("payload_sha256") != PRIOR_RESULT_PAYLOAD
        or result.get("release") != "7.1"
        or result.get("status") != "selection_falsified_no_candidate"
        or result.get("selected_candidate_id") is not None
        or result.get("audit_status") != "not_opened"
        or result.get("historical_audit") is not None
        or result.get("winner_freeze", {}).get("file_sha256")
        != PRIOR_FREEZE_FILE_SHA256
        or result.get("winner_freeze", {}).get("payload_sha256")
        != PRIOR_FREEZE_PAYLOAD
    ):
        raise ValueError("published 7.1 closure/freeze/result boundary differs")

    prior_release = {
        "release": "7.1",
        "tag": PRIOR_TAG,
        "annotated_tag_object": PRIOR_TAG_OBJECT,
        "peeled_commit": PRIOR_COMMIT,
        "preselection_closure": {
            "path": PRIOR_CLOSURE_PATH.as_posix(),
            "file_sha256": PRIOR_CLOSURE_FILE_SHA256,
            "payload_sha256": PRIOR_CLOSURE_PAYLOAD,
            "status": closure["status"],
        },
        "winner_freeze": {
            "path": PRIOR_FREEZE_PATH.as_posix(),
            "file_sha256": PRIOR_FREEZE_FILE_SHA256,
            "payload_sha256": PRIOR_FREEZE_PAYLOAD,
            "status": freeze["status"],
        },
        "terminal_result": {
            "path": PRIOR_RESULT_PATH.as_posix(),
            "file_sha256": PRIOR_RESULT_FILE_SHA256,
            "payload_sha256": PRIOR_RESULT_PAYLOAD,
            "status": result["status"],
        },
    }
    train = freeze["train"]
    metrics = train["metrics"]
    prior_train_exposure = {
        "source_release": "7.1",
        "winner_freeze_path": PRIOR_FREEZE_PATH.as_posix(),
        "winner_freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD,
        "source_manifest_payload_sha256": train["source_manifest_payload_sha256"],
        "stage_binding_payload_sha256": train["stage_binding_payload_sha256"],
        "evaluation_payload_sha256": train["evaluation_payload_sha256"],
        "combined_metrics_sha256": canonical_payload_sha256(metrics),
        "static_control_metrics_sha256": canonical_payload_sha256(
            metrics["control"]
        ),
        "train_gate_sha256": canonical_payload_sha256({"gate": train["gate"]}),
        "train_gate_passed": False,
        "selected_candidate_id": None,
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
    }
    return prior_release, prior_train_exposure


def _validate_protocol(protocol: Mapping[str, Any]) -> None:
    prior = protocol.get("prior_release")
    claim = protocol.get("claim_contract")
    inherited = protocol.get("inherited_data_execution_contract")
    inherited_source = (
        inherited.get("source") if isinstance(inherited, Mapping) else None
    )
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD
        or protocol.get("release") != RELEASE
        or protocol.get("direction_change") is not True
        or protocol.get("protocol_id")
        != "factor-lab/8.0/strategic-static-capital-budget-beta-v1"
        or protocol.get("route") != "strategic_static_capital_budget_beta"
        or protocol.get("strategy_registry", [{}])[0].get("strategy_id")
        != "static_risk_budget"
        or protocol.get("cash_comparator", {}).get("comparator_id")
        != "cash_only_511880"
        or protocol.get("prior_train_exposure", {}).get(
            "static_control_metrics_sha256"
        )
        != "fb1b146e34d62486dfd2c7ff39102ca7418419260f7eda99b11b6c2768c12492"
        or not isinstance(prior, Mapping)
        or prior.get("release") != "7.1"
        or prior.get("tag") != PRIOR_TAG
        or prior.get("annotated_tag_object") != PRIOR_TAG_OBJECT
        or prior.get("peeled_commit") != PRIOR_COMMIT
        or prior.get("preselection_closure", {}).get("path")
        != PRIOR_CLOSURE_PATH.as_posix()
        or prior.get("preselection_closure", {}).get("file_sha256")
        != PRIOR_CLOSURE_FILE_SHA256
        or prior.get("preselection_closure", {}).get("payload_sha256")
        != PRIOR_CLOSURE_PAYLOAD
        or prior.get("winner_freeze", {}).get("path")
        != PRIOR_FREEZE_PATH.as_posix()
        or prior.get("winner_freeze", {}).get("file_sha256")
        != PRIOR_FREEZE_FILE_SHA256
        or prior.get("winner_freeze", {}).get("payload_sha256")
        != PRIOR_FREEZE_PAYLOAD
        or prior.get("winner_freeze", {}).get("status")
        != "selected_null_frozen_train_failed"
        or prior.get("winner_freeze", {}).get("selected_candidate_id") is not None
        or prior.get("terminal_result", {}).get("path")
        != PRIOR_RESULT_PATH.as_posix()
        or prior.get("terminal_result", {}).get("file_sha256")
        != PRIOR_RESULT_FILE_SHA256
        or prior.get("terminal_result", {}).get("payload_sha256")
        != PRIOR_RESULT_PAYLOAD
        or prior.get("terminal_result", {}).get("status")
        != "selection_falsified_no_candidate"
        or prior.get("terminal_result", {}).get("selected_candidate_id") is not None
        or prior.get("terminal_result", {}).get("audit_status") != "not_opened"
        or not isinstance(claim, Mapping)
        or claim.get("alpha_claim_allowed") is not False
        or claim.get("profit_claim_allowed") is not False
        or claim.get("stable_future_profit_claim_allowed") is not False
        or claim.get("fresh_future_evidence_required") is not True
        or claim.get("minimum_fresh_sessions") != 252
        or claim.get("minimum_fresh_monthly_executions") != 12
        or claim.get("investment_recommendation_allowed") is not False
        or not isinstance(inherited_source, Mapping)
        or inherited_source.get("path") != INHERITED_PROTOCOL_PATH.as_posix()
        or inherited_source.get("file_sha256")
        != INHERITED_PROTOCOL_FILE_SHA256
        or inherited_source.get("payload_sha256") != INHERITED_PROTOCOL_PAYLOAD
    ):
        raise ValueError("unexpected 8.0 protocol contract")
    inherited_protocol = _json(INHERITED_PROTOCOL_PATH)
    if (
        inherited_protocol.get("payload_sha256") != INHERITED_PROTOCOL_PAYLOAD
        or canonical_payload_sha256(inherited_protocol) != INHERITED_PROTOCOL_PAYLOAD
        or file_sha256(ROOT / INHERITED_PROTOCOL_PATH)
        != INHERITED_PROTOCOL_FILE_SHA256
    ):
        raise ValueError("inherited 7.0 data/execution protocol bytes differ")


def main() -> int:
    if (ROOT / CLOSURE_PATH).exists() or (ROOT / CLOSURE_PATH).is_symlink():
        raise FileExistsError("8.0 preselection closure is create-only")
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"8.0 closure requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("8.0 closure requires a clean implementation commit")
    for path in FORBIDDEN_BEFORE_CLOSURE:
        if (ROOT / path).exists() or (ROOT / path).is_symlink():
            raise RuntimeError(f"formal 8.0 returns or evidence already exist: {path}")

    commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    helpers = _runner_helpers()
    implementation_paths = _verify_runner_contract(helpers)
    helpers._require_source_imports()
    helpers._require_head_pushed_and_ci_success(commit)
    runtime = helpers._runtime_identity()

    protocol = _json(PROTOCOL_PATH)
    _validate_protocol(protocol)
    prior_release, prior_train_exposure = _verify_prior_release()

    tree = _git("rev-parse", "HEAD^{tree}").decode("ascii").strip()
    implementation: dict[str, dict[str, str]] = {}
    for relative in implementation_paths:
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
        INHERITED_PROTOCOL_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_FREEZE_PATH,
        PRIOR_RESULT_PATH,
    ):
        if _git("show", f"{commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(f"implementation commit lacks contract bytes: {relative}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": RELEASE,
        "closure_role": "static_capital_budget_prevalidation_root",
        "direction_change": True,
        "route": protocol["route"],
        "status": "implementation_frozen_before_8_0_replay",
        "prior_train_returns_opened": True,
        "validation_market_outcomes_opened": False,
        "audit_status": "not_opened",
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
            "payload_sha256": protocol["payload_sha256"],
            "protocol_id": protocol["protocol_id"],
        },
        "prior_release": prior_release,
        "prior_train_exposure": prior_train_exposure,
        "implementation_commit": commit,
        "implementation_tree": tree,
        "implementation": implementation,
        "runtime": runtime,
        "formal_data": {},
        "claim_contract": protocol["claim_contract"],
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if set(closure) != CLOSURE_FIELDS:
        raise RuntimeError("8.0 closure builder emitted an unexpected field set")
    if _git("rev-parse", "HEAD").decode("ascii").strip() != commit:
        raise RuntimeError("HEAD changed while building the 8.0 closure")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("worktree changed while building the 8.0 closure")
    helpers._require_head_pushed_and_ci_success(commit)
    _create_only(CLOSURE_PATH, closure)
    print(f"implementation_commit={commit}")
    print(f"payload_sha256={closure['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
