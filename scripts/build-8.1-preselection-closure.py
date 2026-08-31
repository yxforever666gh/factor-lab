#!/usr/bin/env python
"""Freeze the clean 8.1 metric-reclassification implementation."""

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


RELEASE = "8.1"
PROTOCOL_PATH = Path("protocols/8.1-policy-operational-metric-reclassification.json")
CLOSURE_PATH = Path("protocols/8.1-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/8.1")
WORK_ROOT = Path("runtime/data/multi-asset-8.1")
RUNNER_PATH = Path("scripts/run-multi-asset-evidence.py")

PRIOR_TAG = "8.0"
PRIOR_TAG_OBJECT = "3fcbd73f7497b074e484ce7793e2d3603bf5a177"
PRIOR_COMMIT = "78aba86bf4e741699afca1acd1470493785fd952"
PRIOR_PROTOCOL_PATH = Path("protocols/8.0-static-capital-budget.json")
PRIOR_CLOSURE_PATH = Path("protocols/8.0-release.json")
PRIOR_RECEIPT_PATH = Path("protocols/evidence/8.0/execution-failure.json")
PRIOR_PROTOCOL_FILE_SHA256 = (
    "ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc"
)
PRIOR_PROTOCOL_PAYLOAD = (
    "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
)
PRIOR_CLOSURE_FILE_SHA256 = (
    "8e4fe890efb746c15ae5f0375d8a1dfd85a061172426165af1441d5011bfa97d"
)
PRIOR_CLOSURE_PAYLOAD = (
    "7bdd27bc6365c936c7e17736920d5fbf2556608e8b59b0869b3e70b9e61e5de7"
)
PRIOR_RECEIPT_FILE_SHA256 = (
    "6af779495081f6ee391c6388a1e4342b878168b529f8074cf03d9ec2cc50eeaa"
)
PRIOR_RECEIPT_PAYLOAD = (
    "751b85c6c2e52b450e9c3549f7f4504af50b634599be4c32e240ee503de9823a"
)
PROTOCOL_PAYLOAD = (
    "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5"
)
PROTOCOL_FILE_SHA256 = (
    "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583"
)
GITHUB_REPOSITORY = "yxforever666gh/factor-lab"
SOLE_CORRECTION = (
    "Restrict policy admission turnover, fill and capacity aggregation to primary "
    "and stress; keep cash and cash_stress execution diagnostics disclosed, keep "
    "four-role accounting validity, and keep every asset, weight, target, return, "
    "cost, date and economic threshold unchanged."
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
    "post_hoc_reclassification",
    "prior_train_returns_opened",
    "train_reexecution_allowed",
    "train_reclassification_status",
    "validation_market_outcomes_opened",
    "audit_status",
    "protocol",
    "prior_release",
    "train_reclassification_source",
    "implementation_commit",
    "implementation_tree",
    "implementation",
    "runtime",
    "formal_data",
    "claim_contract",
    "payload_sha256",
}


class _GitCommandError(RuntimeError):
    def __init__(self, message: str, *, returncode: int, stdout: bytes) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout


def _git(*args: str, check: bool = True) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and completed.returncode != 0:
        raise _GitCommandError(
            f"git command failed: {args!r}: "
            + completed.stderr.decode("utf-8", errors="replace").strip(),
            returncode=completed.returncode,
            stdout=completed.stdout,
        )
    return completed.stdout


def _github_api(path: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["gh", "api", "--method", "GET", path],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "GitHub API fallback failed: "
            + completed.stderr.decode("utf-8", errors="replace").strip()
        )
    try:
        value = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("GitHub API fallback returned malformed JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("GitHub API fallback must return a JSON object")
    return value


def _runner_helpers() -> Any:
    path = ROOT / RUNNER_PATH
    spec = importlib.util.spec_from_file_location(
        "factor_lab_v81_closure_helpers", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the frozen 8.1 runner helpers")
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
        raise ValueError("8.1 runner lacks an exact implementation path set")
    paths = tuple(sorted(map(str, expected_paths)))
    required = {RUNNER_PATH.as_posix(), "scripts/build-8.1-preselection-closure.py"}
    if (
        getattr(helpers, "RELEASE", None) != RELEASE
        or Path(getattr(helpers, "PROTOCOL_PATH", "")) != PROTOCOL_PATH
        or getattr(helpers, "PROTOCOL_PAYLOAD", None) != PROTOCOL_PAYLOAD
        or getattr(helpers, "PROTOCOL_FILE_SHA256", None) != PROTOCOL_FILE_SHA256
        or Path(getattr(helpers, "CLOSURE_PATH", "")) != CLOSURE_PATH
        or Path(getattr(helpers, "EVIDENCE_ROOT", "")) != EVIDENCE_ROOT
        or Path(getattr(helpers, "WORK_ROOT", "")).resolve()
        != (ROOT / WORK_ROOT).resolve()
        or Path(getattr(helpers, "PRIOR_RECEIPT_PATH", ""))
        != PRIOR_RECEIPT_PATH
        or not required.issubset(set(paths))
    ):
        raise ValueError("runner has not migrated to the exact 8.1 namespace")
    return paths


def _github_remote_tag_refs() -> dict[str, str]:
    ref = _github_api(f"repos/{GITHUB_REPOSITORY}/git/ref/tags/{PRIOR_TAG}")
    obj = ref.get("object")
    if (
        ref.get("ref") != f"refs/tags/{PRIOR_TAG}"
        or not isinstance(obj, Mapping)
        or obj.get("type") != "tag"
        or obj.get("sha") != PRIOR_TAG_OBJECT
    ):
        raise ValueError("GitHub API 8.0 tag object binding differs")
    tag = _github_api(f"repos/{GITHUB_REPOSITORY}/git/tags/{PRIOR_TAG_OBJECT}")
    peeled = tag.get("object")
    if (
        tag.get("sha") != PRIOR_TAG_OBJECT
        or not isinstance(peeled, Mapping)
        or peeled.get("type") != "commit"
        or peeled.get("sha") != PRIOR_COMMIT
    ):
        raise ValueError("GitHub API 8.0 peeled tag binding differs")
    return {
        f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
        f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
    }


def _remote_tag_refs() -> dict[str, str]:
    try:
        remote = _git(
            "ls-remote",
            "--exit-code",
            "origin",
            f"refs/tags/{PRIOR_TAG}",
            f"refs/tags/{PRIOR_TAG}^{{}}",
        ).decode("ascii")
    except _GitCommandError as exc:
        if exc.returncode == 2 and not exc.stdout.strip():
            return {}
        return _github_remote_tag_refs()
    except (RuntimeError, UnicodeDecodeError):
        return _github_remote_tag_refs()
    if not remote.strip():
        return {}
    try:
        refs = {
            ref: object_id
            for object_id, ref in (line.split() for line in remote.splitlines())
        }
    except ValueError as exc:
        raise ValueError("remote 8.0 tag response is malformed") from exc
    return refs


def _tag_blob(path: Path) -> bytes:
    current = (ROOT / path).read_bytes()
    tagged = _git("show", f"{PRIOR_COMMIT}:{path.as_posix()}")
    if current != tagged:
        raise ValueError(f"current prior-release file differs from 8.0 tag: {path}")
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
        raise ValueError("published 8.0 annotated tag binding differs")
    _git("merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD")
    if _remote_tag_refs() != {
        f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
        f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
    }:
        raise ValueError("remote 8.0 annotated tag binding differs")

    protocol_bytes = _tag_blob(PRIOR_PROTOCOL_PATH)
    closure_bytes = _tag_blob(PRIOR_CLOSURE_PATH)
    receipt_bytes = _tag_blob(PRIOR_RECEIPT_PATH)
    protocol = json.loads(protocol_bytes.decode("utf-8"))
    closure = json.loads(closure_bytes.decode("utf-8"))
    receipt = json.loads(receipt_bytes.decode("utf-8"))
    for role, value in (
        ("8.0 protocol", protocol),
        ("8.0 closure", closure),
        ("8.0 execution-failure receipt", receipt),
    ):
        if not isinstance(value, dict) or value.get(
            "payload_sha256"
        ) != canonical_payload_sha256(value):
            raise ValueError(f"published {role} has an invalid canonical payload")

    failure = receipt.get("failure_boundary")
    archive = receipt.get("archive_contract")
    train = receipt.get("train_stage")
    observed = train.get("observed_gate") if isinstance(train, Mapping) else None
    role_metrics = train.get("role_gate_metrics") if isinstance(train, Mapping) else None
    if (
        hashlib.sha256(protocol_bytes).hexdigest() != PRIOR_PROTOCOL_FILE_SHA256
        or protocol.get("payload_sha256") != PRIOR_PROTOCOL_PAYLOAD
        or protocol.get("release") != "8.0"
        or hashlib.sha256(closure_bytes).hexdigest() != PRIOR_CLOSURE_FILE_SHA256
        or closure.get("payload_sha256") != PRIOR_CLOSURE_PAYLOAD
        or closure.get("release") != "8.0"
        or hashlib.sha256(receipt_bytes).hexdigest() != PRIOR_RECEIPT_FILE_SHA256
        or receipt.get("payload_sha256") != PRIOR_RECEIPT_PAYLOAD
        or receipt.get("release") != "8.0"
        or receipt.get("status") != "selection_inconclusive_execution_failure"
        or receipt.get("classification")
        != "post_evaluation_github_ci_transport_failure"
        or not isinstance(failure, Mapping)
        or failure.get("train_phase_deep_verified") is not True
        or failure.get("train_evaluation_persisted") is not True
        or failure.get("train_admission_created") is not False
        or failure.get("validation_market_outcomes_opened") is not False
        or failure.get("audit_market_outcomes_opened") is not False
        or not isinstance(archive, Mapping)
        or archive.get("next_release") != RELEASE
        or archive.get("direction_change") is not False
        or archive.get("correction_chosen_after_observing_8_0_train_failure")
        is not True
        or archive.get("permitted_change") != SOLE_CORRECTION
        or not isinstance(observed, Mapping)
        or observed.get("gate_passed") is not False
        or observed.get("failed_checks")
        != ["requested_notional_fill_ratio_at_least"]
        or not isinstance(role_metrics, Mapping)
        or set(role_metrics) != {"primary", "stress", "cash", "cash_stress"}
        or canonical_payload_sha256(role_metrics)
        != "9bcc5b80025d57837e9330a33953040acf0cecb4185b3e4791146623b3b146ab"
    ):
        raise ValueError("published 8.0 protocol/closure/receipt boundary differs")

    prior_release = {
        "release": "8.0",
        "tag": PRIOR_TAG,
        "annotated_tag_object": PRIOR_TAG_OBJECT,
        "peeled_commit": PRIOR_COMMIT,
        "protocol": {
            "path": PRIOR_PROTOCOL_PATH.as_posix(),
            "file_sha256": PRIOR_PROTOCOL_FILE_SHA256,
            "payload_sha256": PRIOR_PROTOCOL_PAYLOAD,
        },
        "prevalidation_closure": {
            "path": PRIOR_CLOSURE_PATH.as_posix(),
            "file_sha256": PRIOR_CLOSURE_FILE_SHA256,
            "payload_sha256": PRIOR_CLOSURE_PAYLOAD,
        },
        "execution_failure_receipt": {
            "path": PRIOR_RECEIPT_PATH.as_posix(),
            "file_sha256": PRIOR_RECEIPT_FILE_SHA256,
            "payload_sha256": PRIOR_RECEIPT_PAYLOAD,
            "status": receipt["status"],
            "classification": receipt["classification"],
        },
    }
    evaluation = train["evaluation"]
    binding = train["binding"]
    train_reclassification_source = {
        "source_release": "8.0",
        "receipt_path": PRIOR_RECEIPT_PATH.as_posix(),
        "receipt_file_sha256": PRIOR_RECEIPT_FILE_SHA256,
        "receipt_payload_sha256": PRIOR_RECEIPT_PAYLOAD,
        "execution_commit": binding["execution_commit"],
        "run_nonce": binding["run_nonce"],
        "source_manifest_payload_sha256": train["manifest"]["payload_sha256"],
        "stage_binding_payload_sha256": binding["payload_sha256"],
        "evaluation_payload_sha256": evaluation["payload_sha256"],
        "evaluation_file_sha256": evaluation["file_sha256"],
        "role_gate_metrics_sha256": canonical_payload_sha256(role_metrics),
        "original_gate_sha256": canonical_payload_sha256(observed),
        "original_gate_passed": False,
        "train_strategy_rerun_allowed": False,
        "economic_role_metrics_source": "published_receipt_only",
        "validity_source": "receipt_bound_retained_8_0_artifacts_read_only",
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
    }
    return prior_release, train_reclassification_source


def _validate_protocol(protocol: Mapping[str, Any]) -> None:
    prior = protocol.get("prior_release")
    correction = protocol.get("correction_boundary")
    frozen = protocol.get("frozen_economic_contract")
    train_input = protocol.get("train_reclassification_input")
    roles = protocol.get("metric_role_contract")
    validity = protocol.get("execution_validity_hard_fail")
    phases = protocol.get("physical_phases")
    transport = protocol.get("transport_verification")
    claim = protocol.get("claim_contract")
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(protocol) != PROTOCOL_PAYLOAD
        or protocol.get("release") != RELEASE
        or protocol.get("direction_change") is not False
        or protocol.get("protocol_id")
        != "factor-lab/8.1/policy-operational-metric-reclassification-v1"
        or protocol.get("route") != "policy_operational_metric_reclassification"
        or not isinstance(prior, Mapping)
        or prior.get("tag") != PRIOR_TAG
        or prior.get("annotated_tag_object") != PRIOR_TAG_OBJECT
        or prior.get("peeled_commit") != PRIOR_COMMIT
        or prior.get("execution_failure_receipt", {}).get("file_sha256")
        != PRIOR_RECEIPT_FILE_SHA256
        or prior.get("execution_failure_receipt", {}).get("payload_sha256")
        != PRIOR_RECEIPT_PAYLOAD
        or not isinstance(correction, Mapping)
        or correction.get("sole_research_correction") != SOLE_CORRECTION
        or correction.get("post_hoc_reclassification") is not True
        or correction.get("train_strategy_rerun_allowed") is not False
        or correction.get("train_artifact_metric_recomputation_allowed") is not False
        or not isinstance(frozen, Mapping)
        or frozen.get("source", {}).get("file_sha256")
        != PRIOR_PROTOCOL_FILE_SHA256
        or frozen.get("source", {}).get("payload_sha256")
        != PRIOR_PROTOCOL_PAYLOAD
        or frozen.get("strategy_id") != "static_risk_budget"
        or frozen.get("cash_comparator_id") != "cash_only_511880"
        or frozen.get("economic_thresholds_unchanged") is not True
        or not isinstance(train_input, Mapping)
        or train_input.get("source_receipt", {}).get("file_sha256")
        != PRIOR_RECEIPT_FILE_SHA256
        or train_input.get("source_receipt", {}).get("payload_sha256")
        != PRIOR_RECEIPT_PAYLOAD
        or train_input.get("role_gate_metrics_sha256")
        != "9bcc5b80025d57837e9330a33953040acf0cecb4185b3e4791146623b3b146ab"
        or canonical_payload_sha256(train_input.get("role_gate_metrics", {}))
        != train_input.get("role_gate_metrics_sha256")
        or train_input.get("economic_role_metrics_source_is_receipt_only") is not True
        or train_input.get("receipt_bound_8_0_runtime_read_required") is not True
        or not isinstance(roles, Mapping)
        or roles.get("required_roles_exact")
        != ["primary", "stress", "cash", "cash_stress"]
        or roles.get("policy_operational_metrics", {}).get("roles")
        != ["primary", "stress"]
        or roles.get("policy_operational_metrics", {}).get("cash_roles_included")
        is not False
        or roles.get("accounting_validity", {}).get("roles")
        != ["primary", "stress", "cash", "cash_stress"]
        or not isinstance(validity, Mapping)
        or validity.get(
            "blocked_missing_open_trade_count_must_be_exact_nonnegative_integer"
        )
        is not True
        or validity.get("blocked_missing_open_trade_count_is_hard_failure")
        is not False
        or validity.get(
            "blocked_capacity_trade_count_must_be_exact_nonnegative_integer"
        )
        is not True
        or validity.get("blocked_capacity_trade_count_is_hard_failure") is not False
        or validity.get("capacity_violation_count_at_most") != 0
        or validity.get("capacity_violation_is_frozen_execution_contract_breach")
        is not True
        or validity.get("negative_cash_observation_count_at_most") != 0
        or validity.get("leverage_observation_count_at_most") != 0
        or not isinstance(phases, Mapping)
        or phases.get("train_reclassification", {}).get("runtime_stage")
        != "runtime/data/multi-asset-8.0"
        or phases.get("validation", {}).get("source_root")
        != "runtime/data/multi-asset-8.1/sources/stage=validation"
        or phases.get("audit", {}).get("source_root")
        != "runtime/data/multi-asset-8.1/sources/stage=audit"
        or not isinstance(transport, Mapping)
        or transport.get("github_repository") != GITHUB_REPOSITORY
        or transport.get("missing_or_mismatched_remote_identity_allowed") is not False
        or not isinstance(claim, Mapping)
        or claim.get("alpha_claim_allowed") is not False
        or claim.get("profit_claim_allowed") is not False
        or claim.get("stable_future_profit_claim_allowed") is not False
        or claim.get("fresh_future_evidence_required") is not True
        or claim.get("minimum_fresh_sessions") != 252
        or claim.get("minimum_fresh_monthly_executions") != 12
        or claim.get("investment_recommendation_allowed") is not False
    ):
        raise ValueError("unexpected 8.1 protocol contract")

    prior_protocol = _json(PRIOR_PROTOCOL_PATH)
    receipt = _json(PRIOR_RECEIPT_PATH)
    if (
        file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or file_sha256(ROOT / PRIOR_PROTOCOL_PATH) != PRIOR_PROTOCOL_FILE_SHA256
        or prior_protocol.get("payload_sha256") != PRIOR_PROTOCOL_PAYLOAD
        or protocol.get("shared_absolute_gate")
        != prior_protocol.get("shared_absolute_gate")
        or protocol.get("claim_contract") != prior_protocol.get("claim_contract")
        or file_sha256(ROOT / PRIOR_RECEIPT_PATH) != PRIOR_RECEIPT_FILE_SHA256
        or receipt.get("payload_sha256") != PRIOR_RECEIPT_PAYLOAD
        or train_input.get("role_gate_metrics")
        != receipt.get("train_stage", {}).get("role_gate_metrics")
        or train_input.get("historical_summary")
        != receipt.get("train_stage", {}).get("historical_summary")
        or receipt.get("archive_contract", {}).get("permitted_change")
        != SOLE_CORRECTION
    ):
        raise ValueError("8.1 frozen source bytes or receipt metrics differ")


def _verify_prior_runtime_admission(
    helpers: Any, protocol: Mapping[str, Any]
) -> dict[str, Any]:
    """Require retained 8.0 train integrity before freezing the 8.1 closure."""

    for name in (
        "_verify_prior_train_artifacts",
        "_combine_receipt_role_gate_metrics",
        "_require_execution_validity",
    ):
        if not callable(getattr(helpers, name, None)):
            raise ValueError(f"8.1 runner lacks prior-runtime admission helper: {name}")
    receipt = _json(PRIOR_RECEIPT_PATH)
    execution_validity = helpers._verify_prior_train_artifacts(receipt)
    if not isinstance(execution_validity, Mapping):
        raise ValueError("8.1 prior-runtime admission did not return an object")
    metrics = helpers._combine_receipt_role_gate_metrics(
        receipt["train_stage"]["role_gate_metrics"],
        execution_validity=execution_validity,
    )
    helpers._require_execution_validity(
        metrics, protocol["execution_validity_hard_fail"]
    )
    if (
        execution_validity.get("source")
        != "receipt_bound_8_0_train_artifacts"
        or execution_validity.get("artifact_parquet_count") != 20
        or execution_validity.get("artifact_row_count") != 43222
    ):
        raise ValueError("8.1 prior-runtime admission identity differs")
    return dict(execution_validity)


def main() -> int:
    if (ROOT / CLOSURE_PATH).exists() or (ROOT / CLOSURE_PATH).is_symlink():
        raise FileExistsError("8.1 preselection closure is create-only")
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"8.1 closure requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("8.1 closure requires a clean implementation commit")
    for path in FORBIDDEN_BEFORE_CLOSURE:
        if (ROOT / path).exists() or (ROOT / path).is_symlink():
            raise RuntimeError(f"formal 8.1 runtime, evidence or closure already exists: {path}")

    commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    helpers = _runner_helpers()
    implementation_paths = _verify_runner_contract(helpers)
    helpers._require_source_imports()
    helpers._require_head_pushed_and_ci_success(commit)
    runtime = helpers._runtime_identity()

    protocol = _json(PROTOCOL_PATH)
    _validate_protocol(protocol)
    prior_release, train_source = _verify_prior_release()
    execution_validity = _verify_prior_runtime_admission(helpers, protocol)
    train_source = {
        **train_source,
        "execution_validity_sha256": canonical_payload_sha256(execution_validity),
        "artifact_parquet_count": execution_validity["artifact_parquet_count"],
        "artifact_row_count": execution_validity["artifact_row_count"],
    }

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
        PRIOR_PROTOCOL_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_RECEIPT_PATH,
    ):
        if _git("show", f"{commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(f"implementation commit lacks contract bytes: {relative}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": RELEASE,
        "closure_role": "policy_operational_reclassification_prevalidation_root",
        "direction_change": False,
        "route": protocol["route"],
        "status": "implementation_frozen_before_8_1_reclassification",
        "post_hoc_reclassification": True,
        "prior_train_returns_opened": True,
        "train_reexecution_allowed": False,
        "train_reclassification_status": "pending",
        "validation_market_outcomes_opened": False,
        "audit_status": "not_opened",
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
            "payload_sha256": protocol["payload_sha256"],
            "protocol_id": protocol["protocol_id"],
        },
        "prior_release": prior_release,
        "train_reclassification_source": train_source,
        "implementation_commit": commit,
        "implementation_tree": tree,
        "implementation": implementation,
        "runtime": runtime,
        "formal_data": {},
        "claim_contract": protocol["claim_contract"],
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if set(closure) != CLOSURE_FIELDS:
        raise RuntimeError("8.1 closure builder emitted an unexpected field set")
    if _git("rev-parse", "HEAD").decode("ascii").strip() != commit:
        raise RuntimeError("HEAD changed while building the 8.1 closure")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("worktree changed while building the 8.1 closure")
    helpers._require_head_pushed_and_ci_success(commit)
    _create_only(CLOSURE_PATH, closure)
    print(f"implementation_commit={commit}")
    print(f"payload_sha256={closure['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
