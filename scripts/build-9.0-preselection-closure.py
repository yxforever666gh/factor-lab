#!/usr/bin/env python
"""Freeze the clean 9.0 volatility-balanced implementation before replay."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256  # noqa: E402


RELEASE = "9.0"
ROUTE = "causal_monthly_volatility_balanced_budget"
PROTOCOL_ID = "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
STRATEGY_ID = ROUTE
SCOUT_PATH = Path("protocols/9.0-preprotocol-scout.json")
PROTOCOL_PATH = Path("protocols/9.0-causal-volatility-balanced-budget.json")
CLOSURE_PATH = Path("protocols/9.0-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/9.0")
WORK_ROOT = Path("runtime/data/multi-asset-9.0")
RUNNER_PATH = Path("scripts/run-multi-asset-evidence.py")
INHERITED_CONTRACT_PATHS = (
    Path("protocols/7.0-asset-selection.json"),
    Path("protocols/7.0-multi-asset.json"),
    Path("protocols/8.0-static-capital-budget.json"),
)

SCOUT_PAYLOAD = "71926f08ce5ca2ab1b6470f7d3ee385371c4bfaf3243c5f942a891f63a8075a0"
SCOUT_FILE_SHA256 = "44b90b964ecca9a30029b1dfad45ae313ae4a5c12a91d82ba885ceecb826b857"
PROTOCOL_PAYLOAD = "f6c7cce39e8b9a1ae5df10965a2dd607916095b2caf24fcf0a29b625c5bafc3e"
PROTOCOL_FILE_SHA256 = "19ecf56b5bd9c8b42b9f4df50761f719e2ca544eaea959a88c62d0ea4178d620"
BINARY64_NORMALIZATION = (
    "in fixed RISK_CODES order compute the first four weights as raw_i / "
    "math.fsum(raw), then set the fifth weight to 1.0 - math.fsum(the first "
    "four); the residual-normalized risk-weight sum is accepted within the "
    "frozen 1e-12 target-sum tolerance"
)

PRIOR_TAG = "8.1"
PRIOR_TAG_OBJECT = "8f575ed3833c8cc01f89e7a951d4234bd7ee6622"
PRIOR_COMMIT = "a4c0d36f727e99f6b2353facf24fd3cdedba958e"
PRIOR_FILES: dict[Path, dict[str, Any]] = {
    Path("protocols/8.1-policy-operational-metric-reclassification.json"): {
        "file_sha256": "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583",
        "payload_sha256": "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5",
        "status": "frozen_after_8_0_train_evaluation_before_8_1_reclassification",
    },
    Path("protocols/8.1-release.json"): {
        "file_sha256": "ef1596fa5cfbfdfd0c27d74c2747dcc852b7f209a4e27de2b7c01c6d8dbcc557",
        "payload_sha256": "f4a47421d08ca77eca6b27fd6417909a04c3eaf789c11d9ca069366412440ef5",
        "status": "implementation_frozen_before_8_1_reclassification",
    },
    Path("protocols/evidence/8.1/train-reclassification.json"): {
        "file_sha256": "bfd2c0c801259394861eba000a8e34bc9617cba3adcf6629d7e8b501ccf3c51b",
        "payload_sha256": "4f498ffc12deac61144c77c56ba89cb9abccc034d2d73df4f1df8a6c50184c79",
        "status": "train_reclassification_passed",
    },
    Path("protocols/evidence/8.1/winner-freeze.json"): {
        "file_sha256": "b865e80cb899f7e5274d72b46ab1e0d88dad64b0ab2eb4e46750c5cec2167387",
        "payload_sha256": "d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76",
        "status": "selected_null_frozen_validation_failed",
    },
    Path("protocols/evidence/8.1/result.json"): {
        "file_sha256": "bcbcb09974e6314190de7a835560c4abbc1cde79734ed4fcef759061653cd95d",
        "payload_sha256": "d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4",
        "status": "selection_falsified_no_candidate",
    },
}
PRIOR_VALIDATION_IDENTITIES = {
    "validation_manifest_payload_sha256": "f5903d2b24b47662a9ba4ea3d2d127c9b5dee385d5b927140b25eda68b3ff060",
    "validation_binding_payload_sha256": "7479aa06071d34544b6ce880d6a2986a09988e3905853dbab7127eaeb0e13d5b",
    "validation_evaluation_payload_sha256": "7794ee8c81cc784d262a464c55d37f3017b1e75cbc4bb421b5e4b8eb85685981",
    "artifact_parquet_count": 20,
    "artifact_row_count": 62654,
}
GITHUB_REPOSITORY = "yxforever666gh/factor-lab"
FORBIDDEN_BEFORE_CLOSURE = (WORK_ROOT, EVIDENCE_ROOT, CLOSURE_PATH)
EXPECTED_IMPLEMENTATION_PATHS = {
    ".github/workflows/ci.yml",
    "configs/data.json",
    "pyproject.toml",
    "scripts/build-9.0-preselection-closure.py",
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
}
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
CLOSURE_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "closure_role",
    "direction_change",
    "route",
    "status",
    "development_outcomes_opened",
    "audit_market_outcomes_opened",
    "protocol",
    "preprotocol_scout",
    "prior_8_1_archive",
    "implementation_commit",
    "implementation_tree",
    "implementation",
    "runtime",
    "formal_data",
    "claim_contract",
    "payload_sha256",
}


class _GitCommandError(RuntimeError):
    def __init__(
        self, message: str, *, returncode: int, stdout: bytes, stderr: bytes
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


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
            stderr=completed.stderr,
        )
    return completed.stdout


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_commit(value: Any) -> bool:
    return isinstance(value, str) and _COMMIT_RE.fullmatch(value) is not None


def _is_transport_failure(error: _GitCommandError) -> bool:
    if error.returncode in (0, 2) or error.stdout.strip():
        return False
    message = error.stderr.decode("utf-8", errors="replace").casefold()
    return any(
        marker in message
        for marker in (
            "could not resolve host",
            "failed to connect",
            "connection timed out",
            "connection reset",
            "network is unreachable",
            "empty reply from server",
            "tls connection",
            "ssl connect error",
            "the remote end hung up unexpectedly",
        )
    )


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
        raise ValueError("GitHub API fallback must return an object")
    return value


def _github_prior_tag_refs() -> dict[str, str]:
    ref = _github_api(f"repos/{GITHUB_REPOSITORY}/git/ref/tags/{PRIOR_TAG}")
    object_value = ref.get("object")
    if (
        ref.get("ref") != f"refs/tags/{PRIOR_TAG}"
        or not isinstance(object_value, Mapping)
        or object_value.get("type") != "tag"
        or object_value.get("sha") != PRIOR_TAG_OBJECT
    ):
        raise ValueError("GitHub 8.1 tag object differs")
    tag = _github_api(
        f"repos/{GITHUB_REPOSITORY}/git/tags/{PRIOR_TAG_OBJECT}"
    )
    peeled = tag.get("object")
    if (
        tag.get("sha") != PRIOR_TAG_OBJECT
        or not isinstance(peeled, Mapping)
        or peeled.get("type") != "commit"
        or peeled.get("sha") != PRIOR_COMMIT
    ):
        raise ValueError("GitHub 8.1 peeled tag differs")
    return {
        f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
        f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
    }


def _remote_prior_tag_refs() -> dict[str, str]:
    try:
        raw = _git(
            "ls-remote",
            "--exit-code",
            "origin",
            f"refs/tags/{PRIOR_TAG}",
            f"refs/tags/{PRIOR_TAG}^{{}}",
        ).decode("ascii")
    except _GitCommandError as exc:
        if exc.returncode == 2 and not exc.stdout.strip():
            return {}
        if _is_transport_failure(exc):
            return _github_prior_tag_refs()
        raise
    except UnicodeDecodeError as exc:
        raise ValueError("remote 8.1 tag response is malformed") from exc
    if not raw.strip():
        return {}
    try:
        pairs = [line.split() for line in raw.splitlines()]
        if any(len(pair) != 2 for pair in pairs):
            raise ValueError
        refs = {pair[1]: pair[0] for pair in pairs}
    except ValueError as exc:
        raise ValueError("remote 8.1 tag response is malformed") from exc
    return refs


def _read_json(path: Path) -> dict[str, Any]:
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


def _runner_helpers() -> Any:
    path = ROOT / RUNNER_PATH
    spec = importlib.util.spec_from_file_location("factor_lab_v90_closure_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the frozen 9.0 runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _verify_runner_contract(helpers: Any) -> tuple[str, ...]:
    expected_paths = getattr(helpers, "EXPECTED_IMPLEMENTATION_PATHS", None)
    if not isinstance(expected_paths, (set, frozenset)) or not expected_paths:
        raise ValueError("9.0 runner lacks an exact implementation path set")
    paths = tuple(sorted(map(str, expected_paths)))
    stages = getattr(helpers, "STAGES", None)
    prior_verifier = getattr(helpers, "_verify_prior_8_1_archive", None)
    protocol_verifier = getattr(helpers, "_v9_verify_protocol_contract", None)
    if (
        getattr(helpers, "RELEASE", None) != RELEASE
        or getattr(helpers, "ROUTE", None) != ROUTE
        or getattr(helpers, "PROTOCOL_ID", None) != PROTOCOL_ID
        or Path(getattr(helpers, "SCOUT_PATH", "")) != SCOUT_PATH
        or Path(getattr(helpers, "PROTOCOL_PATH", "")) != PROTOCOL_PATH
        or getattr(helpers, "PROTOCOL_PAYLOAD", None) != PROTOCOL_PAYLOAD
        or getattr(helpers, "PROTOCOL_FILE_SHA256", None) != PROTOCOL_FILE_SHA256
        or Path(getattr(helpers, "CLOSURE_PATH", "")) != CLOSURE_PATH
        or Path(getattr(helpers, "EVIDENCE_ROOT", "")) != EVIDENCE_ROOT
        or Path(getattr(helpers, "WORK_ROOT", "")).resolve()
        != (ROOT / WORK_ROOT).resolve()
        or getattr(helpers, "PRIOR_TAG", None) != PRIOR_TAG
        or getattr(helpers, "PRIMARY_ID", None) != STRATEGY_ID
        or not callable(prior_verifier)
        or not callable(protocol_verifier)
        or not isinstance(stages, Mapping)
        or set(stages) != {"development", "audit"}
        or stages["development"]
        != {
            "source_start": "2014-01-15",
            "source_end": "2022-12-30",
            "performance_start": "2015-03-02",
            "performance_end": "2022-12-30",
        }
        or stages["audit"]
        != {
            "source_start": "2014-01-15",
            "source_end": "2026-08-28",
            "performance_start": "2023-01-03",
            "performance_end": "2026-08-28",
        }
        or set(paths) != EXPECTED_IMPLEMENTATION_PATHS
    ):
        raise ValueError("runner has not migrated to the exact 9.0 namespace")
    return paths


def _validate_scout(scout: Mapping[str, Any]) -> None:
    decision = scout.get("selection_decision")
    prototypes = scout.get("prototypes")
    claim = scout.get("claim_contract")
    if (
        scout.get("payload_sha256") != SCOUT_PAYLOAD
        or canonical_payload_sha256(scout) != SCOUT_PAYLOAD
        or file_sha256(ROOT / SCOUT_PATH) != SCOUT_FILE_SHA256
        or scout.get("release") != RELEASE
        or scout.get("route") != ROUTE
        or scout.get("direction_change") is not True
        or scout.get("status")
        != "selected_volatility_balanced_after_fully_exposed_development"
        or scout.get("scope", {}).get("market_data_maximum_date") != "2022-12-30"
        or scout.get("scope", {}).get("audit_2023_plus_read") is not False
        or scout.get("uniform_development_contract", {}).get(
            "fresh_cash_each_development_subperiod"
        )
        is not True
        or scout.get("uniform_development_contract", {}).get(
            "development_subperiod_account_reuse"
        )
        is not False
        or not isinstance(prototypes, Mapping)
        or set(prototypes)
        != {
            "causal_volatility_balanced_budget_v0",
            "causal_three_expert_exponentiated_gradient_v0",
        }
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "selected_for_formalization"
        )
        is not True
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "formula", {}
        ).get("volatility_floor")
        != 1e-12
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "formula", {}
        ).get("normalization")
        != BINARY64_NORMALIZATION
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "captured_artifacts", {}
        ).get("prototype_file_sha256")
        != "8dcf6617a25d49be7929b9f5348983f84daaf81c07e24a4c3432e602e1f5b218"
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "captured_artifacts", {}
        ).get("result_file_sha256")
        != "665344f3bced8f7dad1437fac60a6c10e74d5be36dcd37b072582ff48e0dbcf3"
        or prototypes["causal_volatility_balanced_budget_v0"].get(
            "captured_artifacts", {}
        ).get("accounting_annotation_audit", {}).get(
            "corrected_source_and_result_annotations_match"
        )
        is not True
        or prototypes["causal_three_expert_exponentiated_gradient_v0"].get(
            "selected_for_formalization"
        )
        is not False
        or not isinstance(decision, Mapping)
        or decision.get("selected_strategy_id") != STRATEGY_ID
        or decision.get("formal_candidate_count") != 1
        or decision.get("runner_up_fallback") is not False
        or not isinstance(claim, Mapping)
        or claim.get("research_object")
        != "causal_volatility_balanced_strategic_beta"
        or claim.get("development_is_fully_exposed_non_oos") is not True
        or claim.get("profit_claim_allowed") is not False
    ):
        raise ValueError("unexpected 9.0 scout contract")


def _validate_protocol(
    protocol: Mapping[str, Any], scout: Mapping[str, Any]
) -> None:
    strategy = protocol.get("candidate_registry", {}).get("strategy")
    shared = protocol.get("shared_absolute_gate")
    relative = protocol.get("relative_stability_gate")
    phases = protocol.get("physical_phases")
    selection = protocol.get("selection_contract")
    claim = protocol.get("claim_contract")
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(protocol) != PROTOCOL_PAYLOAD
        or file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or protocol.get("protocol_id") != PROTOCOL_ID
        or protocol.get("release") != RELEASE
        or protocol.get("direction_change") is not True
        or protocol.get("route") != ROUTE
        or protocol.get("strategy_id") != STRATEGY_ID
        or protocol.get("status")
        != "frozen_after_fully_exposed_development_before_formal_replay_and_first_2023_plus_audit"
        or protocol.get("preprotocol_scout", {}).get("payload_sha256")
        != scout.get("payload_sha256")
        or protocol.get("preprotocol_scout", {}).get("file_sha256")
        != file_sha256(ROOT / SCOUT_PATH)
        or not isinstance(strategy, Mapping)
        or strategy.get("strategy_id") != STRATEGY_ID
        or strategy.get("volatility_return_count") != 126
        or strategy.get("required_observed_total_return_levels") != 127
        or strategy.get("volatility_floor") != 1e-12
        or strategy.get("normalization") != BINARY64_NORMALIZATION
        or strategy.get("parameter_grid") is not False
        or strategy.get("per_asset_cap") is not None
        or strategy.get("portfolio_target_volatility") is not None
        or strategy.get("runner_up_fallback") is not False
        or protocol.get("development_disclosure", {}).get(
            "fresh_cash_each_development_subperiod"
        )
        is not True
        or protocol.get("development_disclosure", {}).get(
            "development_subperiod_account_reuse"
        )
        is not False
        or not isinstance(shared, Mapping)
        or shared.get("base", {}).get("net_sharpe_at_least") != 0.3
        or shared.get("stress_16bp", {}).get("net_sharpe_at_least") != 0.25
        or shared.get("operational", {}).get("annualized_turnover_at_most")
        != 1.0
        or relative
        != {
            "sharpe_delta_at_least": 0.0,
            "max_drawdown_delta_at_least": 0.0,
            "positive_complete_year_ratio_delta_at_least": 0.0,
        }
        or not isinstance(phases, Mapping)
        or phases.get("development", {}).get("source_root")
        != "runtime/data/multi-asset-8.1/sources/stage=validation"
        or phases.get("development", {}).get("runtime_stage") is not None
        or phases.get("development", {}).get("fresh_source_stage_created")
        is not False
        or phases.get("development", {}).get(
            "fresh_cash_each_development_subperiod"
        )
        is not True
        or phases.get("development", {}).get(
            "development_subperiod_account_reuse"
        )
        is not False
        or phases.get("audit", {}).get("market_outcome_opened") is not False
        or phases.get("audit", {}).get("minimum_positive_complete_year_count")
        != 2
        or not isinstance(selection, Mapping)
        or selection.get("candidate_count") != 1
        or selection.get("strategy_id") != STRATEGY_ID
        or selection.get("parameter_grid") is not False
        or selection.get("runner_up_fallback") is not False
        or not isinstance(claim, Mapping)
        or claim.get("research_object")
        != "causal_volatility_balanced_strategic_beta"
        or claim.get("development_is_fully_exposed_non_oos") is not True
        or any(
            claim.get(key) is not False
            for key in (
                "alpha_claim_allowed",
                "profit_claim_allowed",
                "stable_future_profit_claim_allowed",
                "investment_recommendation_allowed",
            )
        )
    ):
        raise ValueError("unexpected 9.0 protocol contract")


def _verify_prior_release() -> dict[str, Any]:
    if (
        _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
        or _remote_prior_tag_refs()
        != {
            f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
            f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
        }
    ):
        raise ValueError("published 8.1 annotated tag binding differs")
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("9.0 HEAD does not descend from published 8.1")

    bindings: dict[str, Any] = {}
    values: dict[Path, dict[str, Any]] = {}
    for path, expected in PRIOR_FILES.items():
        current = (ROOT / path).read_bytes()
        tagged = _git("show", f"{PRIOR_COMMIT}:{path.as_posix()}")
        value = json.loads(current.decode("utf-8"))
        if (
            current != tagged
            or hashlib.sha256(current).hexdigest() != expected["file_sha256"]
            or not isinstance(value, dict)
            or value.get("payload_sha256") != expected["payload_sha256"]
            or canonical_payload_sha256(value) != expected["payload_sha256"]
            or value.get("status") != expected["status"]
        ):
            raise ValueError(f"published 8.1 file binding differs: {path}")
        values[path] = value
        bindings[path.as_posix()] = {
            "path": path.as_posix(),
            "file_sha256": expected["file_sha256"],
            "payload_sha256": expected["payload_sha256"],
            "status": expected["status"],
        }
    freeze = values[Path("protocols/evidence/8.1/winner-freeze.json")]
    result = values[Path("protocols/evidence/8.1/result.json")]
    if (
        freeze.get("selected_candidate_id") is not None
        or freeze.get("validation_market_outcomes_opened") is not True
        or freeze.get("audit_market_outcomes_opened") is not False
        or result.get("selected_candidate_id") is not None
        or result.get("audit_status") != "not_opened"
    ):
        raise ValueError("published 8.1 null/audit boundary differs")
    return {
        "release": "8.1",
        "tag": PRIOR_TAG,
        "tag_object": PRIOR_TAG_OBJECT,
        "tag_commit": PRIOR_COMMIT,
        "files": bindings,
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
    }


def _verify_prior_runtime_admission(helpers: Any) -> dict[str, Any]:
    state = helpers._verify_prior_8_1_archive(
        verify_data=True, verify_runtime=True
    )
    if not isinstance(state, Mapping):
        raise ValueError("9.0 runner prior-archive verifier returned no object")
    expected = {
        "release": "8.1",
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "deep_data_verified": True,
        "deep_runtime_verified": True,
        **PRIOR_VALIDATION_IDENTITIES,
    }
    if any(state.get(key) != value for key, value in expected.items()):
        raise ValueError("retained 8.1 validation admission differs")
    archive_identity = state.get("archive_identity_sha256")
    if not _is_sha256(archive_identity):
        raise ValueError("retained 8.1 archive lacks its identity hash")
    return dict(state)


def main() -> int:
    if (ROOT / CLOSURE_PATH).exists() or (ROOT / CLOSURE_PATH).is_symlink():
        raise FileExistsError("9.0 preselection closure is create-only")
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"9.0 closure requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("9.0 closure requires a clean implementation commit")
    for path in FORBIDDEN_BEFORE_CLOSURE:
        if (ROOT / path).exists() or (ROOT / path).is_symlink():
            raise RuntimeError(
                f"formal 9.0 runtime, evidence or closure already exists: {path}"
            )

    commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    helpers = _runner_helpers()
    implementation_paths = _verify_runner_contract(helpers)
    helpers._require_source_imports()
    helpers._require_head_pushed_and_ci_success(commit)
    runtime = helpers._runtime_identity()

    scout = _read_json(SCOUT_PATH)
    protocol = _read_json(PROTOCOL_PATH)
    _validate_scout(scout)
    _validate_protocol(protocol, scout)
    helpers._v9_verify_protocol_contract(protocol)
    prior_release = _verify_prior_release()
    prior_archive = _verify_prior_runtime_admission(helpers)
    prior_archive = {
        **prior_release,
        **prior_archive,
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
        *PRIOR_FILES,
        *INHERITED_CONTRACT_PATHS,
        SCOUT_PATH,
        PROTOCOL_PATH,
    ):
        if _git("show", f"{commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(f"implementation commit lacks contract bytes: {relative}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": RELEASE,
        "closure_role": "causal_volatility_balanced_preselection_root",
        "direction_change": True,
        "route": ROUTE,
        "status": "implementation_frozen_before_formal_development_replay",
        "development_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / PROTOCOL_PATH),
            "payload_sha256": protocol["payload_sha256"],
            "protocol_id": protocol["protocol_id"],
        },
        "preprotocol_scout": {
            "path": SCOUT_PATH.as_posix(),
            "file_sha256": file_sha256(ROOT / SCOUT_PATH),
            "payload_sha256": scout["payload_sha256"],
            "status": scout["status"],
        },
        "prior_8_1_archive": prior_archive,
        "implementation_commit": commit,
        "implementation_tree": tree,
        "implementation": implementation,
        "runtime": runtime,
        "formal_data": {},
        "claim_contract": protocol["claim_contract"],
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if set(closure) != CLOSURE_FIELDS:
        raise RuntimeError("9.0 closure builder emitted an unexpected field set")
    if _git("rev-parse", "HEAD").decode("ascii").strip() != commit:
        raise RuntimeError("HEAD changed while building the 9.0 closure")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("worktree changed while building the 9.0 closure")
    helpers._require_head_pushed_and_ci_success(commit)
    _create_only(CLOSURE_PATH, closure)
    print(f"implementation_commit={commit}")
    print(f"payload_sha256={closure['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
