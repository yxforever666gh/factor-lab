"""Shared integrity root for the pre-selection 6.1 implementation closure."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import platform
import subprocess
import sys
from typing import Any, Mapping


PROTOCOL_ID = "factor-lab/6.1/widened-opportunity-set-v1"
RUNTIME_ID = "factor-lab/6.1/windows-cpython-3.10.16"
RUNTIME_PATH = "protocols/6.1-runtime.json"
WINNER_FREEZE_PATH = "protocols/evidence/6.1/winner-freeze.json"
AUDIT_EVIDENCE_PATH = "protocols/evidence/6.1/historical-audit.json"
RELEASE_RESULT_PATH = "protocols/evidence/6.1/result.json"
FROZEN_HISTORICAL_AUDIT = {
    "signal_start": "2025-01-01",
    "physical_market_data_end": "2026-08-21",
    "single_frozen_cutoff": True,
    "result_write_policy": "create_only",
}
FROZEN_IMPLEMENTATION_PATHS = {
    "git_attributes": ".gitattributes",
    "package_init": "src/factor_lab/__init__.py",
    "package_manifest": "pyproject.toml",
    "release_integrity": "src/factor_lab/release_integrity.py",
    "closure_builder": "scripts/build-6.1-preselection-closure.py",
    "release_script": "scripts/publish-tag.ps1",
    "releasing_guide": "RELEASING.md",
    "cli": "src/factor_lab/cli.py",
    "ci_workflow": ".github/workflows/ci.yml",
    "data_catalog": "src/factor_lab/data/catalog.py",
    "data_config": "configs/data.json",
    "data_init": "src/factor_lab/data/__init__.py",
    "data_build": "src/factor_lab/data/build.py",
    "data_enrich": "src/factor_lab/data/enrich.py",
    "data_pit_lineage": "src/factor_lab/data/pit_lineage.py",
    "data_sources": "src/factor_lab/data/sources.py",
    "execution_kernel": "src/factor_lab/portfolio/execution.py",
    "long_only": "src/factor_lab/portfolio/long_only.py",
    "opportunity_set": "src/factor_lab/data/opportunity_set.py",
    "portfolio_init": "src/factor_lab/portfolio/__init__.py",
    "research_config": "configs/research.json",
    "research_contracts": "src/factor_lab/research/contracts.py",
    "research_init": "src/factor_lab/research/__init__.py",
    "research_reporting": "src/factor_lab/research/reporting.py",
    "research_runner": "src/factor_lab/research/runner.py",
    "research_signals": "src/factor_lab/research/signals.py",
    "research_validation": "src/factor_lab/research/validation.py",
    "research_walk_forward": "src/factor_lab/research/walk_forward.py",
    "research_walk_forward_runtime": (
        "src/factor_lab/research/walk_forward_runtime.py"
    ),
    "security_master": "src/factor_lab/data/security_master.py",
    "strategy": "src/factor_lab/strategy.py",
    "suspensions": "src/factor_lab/data/suspensions.py",
    "wide_pricing": "src/factor_lab/data/wide_pricing.py",
    "wide_runner": "scripts/run-wide-universe-evidence.py",
    "wide_universe": "src/factor_lab/research/wide_universe.py",
}

_CLOSURE_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "closure_role",
    "direction_change",
    "status",
    "selection_returns_opened",
    "route",
    "selected_candidate_id",
    "audit_status",
    "historical_audit",
    "protocol",
    "protocol_amendment",
    "runtime",
    "implementation",
    "evidence",
    "canonical_data",
    "claim_contract",
    "implementation_commit",
    "implementation_tree",
    "payload_sha256",
}
_WINNER_FREEZE_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "protocol_payload_sha256",
    "protocol_amendment_payload_sha256",
    "implementation_closure_payload_sha256",
    "selected_candidate_id",
    "runner_up_fallback_after_audit_fail",
    "train_passers",
    "train_gates",
    "validation_gates",
    "turnover_by_candidate",
    "train_manifest_payload_sha256",
    "validation_manifest_payload_sha256",
    "selection_status_sources",
    "train_phase_replay_sha256",
    "validation_phase_replay_sha256",
    "selected_definition",
    "audit_market_outcomes_opened",
    "selection_execution_commit",
    "payload_sha256",
}
_AUDIT_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "selected_candidate_id",
    "winner_freeze_payload_sha256",
    "audit_manifest_payload_sha256",
    "audit_end",
    "gate",
    "runner_up_fallback_used",
    "profit_claim_allowed",
    "audit_execution_commit",
    "payload_sha256",
}
_RESULT_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "status",
    "preselection_closure_payload_sha256",
    "winner_freeze",
    "selected_candidate_id",
    "audit_status",
    "audit",
    "runner_up_fallback_used",
    "claim_contract",
    "payload_sha256",
}
_RUNTIME_FIELDS = {
    "schema_version",
    "kind",
    "runtime_id",
    "status",
    "source_package_version",
    "python",
    "platform",
    "packages",
    "distribution_contents",
    "conda_artifacts",
    "numeric_backend",
    "package_pin_source",
    "payload_sha256",
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"release artifact is outside the project root: {path}") from exc


def _require_tracked_head_blob(root: Path, path: Path) -> None:
    relative = _relative(root, path)
    committed = _git(root, "show", f"HEAD:{relative}").stdout
    if committed != path.read_bytes():
        raise ValueError(f"artifact is not the exact tracked HEAD blob: {relative}")


def _require_terminal_binding(
    root: Path,
    raw: Any,
    *,
    expected_relative_path: str,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != {
        "path",
        "file_sha256",
        "payload_sha256",
    }:
        raise ValueError(f"terminal binding fields differ: {expected_relative_path}")
    if raw.get("path") != expected_relative_path:
        raise ValueError(f"terminal binding path differs: {expected_relative_path}")
    path = (root / expected_relative_path).resolve()
    if (
        not path.is_file()
        or path.is_symlink()
        or file_sha256(path) != raw.get("file_sha256")
    ):
        raise ValueError(f"terminal binding file differs: {expected_relative_path}")
    value = _read_json(path)
    if (
        value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("payload_sha256") != raw.get("payload_sha256")
    ):
        raise ValueError(f"terminal binding payload differs: {expected_relative_path}")
    _require_tracked_head_blob(root, path)
    return value


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_replay_map(
    raw: Any,
    *,
    expected_candidates: tuple[str, ...],
    label: str,
) -> None:
    if not isinstance(raw, Mapping) or set(raw) != set(expected_candidates):
        raise ValueError(f"{label} candidate set differs")
    for candidate_id in expected_candidates:
        values = raw.get(candidate_id)
        if (
            not isinstance(values, list)
            or len(values) != 10
            or not all(_is_sha256(value) for value in values)
        ):
            raise ValueError(f"{label} does not bind ten exact phase payloads")


def _winner_from_frozen_gates(freeze: Mapping[str, Any]) -> str | None:
    """Recompute the unique selection outcome from the frozen gate summaries."""

    from factor_lab.research.wide_universe import (  # local to keep CLI startup light
        CHALLENGER_IDS,
        select_winner,
    )

    train_gates = freeze.get("train_gates")
    validation_gates = freeze.get("validation_gates")
    turnover = freeze.get("turnover_by_candidate")
    train_passers = freeze.get("train_passers")
    if (
        not isinstance(train_gates, Mapping)
        or set(train_gates) != set(CHALLENGER_IDS)
        or not isinstance(validation_gates, Mapping)
        or not isinstance(turnover, Mapping)
        or not isinstance(train_passers, list)
    ):
        raise ValueError("winner-freeze gate collections differ")
    for candidate_id in CHALLENGER_IDS:
        gate = train_gates[candidate_id]
        if not isinstance(gate, Mapping) or not isinstance(gate.get("passed"), bool):
            raise ValueError("winner-freeze train gate is malformed")
    expected_passers = [
        candidate_id
        for candidate_id in CHALLENGER_IDS
        if train_gates[candidate_id]["passed"] is True
    ]
    if train_passers != expected_passers:
        raise ValueError("winner-freeze train passers differ from train gates")
    if set(validation_gates) != set(expected_passers) or set(turnover) != set(
        expected_passers
    ):
        raise ValueError("winner-freeze validation candidates differ from train passers")
    for candidate_id in expected_passers:
        gate = validation_gates[candidate_id]
        value = turnover[candidate_id]
        if not isinstance(gate, Mapping) or not isinstance(gate.get("passed"), bool):
            raise ValueError("winner-freeze validation gate is malformed")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0.0
        ):
            raise ValueError("winner-freeze turnover is invalid")
    return select_winner(
        train_gates,
        validation_gates,
        turnover_by_candidate={
            candidate_id: float(turnover[candidate_id])
            for candidate_id in expected_passers
        },
    )


def _require_file_binding(
    root: Path,
    binding: Mapping[str, Any],
    *,
    expected_relative_path: str,
    payload_id_field: str,
    payload_id: str,
) -> dict[str, Any]:
    if set(binding) != {
        "path",
        "file_sha256",
        "payload_sha256",
        payload_id_field,
    }:
        raise ValueError(f"release binding fields differ: {expected_relative_path}")
    if binding.get("path") != expected_relative_path:
        raise ValueError(f"release binding path differs: {expected_relative_path}")
    path = (root / expected_relative_path).resolve()
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"release binding is not a regular file: {path}")
    if file_sha256(path) != str(binding.get("file_sha256") or ""):
        raise ValueError(f"release binding file hash differs: {expected_relative_path}")
    payload = _read_json(path)
    if (
        payload.get("payload_sha256") != canonical_payload_sha256(payload)
        or payload.get("payload_sha256") != binding.get("payload_sha256")
        or payload.get(payload_id_field) != payload_id
        or binding.get(payload_id_field) != payload_id
    ):
        raise ValueError(f"release binding payload differs: {expected_relative_path}")
    return payload


def verify_preselection_closure(
    project_root: Path,
    *,
    closure_path: Path,
    protocol_path: Path,
    amendment_path: Path,
) -> dict[str, Any]:
    """Verify the same strict pre-selection contract in the runner and CI CLI."""

    root = project_root.resolve()
    closure_path = closure_path.resolve()
    protocol_path = protocol_path.resolve()
    amendment_path = amendment_path.resolve()
    if _relative(root, closure_path) != "protocols/6.1-release.json":
        raise ValueError("unexpected 6.1 closure path")
    if _relative(root, protocol_path) != "protocols/6.1-wide-universe.json":
        raise ValueError("unexpected 6.1 protocol path")
    if (
        _relative(root, amendment_path)
        != "protocols/6.1-wide-universe-amendment-1.json"
    ):
        raise ValueError("unexpected 6.1 amendment path")

    closure = _read_json(closure_path)
    if set(closure) != _CLOSURE_FIELDS:
        raise ValueError("6.1 closure contains missing or unknown fields")
    if closure.get("payload_sha256") != canonical_payload_sha256(closure):
        raise ValueError("6.1 release closure payload hash is invalid")
    if (
        closure.get("schema_version") != 1
        or closure.get("kind") != "factor_lab_release_closure"
        or closure.get("release") != "6.1"
        or closure.get("closure_role") != "immutable_preselection_root"
        or closure.get("direction_change") is not False
        or closure.get("status") != "implementation_frozen_before_selection"
        or closure.get("selection_returns_opened") is not False
        or closure.get("route") != "widened_opportunity_set"
        or closure.get("selected_candidate_id") is not None
        or closure.get("audit_status") != "not_opened"
        or closure.get("historical_audit") != FROZEN_HISTORICAL_AUDIT
        or closure.get("evidence") != {}
        or closure.get("canonical_data") != {}
        or closure.get("claim_contract")
        != {
            "incremental_universe_effect_only": True,
            "validates_fixed_core_alpha": False,
            "historical_evidence_class": (
                "pre_registered_historical_diagnostic_only"
            ),
            "historical_qualification_allowed": False,
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        }
    ):
        raise ValueError("6.1 release closure is not the pre-selection freeze")

    protocol_binding = closure.get("protocol")
    amendment_binding = closure.get("protocol_amendment")
    if not isinstance(protocol_binding, Mapping) or not isinstance(
        amendment_binding, Mapping
    ):
        raise ValueError("6.1 release closure lacks protocol bindings")
    protocol = _require_file_binding(
        root,
        protocol_binding,
        expected_relative_path="protocols/6.1-wide-universe.json",
        payload_id_field="protocol_id",
        payload_id=PROTOCOL_ID,
    )
    amendment = _require_file_binding(
        root,
        amendment_binding,
        expected_relative_path="protocols/6.1-wide-universe-amendment-1.json",
        payload_id_field="amendment_id",
        payload_id=f"{PROTOCOL_ID}/amendment-1",
    )
    runtime_binding = closure.get("runtime")
    if not isinstance(runtime_binding, Mapping):
        raise ValueError("6.1 release closure lacks its runtime binding")
    _require_file_binding(
        root,
        runtime_binding,
        expected_relative_path=RUNTIME_PATH,
        payload_id_field="runtime_id",
        payload_id=RUNTIME_ID,
    )
    base = amendment.get("base_protocol")
    if (
        amendment.get("protocol_id") != PROTOCOL_ID
        or amendment.get("wide_return_evaluation_opened_before_freeze") is not False
        or not isinstance(base, Mapping)
        or base.get("path") != protocol_binding.get("path")
        or base.get("file_sha256") != protocol_binding.get("file_sha256")
        or base.get("payload_sha256") != protocol.get("payload_sha256")
    ):
        raise ValueError("6.1 amendment does not bind the frozen protocol")

    implementation = closure.get("implementation")
    if not isinstance(implementation, Mapping) or set(implementation) != set(
        FROZEN_IMPLEMENTATION_PATHS
    ):
        raise ValueError("6.1 release closure implementation allowlist mismatch")
    implementation_commit = str(closure.get("implementation_commit") or "")
    resolved_commit = _git(
        root, "rev-parse", "--verify", f"{implementation_commit}^{{commit}}"
    ).stdout.decode("ascii").strip()
    if resolved_commit != implementation_commit:
        raise ValueError("6.1 implementation commit must be a full commit id")
    resolved_tree = _git(
        root, "rev-parse", f"{implementation_commit}^{{tree}}"
    ).stdout.decode("ascii").strip()
    if str(closure.get("implementation_tree") or "") != resolved_tree:
        raise ValueError("6.1 implementation tree differs from the frozen commit")
    if _git(
        root,
        "merge-base",
        "--is-ancestor",
        implementation_commit,
        "HEAD",
        check=False,
    ).returncode != 0:
        raise ValueError("6.1 implementation commit is not an ancestor of HEAD")

    for name, expected_relative_path in sorted(FROZEN_IMPLEMENTATION_PATHS.items()):
        raw = implementation[name]
        if not isinstance(raw, Mapping) or set(raw) != {"path", "sha256"}:
            raise ValueError(f"implementation binding fields differ: {name}")
        if raw.get("path") != expected_relative_path:
            raise ValueError(f"implementation binding path differs: {name}")
        path = (root / expected_relative_path).resolve()
        expected_sha256 = str(raw.get("sha256") or "")
        if (
            not path.is_file()
            or path.is_symlink()
            or file_sha256(path) != expected_sha256
        ):
            raise ValueError(f"implementation working-file binding failed: {name}")
        committed = _git(
            root, "show", f"{implementation_commit}:{expected_relative_path}"
        ).stdout
        if hashlib.sha256(committed).hexdigest() != expected_sha256:
            raise ValueError(f"implementation Git-blob binding failed: {name}")

    for name, path, binding in (
        ("protocol", protocol_path, protocol_binding),
        ("protocol_amendment", amendment_path, amendment_binding),
        ("runtime", root / RUNTIME_PATH, runtime_binding),
    ):
        relative_path = _relative(root, path)
        committed = _git(
            root, "show", f"{implementation_commit}:{relative_path}"
        ).stdout
        if hashlib.sha256(committed).hexdigest() != binding.get("file_sha256"):
            raise ValueError(f"frozen commit lacks bound {name}")

    _require_tracked_head_blob(root, closure_path)
    return closure


def verify_historical_audit(
    project_root: Path,
    *,
    preselection_closure: Mapping[str, Any],
    winner_freeze: Mapping[str, Any],
    audit_path: Path,
) -> dict[str, Any]:
    """Verify the tracked historical audit before creating the terminal result."""

    root = project_root.resolve()
    audit_path = audit_path.resolve()
    if _relative(root, audit_path) != AUDIT_EVIDENCE_PATH:
        raise ValueError("unexpected 6.1 historical-audit path")
    if not audit_path.is_file() or audit_path.is_symlink():
        raise ValueError("6.1 historical audit is not a regular file")
    audit = _read_json(audit_path)
    gate = audit.get("gate")
    if (
        set(audit) != _AUDIT_FIELDS
        or audit.get("payload_sha256") != canonical_payload_sha256(audit)
        or audit.get("schema_version") != 1
        or audit.get("kind") != "factor_lab_6_1_historical_audit"
        or not isinstance(gate, Mapping)
        or not isinstance(gate.get("passed"), bool)
        or audit.get("selected_candidate_id")
        != winner_freeze.get("selected_candidate_id")
        or audit.get("winner_freeze_payload_sha256")
        != winner_freeze.get("payload_sha256")
        or audit.get("audit_end")
        != FROZEN_HISTORICAL_AUDIT["physical_market_data_end"]
        or audit.get("runner_up_fallback_used") is not False
        or audit.get("profit_claim_allowed") is not False
        or not _is_sha256(audit.get("audit_manifest_payload_sha256"))
    ):
        raise ValueError("historical audit contract differs")
    expected_status = (
        "historical_holdout_passed_requires_fresh_future"
        if gate["passed"] is True
        else "audit_falsified"
    )
    if audit.get("status") != expected_status:
        raise ValueError("historical audit status differs from its gate")
    audit_commit = str(audit.get("audit_execution_commit") or "")
    resolved_audit_commit = _git(
        root, "rev-parse", "--verify", f"{audit_commit}^{{commit}}"
    ).stdout.decode("ascii").strip()
    if resolved_audit_commit != audit_commit or _git(
        root,
        "merge-base",
        "--is-ancestor",
        audit_commit,
        "HEAD",
        check=False,
    ).returncode != 0:
        raise ValueError("historical audit execution commit differs")
    frozen_at_audit = _git(
        root, "show", f"{audit_commit}:{WINNER_FREEZE_PATH}"
    ).stdout
    if frozen_at_audit != (root / WINNER_FREEZE_PATH).read_bytes():
        raise ValueError("historical audit did not use the tracked winner freeze")
    closure_at_audit = _git(
        root, "show", f"{audit_commit}:protocols/6.1-release.json"
    ).stdout
    if (
        closure_at_audit != (root / "protocols/6.1-release.json").read_bytes()
        or preselection_closure.get("payload_sha256")
        != canonical_payload_sha256(preselection_closure)
    ):
        raise ValueError("historical audit did not use the exact preselection closure")
    if _git(
        root,
        "cat-file",
        "-e",
        f"{audit_commit}:{AUDIT_EVIDENCE_PATH}",
        check=False,
    ).returncode == 0:
        raise ValueError("historical audit evidence predates its execution")
    _require_tracked_head_blob(root, audit_path)
    return audit


def verify_release_result(
    project_root: Path,
    *,
    preselection_closure: Mapping[str, Any],
    result_path: Path,
) -> dict[str, Any]:
    """Verify the optional immutable terminal result chained from selection."""

    root = project_root.resolve()
    result_path = result_path.resolve()
    if _relative(root, result_path) != RELEASE_RESULT_PATH:
        raise ValueError("unexpected 6.1 result path")
    result = _read_json(result_path)
    if set(result) != _RESULT_FIELDS:
        raise ValueError("6.1 result contains missing or unknown fields")
    if (
        result.get("payload_sha256") != canonical_payload_sha256(result)
        or result.get("schema_version") != 1
        or result.get("kind") != "factor_lab_6_1_release_result"
        or result.get("release") != "6.1"
        or result.get("preselection_closure_payload_sha256")
        != preselection_closure.get("payload_sha256")
        or result.get("runner_up_fallback_used") is not False
    ):
        raise ValueError("6.1 terminal result contract differs")

    freeze = _require_terminal_binding(
        root,
        result.get("winner_freeze"),
        expected_relative_path=WINNER_FREEZE_PATH,
    )
    verified_freeze = verify_winner_freeze(
        root,
        preselection_closure=preselection_closure,
        freeze_path=root / WINNER_FREEZE_PATH,
    )
    if freeze != verified_freeze:
        raise ValueError("terminal winner-freeze reads are inconsistent")
    winner = freeze.get("selected_candidate_id")
    if result.get("selected_candidate_id") != winner:
        raise ValueError("terminal result winner differs from winner freeze")
    claim = result.get("claim_contract")
    audit_binding = result.get("audit")
    if winner is None:
        expected_claim = {
            "historical_evidence_class": "pre_registered_selection_falsified",
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        }
        if (
            result.get("status") != "selection_falsified_no_candidate"
            or result.get("audit_status") != "not_opened"
            or audit_binding is not None
            or claim != expected_claim
        ):
            raise ValueError("null-selection terminal result differs")
    else:
        bound_audit = _require_terminal_binding(
            root,
            audit_binding,
            expected_relative_path=AUDIT_EVIDENCE_PATH,
        )
        audit = verify_historical_audit(
            root,
            preselection_closure=preselection_closure,
            winner_freeze=freeze,
            audit_path=root / AUDIT_EVIDENCE_PATH,
        )
        if audit != bound_audit:
            raise ValueError("terminal historical-audit reads are inconsistent")
        audit_status = str(audit.get("status") or "")
        expected_claim = {
            "historical_evidence_class": audit_status,
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        }
        if (
            result.get("status") != audit_status
            or result.get("audit_status") != audit_status
            or claim != expected_claim
        ):
            raise ValueError("historical-audit terminal result differs")
    _require_tracked_head_blob(root, result_path)
    return result


def verify_winner_freeze(
    project_root: Path,
    *,
    preselection_closure: Mapping[str, Any],
    freeze_path: Path,
) -> dict[str, Any]:
    """Verify the unique tracked selection terminal before opening audit data."""

    root = project_root.resolve()
    freeze_path = freeze_path.resolve()
    if _relative(root, freeze_path) != WINNER_FREEZE_PATH:
        raise ValueError("unexpected 6.1 winner-freeze path")
    if not freeze_path.is_file() or freeze_path.is_symlink():
        raise ValueError("6.1 winner freeze is not a regular file")
    freeze = _read_json(freeze_path)
    if (
        set(freeze) != _WINNER_FREEZE_FIELDS
        or freeze.get("payload_sha256") != canonical_payload_sha256(freeze)
        or freeze.get("schema_version") != 1
        or freeze.get("kind") != "factor_lab_6_1_winner_freeze"
        or freeze.get("implementation_closure_payload_sha256")
        != preselection_closure.get("payload_sha256")
        or freeze.get("protocol_payload_sha256")
        != (preselection_closure.get("protocol") or {}).get("payload_sha256")
        or freeze.get("protocol_amendment_payload_sha256")
        != (preselection_closure.get("protocol_amendment") or {}).get(
            "payload_sha256"
        )
        or freeze.get("runner_up_fallback_after_audit_fail") is not False
        or freeze.get("audit_market_outcomes_opened") is not False
    ):
        raise ValueError("6.1 winner freeze contract differs")
    selection_commit = str(freeze.get("selection_execution_commit") or "")
    resolved_selection_commit = _git(
        root, "rev-parse", "--verify", f"{selection_commit}^{{commit}}"
    ).stdout.decode("ascii").strip()
    if resolved_selection_commit != selection_commit or _git(
        root,
        "merge-base",
        "--is-ancestor",
        selection_commit,
        "HEAD",
        check=False,
    ).returncode != 0:
        raise ValueError("winner-freeze selection commit is not an ancestor")
    if _git(
        root,
        "cat-file",
        "-e",
        f"{selection_commit}:{WINNER_FREEZE_PATH}",
        check=False,
    ).returncode == 0:
        raise ValueError("winner freeze predates its selection execution")
    closure_at_selection = _git(
        root, "show", f"{selection_commit}:protocols/6.1-release.json"
    ).stdout
    if closure_at_selection != (root / "protocols/6.1-release.json").read_bytes():
        raise ValueError("winner freeze does not descend from the exact closure")

    from factor_lab.research.wide_universe import CONTROL_ID

    recomputed_winner = _winner_from_frozen_gates(freeze)
    train_passers = tuple(freeze.get("train_passers") or [])
    validation_manifest = freeze.get("validation_manifest_payload_sha256")
    status_sources = freeze.get("selection_status_sources")
    if (
        not _is_sha256(freeze.get("train_manifest_payload_sha256"))
        or not isinstance(status_sources, Mapping)
        or set(status_sources) != {"train", "validation"}
        or not isinstance(status_sources.get("train"), Mapping)
    ):
        raise ValueError("winner-freeze stage identities differ")
    _require_replay_map(
        freeze.get("train_phase_replay_sha256"),
        expected_candidates=(CONTROL_ID, *tuple(freeze["train_gates"])),
        label="winner-freeze train replay",
    )
    if train_passers:
        if (
            not _is_sha256(validation_manifest)
            or not isinstance(status_sources.get("validation"), Mapping)
        ):
            raise ValueError("winner-freeze validation stage identity differs")
        _require_replay_map(
            freeze.get("validation_phase_replay_sha256"),
            expected_candidates=(CONTROL_ID, *train_passers),
            label="winner-freeze validation replay",
        )
    elif (
        validation_manifest is not None
        or status_sources.get("validation") is not None
        or freeze.get("validation_phase_replay_sha256") != {}
    ):
        raise ValueError("winner-freeze opened validation without a train passer")

    winner = freeze.get("selected_candidate_id")
    if winner != recomputed_winner:
        raise ValueError("winner-freeze selected candidate differs from frozen gates")
    if winner is None:
        if (
            freeze.get("status") != "selected_null_frozen"
            or freeze.get("selected_definition") is not None
        ):
            raise ValueError("null-selection winner freeze differs")
    else:
        expected_definition = {
            "candidate_id": winner,
            "signal": "fixed_core_defensive_weight_0.70",
            "portfolio": "Top10_exit25_10_offsets",
            "capital": 50_000_000.0,
            "max_adv_participation": 0.05,
            "costs_source": "configs/research.json",
        }
        if (
            winner not in {"daily_adv20_ge_100m", "daily_adv20_top1500"}
            or freeze.get("status") != "selected_definition_frozen"
            or freeze.get("selected_definition") != expected_definition
            or winner not in list(freeze.get("train_passers") or [])
        ):
            raise ValueError("selected winner definition differs")
    _require_tracked_head_blob(root, freeze_path)
    return freeze


def _distribution_content_identity(name: str) -> dict[str, Any]:
    """Hash installed distribution bytes while excluding generated bytecode."""

    prefix = Path(sys.prefix).resolve()
    distribution = importlib.metadata.distribution(name)
    files = distribution.files
    if files is None:
        raise ValueError(f"installed distribution has no file inventory: {name}")
    records: list[dict[str, Any]] = []
    for entry in sorted(files, key=lambda value: str(value).replace("\\", "/")):
        portable_entry = str(entry).replace("\\", "/")
        if portable_entry.endswith(".pyc") or "__pycache__/" in portable_entry:
            continue
        path = Path(distribution.locate_file(entry)).resolve()
        try:
            relative = path.relative_to(prefix).as_posix()
        except ValueError as exc:
            raise ValueError(f"distribution file is outside the runtime: {path}") from exc
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"distribution file is absent or indirect: {path}")
        records.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    encoded = json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return {
        "file_count": len(records),
        "content_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _active_conda_artifacts(names: set[str]) -> dict[str, dict[str, Any]]:
    metadata_root = Path(sys.prefix).resolve() / "conda-meta"
    if not metadata_root.is_dir():
        raise ValueError("frozen 6.1 runtime requires its Conda metadata")
    found: dict[str, dict[str, Any]] = {}
    for path in metadata_root.glob("*.json"):
        raw = _read_json(path)
        name = str(raw.get("name") or "")
        if name not in names:
            continue
        if name in found:
            raise ValueError(f"duplicate Conda package identity: {name}")
        found[name] = {
            "version": raw.get("version"),
            "build": raw.get("build"),
            "subdir": raw.get("subdir"),
            "sha256": raw.get("sha256"),
        }
    if set(found) != names:
        raise ValueError("active Conda artifact set differs from the frozen runtime")
    return found


def _active_numeric_backend() -> dict[str, Any]:
    import numpy as np

    config = np.__config__.CONFIG
    blas = config["Build Dependencies"]["blas"]
    compiler = config["Compilers"]["c"]
    return {
        "blas_name": blas["name"],
        "blas_version": blas["version"],
        "c_compiler_name": compiler["name"],
        "c_compiler_version": compiler["version"],
        "simd_found": config["SIMD Extensions"]["found"],
    }


def verify_active_runtime(project_root: Path) -> dict[str, Any]:
    """Require the exact local runtime frozen before formal return opening."""

    path = project_root.resolve() / RUNTIME_PATH
    value = _read_json(path)
    if (
        set(value) != _RUNTIME_FIELDS
        or value.get("runtime_id") != RUNTIME_ID
        or value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("schema_version") != 1
        or value.get("kind") != "factor_lab_frozen_research_runtime"
        or value.get("status") != "frozen_before_any_wide_return_evaluation"
    ):
        raise ValueError("6.1 runtime artifact is invalid")
    expected_python = dict(value.get("python") or {})
    expected_platform = dict(value.get("platform") or {})
    if (
        platform.python_implementation() != expected_python.get("implementation")
        or platform.python_version() != expected_python.get("version")
        or sys.version != expected_python.get("full_version")
        or platform.system() != expected_platform.get("system")
        or platform.release() != expected_platform.get("release")
        or list(platform.architecture()) != expected_platform.get("architecture")
    ):
        raise ValueError("active Python/platform differs from the frozen 6.1 runtime")
    from factor_lab import __version__

    if __version__ != value.get("source_package_version"):
        raise ValueError("active source package version differs from the frozen runtime")
    expected_packages = value.get("packages")
    if not isinstance(expected_packages, Mapping):
        raise ValueError("6.1 runtime package map is missing")
    actual_packages = {
        str(name): importlib.metadata.version(str(name))
        for name in expected_packages
    }
    if actual_packages != dict(expected_packages):
        raise ValueError("active packages differ from the frozen 6.1 runtime")
    expected_contents = value.get("distribution_contents")
    if not isinstance(expected_contents, Mapping):
        raise ValueError("6.1 runtime distribution-content map is missing")
    actual_contents = {
        str(name): _distribution_content_identity(str(name))
        for name in expected_contents
    }
    if actual_contents != dict(expected_contents):
        raise ValueError("active distribution bytes differ from the frozen 6.1 runtime")
    expected_conda = value.get("conda_artifacts")
    if not isinstance(expected_conda, Mapping) or _active_conda_artifacts(
        set(map(str, expected_conda))
    ) != dict(expected_conda):
        raise ValueError("active native artifacts differ from the frozen 6.1 runtime")
    if _active_numeric_backend() != value.get("numeric_backend"):
        raise ValueError("active numeric backend differs from the frozen 6.1 runtime")
    return value


__all__ = [
    "FROZEN_HISTORICAL_AUDIT",
    "FROZEN_IMPLEMENTATION_PATHS",
    "PROTOCOL_ID",
    "RUNTIME_ID",
    "RUNTIME_PATH",
    "AUDIT_EVIDENCE_PATH",
    "RELEASE_RESULT_PATH",
    "WINNER_FREEZE_PATH",
    "canonical_payload_sha256",
    "file_sha256",
    "verify_historical_audit",
    "verify_preselection_closure",
    "verify_release_result",
    "verify_winner_freeze",
    "verify_active_runtime",
]
