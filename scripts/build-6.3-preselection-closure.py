#!/usr/bin/env python
"""Create the immutable 6.3 corrective-replay closure from one clean commit."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from factor_lab.release_integrity import (  # noqa: E402
    BASE_PROTOCOL_AMENDMENT_FILE_SHA256,
    BASE_PROTOCOL_AMENDMENT_ID,
    BASE_PROTOCOL_AMENDMENT_PATH,
    BASE_PROTOCOL_AMENDMENT_PAYLOAD_SHA256,
    BASE_PROTOCOL_FILE_SHA256,
    BASE_PROTOCOL_PATH,
    BASE_PROTOCOL_PAYLOAD_SHA256,
    CORRECTIVE_AMENDMENT_FILE_SHA256,
    CORRECTIVE_AMENDMENT_ID,
    CORRECTIVE_AMENDMENT_PATH,
    CORRECTIVE_AMENDMENT_PAYLOAD_SHA256,
    FROZEN_HISTORICAL_AUDIT,
    FROZEN_IMPLEMENTATION_PATHS,
    PREDECESSOR_CLOSURE_CREATION_COMMIT,
    PREDECESSOR_CLOSURE_FILE_SHA256,
    PREDECESSOR_CLOSURE_PAYLOAD_SHA256,
    PREDECESSOR_RELEASE_COMMIT,
    PREDECESSOR_RELEASE_TAG,
    PREDECESSOR_RELEASE_TAG_OBJECT,
    PRESELECTION_CLOSURE_PATH,
    PRESELECTION_SUPERSESSION_REASON,
    PRIOR_EXECUTION_FAILURE_CREATION_COMMIT,
    PRIOR_EXECUTION_FAILURE_FILE_SHA256,
    PRIOR_EXECUTION_FAILURE_PATH,
    PRIOR_EXECUTION_FAILURE_PAYLOAD_SHA256,
    PROTOCOL_ID,
    RUNTIME_FILE_SHA256,
    RUNTIME_ID,
    RUNTIME_PATH,
    RUNTIME_PAYLOAD_SHA256,
    SUPERSEDED_PRESELECTION_CLOSURE_PATH,
    _verify_integrated_prior_amendment,
    _verify_prior_release,
    _verify_superseded_preselection_closure,
    canonical_payload_sha256,
    file_sha256,
    verify_corrective_amendment_contract,
    verify_frozen_runtime_contract,
    verify_prior_execution_failure,
    verify_wide_protocol_contract,
)


CLOSURE_PATH = PROJECT_ROOT / PRESELECTION_CLOSURE_PATH
SUPERSEDED_CLOSURE_PATH = PROJECT_ROOT / SUPERSEDED_PRESELECTION_CLOSURE_PATH
PROTOCOL_PATH = PROJECT_ROOT / BASE_PROTOCOL_PATH
AMENDMENT_PATH = PROJECT_ROOT / BASE_PROTOCOL_AMENDMENT_PATH
CORRECTIVE_PATH = PROJECT_ROOT / CORRECTIVE_AMENDMENT_PATH
EXECUTION_FAILURE_PATH = PROJECT_ROOT / PRIOR_EXECUTION_FAILURE_PATH


def _git(*args: str, check: bool = True) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    if value.get("payload_sha256") != canonical_payload_sha256(value):
        raise ValueError(f"invalid canonical payload: {path}")
    return value


def _binding(
    path: Path,
    *,
    id_field: str,
    expected_id: str,
    expected_file_sha256: str,
    expected_payload_sha256: str,
) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"release binding is indirect: {path}")
    value = _read_json(path)
    if (
        value.get(id_field) != expected_id
        or file_sha256(path) != expected_file_sha256
        or value.get("payload_sha256") != expected_payload_sha256
    ):
        raise ValueError(f"unexpected immutable binding: {path}")
    return {
        "path": path.relative_to(PROJECT_ROOT).as_posix(),
        "file_sha256": expected_file_sha256,
        "payload_sha256": expected_payload_sha256,
        id_field: expected_id,
    }


def _write_create_only(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise


def _superseded_closure_binding(
    *,
    replacement_implementation_commit: str,
) -> dict[str, Any]:
    old = _read_json(SUPERSEDED_CLOSURE_PATH)
    if (
        file_sha256(SUPERSEDED_CLOSURE_PATH)
        != PREDECESSOR_CLOSURE_FILE_SHA256
        or old.get("payload_sha256") != PREDECESSOR_CLOSURE_PAYLOAD_SHA256
    ):
        raise ValueError("published 6.2 closure bytes differ")
    binding = {
        "path": SUPERSEDED_PRESELECTION_CLOSURE_PATH,
        "file_sha256": PREDECESSOR_CLOSURE_FILE_SHA256,
        "payload_sha256": PREDECESSOR_CLOSURE_PAYLOAD_SHA256,
        "closure_commit": PREDECESSOR_CLOSURE_CREATION_COMMIT,
        "published_tag": PREDECESSOR_RELEASE_TAG,
        "annotated_tag_object": PREDECESSOR_RELEASE_TAG_OBJECT,
        "peeled_commit": PREDECESSOR_RELEASE_COMMIT,
        "selection_returns_opened_at_closure": False,
        "replacement_reason": PRESELECTION_SUPERSESSION_REASON,
    }
    _verify_superseded_preselection_closure(
        PROJECT_ROOT,
        binding,
        replacement_implementation_commit=replacement_implementation_commit,
    )
    return binding


def _execution_failure_binding() -> dict[str, Any]:
    failure = verify_prior_execution_failure(PROJECT_ROOT)
    if file_sha256(EXECUTION_FAILURE_PATH) != PRIOR_EXECUTION_FAILURE_FILE_SHA256:
        raise ValueError("published 6.2 execution-failure bytes differ")
    return {
        "path": PRIOR_EXECUTION_FAILURE_PATH,
        "file_sha256": PRIOR_EXECUTION_FAILURE_FILE_SHA256,
        "payload_sha256": PRIOR_EXECUTION_FAILURE_PAYLOAD_SHA256,
        "status": failure["status"],
        "classification": failure["classification"],
        "creation_commit": PRIOR_EXECUTION_FAILURE_CREATION_COMMIT,
    }


def main() -> int:
    if CLOSURE_PATH.exists():
        raise FileExistsError("6.3 preselection closure is create-only")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("preselection closure requires a clean implementation commit")

    protocol_value = _read_json(PROTOCOL_PATH)
    amendment_value = _read_json(AMENDMENT_PATH)
    corrective_value = _read_json(CORRECTIVE_PATH)
    runtime_value = _read_json(PROJECT_ROOT / RUNTIME_PATH)
    verify_wide_protocol_contract(protocol_value)
    _verify_prior_release(PROJECT_ROOT, protocol_value.get("prior_release"))
    _verify_integrated_prior_amendment(PROJECT_ROOT, amendment_value)
    verify_corrective_amendment_contract(PROJECT_ROOT, corrective_value)
    verify_frozen_runtime_contract(runtime_value)

    implementation_commit = _git("rev-parse", "HEAD").decode("ascii").strip()
    implementation_tree = _git(
        "rev-parse", f"{implementation_commit}^{{tree}}"
    ).decode("ascii").strip()

    implementation: dict[str, dict[str, str]] = {}
    for name, relative_path in sorted(FROZEN_IMPLEMENTATION_PATHS.items()):
        path = PROJECT_ROOT / relative_path
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"frozen implementation path is indirect: {relative_path}")
        working = path.read_bytes()
        committed = _git("show", f"{implementation_commit}:{relative_path}")
        if working != committed:
            raise ValueError(
                f"working bytes differ from the implementation commit: {relative_path}"
            )
        implementation[name] = {
            "path": relative_path,
            "sha256": hashlib.sha256(working).hexdigest(),
        }

    protocol = _binding(
        PROTOCOL_PATH,
        id_field="protocol_id",
        expected_id=PROTOCOL_ID,
        expected_file_sha256=BASE_PROTOCOL_FILE_SHA256,
        expected_payload_sha256=BASE_PROTOCOL_PAYLOAD_SHA256,
    )
    amendment = _binding(
        AMENDMENT_PATH,
        id_field="amendment_id",
        expected_id=BASE_PROTOCOL_AMENDMENT_ID,
        expected_file_sha256=BASE_PROTOCOL_AMENDMENT_FILE_SHA256,
        expected_payload_sha256=BASE_PROTOCOL_AMENDMENT_PAYLOAD_SHA256,
    )
    corrective = _binding(
        CORRECTIVE_PATH,
        id_field="amendment_id",
        expected_id=CORRECTIVE_AMENDMENT_ID,
        expected_file_sha256=CORRECTIVE_AMENDMENT_FILE_SHA256,
        expected_payload_sha256=CORRECTIVE_AMENDMENT_PAYLOAD_SHA256,
    )
    runtime = _binding(
        PROJECT_ROOT / RUNTIME_PATH,
        id_field="runtime_id",
        expected_id=RUNTIME_ID,
        expected_file_sha256=RUNTIME_FILE_SHA256,
        expected_payload_sha256=RUNTIME_PAYLOAD_SHA256,
    )
    for binding in (protocol, amendment, corrective, runtime):
        relative_path = str(binding["path"])
        committed = _git("show", f"{implementation_commit}:{relative_path}")
        if hashlib.sha256(committed).hexdigest() != binding["file_sha256"]:
            raise ValueError(f"implementation commit lacks binding: {relative_path}")
    for relative_path, expected_sha256 in (
        (SUPERSEDED_PRESELECTION_CLOSURE_PATH, PREDECESSOR_CLOSURE_FILE_SHA256),
        (PRIOR_EXECUTION_FAILURE_PATH, PRIOR_EXECUTION_FAILURE_FILE_SHA256),
    ):
        committed = _git("show", f"{implementation_commit}:{relative_path}")
        if (
            hashlib.sha256(committed).hexdigest() != expected_sha256
            or committed != (PROJECT_ROOT / relative_path).read_bytes()
        ):
            raise ValueError(f"implementation commit lacks lineage: {relative_path}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": "6.3",
        "closure_role": "immutable_preselection_root",
        "direction_change": False,
        "status": "implementation_frozen_before_selection",
        "selection_returns_opened": False,
        "route": "widened_opportunity_set",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "historical_audit": FROZEN_HISTORICAL_AUDIT,
        "protocol": protocol,
        "protocol_amendment": amendment,
        "corrective_amendment": corrective,
        "runtime": runtime,
        "superseded_preselection_closure": _superseded_closure_binding(
            replacement_implementation_commit=implementation_commit,
        ),
        "implementation": implementation,
        "evidence": {"prior_execution_failure": _execution_failure_binding()},
        "canonical_data": {},
        "claim_contract": {
            "incremental_universe_effect_only": True,
            "validates_fixed_core_alpha": False,
            "historical_evidence_class": (
                "pre_registered_historical_diagnostic_only"
            ),
            "historical_qualification_allowed": False,
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        },
        "implementation_commit": implementation_commit,
        "implementation_tree": implementation_tree,
    }
    closure["payload_sha256"] = canonical_payload_sha256(closure)
    if _git("rev-parse", "HEAD").decode("ascii").strip() != implementation_commit:
        raise RuntimeError("implementation commit changed while building closure")
    _write_create_only(CLOSURE_PATH, closure)
    print(
        f"implementation_commit={implementation_commit}\n"
        f"payload_sha256={closure['payload_sha256']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
