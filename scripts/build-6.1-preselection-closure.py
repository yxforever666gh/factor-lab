#!/usr/bin/env python
"""Create the immutable 6.1 preselection closure from one clean commit."""

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
    FROZEN_HISTORICAL_AUDIT,
    FROZEN_IMPLEMENTATION_PATHS,
    PRESELECTION_CLOSURE_PATH,
    PRESELECTION_SUPERSESSION_REASON,
    PROTOCOL_ID,
    RUNTIME_ID,
    RUNTIME_PATH,
    SUPERSEDED_PRESELECTION_CLOSURE_PATH,
    canonical_payload_sha256,
    file_sha256,
)


CLOSURE_PATH = PROJECT_ROOT / PRESELECTION_CLOSURE_PATH
SUPERSEDED_CLOSURE_PATH = PROJECT_ROOT / SUPERSEDED_PRESELECTION_CLOSURE_PATH
PROTOCOL_PATH = PROJECT_ROOT / "protocols" / "6.1-wide-universe.json"
AMENDMENT_PATH = (
    PROJECT_ROOT / "protocols" / "6.1-wide-universe-amendment-1.json"
)


def _git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
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
) -> dict[str, Any]:
    value = _read_json(path)
    if value.get(id_field) != expected_id:
        raise ValueError(f"unexpected {id_field}: {path}")
    return {
        "path": path.relative_to(PROJECT_ROOT).as_posix(),
        "file_sha256": file_sha256(path),
        "payload_sha256": value["payload_sha256"],
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


def _superseded_closure_binding() -> dict[str, Any]:
    old = _read_json(SUPERSEDED_CLOSURE_PATH)
    if (
        old.get("status") != "implementation_frozen_before_selection"
        or old.get("selection_returns_opened") is not False
        or old.get("selected_candidate_id") is not None
        or old.get("audit_status") != "not_opened"
    ):
        raise ValueError("superseded closure had opened selection or audit evidence")
    commits = _git(
        "log",
        "--diff-filter=A",
        "--format=%H",
        "--",
        SUPERSEDED_PRESELECTION_CLOSURE_PATH,
    ).decode("ascii").splitlines()
    if len(commits) != 1:
        raise ValueError("superseded closure must have one immutable creation commit")
    closure_commit = commits[0]
    if _git(
        "show", f"{closure_commit}:{SUPERSEDED_PRESELECTION_CLOSURE_PATH}"
    ) != SUPERSEDED_CLOSURE_PATH.read_bytes():
        raise ValueError("superseded closure differs from its creation commit")
    return {
        "path": SUPERSEDED_PRESELECTION_CLOSURE_PATH,
        "file_sha256": file_sha256(SUPERSEDED_CLOSURE_PATH),
        "payload_sha256": old["payload_sha256"],
        "closure_commit": closure_commit,
        "selection_returns_opened": False,
        "replacement_reason": PRESELECTION_SUPERSESSION_REASON,
    }


def main() -> int:
    if CLOSURE_PATH.exists():
        raise FileExistsError("6.1 preselection closure is create-only")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("preselection closure requires a clean implementation commit")
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
    )
    amendment = _binding(
        AMENDMENT_PATH,
        id_field="amendment_id",
        expected_id=f"{PROTOCOL_ID}/amendment-1",
    )
    runtime = _binding(
        PROJECT_ROOT / RUNTIME_PATH,
        id_field="runtime_id",
        expected_id=RUNTIME_ID,
    )
    for binding in (protocol, amendment, runtime):
        relative_path = str(binding["path"])
        committed = _git("show", f"{implementation_commit}:{relative_path}")
        if hashlib.sha256(committed).hexdigest() != binding["file_sha256"]:
            raise ValueError(f"implementation commit lacks binding: {relative_path}")

    closure: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": "6.1",
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
        "runtime": runtime,
        "superseded_preselection_closure": _superseded_closure_binding(),
        "implementation": implementation,
        "evidence": {},
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
