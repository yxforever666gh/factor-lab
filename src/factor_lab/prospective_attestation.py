"""Auditable GitHub artifact-attestation command construction and validation.

This module does not dispatch or poll by itself.  Callers explicitly execute
the returned argv/stdin contracts, then feed responses back into the strict
parsers below.  Keeping network execution outside the evidence codec makes it
possible to black-box every trust decision before the 5.0 release is active.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
import unicodedata

from factor_lab.prospective_ledger import (
    LedgerIntegrityError,
    LedgerStateError,
    canonical_json_bytes,
    create_only_file,
    sha256_bytes,
    strict_load_canonical,
)


API_VERSION = "2026-03-10"
DEFAULT_REPOSITORY = "yxforever666gh/factor-lab"
DEFAULT_WORKFLOW = "prospective-attest.yml"
WORKFLOW_PATH = ".github/workflows/prospective-attest.yml"
ATTEST_ACTION_COMMIT = "f7c74d28b9d84cb8768d0b8ca14a4bac6ef463e6"
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.ya?ml$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
TAG_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
UTC_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")


class AttestationError(RuntimeError):
    """Raised when external attestation evidence does not satisfy the contract."""


@dataclass(frozen=True, slots=True)
class CommandSpec:
    argv: tuple[str, ...]
    stdin: bytes | None = None
    cwd: Path | None = None


@dataclass(frozen=True, slots=True)
class DispatchRequest:
    command: CommandSpec
    request_id: str
    snapshot_sha256: str
    snapshot_name: str
    repository: str
    release_tag: str
    workflow: str


def _require_repository(value: str) -> str:
    if not REPO_RE.fullmatch(value):
        raise ValueError(f"invalid GitHub repository name: {value!r}")
    return value


def _require_workflow(value: str) -> str:
    if not WORKFLOW_RE.fullmatch(value) or value != DEFAULT_WORKFLOW:
        raise ValueError(f"workflow must be {DEFAULT_WORKFLOW!r}")
    return value


def _require_release_tag(value: str) -> str:
    if not TAG_RE.fullmatch(value):
        raise ValueError("release tag must use canonical major.minor")
    return value


def _require_sha256(value: str, field_name: str) -> str:
    if not SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256")
    return value


def _require_oid(value: str, field_name: str) -> str:
    if not OID_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase Git object id")
    return value


def _parse_utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise AttestationError(f"{field_name} must be canonical UTC seconds")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise AttestationError(f"{field_name} is not a valid timestamp") from exc


def _parse_rfc3339(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise AttestationError(f"{field_name} must be an RFC3339 timestamp")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise AttestationError(f"{field_name} is not a valid RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise AttestationError(f"{field_name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _pairs_unique(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise AttestationError(f"duplicate JSON response key: {key!r}")
        result[key] = value
    return result


def _load_response(value: bytes | str) -> Any:
    try:
        text = value.decode("utf-8") if isinstance(value, bytes) else value
        return json.loads(text, object_pairs_hook=_pairs_unique)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AttestationError("external command returned invalid JSON") from exc


def attestation_request_id(
    *,
    repository: str,
    release_tag: str,
    workflow: str,
    snapshot_sha256: str,
) -> str:
    _require_repository(repository)
    _require_release_tag(release_tag)
    _require_workflow(workflow)
    _require_sha256(snapshot_sha256, "snapshot_sha256")
    return sha256_bytes(
        canonical_json_bytes(
            {
                "release_ref": f"refs/tags/{release_tag}",
                "repository": repository,
                "snapshot_sha256": snapshot_sha256,
                "workflow": workflow,
            }
        )
    )


def build_dispatch_request(
    snapshot: bytes,
    *,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str = "5.0",
    workflow: str = DEFAULT_WORKFLOW,
) -> DispatchRequest:
    _require_repository(repository)
    _require_release_tag(release_tag)
    _require_workflow(workflow)
    parsed = strict_load_canonical(snapshot)
    if not isinstance(parsed, Mapping):
        raise LedgerIntegrityError("attestation snapshot must be a canonical JSON object")
    snapshot_sha256 = sha256_bytes(snapshot)
    if parsed.get("release_tag") != release_tag:
        raise LedgerStateError("snapshot release tag differs from dispatch ref")
    request_id = attestation_request_id(
        repository=repository,
        release_tag=release_tag,
        workflow=workflow,
        snapshot_sha256=snapshot_sha256,
    )
    snapshot_name = f"prospective-snapshot-{snapshot_sha256}.json"
    body = canonical_json_bytes(
        {
            "ref": release_tag,
            "inputs": {
                "request_id": request_id,
                "snapshot_b64": base64.b64encode(snapshot).decode("ascii"),
                "snapshot_sha256": snapshot_sha256,
            },
        }
    )
    endpoint = (
        f"repos/{repository}/actions/workflows/{workflow}/dispatches"
    )
    command = CommandSpec(
        argv=(
            "gh",
            "api",
            endpoint,
            "--method",
            "POST",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            f"X-GitHub-Api-Version: {API_VERSION}",
            "--input",
            "-",
        ),
        stdin=body,
    )
    return DispatchRequest(
        command=command,
        request_id=request_id,
        snapshot_sha256=snapshot_sha256,
        snapshot_name=snapshot_name,
        repository=repository,
        release_tag=release_tag,
        workflow=workflow,
    )


def parse_dispatch_response(
    value: bytes | str,
    *,
    repository: str = DEFAULT_REPOSITORY,
) -> dict[str, Any]:
    _require_repository(repository)
    payload = _load_response(value)
    if not isinstance(payload, Mapping):
        raise AttestationError("workflow dispatch response must be an object")
    run_id = payload.get("workflow_run_id")
    if type(run_id) is not int or run_id <= 0:
        raise AttestationError("workflow dispatch response lacks a positive workflow_run_id")
    run_url = payload.get("run_url")
    html_url = payload.get("html_url")
    expected_run_url = (
        f"https://api.github.com/repos/{repository}/actions/runs/{run_id}"
    )
    expected_html_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    if run_url != expected_run_url:
        raise AttestationError("workflow dispatch run_url is not a GitHub API URL")
    if html_url != expected_html_url:
        raise AttestationError("workflow dispatch html_url is not a GitHub URL")
    return {"workflow_run_id": run_id, "run_url": run_url, "html_url": html_url}


def build_workflow_run_command(
    workflow_run_id: int,
    *,
    repository: str = DEFAULT_REPOSITORY,
) -> CommandSpec:
    _require_repository(repository)
    if type(workflow_run_id) is not int or workflow_run_id <= 0:
        raise ValueError("workflow_run_id must be a positive integer")
    endpoint = f"repos/{repository}/actions/runs/{workflow_run_id}"
    return CommandSpec(
        argv=(
            "gh",
            "api",
            endpoint,
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            f"X-GitHub-Api-Version: {API_VERSION}",
        )
    )


def validate_workflow_run(
    value: bytes | str | Mapping[str, Any],
    *,
    workflow_run_id: int,
    request_id: str,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str,
    release_commit_oid: str,
    workflow_path: str = WORKFLOW_PATH,
    admission_deadline_utc: str | None = None,
) -> dict[str, Any]:
    _require_repository(repository)
    _require_sha256(request_id, "request_id")
    _require_release_tag(release_tag)
    _require_oid(release_commit_oid, "release_commit_oid")
    payload = _load_response(value) if isinstance(value, (bytes, str)) else value
    if not isinstance(payload, Mapping):
        raise AttestationError("workflow run response must be an object")
    if payload.get("id") != workflow_run_id:
        raise AttestationError("workflow run id differs from dispatch receipt")
    if payload.get("event") != "workflow_dispatch":
        raise AttestationError("attestation run was not triggered by workflow_dispatch")
    if payload.get("status") != "completed" or payload.get("conclusion") != "success":
        raise AttestationError("attestation workflow is not completed successfully")
    if payload.get("head_sha") != release_commit_oid:
        raise AttestationError("attestation workflow head SHA differs from release commit")
    if payload.get("head_branch") != release_tag:
        raise AttestationError("attestation workflow head branch differs from release tag")
    if payload.get("display_title") != f"prospective-{request_id}":
        raise AttestationError("attestation workflow title differs from request id")
    run_attempt = payload.get("run_attempt")
    if type(run_attempt) is not int or run_attempt <= 0:
        raise AttestationError("attestation workflow lacks a positive run attempt")
    # GitHub's documented workflow-run examples use ``path@ref-name`` while
    # current workflow-dispatch responses can return the canonical path alone.
    # The ref is independently pinned below by head_branch/head_sha and later
    # by the certificate source-ref contract, so accept only these two exact
    # API encodings rather than weakening the workflow identity check.
    accepted_paths = {workflow_path, f"{workflow_path}@{release_tag}"}
    if payload.get("path") not in accepted_paths:
        raise AttestationError("attestation workflow path/ref differs from frozen workflow")
    workflow_id = payload.get("workflow_id")
    if type(workflow_id) is not int or workflow_id <= 0:
        raise AttestationError("attestation workflow lacks a positive workflow id")
    expected_workflow_url = (
        f"https://api.github.com/repos/{repository}/actions/workflows/{workflow_id}"
    )
    if payload.get("workflow_url") != expected_workflow_url:
        raise AttestationError("attestation workflow_url differs from workflow id")
    run_repository = payload.get("repository")
    if not isinstance(run_repository, Mapping) or (
        run_repository.get("full_name") != repository
    ):
        raise AttestationError("attestation run repository differs from request")
    html_url = payload.get("html_url")
    expected_html_url = (
        f"https://github.com/{repository}/actions/runs/{workflow_run_id}"
    )
    if html_url != expected_html_url:
        raise AttestationError("workflow run html_url is not a GitHub URL")
    created = _parse_utc(payload.get("created_at"), "workflow created_at")
    completed = _parse_utc(payload.get("updated_at"), "workflow updated_at")
    if completed < created:
        raise AttestationError("workflow completion timestamp precedes creation")
    if admission_deadline_utc is not None:
        deadline = _parse_utc(admission_deadline_utc, "admission_deadline_utc")
        if created >= deadline:
            raise AttestationError("workflow was created at or after prospective admission")
    return {
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": run_attempt,
        "request_id": request_id,
        "workflow_run_display_title": payload["display_title"],
        "html_url": html_url,
        "created_at_utc": payload["created_at"],
        "completed_at_utc": payload["updated_at"],
        "workflow_path": workflow_path,
        "workflow_ref": f"refs/tags/{release_tag}",
        "workflow_source_commit_oid": release_commit_oid,
    }


def build_attestation_download_command(
    snapshot_path: str | Path,
    *,
    repository: str = DEFAULT_REPOSITORY,
    output_directory: str | Path,
) -> CommandSpec:
    _require_repository(repository)
    return CommandSpec(
        argv=(
            "gh",
            "attestation",
            "download",
            str(Path(snapshot_path)),
            "--repo",
            repository,
        ),
        cwd=Path(output_directory),
    )


def certificate_identity(
    *,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str = "5.0",
    workflow_path: str = WORKFLOW_PATH,
) -> str:
    _require_repository(repository)
    _require_release_tag(release_tag)
    return f"https://github.com/{repository}/{workflow_path}@refs/tags/{release_tag}"


def build_attestation_verify_command(
    snapshot_path: str | Path,
    bundle_path: str | Path,
    *,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str = "5.0",
    release_commit_oid: str,
    workflow_path: str = WORKFLOW_PATH,
) -> CommandSpec:
    _require_repository(repository)
    _require_release_tag(release_tag)
    _require_oid(release_commit_oid, "release_commit_oid")
    identity = certificate_identity(
        repository=repository, release_tag=release_tag, workflow_path=workflow_path
    )
    return CommandSpec(
        argv=(
            "gh",
            "attestation",
            "verify",
            str(Path(snapshot_path)),
            "--bundle",
            str(Path(bundle_path)),
            "--repo",
            repository,
            "--cert-identity",
            identity,
            "--source-ref",
            f"refs/tags/{release_tag}",
            "--source-digest",
            release_commit_oid,
            "--deny-self-hosted-runners",
            "--format",
            "json",
        )
    )


def validate_verification_output(
    value: bytes | str | Sequence[Mapping[str, Any]],
    *,
    snapshot_sha256: str,
    snapshot_name: str,
    expected_certificate_identity: str,
    repository: str,
    workflow_run_id: int,
    workflow_run_attempt: int,
    admission_deadline_utc: str | None = None,
) -> dict[str, Any]:
    _require_sha256(snapshot_sha256, "snapshot_sha256")
    _require_repository(repository)
    if type(workflow_run_id) is not int or workflow_run_id <= 0:
        raise ValueError("workflow_run_id must be a positive integer")
    if type(workflow_run_attempt) is not int or workflow_run_attempt <= 0:
        raise ValueError("workflow_run_attempt must be a positive integer")
    deadline = (
        _parse_utc(admission_deadline_utc, "admission_deadline_utc")
        if admission_deadline_utc is not None
        else None
    )
    expected_run_invocation_uri = (
        f"https://github.com/{repository}/actions/runs/{workflow_run_id}"
        f"/attempts/{workflow_run_attempt}"
    )
    payload = _load_response(value) if isinstance(value, (bytes, str)) else value
    if not isinstance(payload, list) or not payload:
        raise AttestationError("gh attestation verify returned no verification results")
    matches: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        result = item.get("verificationResult")
        if not isinstance(result, Mapping):
            continue
        statement = result.get("statement")
        if not isinstance(statement, Mapping) or statement.get("predicateType") != "https://slsa.dev/provenance/v1":
            continue
        subjects = statement.get("subject")
        if not isinstance(subjects, list):
            continue
        subject_match = any(
            isinstance(subject, Mapping)
            and subject.get("name") == snapshot_name
            and isinstance(subject.get("digest"), Mapping)
            and subject["digest"].get("sha256") == snapshot_sha256
            for subject in subjects
        )
        signature = result.get("signature")
        certificate = signature.get("certificate") if isinstance(signature, Mapping) else None
        if not (
            subject_match
            and isinstance(certificate, Mapping)
            and certificate.get("subjectAlternativeName")
            == expected_certificate_identity
            and certificate.get("runInvocationURI")
            == expected_run_invocation_uri
            and certificate.get("runnerEnvironment") == "github-hosted"
        ):
            continue
        timestamps = result.get("verifiedTimestamps")
        if not isinstance(timestamps, list) or not timestamps:
            raise AttestationError("matching attestation has no verified timestamps")
        normalized_timestamps: list[dict[str, Any]] = []
        for index, timestamp in enumerate(timestamps):
            if not isinstance(timestamp, Mapping):
                raise AttestationError(
                    f"verified timestamp {index} is not an object"
                )
            timestamp_type = timestamp.get("type")
            timestamp_uri = timestamp.get("uri")
            if not isinstance(timestamp_type, str) or not timestamp_type:
                raise AttestationError(
                    f"verified timestamp {index} has no type"
                )
            if not isinstance(timestamp_uri, str):
                raise AttestationError(
                    f"verified timestamp {index} has no URI"
                )
            parsed_timestamp = _parse_rfc3339(
                timestamp.get("timestamp"),
                f"verified timestamp {index}",
            )
            normalized_timestamps.append(
                {
                    "type": timestamp_type,
                    "uri": timestamp_uri,
                    "timestamp_utc": _utc_text(parsed_timestamp),
                    "_parsed": parsed_timestamp,
                }
            )
        tlog_timestamps = [
            timestamp
            for timestamp in normalized_timestamps
            if timestamp["type"] == "Tlog" and timestamp["uri"]
        ]
        if not tlog_timestamps:
            raise AttestationError(
                "matching attestation has no trusted transparency-log timestamp"
            )
        trusted_tlog = min(
            tlog_timestamps,
            key=lambda timestamp: (
                timestamp["_parsed"],
                timestamp["uri"],
            ),
        )
        if deadline is not None and trusted_tlog["_parsed"] >= deadline:
            raise AttestationError(
                "transparency-log timestamp is at or after prospective admission"
            )
        matches.append(
            {
                "verified_timestamp_count": len(normalized_timestamps),
                "verified_timestamps": [
                    {
                        "type": timestamp["type"],
                        "uri": timestamp["uri"],
                        "timestamp_utc": timestamp["timestamp_utc"],
                    }
                    for timestamp in normalized_timestamps
                ],
                "verified_tlog_type": trusted_tlog["type"],
                "verified_tlog_uri": trusted_tlog["uri"],
                "verified_tlog_timestamp_utc": trusted_tlog["timestamp_utc"],
                "certificate_identity": expected_certificate_identity,
                "run_invocation_uri": expected_run_invocation_uri,
                "workflow_run_id": workflow_run_id,
                "workflow_run_attempt": workflow_run_attempt,
                "subject_name": snapshot_name,
                "subject_sha256": snapshot_sha256,
            }
        )
    if len(matches) != 1:
        raise AttestationError(
            f"expected exactly one strict snapshot attestation, found {len(matches)}"
        )
    return matches[0]


def store_attestation_bundle(
    bundle_path: str | Path,
    destination_directory: str | Path,
    *,
    snapshot_sha256: str,
) -> dict[str, Any]:
    _require_sha256(snapshot_sha256, "snapshot_sha256")
    source = Path(bundle_path)
    content = source.read_bytes()
    if not content:
        raise AttestationError("attestation bundle is empty")
    digest = sha256_bytes(content)
    target = Path(destination_directory) / f"{snapshot_sha256}-{digest}.jsonl"
    created = create_only_file(target, content)
    return {
        "attestation_bundle_sha256": digest,
        "path": str(target),
        "created": created,
    }


def build_receipt_payload(
    *,
    purpose: str,
    snapshot: bytes,
    request: DispatchRequest,
    dispatch_response: Mapping[str, Any],
    workflow_run: Mapping[str, Any],
    verification: Mapping[str, Any],
    attestation_bundle_sha256: str,
    decision_record_sha256: str | None,
    admission_deadline_utc: str | None = None,
) -> dict[str, Any]:
    parsed_snapshot = strict_load_canonical(snapshot)
    if not isinstance(parsed_snapshot, Mapping):
        raise AttestationError("snapshot must be a canonical JSON object")
    if sha256_bytes(snapshot) != request.snapshot_sha256:
        raise AttestationError("snapshot bytes changed after dispatch construction")
    _require_sha256(attestation_bundle_sha256, "attestation_bundle_sha256")
    if dispatch_response.get("workflow_run_id") != workflow_run.get("workflow_run_id"):
        raise AttestationError("workflow run differs from dispatch response")
    if workflow_run.get("request_id") != request.request_id:
        raise AttestationError("workflow run differs from attestation request")
    if workflow_run.get("workflow_run_display_title") != (
        f"prospective-{request.request_id}"
    ):
        raise AttestationError("workflow run title differs from attestation request")
    if verification.get("workflow_run_id") != workflow_run.get("workflow_run_id"):
        raise AttestationError("verified attestation differs from workflow run")
    if verification.get("workflow_run_attempt") != workflow_run.get(
        "workflow_run_attempt"
    ):
        raise AttestationError("verified attestation differs from workflow attempt")
    expected_run_invocation_uri = (
        f"{workflow_run.get('html_url')}/attempts/"
        f"{workflow_run.get('workflow_run_attempt')}"
    )
    if verification.get("run_invocation_uri") != expected_run_invocation_uri:
        raise AttestationError("verified attestation differs from workflow invocation")
    if verification.get("verified_tlog_type") != "Tlog":
        raise AttestationError("verified attestation lacks a transparency-log timestamp")
    if purpose == "decision_anchor":
        if decision_record_sha256 != parsed_snapshot.get("head_record_sha256"):
            raise AttestationError("decision receipt does not bind the snapshot head")
        if admission_deadline_utc is None:
            raise AttestationError("decision receipt requires an admission deadline")
        if _parse_rfc3339(
            verification.get("verified_tlog_timestamp_utc"),
            "verified_tlog_timestamp_utc",
        ) >= _parse_utc(admission_deadline_utc, "admission_deadline_utc"):
            raise AttestationError(
                "transparency-log timestamp is at or after prospective admission"
            )
    elif purpose == "activation_canary":
        if decision_record_sha256 is not None or admission_deadline_utc is not None:
            raise AttestationError("activation canary cannot reference a decision")
    else:
        raise AttestationError("unsupported receipt purpose")
    return {
        "purpose": purpose,
        "snapshot_sha256": request.snapshot_sha256,
        "snapshot_head_record_sha256": parsed_snapshot["head_record_sha256"],
        "decision_record_sha256": decision_record_sha256,
        "request_id": request.request_id,
        "workflow_run_id": workflow_run["workflow_run_id"],
        "workflow_run_attempt": workflow_run["workflow_run_attempt"],
        "workflow_run_display_title": workflow_run["workflow_run_display_title"],
        "workflow_run_url": workflow_run["html_url"],
        "workflow_run_created_at_utc": workflow_run["created_at_utc"],
        "workflow_run_completed_at_utc": workflow_run["completed_at_utc"],
        "workflow_path": workflow_run["workflow_path"],
        "workflow_ref": workflow_run["workflow_ref"],
        "workflow_source_commit_oid": workflow_run["workflow_source_commit_oid"],
        "attestation_bundle_sha256": attestation_bundle_sha256,
        "certificate_identity": verification["certificate_identity"],
        "run_invocation_uri": verification["run_invocation_uri"],
        "verified_timestamp_count": verification["verified_timestamp_count"],
        "verified_timestamps": verification["verified_timestamps"],
        "verified_tlog_type": verification["verified_tlog_type"],
        "verified_tlog_uri": verification["verified_tlog_uri"],
        "verified_tlog_timestamp_utc": verification[
            "verified_tlog_timestamp_utc"
        ],
        "subject_name": verification["subject_name"],
        "subject_sha256": verification["subject_sha256"],
    }


__all__ = [
    "API_VERSION",
    "ATTEST_ACTION_COMMIT",
    "AttestationError",
    "CommandSpec",
    "DEFAULT_REPOSITORY",
    "DEFAULT_WORKFLOW",
    "DispatchRequest",
    "WORKFLOW_PATH",
    "attestation_request_id",
    "build_attestation_download_command",
    "build_attestation_verify_command",
    "build_dispatch_request",
    "build_receipt_payload",
    "build_workflow_run_command",
    "certificate_identity",
    "parse_dispatch_response",
    "store_attestation_bundle",
    "validate_verification_output",
    "validate_workflow_run",
]
