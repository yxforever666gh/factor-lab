#!/usr/bin/env python
"""Run the staged 8.1 policy-operational metric correction and evaluation."""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import importlib.metadata
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
from typing import Any, Mapping
import uuid

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.catalog import RuntimeLayout, load_data_config  # noqa: E402
from factor_lab.data.etf_assets import (  # noqa: E402
    MultiAssetStage,
    capture_multi_asset_stage,
    load_multi_asset_stage,
)
from factor_lab.data.sources import _configured_tushare_client  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)
from factor_lab.research.multi_asset import (  # noqa: E402
    CASH_ONLY_ID,
    CONTROL_ID,
    SimulationConfig,
    build_monthly_targets,
    phase_metrics,
    simulate_targets,
)


RELEASE = "8.1"
ROUTE = "policy_operational_metric_reclassification"
PROTOCOL_ID = "factor-lab/8.1/policy-operational-metric-reclassification-v1"
PROTOCOL_PATH = Path("protocols/8.1-policy-operational-metric-reclassification.json")
ASSET_SELECTION_PATH = Path("protocols/7.0-asset-selection.json")
INHERITED_PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
CLOSURE_PATH = Path("protocols/8.1-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/8.1")
TRAIN_RECLASSIFICATION_PATH = EVIDENCE_ROOT / "train-reclassification.json"
WINNER_FREEZE_PATH = EVIDENCE_ROOT / "winner-freeze.json"
AUDIT_PATH = EVIDENCE_ROOT / "historical-audit.json"
RESULT_PATH = EVIDENCE_ROOT / "result.json"
PRIOR_RECEIPT_PATH = Path("protocols/evidence/8.0/execution-failure.json")
PRIOR_PROTOCOL_PATH = Path("protocols/8.0-static-capital-budget.json")
PRIOR_CLOSURE_PATH = Path("protocols/8.0-release.json")
PRIOR_TAG = "8.0"
PRIOR_TAG_OBJECT = "3fcbd73f7497b074e484ce7793e2d3603bf5a177"
PRIOR_COMMIT = "78aba86bf4e741699afca1acd1470493785fd952"
PRIOR_RECEIPT_PAYLOAD = "751b85c6c2e52b450e9c3549f7f4504af50b634599be4c32e240ee503de9823a"
PRIOR_RECEIPT_FILE_SHA256 = "6af779495081f6ee391c6388a1e4342b878168b529f8074cf03d9ec2cc50eeaa"
PRIOR_CLOSURE_PAYLOAD = "7bdd27bc6365c936c7e17736920d5fbf2556608e8b59b0869b3e70b9e61e5de7"
PRIOR_CLOSURE_FILE_SHA256 = "8e4fe890efb746c15ae5f0375d8a1dfd85a061172426165af1441d5011bfa97d"
PRIOR_PROTOCOL_PAYLOAD = "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
PRIOR_PROTOCOL_FILE_SHA256 = "ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc"
PROTOCOL_PAYLOAD = "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5"
PROTOCOL_FILE_SHA256 = "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583"
INHERITED_PROTOCOL_FILE_SHA256 = (
    "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
)
INHERITED_PROTOCOL_PAYLOAD = (
    "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
)
PRIMARY_ID = CONTROL_ID
WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-8.1"
PRIOR_WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-8.0"
SOURCE_ROOT = WORK_ROOT / "sources"
EVALUATION_ROOT = WORK_ROOT / "evaluations"
BINDING_ROOT = WORK_ROOT / "stage-bindings"
EXPECTED_IMPLEMENTATION_PATHS = {
    ".github/workflows/ci.yml",
    "configs/data.json",
    "pyproject.toml",
    "scripts/build-8.1-preselection-closure.py",
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
RUNTIME_DISTRIBUTIONS = ("numpy", "pandas", "pyarrow", "scipy", "tushare")
NATIVE_SUFFIXES = (".dll", ".dylib", ".pyd", ".so")
EVALUATION_ROLES = ("primary", "stress", "cash", "cash_stress")
EVALUATION_ARTIFACTS = ("targets", "orders", "daily_nav", "holdings", "trades")
GITHUB_REPOSITORY = "yxforever666gh/factor-lab"
ALLOWED_TRADE_STATUSES = frozenset(
    {
        "executed",
        "partial_cash",
        "blocked_cash",
        "blocked_missing_open",
        "blocked_capacity",
    }
)
NOTIONAL_ABS_TOL_RMB = 1e-6
NOTIONAL_REL_TOL = 1e-12
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
STAGES: dict[str, dict[str, str]] = {
    "validation": {
        "source_start": "2014-01-15",
        "source_end": "2022-12-30",
        "performance_start": "2020-01-02",
        "performance_end": "2022-12-30",
    },
    "audit": {
        "source_start": "2014-01-15",
        "source_end": "2026-08-28",
        "performance_start": "2023-01-03",
        "performance_end": "2026-08-28",
    },
}
_CLOSURE_FIELDS = {
    "schema_version", "kind", "release", "closure_role", "direction_change",
    "route", "status", "post_hoc_reclassification", "prior_train_returns_opened",
    "train_reexecution_allowed", "train_reclassification_status",
    "validation_market_outcomes_opened", "audit_status", "protocol",
    "prior_release", "train_reclassification_source",
    "implementation_commit", "implementation_tree", "implementation", "runtime",
    "formal_data", "claim_contract", "payload_sha256",
}
_FREEZE_FIELDS = {
    "schema_version", "kind", "release", "status", "protocol_payload_sha256",
    "asset_selection_payload_sha256", "implementation_closure_payload_sha256",
    "selection_execution_commit", "run_nonce", "candidate_registry",
    "selected_candidate_id", "train_reclassification", "validation",
    "validation_market_outcomes_opened", "audit_market_outcomes_opened",
    "runner_up_fallback", "claim_contract", "payload_sha256",
}
_TRAIN_RECLASSIFICATION_FIELDS = {
    "schema_version", "kind", "release", "status",
    "protocol_payload_sha256", "asset_selection_payload_sha256",
    "implementation_closure_payload_sha256", "reclassification_execution_commit",
    "run_nonce", "source_receipt", "role_gate_metrics", "metrics", "corrected_gate",
    "execution_validity",
    "post_hoc_non_independent", "new_market_data_queried",
    "retained_8_0_train_artifacts_accessed", "runtime_created",
    "validation_market_outcomes_opened",
    "audit_market_outcomes_opened", "claim_contract", "payload_sha256",
}
_AUDIT_FIELDS = {
    "schema_version", "kind", "release", "status", "selected_candidate_id",
    "winner_freeze_payload_sha256", "protocol_payload_sha256",
    "asset_selection_payload_sha256", "implementation_closure_payload_sha256",
    "audit_execution_commit", "run_nonce", "audit", "runner_up_fallback",
    "claim_contract", "payload_sha256",
}
_RESULT_FIELDS = {
    "schema_version", "kind", "release", "status", "selected_candidate_id",
    "audit_status", "winner_freeze", "historical_audit",
    "protocol_payload_sha256", "asset_selection_payload_sha256",
    "implementation_closure_payload_sha256", "claim_contract", "payload_sha256",
}
_PHASE_FIELDS = {
    "source_manifest_payload_sha256", "stage_binding_payload_sha256",
    "evaluation_payload_sha256", "evaluation_file_sha256", "metrics", "gate",
}
_STAGE_BINDING_FIELDS = {
    "schema_version", "kind", "release", "stage", "stage_manifest_payload_sha256",
    "stage_manifest_file_sha256", "implementation_closure_payload_sha256",
    "execution_commit", "predecessor", "run_nonce", "payload_sha256",
}
_EVALUATION_FIELDS = {
    "schema_version", "kind", "stage", "source_manifest_payload_sha256",
    "stage_binding_payload_sha256", "implementation_closure_payload_sha256",
    "execution_commit", "predecessor", "run_nonce", "metrics", "gate",
    "artifacts", "payload_sha256",
}


def _git(*args: str, check: bool = True) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_commit(value: Any) -> bool:
    return isinstance(value, str) and _COMMIT_RE.fullmatch(value) is not None


def _safe_repo_file(relative: str) -> Path:
    if not relative or Path(relative).is_absolute():
        raise ValueError(f"repository-relative file required: {relative!r}")
    root = ROOT.resolve()
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path escapes the repository: {relative}") from exc
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"repository file is absent or indirect: {relative}")
    return path


def _distribution_identity(name: str) -> dict[str, Any]:
    distribution = importlib.metadata.distribution(name)
    files = distribution.files
    if files is None:
        raise ValueError(f"installed distribution has no file inventory: {name}")
    prefix = Path(sys.prefix).resolve()
    records: list[dict[str, Any]] = []
    native_records: list[dict[str, Any]] = []
    for entry in sorted(files, key=lambda value: str(value).replace("\\", "/")):
        portable = str(entry).replace("\\", "/")
        if portable.endswith(".pyc") or "__pycache__/" in portable:
            continue
        path = Path(distribution.locate_file(entry))
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"distribution file is absent or indirect: {name}:{portable}")
        resolved = path.resolve()
        try:
            relative = resolved.relative_to(prefix).as_posix()
        except ValueError as exc:
            raise ValueError(f"distribution file is outside the runtime: {resolved}") from exc
        record = {
            "path": relative,
            "size_bytes": resolved.stat().st_size,
            "sha256": file_sha256(resolved),
        }
        records.append(record)
        if resolved.suffix.casefold() in NATIVE_SUFFIXES or ".libs/" in relative.casefold():
            native_records.append(record)
    if not records:
        raise ValueError(f"installed distribution is empty: {name}")
    return {
        "version": distribution.version,
        "file_count": len(records),
        "file_tree_sha256": canonical_payload_sha256({"files": records}),
        "native_file_count": len(native_records),
        "native_file_tree_sha256": canonical_payload_sha256(
            {"files": native_records}
        ),
    }


@functools.lru_cache(maxsize=1)
def _runtime_identity() -> dict[str, Any]:
    executable = Path(sys.executable)
    if executable.is_symlink() or not executable.is_file():
        raise ValueError(f"Python executable is absent or indirect: {executable}")
    resolved = executable.resolve()
    prefix = Path(sys.prefix).resolve()
    native_candidates: set[Path] = set(prefix.glob("python*.dll"))
    native_root = prefix / "Library" / "bin"
    for pattern in (
        "mkl*.dll",
        "libiomp*.dll",
        "libopenblas*.dll",
        "vcomp*.dll",
    ):
        if native_root.is_dir():
            native_candidates.update(native_root.glob(pattern))
    native_records: list[dict[str, Any]] = []
    for path in sorted(native_candidates, key=lambda value: value.as_posix().casefold()):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"native runtime file is absent or indirect: {path}")
        native_records.append(
            {
                "path": path.resolve().relative_to(prefix).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    conda_records: list[dict[str, Any]] = []
    managed_paths: set[str] = set()
    conda_meta = prefix / "conda-meta"
    if conda_meta.is_dir():
        for path in sorted(conda_meta.glob("*.json"), key=lambda value: value.name.casefold()):
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Conda metadata is absent or indirect: {path}")
            conda_records.append(
                {
                    "path": path.resolve().relative_to(prefix).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                }
            )
            metadata_value = json.loads(path.read_text(encoding="utf-8"))
            for relative in metadata_value.get("files") or []:
                portable = str(relative).replace("\\", "/")
                if portable.endswith(".pyc") or "__pycache__/" in portable:
                    continue
                managed_paths.add(portable)
    managed_records: list[dict[str, Any]] = []
    missing_managed: list[str] = []
    for relative in sorted(managed_paths):
        path = (prefix / relative).resolve()
        try:
            path.relative_to(prefix)
        except ValueError as exc:
            raise ValueError(f"Conda-managed path escapes the runtime: {relative}") from exc
        if path.is_symlink():
            raise ValueError(f"Conda-managed path is indirect: {relative}")
        if not path.is_file():
            missing_managed.append(relative)
            continue
        managed_records.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    import numpy as np

    config_output = io.StringIO()
    with contextlib.redirect_stdout(config_output):
        np.show_config()
    cpu_features = dict(
        sorted(
            getattr(np.core._multiarray_umath, "__cpu_features__", {}).items()
        )
    )
    return {
        "python_implementation": sys.implementation.name,
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
        "python_executable": str(resolved),
        "python_executable_size_bytes": resolved.stat().st_size,
        "python_executable_sha256": file_sha256(resolved),
        "native_runtime": {
            "file_count": len(native_records),
            "files": native_records,
            "file_tree_sha256": canonical_payload_sha256(
                {"files": native_records}
            ),
        },
        "conda_metadata": {
            "file_count": len(conda_records),
            "file_tree_sha256": canonical_payload_sha256(
                {"files": conda_records}
            ),
            "managed_file_count": len(managed_records),
            "managed_file_tree_sha256": canonical_payload_sha256(
                {"files": managed_records}
            ),
            "missing_managed_file_count": len(missing_managed),
            "missing_managed_paths_sha256": canonical_payload_sha256(
                {"paths": missing_managed}
            ),
        },
        "cpu": {
            "machine": platform.machine(),
            "processor": platform.processor(),
            "numpy_cpu_features": cpu_features,
            "numpy_show_config_sha256": hashlib.sha256(
                config_output.getvalue().encode("utf-8")
            ).hexdigest(),
        },
        "distributions": {
            name: _distribution_identity(name) for name in RUNTIME_DISTRIBUTIONS
        },
    }


def _require_source_imports() -> None:
    source_root = SRC.resolve()
    for value in (
        canonical_payload_sha256,
        load_data_config,
        capture_multi_asset_stage,
        simulate_targets,
    ):
        module = sys.modules.get(value.__module__)
        module_file = Path(str(getattr(module, "__file__", ""))).resolve()
        try:
            module_file.relative_to(source_root)
        except ValueError as exc:
            raise ValueError(
                f"formal import did not resolve under frozen source: {module_file}"
            ) from exc
    loaded_paths: set[str] = set()
    for name, module in tuple(sys.modules.items()):
        if not name.startswith("factor_lab"):
            continue
        raw_file = getattr(module, "__file__", None)
        if not raw_file:
            continue
        module_file = Path(str(raw_file)).resolve()
        try:
            relative = module_file.relative_to(ROOT.resolve()).as_posix()
        except ValueError:
            continue
        if relative.endswith(".py"):
            loaded_paths.add(relative)
    unexpected = sorted(loaded_paths - EXPECTED_IMPLEMENTATION_PATHS)
    if unexpected:
        raise ValueError(f"loaded Factor Lab sources are not frozen: {unexpected}")


def _is_explicit_git_transport_failure(
    *, returncode: int, stdout: bytes, stderr: bytes
) -> bool:
    """Distinguish network transport loss from a missing or malformed ref."""

    if returncode in (0, 2) or stdout.strip():
        return False
    message = stderr.decode("utf-8", errors="replace").lower()
    markers = (
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
    return any(marker in message for marker in markers)


def _require_head_pushed_and_ci_success(head: str) -> None:
    if not _is_commit(head):
        raise RuntimeError(f"invalid formal execution commit: {head!r}")
    local_remote = _git("rev-parse", "--verify", "refs/remotes/origin/main").decode(
        "ascii"
    ).strip()
    if local_remote != head:
        raise RuntimeError(f"formal HEAD {head} differs from local origin/main {local_remote}")
    remote = subprocess.run(
        ["git", "ls-remote", "--exit-code", "origin", "refs/heads/main"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    expected_remote_line = f"{head}\trefs/heads/main"
    if remote.returncode == 0:
        try:
            remote_lines = remote.stdout.decode("ascii").splitlines()
        except UnicodeDecodeError as exc:
            raise RuntimeError("formal HEAD remote identity response is malformed") from exc
        if remote_lines != [expected_remote_line]:
            raise RuntimeError("formal HEAD remote identity response is malformed or mismatched")
        remote_head = head
    elif _is_explicit_git_transport_failure(
        returncode=remote.returncode,
        stdout=remote.stdout,
        stderr=remote.stderr,
    ):
        # A transport failure is not evidence that the commit was pushed.  The
        # authenticated API is a second exact identity source, not a bypass.
        api = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{GITHUB_REPOSITORY}/commits/main",
                "--jq",
                ".sha",
            ],
            cwd=ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        api_values = api.stdout.decode("ascii", errors="replace").split()
        if api.returncode != 0 or len(api_values) != 1:
            raise RuntimeError(
                "could not verify exact GitHub main SHA after git ls-remote failure"
            )
        remote_head = api_values[0]
    else:
        raise RuntimeError(
            "git ls-remote did not return the exact origin/main identity and "
            "was not an explicit transport failure"
        )
    if remote_head != head:
        raise RuntimeError("formal HEAD is not the current pushed origin/main commit")
    completed = subprocess.run(
        [
            "gh",
            "run",
            "list",
            "--repo",
            GITHUB_REPOSITORY,
            "--commit",
            head,
            "--branch",
            "main",
            "--event",
            "push",
            "--workflow",
            "ci.yml",
            "--limit",
            "20",
            "--json",
            "headSha,status,conclusion,event",
        ],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "could not verify GitHub push CI for formal HEAD: "
            + completed.stderr.decode("utf-8", errors="replace").strip()
        )
    try:
        runs = json.loads(completed.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub CI response is not valid JSON") from exc
    if not any(
        isinstance(run, Mapping)
        and run.get("headSha") == head
        and run.get("event") == "push"
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        for run in runs
    ):
        raise RuntimeError(f"formal HEAD lacks a successful completed GitHub push CI run: {head}")


def _read_json(path: Path, *, self_hashed: bool = True) -> dict[str, Any]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    if self_hashed and value.get("payload_sha256") != canonical_payload_sha256(value):
        raise ValueError(f"invalid canonical payload: {path}")
    return value


def _verify_prior_train_artifacts(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Deep-verify the receipt-bound 8.0 artifacts and extract validity only.

    Economic role metrics remain sourced from the published receipt.  This
    function never calls a provider or creates/replays an 8.1 train stage.
    """

    train = receipt.get("train_stage")
    boundary = receipt.get("failure_boundary")
    if (
        not isinstance(train, Mapping)
        or not isinstance(boundary, Mapping)
        or boundary.get("train_phase_deep_verified") is not True
        or boundary.get("validation_market_outcomes_opened") is not False
        or boundary.get("audit_market_outcomes_opened") is not False
    ):
        raise ValueError("8.0 receipt lacks its closed, deeply verified train boundary")
    manifest_receipt = train.get("manifest")
    binding_receipt = train.get("binding")
    evaluation_receipt = train.get("evaluation")
    role_metrics = train.get("role_gate_metrics")
    if any(
        not isinstance(item, Mapping)
        for item in (manifest_receipt, binding_receipt, evaluation_receipt, role_metrics)
    ):
        raise ValueError("8.0 receipt lacks train artifact bindings")

    source_root = PRIOR_WORK_ROOT / "sources"
    stage = load_multi_asset_stage(source_root, "train")
    manifest_path = stage.path / "manifest.json"
    if (
        manifest_path.resolve() != (ROOT / str(manifest_receipt["path"])).resolve()
        or stage.manifest.get("payload_sha256")
        != manifest_receipt.get("payload_sha256")
        or file_sha256(manifest_path) != manifest_receipt.get("file_sha256")
    ):
        raise ValueError("8.0 train manifest differs from its published receipt")

    binding_path = PRIOR_WORK_ROOT / "stage-bindings" / "train.json"
    binding = _read_json(binding_path)
    if (
        binding_path.resolve() != (ROOT / str(binding_receipt["path"])).resolve()
        or binding.get("release") != "8.0"
        or binding.get("stage") != "train"
        or binding.get("payload_sha256") != binding_receipt.get("payload_sha256")
        or file_sha256(binding_path) != binding_receipt.get("file_sha256")
        or binding.get("stage_manifest_payload_sha256")
        != stage.manifest.get("payload_sha256")
    ):
        raise ValueError("8.0 train stage binding differs from its published receipt")

    evaluation_dir = PRIOR_WORK_ROOT / "evaluations" / "stage=train"
    evaluation_path = evaluation_dir / "evaluation.json"
    evaluation = _read_json(evaluation_path)
    expected_files = {
        "evaluation.json",
        *(
            f"{role}-{artifact}.parquet"
            for role in EVALUATION_ROLES
            for artifact in EVALUATION_ARTIFACTS
        ),
    }
    if (
        evaluation_path.resolve()
        != (ROOT / str(evaluation_receipt["path"])).resolve()
        or evaluation_dir.is_symlink()
        or {path.name for path in evaluation_dir.iterdir()} != expected_files
        or evaluation.get("payload_sha256")
        != evaluation_receipt.get("payload_sha256")
        or file_sha256(evaluation_path) != evaluation_receipt.get("file_sha256")
        or evaluation.get("source_manifest_payload_sha256")
        != stage.manifest.get("payload_sha256")
        or evaluation.get("stage_binding_payload_sha256")
        != binding.get("payload_sha256")
        or canonical_payload_sha256(evaluation.get("metrics", {}))
        != evaluation_receipt.get("metrics_sha256")
        or canonical_payload_sha256(evaluation.get("gate", {}))
        != evaluation_receipt.get("gate_sha256")
    ):
        raise ValueError("8.0 train evaluation differs from its published receipt")
    artifacts = evaluation.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(EVALUATION_ROLES):
        raise ValueError("8.0 train evaluation role set differs")
    role_validity: dict[str, Any] = {}
    artifact_rows = 0
    for role in EVALUATION_ROLES:
        entries = artifacts.get(role)
        if not isinstance(entries, Mapping) or set(entries) != set(EVALUATION_ARTIFACTS):
            raise ValueError(f"8.0 {role} artifact set differs")
        frames: dict[str, Any] = {}
        for artifact in EVALUATION_ARTIFACTS:
            entry = entries[artifact]
            name = f"{role}-{artifact}.parquet"
            path = evaluation_dir / name
            if (
                not isinstance(entry, Mapping)
                or entry.get("path") != name
                or path.is_symlink()
                or not path.is_file()
                or path.stat().st_size != entry.get("size_bytes")
                or file_sha256(path) != entry.get("file_sha256")
            ):
                raise ValueError(f"8.0 train artifact differs: {name}")
            frame = pd.read_parquet(path)
            if len(frame) != entry.get("rows"):
                raise ValueError(f"8.0 train artifact row count differs: {name}")
            artifact_rows += len(frame)
            frames[artifact] = frame
        role_validity[role] = _role_execution_validity(
            frames, expected_role_metrics=role_metrics[role]
        )
    if (
        evaluation_receipt.get("artifact_parquet_count") != 20
        or evaluation_receipt.get("artifact_row_count") != artifact_rows
    ):
        raise ValueError("8.0 train artifact aggregate differs from its receipt")
    return {
        "source": "receipt_bound_8_0_train_artifacts",
        "receipt_train_phase_deep_verified": True,
        "artifact_parquet_count": 20,
        "artifact_row_count": artifact_rows,
        "roles": role_validity,
    }


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


def _require_clean_main() -> str:
    branch = _git("branch", "--show-current").decode("utf-8").strip()
    if branch != "main":
        raise RuntimeError(f"formal {RELEASE} evidence requires main, found {branch!r}")
    if _git("status", "--porcelain").strip():
        raise RuntimeError(f"formal {RELEASE} evidence requires a clean worktree")
    head = _git("rev-parse", "HEAD").decode("ascii").strip()
    _require_head_pushed_and_ci_success(head)
    return head


def _require_committed(path: Path) -> bytes:
    working = (ROOT / path).read_bytes()
    committed = _git("show", f"HEAD:{path.as_posix()}")
    if working != committed:
        raise RuntimeError(f"tracked evidence is not the exact HEAD blob: {path}")
    return working


def _verify_execution_lineage(
    commit: str,
    *,
    evidence_path: Path,
    required_files: tuple[Path, ...],
) -> None:
    if not _is_commit(commit):
        raise ValueError(f"invalid execution commit: {commit!r}")
    resolved = _git("rev-parse", "--verify", f"{commit}^{{commit}}").decode(
        "ascii"
    ).strip()
    if resolved != commit or subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("execution commit is not an ancestor of HEAD")
    for relative in required_files:
        if _git("show", f"{commit}:{relative.as_posix()}") != (ROOT / relative).read_bytes():
            raise ValueError(f"execution commit lacks the exact predecessor: {relative}")
    if subprocess.run(
        ["git", "cat-file", "-e", f"{commit}:{evidence_path.as_posix()}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode == 0:
        raise ValueError(f"evidence predates its execution: {evidence_path}")


def _verify_train_reclassification_contract(
    reclassification: Mapping[str, Any],
    *,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    selection: Mapping[str, Any],
    verify_data: bool = True,
) -> None:
    receipt = _read_json(PRIOR_RECEIPT_PATH)
    receipt_bytes = _require_committed(PRIOR_RECEIPT_PATH)
    role_metrics = receipt.get("train_stage", {}).get("role_gate_metrics")
    if (
        set(reclassification) != _TRAIN_RECLASSIFICATION_FIELDS
        or reclassification.get("payload_sha256")
        != canonical_payload_sha256(reclassification)
        or reclassification.get("schema_version") != 1
        or reclassification.get("kind")
        != "factor_lab_policy_operational_train_reclassification"
        or reclassification.get("release") != RELEASE
        or reclassification.get("protocol_payload_sha256")
        != protocol.get("payload_sha256")
        or reclassification.get("asset_selection_payload_sha256")
        != selection.get("payload_sha256")
        or reclassification.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
        or reclassification.get("post_hoc_non_independent") is not True
        or reclassification.get("new_market_data_queried") is not False
        or reclassification.get("retained_8_0_train_artifacts_accessed") is not True
        or reclassification.get("runtime_created") is not False
        or reclassification.get("validation_market_outcomes_opened") is not False
        or reclassification.get("audit_market_outcomes_opened") is not False
        or reclassification.get("claim_contract") != protocol.get("claim_contract")
        or not re.fullmatch(
            r"[0-9a-f]{32}", str(reclassification.get("run_nonce") or "")
        )
    ):
        raise ValueError("8.1 train reclassification contract differs")
    execution_commit = str(reclassification["reclassification_execution_commit"])
    _verify_execution_lineage(
        execution_commit,
        evidence_path=TRAIN_RECLASSIFICATION_PATH,
        required_files=(
            CLOSURE_PATH,
            PROTOCOL_PATH,
            INHERITED_PROTOCOL_PATH,
            ASSET_SELECTION_PATH,
            PRIOR_PROTOCOL_PATH,
            PRIOR_CLOSURE_PATH,
            PRIOR_RECEIPT_PATH,
        ),
    )
    expected_source = {
        "path": PRIOR_RECEIPT_PATH.as_posix(),
        "file_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "payload_sha256": receipt["payload_sha256"],
    }
    if (
        reclassification.get("source_receipt") != expected_source
        or receipt.get("payload_sha256") != PRIOR_RECEIPT_PAYLOAD
        or hashlib.sha256(receipt_bytes).hexdigest() != PRIOR_RECEIPT_FILE_SHA256
        or reclassification.get("role_gate_metrics") != role_metrics
        or role_metrics
        != protocol.get("train_reclassification_input", {}).get("role_gate_metrics")
    ):
        raise ValueError("8.1 reclassification does not bind the published 8.0 metrics")
    execution_validity = reclassification.get("execution_validity")
    source_binding = closure.get("train_reclassification_source") or {}
    if (
        not isinstance(execution_validity, Mapping)
        or canonical_payload_sha256(execution_validity)
        != source_binding.get("execution_validity_sha256")
        or execution_validity.get("artifact_parquet_count")
        != source_binding.get("artifact_parquet_count")
        or execution_validity.get("artifact_row_count")
        != source_binding.get("artifact_row_count")
    ):
        raise ValueError("8.1 reclassification validity differs from the closure")
    if verify_data and execution_validity != _verify_prior_train_artifacts(receipt):
        raise ValueError("8.1 train validity differs from the receipt-bound artifacts")
    metrics = _combine_receipt_role_gate_metrics(
        role_metrics,
        execution_validity=execution_validity,
    )
    corrected_gate = _evaluate_static_gate(
        metrics,
        protocol["shared_absolute_gate"],
        protocol["execution_validity_hard_fail"],
    )
    if (
        reclassification.get("metrics") != metrics
        or reclassification.get("corrected_gate") != corrected_gate
    ):
        raise ValueError("8.1 corrected train gate does not replay deterministically")
    expected_status = (
        "train_reclassification_passed"
        if corrected_gate["passed"] is True
        else "train_reclassification_failed"
    )
    if reclassification.get("status") != expected_status:
        raise ValueError("8.1 reclassification status differs from its corrected gate")


def _verify_winner_freeze_contract(
    freeze: Mapping[str, Any],
    *,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    selection: Mapping[str, Any],
    verify_data: bool = True,
) -> None:
    if (
        set(freeze) != _FREEZE_FIELDS
        or freeze.get("payload_sha256") != canonical_payload_sha256(freeze)
        or freeze.get("schema_version") != 1
        or freeze.get("kind") != "factor_lab_multi_asset_winner_freeze"
        or freeze.get("release") != RELEASE
        or freeze.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or freeze.get("asset_selection_payload_sha256") != selection.get("payload_sha256")
        or freeze.get("implementation_closure_payload_sha256") != closure.get("payload_sha256")
        or freeze.get("audit_market_outcomes_opened") is not False
        or freeze.get("candidate_registry") != [PRIMARY_ID]
        or freeze.get("runner_up_fallback") is not False
        or freeze.get("claim_contract") != protocol.get("claim_contract")
        or not re.fullmatch(r"[0-9a-f]{32}", str(freeze.get("run_nonce") or ""))
    ):
        raise ValueError(f"winner freeze does not bind the active {RELEASE} contracts")
    execution_commit = str(freeze["selection_execution_commit"])
    _verify_execution_lineage(
        execution_commit,
        evidence_path=WINNER_FREEZE_PATH,
        required_files=(
            CLOSURE_PATH,
            PROTOCOL_PATH,
            INHERITED_PROTOCOL_PATH,
            ASSET_SELECTION_PATH,
            PRIOR_RECEIPT_PATH,
            TRAIN_RECLASSIFICATION_PATH,
        ),
    )
    reclassification = _read_json(TRAIN_RECLASSIFICATION_PATH)
    reclassification_bytes = _require_committed(TRAIN_RECLASSIFICATION_PATH)
    _verify_train_reclassification_contract(
        reclassification,
        closure=closure,
        protocol=protocol,
        selection=selection,
        verify_data=verify_data,
    )
    if execution_commit == reclassification.get("reclassification_execution_commit"):
        raise ValueError("8.1 freeze validation commit must follow reclassification commit")
    reclassification_binding = freeze.get("train_reclassification")
    if (
        not isinstance(reclassification_binding, Mapping)
        or reclassification_binding
        != {
            "path": TRAIN_RECLASSIFICATION_PATH.as_posix(),
            "file_sha256": hashlib.sha256(reclassification_bytes).hexdigest(),
            "payload_sha256": reclassification["payload_sha256"],
        }
        or freeze.get("run_nonce") == reclassification.get("run_nonce")
    ):
        raise ValueError(
            "8.1 winner freeze does not bind an independently committed reclassification"
        )
    train_passed = reclassification["corrected_gate"]["passed"] is True
    validation = freeze.get("validation")
    if train_passed:
        if not isinstance(validation, Mapping):
            raise ValueError("8.1 reclassification pass requires a complete validation phase")
        _verify_phase_reference(
            validation,
            stage_name="validation",
            gate_config=protocol["shared_absolute_gate"],
            validity_config=protocol["execution_validity_hard_fail"],
            closure_payload=str(closure["payload_sha256"]),
            execution_commit=execution_commit,
            run_nonce=str(freeze["run_nonce"]),
            predecessor={
                "kind": "train_reclassification",
                "payload_sha256": reclassification["payload_sha256"],
            },
            verify_data=verify_data,
        )
    elif validation is not None:
        raise ValueError("8.1 validation opened without a passed corrected train gate")
    validation_passed = bool(
        isinstance(validation, Mapping) and validation["gate"]["passed"] is True
    )
    expected_status = (
        "selected_policy_frozen"
        if validation_passed
        else "selected_null_frozen_validation_failed"
        if validation is not None
        else "selected_null_frozen_reclassification_failed"
    )
    expected_selected = PRIMARY_ID if validation_passed else None
    if (
        freeze.get("status") != expected_status
        or freeze.get("selected_candidate_id") != expected_selected
        or freeze.get("validation_market_outcomes_opened") != train_passed
    ):
        raise ValueError("8.1 winner freeze is inconsistent with recomputed gates")


def _verify_audit_contract(
    audit: Mapping[str, Any],
    *,
    freeze: Mapping[str, Any],
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    selection: Mapping[str, Any],
    verify_data: bool = True,
) -> None:
    if (
        set(audit) != _AUDIT_FIELDS
        or audit.get("payload_sha256") != canonical_payload_sha256(audit)
        or audit.get("schema_version") != 1
        or audit.get("kind") != "factor_lab_multi_asset_historical_audit"
        or audit.get("release") != RELEASE
        or audit.get("selected_candidate_id") != PRIMARY_ID
        or audit.get("winner_freeze_payload_sha256") != freeze.get("payload_sha256")
        or audit.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or audit.get("asset_selection_payload_sha256") != selection.get("payload_sha256")
        or audit.get("implementation_closure_payload_sha256") != closure.get("payload_sha256")
        or audit.get("runner_up_fallback") is not False
        or audit.get("claim_contract") != protocol.get("claim_contract")
        or not re.fullmatch(r"[0-9a-f]{32}", str(audit.get("run_nonce") or ""))
        or audit.get("run_nonce") == freeze.get("run_nonce")
    ):
        raise ValueError("historical audit contract bindings differ")
    execution_commit = str(audit["audit_execution_commit"])
    _verify_execution_lineage(
        execution_commit,
        evidence_path=AUDIT_PATH,
        required_files=(
            CLOSURE_PATH,
            PROTOCOL_PATH,
            INHERITED_PROTOCOL_PATH,
            ASSET_SELECTION_PATH,
            WINNER_FREEZE_PATH,
        ),
    )
    phase = audit.get("audit")
    if not isinstance(phase, Mapping):
        raise ValueError("historical audit lacks a complete audit phase")
    _verify_phase_reference(
        phase,
        stage_name="audit",
        gate_config=protocol["shared_absolute_gate"],
        validity_config=protocol["execution_validity_hard_fail"],
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=execution_commit,
        run_nonce=str(audit["run_nonce"]),
        predecessor={"kind": "winner_freeze", "payload_sha256": freeze["payload_sha256"]},
        verify_data=verify_data,
    )
    expected_status = (
        "historical_audit_passed" if phase["gate"]["passed"] is True else "historical_audit_failed"
    )
    if audit.get("status") != expected_status:
        raise ValueError("historical audit status differs from its recomputed gate")


def _verify_protocol_contract(protocol: Mapping[str, Any]) -> None:
    prior = protocol.get("prior_release")
    correction = protocol.get("correction_boundary")
    frozen = protocol.get("frozen_economic_contract")
    reclassification = protocol.get("train_reclassification_input")
    roles = protocol.get("metric_role_contract")
    validity = protocol.get("execution_validity_hard_fail")
    phases = protocol.get("physical_phases")
    selection = protocol.get("selection_contract")
    transport = protocol.get("transport_verification")
    claim = protocol.get("claim_contract")
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD
        or file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or protocol.get("release") != RELEASE
        or protocol.get("protocol_id") != PROTOCOL_ID
        or protocol.get("direction_change") is not False
        or protocol.get("route") != ROUTE
        or not isinstance(prior, Mapping)
        or prior.get("tag") != PRIOR_TAG
        or prior.get("annotated_tag_object") != PRIOR_TAG_OBJECT
        or prior.get("peeled_commit") != PRIOR_COMMIT
        or prior.get("protocol", {}).get("payload_sha256") != PRIOR_PROTOCOL_PAYLOAD
        or prior.get("prevalidation_closure", {}).get("payload_sha256")
        != PRIOR_CLOSURE_PAYLOAD
        or prior.get("execution_failure_receipt", {}).get("payload_sha256")
        != PRIOR_RECEIPT_PAYLOAD
        or not isinstance(correction, Mapping)
        or correction.get("post_hoc_reclassification") is not True
        or correction.get("train_market_data_reaccess_allowed") is not False
        or correction.get("train_source_rebuild_allowed") is not False
        or correction.get("train_strategy_rerun_allowed") is not False
        or correction.get("validation_market_outcomes_previously_opened") is not False
        or correction.get("audit_market_outcomes_previously_opened") is not False
        or not isinstance(frozen, Mapping)
        or frozen.get("strategy_id") != PRIMARY_ID
        or frozen.get("cash_comparator_id") != CASH_ONLY_ID
        or frozen.get("economic_thresholds_unchanged") is not True
        or not isinstance(reclassification, Mapping)
        or reclassification.get("source_receipt", {}).get("payload_sha256")
        != PRIOR_RECEIPT_PAYLOAD
        or reclassification.get("phase") != "train"
        or reclassification.get("role_gate_metrics_sha256")
        != canonical_payload_sha256(reclassification.get("role_gate_metrics", {}))
        or reclassification.get("economic_role_metrics_source_is_receipt_only")
        is not True
        or reclassification.get("receipt_bound_8_0_runtime_read_required")
        is not True
        or reclassification.get("new_train_market_access_required") is not False
        or not isinstance(roles, Mapping)
        or roles.get("required_roles_exact") != list(EVALUATION_ROLES)
        or roles.get("policy_operational_metrics", {}).get("roles")
        != ["primary", "stress"]
        or roles.get("accounting_validity", {}).get("roles")
        != list(EVALUATION_ROLES)
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
        or validity.get("invalid_execution_cannot_be_reclassified_as_gate_failure")
        is not True
        or not isinstance(phases, Mapping)
        or phases.get("train_reclassification", {}).get("runtime_stage")
        != PRIOR_WORK_ROOT.relative_to(ROOT).as_posix()
        or phases.get("train_reclassification", {}).get("evidence_path")
        != TRAIN_RECLASSIFICATION_PATH.as_posix()
        or phases.get("validation", {}).get("source_root")
        != (WORK_ROOT / "sources" / "stage=validation").relative_to(ROOT).as_posix()
        or phases.get("audit", {}).get("source_root")
        != (WORK_ROOT / "sources" / "stage=audit").relative_to(ROOT).as_posix()
        or not isinstance(selection, Mapping)
        or selection.get("strategy_id") != PRIMARY_ID
        or selection.get("candidate_count") != 1
        or selection.get("runner_up_fallback") is not False
        or not isinstance(transport, Mapping)
        or transport.get("github_repository") != "yxforever666gh/factor-lab"
        or transport.get("fallback_must_verify_exact_origin_main_commit") is not True
        or not isinstance(claim, Mapping)
        or claim.get("alpha_claim_allowed") is not False
        or claim.get("profit_claim_allowed") is not False
        or claim.get("stable_future_profit_claim_allowed") is not False
        or claim.get("fresh_future_evidence_required") is not True
        or claim.get("minimum_fresh_sessions") != 252
        or claim.get("minimum_fresh_monthly_executions") != 12
    ):
        raise ValueError("8.1 corrective protocol differs from its exact contract")


def _verify_implementation_map(
    closure: Mapping[str, Any], implementation_commit: str
) -> None:
    implementation = closure.get("implementation")
    if (
        not isinstance(implementation, Mapping)
        or set(implementation) != EXPECTED_IMPLEMENTATION_PATHS
    ):
        raise ValueError("closure implementation path set differs")
    for key, binding in implementation.items():
        if (
            not isinstance(binding, Mapping)
            or set(binding) != {"path", "sha256"}
            or binding.get("path") != key
            or not _is_sha256(binding.get("sha256"))
        ):
            raise ValueError(f"invalid implementation binding: {key}")
        path = _safe_repo_file(str(key))
        committed = _git("show", f"{implementation_commit}:{key}")
        if (
            file_sha256(path) != binding["sha256"]
            or hashlib.sha256(committed).hexdigest() != binding["sha256"]
        ):
            raise ValueError(f"frozen implementation bytes differ: {key}")


def _verify_prior_runtime_closure_admission(
    source_binding: Any,
    *,
    prior_receipt: Mapping[str, Any],
    protocol: Mapping[str, Any],
    verify_runtime: bool,
) -> dict[str, Any]:
    if (
        not isinstance(source_binding, Mapping)
        or not _is_sha256(source_binding.get("execution_validity_sha256"))
        or source_binding.get("artifact_parquet_count") != 20
        or source_binding.get("artifact_row_count") != 43222
    ):
        raise ValueError("8.1 closure lacks prior-runtime admission identity")
    if verify_runtime:
        execution_validity = _verify_prior_train_artifacts(prior_receipt)
        role_metrics = prior_receipt["train_stage"]["role_gate_metrics"]
        admission_metrics = _combine_receipt_role_gate_metrics(
            role_metrics, execution_validity=execution_validity
        )
        _require_execution_validity(
            admission_metrics, protocol["execution_validity_hard_fail"]
        )
        if (
            canonical_payload_sha256(execution_validity)
            != source_binding["execution_validity_sha256"]
            or execution_validity.get("artifact_parquet_count")
            != source_binding["artifact_parquet_count"]
            or execution_validity.get("artifact_row_count")
            != source_binding["artifact_row_count"]
        ):
            raise ValueError("8.1 closure prior-runtime admission differs")
    return {
        "execution_validity_sha256": source_binding["execution_validity_sha256"],
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
    }


def _verify_closure(
    *, verify_runtime: bool = True
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    closure = _read_json(CLOSURE_PATH)
    protocol = _read_json(PROTOCOL_PATH)
    selection = _read_json(ASSET_SELECTION_PATH)
    inherited_protocol = _read_json(INHERITED_PROTOCOL_PATH)
    prior_protocol = _read_json(PRIOR_PROTOCOL_PATH)
    prior_closure = _read_json(PRIOR_CLOSURE_PATH)
    prior_receipt = _read_json(PRIOR_RECEIPT_PATH)
    _verify_protocol_contract(protocol)
    if (
        inherited_protocol.get("payload_sha256") != INHERITED_PROTOCOL_PAYLOAD
        or canonical_payload_sha256(inherited_protocol) != INHERITED_PROTOCOL_PAYLOAD
        or file_sha256(ROOT / INHERITED_PROTOCOL_PATH)
        != INHERITED_PROTOCOL_FILE_SHA256
    ):
        raise ValueError("inherited 7.0 data/execution protocol bytes differ")
    if (
        set(closure) != _CLOSURE_FIELDS
        or closure.get("schema_version") != 1
        or closure.get("kind") != "factor_lab_release_closure"
        or closure.get("release") != RELEASE
        or closure.get("closure_role")
        != "policy_operational_reclassification_prevalidation_root"
        or closure.get("direction_change") is not False
        or closure.get("route") != protocol.get("route")
        or closure.get("status")
        != "implementation_frozen_before_8_1_reclassification"
        or closure.get("post_hoc_reclassification") is not True
        or closure.get("prior_train_returns_opened") is not True
        or closure.get("train_reexecution_allowed") is not False
        or closure.get("train_reclassification_status") != "pending"
        or closure.get("validation_market_outcomes_opened") is not False
        or closure.get("audit_status") != "not_opened"
        or closure.get("claim_contract") != protocol.get("claim_contract")
        or closure.get("protocol", {}).get("payload_sha256") != PROTOCOL_PAYLOAD
        or closure.get("protocol", {}).get("path") != PROTOCOL_PATH.as_posix()
        or closure.get("protocol", {}).get("protocol_id") != PROTOCOL_ID
        or closure.get("protocol", {}).get("file_sha256")
        != file_sha256(ROOT / PROTOCOL_PATH)
        or closure.get("formal_data") != {}
    ):
        raise ValueError("8.1 prevalidation closure contract differs")
    asset_binding = prior_protocol.get("assets", {}).get(
        "asset_selection_evidence", {}
    )
    if (
        selection.get("payload_sha256")
        != "b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b"
        or asset_binding.get("payload_sha256") != selection.get("payload_sha256")
        or asset_binding.get("file_sha256")
        != file_sha256(ROOT / ASSET_SELECTION_PATH)
        or selection.get("selected_codes")
        != [
            "510300.SH",
            "159920.SZ",
            "513100.SH",
            "518880.SH",
            "511010.SH",
            "511880.SH",
        ]
    ):
        raise ValueError("8.1 inherited asset-selection binding differs")
    current_prior_protocol = (ROOT / PRIOR_PROTOCOL_PATH).read_bytes()
    current_prior_closure = (ROOT / PRIOR_CLOSURE_PATH).read_bytes()
    current_prior_receipt = (ROOT / PRIOR_RECEIPT_PATH).read_bytes()
    expected_prior = protocol["prior_release"]
    prior_train = prior_receipt["train_stage"]
    prior_binding = prior_train["binding"]
    prior_evaluation = prior_train["evaluation"]
    role_metrics = prior_train["role_gate_metrics"]
    observed = prior_train["observed_gate"]
    source_binding = closure.get("train_reclassification_source")
    runtime_admission = _verify_prior_runtime_closure_admission(
        source_binding,
        prior_receipt=prior_receipt,
        protocol=protocol,
        verify_runtime=verify_runtime,
    )
    expected_source = {
        "source_release": "8.0",
        "receipt_path": PRIOR_RECEIPT_PATH.as_posix(),
        "receipt_file_sha256": PRIOR_RECEIPT_FILE_SHA256,
        "receipt_payload_sha256": PRIOR_RECEIPT_PAYLOAD,
        "execution_commit": prior_binding["execution_commit"],
        "run_nonce": prior_binding["run_nonce"],
        "source_manifest_payload_sha256": prior_train["manifest"]["payload_sha256"],
        "stage_binding_payload_sha256": prior_binding["payload_sha256"],
        "evaluation_payload_sha256": prior_evaluation["payload_sha256"],
        "evaluation_file_sha256": prior_evaluation["file_sha256"],
        "role_gate_metrics_sha256": canonical_payload_sha256(role_metrics),
        "original_gate_sha256": canonical_payload_sha256(observed),
        "original_gate_passed": False,
        "train_strategy_rerun_allowed": False,
        "economic_role_metrics_source": "published_receipt_only",
        "validity_source": "receipt_bound_retained_8_0_artifacts_read_only",
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
        **runtime_admission,
    }
    if (
        closure.get("prior_release") != expected_prior
        or closure.get("train_reclassification_source") != expected_source
        or _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
        or hashlib.sha256(current_prior_protocol).hexdigest()
        != PRIOR_PROTOCOL_FILE_SHA256
        or hashlib.sha256(current_prior_closure).hexdigest()
        != PRIOR_CLOSURE_FILE_SHA256
        or hashlib.sha256(current_prior_receipt).hexdigest()
        != PRIOR_RECEIPT_FILE_SHA256
        or prior_protocol.get("payload_sha256") != PRIOR_PROTOCOL_PAYLOAD
        or prior_closure.get("payload_sha256") != PRIOR_CLOSURE_PAYLOAD
        or prior_receipt.get("payload_sha256") != PRIOR_RECEIPT_PAYLOAD
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_PROTOCOL_PATH.as_posix()}")
        != current_prior_protocol
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_CLOSURE_PATH.as_posix()}")
        != current_prior_closure
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_RECEIPT_PATH.as_posix()}")
        != current_prior_receipt
    ):
        raise ValueError("8.1 closure prior-release lineage differs")
    current_runtime = _runtime_identity() if verify_runtime else None
    if verify_runtime and closure.get("runtime") != current_runtime:
        raise ValueError(f"formal runtime differs from closure: {current_runtime!r}")
    implementation_commit = str(closure.get("implementation_commit") or "")
    if not _is_commit(implementation_commit):
        raise ValueError("closure lacks implementation_commit")
    resolved_tree = _git("rev-parse", f"{implementation_commit}^{{tree}}").decode(
        "ascii"
    ).strip()
    if closure.get("implementation_tree") != resolved_tree:
        raise ValueError("closure implementation tree differs from its commit")
    _verify_implementation_map(closure, implementation_commit)
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("current HEAD does not descend from the frozen implementation")
    for relative in (
        PROTOCOL_PATH,
        INHERITED_PROTOCOL_PATH,
        ASSET_SELECTION_PATH,
        PRIOR_PROTOCOL_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_RECEIPT_PATH,
    ):
        if _git("show", f"{implementation_commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(f"implementation commit lacks frozen contract: {relative}")
    if verify_runtime:
        _require_source_imports()
    _require_committed(CLOSURE_PATH)
    return closure, protocol, selection


def _client() -> Any:
    config_path = ROOT / "configs" / "data.json"
    config = load_data_config(config_path)
    layout = RuntimeLayout.from_config(
        config,
        config_path=config_path,
        repo_root=ROOT,
    )
    return _configured_tushare_client(dict(config.get("sync") or {}), layout)


def _combine_static_metrics(
    primary: Mapping[str, Any],
    stress: Mapping[str, Any],
    cash: Mapping[str, Any],
    cash_stress: Mapping[str, Any],
) -> dict[str, Any]:
    roles = (primary, stress, cash, cash_stress)
    for key in (
        "observations",
        "start_date",
        "performance_start",
        "baseline_date",
        "end_date",
        "start_nav",
    ):
        if any(item.get(key) != primary.get(key) for item in roles[1:]):
            raise ValueError(f"8.0 role metrics do not share one phase identity: {key}")
    combined = dict(primary)
    policy_roles = (primary, stress)
    combined.update(
        {
            # 8.1 corrects only the role aggregation.  These are policy
            # operational metrics, so the cash comparator cannot set them.
            "annualized_turnover": max(
                float(item["annualized_turnover"]) for item in policy_roles
            ),
            "stress_cagr": float(stress["cagr"]),
            "stress_cost_cagr": float(stress["cagr"]),
            "cash_cagr": float(cash["cagr"]),
            "cash_stress_cagr": float(cash_stress["cagr"]),
            "cash_excess_cagr": float(primary["cagr"]) - float(cash["cagr"]),
            "stress_cash_excess_cagr": float(stress["cagr"])
            - float(cash_stress["cagr"]),
            "minimum_requested_notional_fill_ratio": min(
                float(item["requested_notional_fill_ratio"])
                for item in policy_roles
            ),
            "maximum_capacity_limited_requested_notional_ratio": max(
                float(item["capacity_limited_requested_notional_ratio"])
                for item in policy_roles
            ),
            "maximum_nav_reconciliation_error": max(
                float(item["nav_reconciliation_error"]) for item in roles
            ),
            "stress": dict(stress),
            "cash": dict(cash),
            "cash_stress": dict(cash_stress),
            "execution_validity": {
                "source": "new_phase_artifacts",
                "roles": {
                    "primary": dict(primary["execution_validity"]),
                    "stress": dict(stress["execution_validity"]),
                    "cash": dict(cash["execution_validity"]),
                    "cash_stress": dict(cash_stress["execution_validity"]),
                },
            },
        }
    )
    return combined


def _combine_receipt_role_gate_metrics(
    role_gate_metrics: Mapping[str, Any], *, execution_validity: Mapping[str, Any]
) -> dict[str, Any]:
    """Reclassify the published 8.0 role metrics without touching train runtime."""

    if set(role_gate_metrics) != set(EVALUATION_ROLES):
        raise ValueError("8.0 receipt role set differs from the exact four-role contract")
    required = {
        "cagr",
        "sharpe",
        "max_drawdown",
        "positive_complete_year_ratio",
        "annualized_turnover",
        "requested_notional_fill_ratio",
        "capacity_limited_requested_notional_ratio",
        "nav_reconciliation_error",
    }
    normalized: dict[str, dict[str, float]] = {}
    for role in EVALUATION_ROLES:
        raw = role_gate_metrics[role]
        if not isinstance(raw, Mapping) or set(raw) != required:
            raise ValueError(f"8.0 receipt {role} gate-metric field set differs")
        values = {key: float(raw[key]) for key in sorted(required)}
        if any(not pd.notna(value) or not float("-inf") < value < float("inf") for value in values.values()):
            raise ValueError(f"8.0 receipt {role} gate metrics are not finite")
        if values["capacity_limited_requested_notional_ratio"] < 0.0:
            raise ValueError(f"8.0 receipt {role} capacity ratio is negative")
        normalized[role] = values
    primary = normalized["primary"]
    stress = normalized["stress"]
    cash = normalized["cash"]
    cash_stress = normalized["cash_stress"]
    return {
        **primary,
        "annualized_turnover": max(
            primary["annualized_turnover"], stress["annualized_turnover"]
        ),
        "stress_cagr": stress["cagr"],
        "stress_cost_cagr": stress["cagr"],
        "cash_cagr": cash["cagr"],
        "cash_stress_cagr": cash_stress["cagr"],
        "cash_excess_cagr": primary["cagr"] - cash["cagr"],
        "stress_cash_excess_cagr": stress["cagr"] - cash_stress["cagr"],
        "minimum_requested_notional_fill_ratio": min(
            primary["requested_notional_fill_ratio"],
            stress["requested_notional_fill_ratio"],
        ),
        "maximum_capacity_limited_requested_notional_ratio": max(
            primary["capacity_limited_requested_notional_ratio"],
            stress["capacity_limited_requested_notional_ratio"],
        ),
        "maximum_nav_reconciliation_error": max(
            values["nav_reconciliation_error"] for values in normalized.values()
        ),
        "stress": stress,
        "cash": cash,
        "cash_stress": cash_stress,
        "role_gate_metrics": normalized,
        "execution_validity": dict(execution_validity),
    }


def _notional_close(left: float, right: float) -> bool:
    return math.isclose(
        float(left),
        float(right),
        rel_tol=NOTIONAL_REL_TOL,
        abs_tol=NOTIONAL_ABS_TOL_RMB,
    )


def _is_exact_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _role_execution_validity(
    result: Mapping[str, Any], *, expected_role_metrics: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Derive hard execution-validity counts and cross-artifact identities."""

    required_frames = set(EVALUATION_ARTIFACTS)
    if any(not isinstance(result.get(name), pd.DataFrame) for name in required_frames):
        raise RuntimeError("new phase result lacks the complete artifact set")
    trades = result["trades"]
    daily = result["daily_nav"]
    holdings = result["holdings"]
    capacity = result.get("capacity")
    if "status" not in trades or trades["status"].isna().any():
        raise RuntimeError("new phase trades lack an exact status column")
    statuses = trades["status"].astype(str)
    if not set(statuses).issubset(ALLOWED_TRADE_STATUSES):
        raise RuntimeError("new phase trades contain an unknown execution status")
    numeric_columns = (
        "requested_execution_notional",
        "actual_executed_notional",
        "capacity_limited_execution_notional",
        "planned_signal_notional",
        "capacity_rmb",
    )
    numeric: dict[str, pd.Series] = {}
    for column in numeric_columns:
        if column not in trades:
            raise RuntimeError(f"new phase trades lack {column}")
        values = pd.to_numeric(trades[column], errors="coerce")
        if values.isna().any() or not values.map(
            lambda value: math.isfinite(float(value))
        ).all():
            raise RuntimeError(f"new phase trades contain invalid {column}")
        if (values < 0.0).any():
            raise RuntimeError(f"new phase trades contain negative {column}")
        numeric[column] = values
    requested_values = numeric["requested_execution_notional"]
    executed_values = numeric["actual_executed_notional"]
    capacity_limited_values = numeric["capacity_limited_execution_notional"]
    if (
        capacity_limited_values > requested_values + NOTIONAL_ABS_TOL_RMB
    ).any():
        raise RuntimeError("capacity-limited notional exceeds requested notional")
    if (executed_values > requested_values + NOTIONAL_ABS_TOL_RMB).any():
        raise RuntimeError("executed notional exceeds requested notional")
    for status, requested_value, executed_value in zip(
        statuses,
        requested_values,
        executed_values,
        strict=True,
    ):
        requested_float = float(requested_value)
        executed_float = float(executed_value)
        if requested_float <= NOTIONAL_ABS_TOL_RMB:
            raise RuntimeError("execution status has no positive requested notional")
        if status == "executed":
            valid_status = executed_float > NOTIONAL_ABS_TOL_RMB and _notional_close(
                executed_float, requested_float
            )
        elif status == "partial_cash":
            valid_status = (
                executed_float > NOTIONAL_ABS_TOL_RMB
                and executed_float < requested_float - NOTIONAL_ABS_TOL_RMB
            )
        else:
            valid_status = executed_float <= NOTIONAL_ABS_TOL_RMB
        if not valid_status:
            raise RuntimeError("execution status and executed notional are inconsistent")
    requested = math.fsum(float(value) for value in requested_values)
    executed = math.fsum(float(value) for value in executed_values)
    capacity_limited = math.fsum(float(value) for value in capacity_limited_values)
    expected_fill_ratio = executed / requested if requested else 1.0
    expected_capacity_ratio = capacity_limited / requested if requested else 0.0
    derived_capacity_violation_count = int(
        (
            numeric["planned_signal_notional"]
            > numeric["capacity_rmb"] + 1e-8
        ).sum()
    )

    capacity_identity_exact = True
    requested_fill_identity_exact = True
    if isinstance(capacity, Mapping):
        summary_fields = (
            "requested_notional_total",
            "executed_notional_total",
            "capacity_limited_requested_notional",
            "capacity_limited_requested_notional_ratio",
            "requested_notional_fill_ratio",
        )
        try:
            summary = {name: float(capacity[name]) for name in summary_fields}
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("new phase capacity summary is incomplete") from exc
        if any(not math.isfinite(value) or value < 0.0 for value in summary.values()):
            raise RuntimeError("new phase capacity summary is invalid")
        summary_violations = capacity.get("capacity_violation_count")
        if (
            not _is_exact_nonnegative_int(summary_violations)
            or summary_violations != derived_capacity_violation_count
            or summary_violations != 0
        ):
            raise RuntimeError("new phase capacity violation summary differs")
        capacity_identity_exact = (
            _notional_close(summary["requested_notional_total"], requested)
            and _notional_close(summary["executed_notional_total"], executed)
            and _notional_close(
                summary["capacity_limited_requested_notional"], capacity_limited
            )
            and math.isclose(
                summary["capacity_limited_requested_notional_ratio"],
                expected_capacity_ratio,
                rel_tol=NOTIONAL_REL_TOL,
                abs_tol=1e-15,
            )
        )
        requested_fill_identity_exact = math.isclose(
            summary["requested_notional_fill_ratio"],
            expected_fill_ratio,
            rel_tol=NOTIONAL_REL_TOL,
            abs_tol=1e-15,
        )
    elif not isinstance(expected_role_metrics, Mapping):
        raise RuntimeError("new phase result lacks capacity summary")

    required_daily = {
        "trade_date",
        "cash",
        "nav",
        "requested_notional",
        "executed_notional",
        "capacity_limited_requested_notional",
        "accounting_error",
    }
    if not required_daily.issubset(daily.columns):
        raise RuntimeError("new phase daily NAV lacks execution/accounting fields")
    daily_numeric: dict[str, pd.Series] = {}
    for column in required_daily - {"trade_date"}:
        values = pd.to_numeric(daily[column], errors="coerce")
        if values.isna().any() or not values.map(
            lambda value: math.isfinite(float(value))
        ).all():
            raise RuntimeError(f"new phase daily NAV contains invalid {column}")
        daily_numeric[column] = values
    cash = daily_numeric["cash"]
    nav = daily_numeric["nav"]
    if (nav <= 0.0).any():
        raise RuntimeError("new phase daily NAV is not strictly positive")
    for column in (
        "requested_notional",
        "executed_notional",
        "capacity_limited_requested_notional",
    ):
        if (daily_numeric[column] < 0.0).any():
            raise RuntimeError(f"new phase daily NAV contains negative {column}")
    daily_requested = math.fsum(
        float(value) for value in daily_numeric["requested_notional"]
    )
    daily_executed = math.fsum(
        float(value) for value in daily_numeric["executed_notional"]
    )
    daily_capacity_limited = math.fsum(
        float(value)
        for value in daily_numeric["capacity_limited_requested_notional"]
    )
    daily_trade_notional_identity_exact = (
        _notional_close(daily_requested, requested)
        and _notional_close(daily_executed, executed)
        and _notional_close(daily_capacity_limited, capacity_limited)
    )
    maximum_accounting_error = float(daily_numeric["accounting_error"].abs().max())
    if isinstance(expected_role_metrics, Mapping):
        try:
            metric_fill = float(expected_role_metrics["requested_notional_fill_ratio"])
            metric_capacity = float(
                expected_role_metrics["capacity_limited_requested_notional_ratio"]
            )
            metric_accounting = float(
                expected_role_metrics["nav_reconciliation_error"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("role metrics lack execution/accounting identities") from exc
        if not all(
            math.isfinite(value)
            for value in (metric_fill, metric_capacity, metric_accounting)
        ):
            raise RuntimeError("role metrics contain invalid execution identities")
        requested_fill_identity_exact = requested_fill_identity_exact and math.isclose(
            metric_fill,
            expected_fill_ratio,
            rel_tol=NOTIONAL_REL_TOL,
            abs_tol=1e-15,
        )
        capacity_identity_exact = capacity_identity_exact and math.isclose(
            metric_capacity,
            expected_capacity_ratio,
            rel_tol=NOTIONAL_REL_TOL,
            abs_tol=1e-15,
        )
        if metric_accounting != maximum_accounting_error:
            raise RuntimeError("role accounting metric differs from daily NAV")
    if not {"trade_date", "market_value"}.issubset(holdings.columns):
        raise RuntimeError("new phase holdings lack trade_date/market_value")
    holding_market_value = pd.to_numeric(holdings["market_value"], errors="coerce")
    if (
        holding_market_value.isna().any()
        or not holding_market_value.map(
            lambda value: math.isfinite(float(value))
        ).all()
    ):
        raise RuntimeError("new phase holdings contain invalid market value")
    gross = (
        holdings.assign(market_value_numeric=holding_market_value.abs())
        .groupby("trade_date", sort=False)["market_value_numeric"]
        .sum()
    )
    daily_indexed = daily.assign(
        trade_date=pd.to_datetime(daily["trade_date"], errors="coerce")
    ).set_index("trade_date")
    gross.index = pd.to_datetime(gross.index, errors="coerce")
    aligned_gross = gross.reindex(daily_indexed.index)
    if aligned_gross.isna().any():
        raise RuntimeError("new phase holdings do not cover every daily NAV date")
    leverage = aligned_gross.to_numpy(dtype=float) > (
        pd.to_numeric(daily_indexed["nav"], errors="coerce").to_numpy(dtype=float)
        + 1e-8
    )
    gross_ratio = aligned_gross.to_numpy(dtype=float) / pd.to_numeric(
        daily_indexed["nav"], errors="coerce"
    ).to_numpy(dtype=float)
    if not all(math.isfinite(float(value)) and value >= 0.0 for value in gross_ratio):
        raise RuntimeError("new phase gross exposure ratio is invalid")
    return {
        "artifact_set_complete": True,
        "status_values_allowed": True,
        "status_execution_identity_exact": True,
        "blocked_missing_open_trade_count": int(
            (statuses == "blocked_missing_open").sum()
        ),
        "blocked_capacity_trade_count": int((statuses == "blocked_capacity").sum()),
        "capacity_violation_count": derived_capacity_violation_count,
        "negative_cash_observation_count": int((cash < -1e-8).sum()),
        "leverage_observation_count": int(leverage.sum()),
        "minimum_cash": float(cash.min()),
        "maximum_gross_exposure_ratio": float(gross_ratio.max()),
        "maximum_nav_reconciliation_error": maximum_accounting_error,
        "requested_notional_total": requested,
        "executed_notional_total": executed,
        "capacity_limited_requested_notional": capacity_limited,
        "capacity_fields_finite_and_nonnegative": True,
        "executed_notional_not_above_requested": True,
        "capacity_limited_notional_not_above_requested": True,
        "capacity_aggregation_identity_exact": bool(capacity_identity_exact),
        "requested_fill_identity_exact": bool(requested_fill_identity_exact),
        "daily_trade_notional_identity_exact": bool(
            daily_trade_notional_identity_exact
        ),
    }


def _phase_role_metrics(
    result: Mapping[str, Any], *, start: str, end: str
) -> dict[str, Any]:
    metrics = phase_metrics(result, start=start, end=end)
    metrics["execution_validity"] = _role_execution_validity(
        result, expected_role_metrics=metrics
    )
    return metrics


def _require_execution_validity(
    metrics: Mapping[str, Any], validity_config: Mapping[str, Any]
) -> None:
    validity = metrics.get("execution_validity")
    if not isinstance(validity, Mapping):
        raise RuntimeError("execution validity evidence is absent")
    source = validity.get("source")
    if source == "receipt_bound_8_0_train_artifacts":
        if (
            set(validity)
            != {
                "source",
                "receipt_train_phase_deep_verified",
                "artifact_parquet_count",
                "artifact_row_count",
                "roles",
            }
            or validity.get("receipt_train_phase_deep_verified") is not True
            or validity.get("artifact_parquet_count") != 20
            or validity.get("artifact_row_count") != 43222
        ):
            raise RuntimeError("receipt-bound execution validity identity differs")
    elif source == "new_phase_artifacts":
        if set(validity) != {"source", "roles"}:
            raise RuntimeError("new-phase execution validity identity differs")
    else:
        raise RuntimeError("execution validity source differs")
    roles = validity.get("roles")
    if not isinstance(roles, Mapping) or set(roles) != set(EVALUATION_ROLES):
        raise RuntimeError("execution validity role set differs")
    configured_counts = {
        "capacity_violation_count": validity_config[
            "capacity_violation_count_at_most"
        ],
        "negative_cash_observation_count": validity_config[
            "negative_cash_observation_count_at_most"
        ],
        "leverage_observation_count": validity_config.get(
            "leverage_observation_count_at_most", 0
        ),
    }
    if not all(_is_exact_nonnegative_int(value) for value in configured_counts.values()):
        raise RuntimeError("execution validity count thresholds must be exact integers")
    required = {
        "artifact_set_complete",
        "status_values_allowed",
        "status_execution_identity_exact",
        "blocked_missing_open_trade_count",
        "blocked_capacity_trade_count",
        "capacity_violation_count",
        "negative_cash_observation_count",
        "leverage_observation_count",
        "minimum_cash",
        "maximum_gross_exposure_ratio",
        "maximum_nav_reconciliation_error",
        "requested_notional_total",
        "executed_notional_total",
        "capacity_limited_requested_notional",
        "capacity_fields_finite_and_nonnegative",
        "executed_notional_not_above_requested",
        "capacity_limited_notional_not_above_requested",
        "capacity_aggregation_identity_exact",
        "requested_fill_identity_exact",
        "daily_trade_notional_identity_exact",
    }
    metric_roles = {
        "primary": metrics,
        "stress": metrics.get("stress"),
        "cash": metrics.get("cash"),
        "cash_stress": metrics.get("cash_stress"),
    }
    for role in EVALUATION_ROLES:
        item = roles[role]
        metric_role = metric_roles[role]
        count_values = {
            key: item.get(key) if isinstance(item, Mapping) else None
            for key in (
                "blocked_missing_open_trade_count",
                "blocked_capacity_trade_count",
                "capacity_violation_count",
                "negative_cash_observation_count",
                "leverage_observation_count",
            )
        }
        if (
            not isinstance(item, Mapping)
            or set(item) != required
            or item.get("artifact_set_complete") is not True
            or item.get("status_values_allowed") is not True
            or item.get("status_execution_identity_exact") is not True
            or not all(
                _is_exact_nonnegative_int(value) for value in count_values.values()
            )
            or any(
                count_values[key] > threshold
                for key, threshold in configured_counts.items()
            )
            or not math.isfinite(float(item["minimum_cash"]))
            or float(item["minimum_cash"]) < -1e-8
            or not math.isfinite(float(item["maximum_gross_exposure_ratio"]))
            or float(item["maximum_gross_exposure_ratio"]) < 0.0
            or float(item["maximum_gross_exposure_ratio"]) > 1.0 + 1e-8
            or not math.isfinite(float(item["maximum_nav_reconciliation_error"]))
            or float(item["maximum_nav_reconciliation_error"]) < 0.0
            or not isinstance(metric_role, Mapping)
            or float(item["maximum_nav_reconciliation_error"])
            != float(metric_role.get("nav_reconciliation_error", float("nan")))
            or not math.isfinite(float(item["requested_notional_total"]))
            or float(item["requested_notional_total"]) < 0.0
            or not math.isfinite(float(item["executed_notional_total"]))
            or float(item["executed_notional_total"]) < 0.0
            or float(item["executed_notional_total"])
            > float(item["requested_notional_total"]) + NOTIONAL_ABS_TOL_RMB
            or not math.isfinite(
                float(item["capacity_limited_requested_notional"])
            )
            or float(item["capacity_limited_requested_notional"]) < 0.0
            or float(item["capacity_limited_requested_notional"])
            > float(item["requested_notional_total"]) + NOTIONAL_ABS_TOL_RMB
            or item.get("capacity_fields_finite_and_nonnegative") is not True
            or item.get("executed_notional_not_above_requested") is not True
            or item.get("capacity_limited_notional_not_above_requested") is not True
            or item.get("capacity_aggregation_identity_exact") is not True
            or item.get("requested_fill_identity_exact") is not True
            or item.get("daily_trade_notional_identity_exact") is not True
        ):
            raise RuntimeError(f"{role} execution validity hard fail")


def _evaluate_static_gate(
    metrics: Mapping[str, Any],
    gate_config: Mapping[str, Any],
    validity_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if validity_config is not None:
        _require_execution_validity(metrics, validity_config)
    base_gate = gate_config["base"]
    stress_gate = gate_config["stress_16bp"]
    operational_gate = gate_config["operational"]
    if any(
        section.get(key) is not True
        for section, key in (
            (base_gate, "net_cagr_strictly_positive"),
            (base_gate, "cash_excess_cagr_strictly_positive"),
            (stress_gate, "net_cagr_strictly_positive"),
            (stress_gate, "cash_excess_cagr_strictly_positive"),
        )
    ):
        raise ValueError("8.1 strict-positive gate switches must remain enabled")
    values = {
        "nominal_cagr_strictly_positive": {
            "metric": float(metrics["cagr"]),
            "threshold": 0.0,
        },
        "sharpe_at_least": {
            "metric": float(metrics["sharpe"]),
            "threshold": float(base_gate["net_sharpe_at_least"]),
        },
        "max_drawdown_at_least": {
            "metric": float(metrics["max_drawdown"]),
            "threshold": float(base_gate["daily_max_drawdown_at_least"]),
        },
        "positive_complete_year_ratio_at_least": {
            "metric": float(metrics["positive_complete_year_ratio"]),
            "threshold": float(
                base_gate["positive_complete_year_ratio_at_least"]
            ),
        },
        "cash_excess_cagr_strictly_positive": {
            "metric": float(metrics["cash_excess_cagr"]),
            "threshold": 0.0,
        },
        "stress_nominal_cagr_strictly_positive": {
            "metric": float(metrics["stress"]["cagr"]),
            "threshold": 0.0,
        },
        "stress_cash_excess_cagr_strictly_positive": {
            "metric": float(metrics["stress_cash_excess_cagr"]),
            "threshold": 0.0,
        },
        "stress_sharpe_at_least": {
            "metric": float(metrics["stress"]["sharpe"]),
            "threshold": float(stress_gate["net_sharpe_at_least"]),
        },
        "stress_max_drawdown_at_least": {
            "metric": float(metrics["stress"]["max_drawdown"]),
            "threshold": float(stress_gate["daily_max_drawdown_at_least"]),
        },
        "stress_positive_complete_year_ratio_at_least": {
            "metric": float(metrics["stress"]["positive_complete_year_ratio"]),
            "threshold": float(
                stress_gate["positive_complete_year_ratio_at_least"]
            ),
        },
        "annualized_turnover_at_most": {
            "metric": float(metrics["annualized_turnover"]),
            "threshold": float(operational_gate["annualized_turnover_at_most"]),
        },
        "requested_notional_fill_ratio_at_least": {
            "metric": float(metrics["minimum_requested_notional_fill_ratio"]),
            "threshold": float(
                operational_gate["requested_notional_fill_ratio_at_least"]
            ),
        },
        "capacity_limited_requested_notional_ratio_at_most": {
            "metric": float(
                metrics["maximum_capacity_limited_requested_notional_ratio"]
            ),
            "threshold": float(
                operational_gate[
                    "capacity_limited_requested_notional_ratio_at_most"
                ]
            ),
        },
        "nav_reconciliation_error_at_most": {
            "metric": float(metrics["maximum_nav_reconciliation_error"]),
            "threshold": float(
                operational_gate["nav_reconciliation_error_at_most"]
            ),
        },
    }
    checks = {
        "nominal_cagr_strictly_positive": values[
            "nominal_cagr_strictly_positive"
        ]["metric"]
        > 0.0,
        "sharpe_at_least": values["sharpe_at_least"]["metric"]
        >= values["sharpe_at_least"]["threshold"],
        "max_drawdown_at_least": values["max_drawdown_at_least"]["metric"]
        >= values["max_drawdown_at_least"]["threshold"],
        "positive_complete_year_ratio_at_least": values[
            "positive_complete_year_ratio_at_least"
        ]["metric"]
        >= values["positive_complete_year_ratio_at_least"]["threshold"],
        "cash_excess_cagr_strictly_positive": values[
            "cash_excess_cagr_strictly_positive"
        ]["metric"]
        > 0.0,
        "stress_nominal_cagr_strictly_positive": values[
            "stress_nominal_cagr_strictly_positive"
        ]["metric"]
        > 0.0,
        "stress_cash_excess_cagr_strictly_positive": values[
            "stress_cash_excess_cagr_strictly_positive"
        ]["metric"]
        > 0.0,
        "stress_sharpe_at_least": values["stress_sharpe_at_least"]["metric"]
        >= values["stress_sharpe_at_least"]["threshold"],
        "stress_max_drawdown_at_least": values[
            "stress_max_drawdown_at_least"
        ]["metric"]
        >= values["stress_max_drawdown_at_least"]["threshold"],
        "stress_positive_complete_year_ratio_at_least": values[
            "stress_positive_complete_year_ratio_at_least"
        ]["metric"]
        >= values["stress_positive_complete_year_ratio_at_least"]["threshold"],
        "annualized_turnover_at_most": values[
            "annualized_turnover_at_most"
        ]["metric"]
        <= values["annualized_turnover_at_most"]["threshold"],
        "requested_notional_fill_ratio_at_least": values[
            "requested_notional_fill_ratio_at_least"
        ]["metric"]
        >= values["requested_notional_fill_ratio_at_least"]["threshold"],
        "capacity_limited_requested_notional_ratio_at_most": values[
            "capacity_limited_requested_notional_ratio_at_most"
        ]["metric"]
        <= values["capacity_limited_requested_notional_ratio_at_most"][
            "threshold"
        ],
        "nav_reconciliation_error_at_most": values[
            "nav_reconciliation_error_at_most"
        ]["metric"]
        <= values["nav_reconciliation_error_at_most"]["threshold"],
    }
    return {"passed": all(checks.values()), "checks": checks, "values": values}


def _binding_path(stage_name: str) -> Path:
    return BINDING_ROOT / f"{stage_name}.json"


def _reject_partial_entries(root: Path) -> None:
    if not root.exists():
        return
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"formal runtime root is indirect or not a directory: {root}")
    partials = sorted(path.name for path in root.iterdir() if ".partial-" in path.name)
    if partials:
        raise RuntimeError(f"stale partial formal artifacts require explicit recovery: {partials}")


def _assert_runtime_layout(allowed_stages: set[str]) -> None:
    if not allowed_stages.issubset({"validation", "audit"}):
        raise RuntimeError("8.1 runtime may contain only validation and audit stages")
    if WORK_ROOT.is_symlink() or (WORK_ROOT.exists() and not WORK_ROOT.is_dir()):
        raise RuntimeError("8.1 runtime root must be a regular local directory")
    if not allowed_stages and WORK_ROOT.exists():
        raise RuntimeError("8.1 reclassification-only state requires an absent runtime root")
    expected = {
        SOURCE_ROOT: {f"stage={stage}" for stage in allowed_stages},
        EVALUATION_ROOT: {f"stage={stage}" for stage in allowed_stages},
        BINDING_ROOT: {f"{stage}.json" for stage in allowed_stages},
    }
    if WORK_ROOT.exists():
        allowed_roots = {root.name for root in expected}
        unexpected_roots = sorted(
            path.name for path in WORK_ROOT.iterdir() if path.name not in allowed_roots
        )
        if unexpected_roots:
            raise RuntimeError(
                f"unexpected entry under the 8.1 runtime root: {unexpected_roots}"
            )
    for root, allowed_names in expected.items():
        if root.is_symlink() or (root.exists() and not root.is_dir()):
            raise RuntimeError(f"formal runtime component is indirect or not a directory: {root}")
        _reject_partial_entries(root)
        if not root.exists():
            continue
        unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed_names)
        if unexpected:
            raise RuntimeError(f"unexpected or renamed formal stage artifacts under {root}: {unexpected}")
    if WORK_ROOT.exists() and PRIOR_WORK_ROOT.exists():
        prior_files = tuple(path for path in PRIOR_WORK_ROOT.rglob("*") if path.is_file())
        for current in (path for path in WORK_ROOT.rglob("*") if path.is_file()):
            for prior in prior_files:
                try:
                    same = os.path.samefile(current, prior)
                except OSError as exc:
                    raise RuntimeError("could not verify 8.1/8.0 physical isolation") from exc
                if same:
                    raise RuntimeError(
                        f"8.1 runtime file reuses an 8.0 physical file: {current}"
                    )


def _assert_evidence_layout(allowed_names: set[str]) -> None:
    root = ROOT / EVIDENCE_ROOT
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise RuntimeError("8.1 evidence root must be a regular local directory")
    if not root.exists():
        return
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed_names)
    if unexpected:
        raise RuntimeError(f"unexpected 8.1 evidence artifact: {unexpected}")
    for path in root.iterdir():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"8.1 evidence artifact is indirect or not a file: {path}")


def _verify_stage_binding(
    stage_name: str,
    stage: MultiAssetStage,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    binding_path = _binding_path(stage_name)
    if binding_path.is_symlink() or not binding_path.is_file():
        raise ValueError(f"existing {stage_name} stage lacks a regular external binding")
    binding = _read_json(binding_path)
    manifest_path = stage.path / "manifest.json"
    if (
        set(binding) != _STAGE_BINDING_FIELDS
        or binding.get("schema_version") != 1
        or binding.get("kind") != "factor_lab_multi_asset_stage_binding"
        or binding.get("release") != RELEASE
        or binding.get("stage") != stage_name
        or binding.get("stage_manifest_payload_sha256") != stage.manifest.get("payload_sha256")
        or binding.get("stage_manifest_file_sha256") != file_sha256(manifest_path)
        or binding.get("implementation_closure_payload_sha256") != closure_payload
        or binding.get("execution_commit") != execution_commit
        or binding.get("predecessor") != dict(predecessor)
        or binding.get("run_nonce") != run_nonce
    ):
        raise ValueError(f"{stage_name} external stage binding differs")
    return binding


def _load_bound_stage(
    stage_name: str,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> tuple[MultiAssetStage, dict[str, Any]]:
    spec = STAGES[stage_name]
    stage = load_multi_asset_stage(SOURCE_ROOT, stage_name)
    manifest = stage.manifest
    if (
        manifest.get("stage") != stage_name
        or manifest.get("price_start_date") != spec["source_start"]
        or manifest.get("price_end_date") != spec["source_end"]
    ):
        raise ValueError(f"{stage_name} source stage does not match the protocol")
    binding = _verify_stage_binding(
        stage_name,
        stage,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    return stage, binding


def _stage(
    stage_name: str,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> tuple[MultiAssetStage, dict[str, Any]]:
    spec = STAGES[stage_name]
    path = SOURCE_ROOT / f"stage={stage_name}"
    binding_path = _binding_path(stage_name)
    _reject_partial_entries(SOURCE_ROOT)
    if path.exists() or path.is_symlink():
        raise RuntimeError(
            f"8.1 corrective run forbids a pre-existing {stage_name} source stage"
        )
    if binding_path.exists() or binding_path.is_symlink():
        raise ValueError(f"{stage_name} binding exists without its stage")
    print(f"capturing {stage_name} source through {spec['source_end']}", flush=True)
    stage = capture_multi_asset_stage(
        _client(), SOURCE_ROOT, spec["source_start"], spec["source_end"], stage_name
    )
    manifest = stage.manifest
    if (
        manifest.get("stage") != stage_name
        or manifest.get("price_start_date") != spec["source_start"]
        or manifest.get("price_end_date") != spec["source_end"]
    ):
        raise ValueError(f"{stage_name} source stage does not match the protocol")
    binding: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_stage_binding",
        "release": RELEASE,
        "stage": stage_name,
        "stage_manifest_payload_sha256": manifest["payload_sha256"],
        "stage_manifest_file_sha256": file_sha256(stage.path / "manifest.json"),
        "implementation_closure_payload_sha256": closure_payload,
        "execution_commit": execution_commit,
        "predecessor": dict(predecessor),
        "run_nonce": run_nonce,
    }
    binding["payload_sha256"] = canonical_payload_sha256(binding)
    _create_only(binding_path, binding)
    return _load_bound_stage(
        stage_name,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )


def _filter_targets(
    targets: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> pd.DataFrame:
    executions = pd.to_datetime(targets["execution_date"], errors="coerce").dt.normalize()
    selected = targets.loc[
        executions.between(pd.Timestamp(start), pd.Timestamp(end))
    ].copy()
    if selected.empty:
        raise ValueError(f"phase has no targets between {start} and {end}")
    return selected.reset_index(drop=True)


def _run_one(
    stage: MultiAssetStage,
    *,
    strategy_id: str,
    start: str,
    end: str,
    cost_bps: float,
) -> dict[str, Any]:
    sessions = tuple(pd.to_datetime(stage.calendar["trade_date"]).dt.normalize())
    targets = build_monthly_targets(stage.assets, sessions, strategy_id)
    targets = _filter_targets(targets, start=start, end=end)
    return simulate_targets(
        stage.assets,
        targets,
        sessions,
        SimulationConfig(cost_bps_per_side=cost_bps),
    )


def _dataframe_artifact(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    frame.to_parquet(path, index=False, compression="zstd")
    return {
        "path": path.name,
        "rows": int(len(frame)),
        "size_bytes": path.stat().st_size,
        "file_sha256": file_sha256(path),
    }


def _persist_evaluation(
    stage_name: str,
    *,
    source_manifest_payload: str,
    results: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, Any],
    gate: Mapping[str, Any],
    closure_payload: str,
    stage_binding_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    destination = EVALUATION_ROOT / f"stage={stage_name}"
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"evaluation is create-only: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.partial-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        artifacts: dict[str, Any] = {}
        for role, result in sorted(results.items()):
            role_artifacts: dict[str, Any] = {}
            for name in ("targets", "orders", "daily_nav", "holdings", "trades"):
                frame = result.get(name)
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError(f"{role} result lacks DataFrame {name}")
                role_artifacts[name] = _dataframe_artifact(
                    staging / f"{role}-{name}.parquet", frame
                )
            artifacts[role] = role_artifacts
        value: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_multi_asset_evaluation",
            "stage": stage_name,
            "source_manifest_payload_sha256": source_manifest_payload,
            "stage_binding_payload_sha256": stage_binding_payload,
            "implementation_closure_payload_sha256": closure_payload,
            "execution_commit": execution_commit,
            "predecessor": dict(predecessor),
            "run_nonce": run_nonce,
            "metrics": dict(metrics),
            "gate": dict(gate),
            "artifacts": artifacts,
        }
        value["payload_sha256"] = canonical_payload_sha256(value)
        (staging / "evaluation.json").write_bytes(
            json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")
            + b"\n"
        )
        os.rename(staging, destination)
        return value
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def _load_evaluation(
    stage_name: str,
    *,
    source_manifest_payload: str,
    stage_binding_payload: str,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    directory = EVALUATION_ROOT / f"stage={stage_name}"
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"{stage_name} evaluation is missing or indirect")
    evaluation_path = directory / "evaluation.json"
    if evaluation_path.is_symlink() or not evaluation_path.is_file():
        raise ValueError(f"{stage_name} evaluation JSON is missing or indirect")
    value = _read_json(evaluation_path)
    expected_files = {
        "evaluation.json",
        *(
            f"{role}-{artifact}.parquet"
            for role in EVALUATION_ROLES
            for artifact in EVALUATION_ARTIFACTS
        ),
    }
    if {path.name for path in directory.iterdir()} != expected_files:
        raise ValueError(f"{stage_name} evaluation contains an unexpected file set")
    if (
        set(value) != _EVALUATION_FIELDS
        or value.get("schema_version") != 1
        or value.get("kind") != "factor_lab_multi_asset_evaluation"
        or value.get("stage") != stage_name
        or value.get("source_manifest_payload_sha256") != source_manifest_payload
        or value.get("stage_binding_payload_sha256") != stage_binding_payload
        or value.get("implementation_closure_payload_sha256") != closure_payload
        or value.get("execution_commit") != execution_commit
        or value.get("predecessor") != dict(predecessor)
        or value.get("run_nonce") != run_nonce
    ):
        raise ValueError(f"{stage_name} evaluation identity differs")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(EVALUATION_ROLES):
        raise ValueError(f"{stage_name} evaluation role set differs")
    for role in EVALUATION_ROLES:
        role_artifacts = artifacts.get(role)
        if not isinstance(role_artifacts, Mapping) or set(role_artifacts) != set(EVALUATION_ARTIFACTS):
            raise ValueError(f"{stage_name} {role} artifact set differs")
        for artifact in EVALUATION_ARTIFACTS:
            entry = role_artifacts[artifact]
            expected_name = f"{role}-{artifact}.parquet"
            path = directory / expected_name
            if (
                not isinstance(entry, Mapping)
                or set(entry) != {"path", "rows", "size_bytes", "file_sha256"}
                or entry.get("path") != expected_name
                or path.is_symlink()
                or not path.is_file()
                or path.stat().st_size != entry.get("size_bytes")
                or file_sha256(path) != entry.get("file_sha256")
                or len(pd.read_parquet(path)) != entry.get("rows")
            ):
                raise ValueError(f"{stage_name} evaluation artifact differs: {expected_name}")
    return value, file_sha256(evaluation_path)


def _phase_from_evaluation(
    evaluation: Mapping[str, Any], evaluation_file_sha256: str
) -> dict[str, Any]:
    return {
        "source_manifest_payload_sha256": evaluation["source_manifest_payload_sha256"],
        "stage_binding_payload_sha256": evaluation["stage_binding_payload_sha256"],
        "evaluation_payload_sha256": evaluation["payload_sha256"],
        "evaluation_file_sha256": evaluation_file_sha256,
        "metrics": evaluation["metrics"],
        "gate": evaluation["gate"],
    }


def _replay_evaluation(
    stage_name: str,
    *,
    stage: MultiAssetStage,
    evaluation: Mapping[str, Any],
    gate_config: Mapping[str, Any],
    validity_config: Mapping[str, Any],
) -> None:
    directory = EVALUATION_ROOT / f"stage={stage_name}"
    sessions = tuple(pd.to_datetime(stage.calendar["trade_date"]).dt.normalize())
    regenerated: dict[str, Mapping[str, Any]] = {}
    for role, strategy_id, cost_bps in (
        ("primary", PRIMARY_ID, 8.0),
        ("stress", PRIMARY_ID, 16.0),
        ("cash", CASH_ONLY_ID, 8.0),
        ("cash_stress", CASH_ONLY_ID, 16.0),
    ):
        target_path = directory / f"{role}-targets.parquet"
        targets = pd.read_parquet(target_path)
        spec = STAGES[stage_name]
        expected_targets = _filter_targets(
            build_monthly_targets(stage.assets, sessions, strategy_id),
            start=spec["performance_start"],
            end=spec["performance_end"],
        )
        try:
            target_sort_keys = ["signal_date", "code"]
            pd.testing.assert_frame_equal(
                targets.sort_values(target_sort_keys, kind="mergesort").reset_index(
                    drop=True
                ),
                expected_targets.sort_values(
                    target_sort_keys, kind="mergesort"
                ).reset_index(drop=True),
                check_dtype=True,
                check_exact=True,
            )
        except AssertionError as exc:
            raise ValueError(
                f"{stage_name} {role} targets do not match the causal builder"
            ) from exc
        result = simulate_targets(
            stage.assets,
            expected_targets,
            sessions,
            SimulationConfig(cost_bps_per_side=cost_bps),
        )
        for artifact in EVALUATION_ARTIFACTS:
            persisted = pd.read_parquet(directory / f"{role}-{artifact}.parquet")
            try:
                pd.testing.assert_frame_equal(
                    persisted.reset_index(drop=True),
                    result[artifact].reset_index(drop=True),
                    check_dtype=True,
                    check_exact=True,
                )
            except AssertionError as exc:
                raise ValueError(
                    f"{stage_name} {role} {artifact} does not replay exactly"
                ) from exc
        regenerated[role] = result
    spec = STAGES[stage_name]
    primary_metrics = _phase_role_metrics(
        regenerated["primary"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    stress_metrics = _phase_role_metrics(
        regenerated["stress"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    cash_metrics = _phase_role_metrics(
        regenerated["cash"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    cash_stress_metrics = _phase_role_metrics(
        regenerated["cash_stress"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    metrics = _combine_static_metrics(
        primary_metrics, stress_metrics, cash_metrics, cash_stress_metrics
    )
    gate = _evaluate_static_gate(metrics, gate_config, validity_config)
    if evaluation.get("metrics") != metrics or evaluation.get("gate") != gate:
        raise ValueError(f"{stage_name} evaluation metrics do not replay")


def _verify_phase_reference(
    phase: Mapping[str, Any],
    *,
    stage_name: str,
    gate_config: Mapping[str, Any],
    validity_config: Mapping[str, Any],
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
    verify_data: bool,
) -> None:
    spec = STAGES[stage_name]
    metrics = phase.get("metrics")
    gate = phase.get("gate")
    if (
        set(phase) != _PHASE_FIELDS
        or not _is_sha256(phase.get("source_manifest_payload_sha256"))
        or not _is_sha256(phase.get("stage_binding_payload_sha256"))
        or not _is_sha256(phase.get("evaluation_payload_sha256"))
        or not _is_sha256(phase.get("evaluation_file_sha256"))
        or not isinstance(metrics, Mapping)
        or not isinstance(gate, Mapping)
        or metrics.get("start_date") != spec["performance_start"]
        or metrics.get("end_date") != spec["performance_end"]
    ):
        raise ValueError(f"{stage_name} phase reference is incomplete or truncated")
    recomputed_gate = _evaluate_static_gate(metrics, gate_config, validity_config)
    if gate != recomputed_gate:
        raise ValueError(f"{stage_name} gate differs from metrics and protocol")
    if not verify_data:
        return
    stage, binding = _load_bound_stage(
        stage_name,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    if (
        stage.manifest.get("payload_sha256") != phase["source_manifest_payload_sha256"]
        or binding.get("payload_sha256") != phase["stage_binding_payload_sha256"]
    ):
        raise ValueError(f"{stage_name} phase source binding differs")
    evaluation, evaluation_file_sha256 = _load_evaluation(
        stage_name,
        source_manifest_payload=str(phase["source_manifest_payload_sha256"]),
        stage_binding_payload=str(phase["stage_binding_payload_sha256"]),
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    if (
        evaluation.get("payload_sha256") != phase["evaluation_payload_sha256"]
        or evaluation_file_sha256 != phase["evaluation_file_sha256"]
        or evaluation.get("metrics") != metrics
        or evaluation.get("gate") != gate
    ):
        raise ValueError(f"{stage_name} evaluation differs from frozen evidence")
    _replay_evaluation(
        stage_name,
        stage=stage,
        evaluation=evaluation,
        gate_config=gate_config,
        validity_config=validity_config,
    )


def _evaluate_stage(
    stage_name: str,
    *,
    gate_config: Mapping[str, Any],
    validity_config: Mapping[str, Any],
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    spec = STAGES[stage_name]
    stage, binding = _stage(
        stage_name,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    source_payload = str(stage.manifest["payload_sha256"])
    destination = EVALUATION_ROOT / f"stage={stage_name}"
    _reject_partial_entries(EVALUATION_ROOT)
    if destination.exists() or destination.is_symlink():
        raise RuntimeError(
            f"8.1 corrective run forbids a pre-existing {stage_name} evaluation"
        )
    primary = _run_one(
        stage,
        strategy_id=PRIMARY_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=8.0,
    )
    stress = _run_one(
        stage,
        strategy_id=PRIMARY_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=16.0,
    )
    cash = _run_one(
        stage,
        strategy_id=CASH_ONLY_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=8.0,
    )
    cash_stress = _run_one(
        stage,
        strategy_id=CASH_ONLY_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=16.0,
    )
    primary_metrics = _phase_role_metrics(
        primary,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    stress_metrics = _phase_role_metrics(
        stress,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    cash_metrics = _phase_role_metrics(
        cash,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    cash_stress_metrics = _phase_role_metrics(
        cash_stress,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    combined = _combine_static_metrics(
        primary_metrics,
        stress_metrics,
        cash_metrics,
        cash_stress_metrics,
    )
    if (
        combined.get("start_date") != spec["performance_start"]
        or combined.get("end_date") != spec["performance_end"]
    ):
        raise RuntimeError(
            f"{stage_name} metrics do not cover the exact protocol boundary"
        )
    gate = _evaluate_static_gate(combined, gate_config, validity_config)
    evaluation = _persist_evaluation(
        stage_name,
        source_manifest_payload=source_payload,
        results={
            "primary": primary,
            "stress": stress,
            "cash": cash,
            "cash_stress": cash_stress,
        },
        metrics=combined,
        gate=gate,
        closure_payload=closure_payload,
        stage_binding_payload=str(binding["payload_sha256"]),
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    verified = load_multi_asset_stage(SOURCE_ROOT, stage_name)
    if verified.manifest.get("payload_sha256") != source_payload:
        raise RuntimeError(f"{stage_name} source changed during evaluation")
    print(
        f"{stage_name} gate passed={gate['passed']} "
        f"cagr={combined['cagr']:.6f} sharpe={combined['sharpe']:.6f} "
        f"maxdd={combined['max_drawdown']:.6f}",
        flush=True,
    )
    phase = _phase_from_evaluation(
        evaluation,
        file_sha256(EVALUATION_ROOT / f"stage={stage_name}" / "evaluation.json"),
    )
    _verify_phase_reference(
        phase,
        stage_name=stage_name,
        gate_config=gate_config,
        validity_config=validity_config,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
        verify_data=True,
    )
    return phase


def run_reclassify() -> int:
    head = _require_clean_main()
    closure, protocol, selection = _verify_closure()
    if (ROOT / TRAIN_RECLASSIFICATION_PATH).exists():
        raise FileExistsError("8.1 train reclassification is create-only")
    if any(
        (ROOT / path).exists()
        for path in (WINNER_FREEZE_PATH, AUDIT_PATH, RESULT_PATH)
    ):
        raise RuntimeError("8.1 reclassification forbids downstream evidence")
    _assert_runtime_layout(set())
    _assert_evidence_layout(set())
    receipt = _read_json(PRIOR_RECEIPT_PATH)
    receipt_bytes = _require_committed(PRIOR_RECEIPT_PATH)
    execution_validity = _verify_prior_train_artifacts(receipt)
    role_metrics = receipt["train_stage"]["role_gate_metrics"]
    metrics = _combine_receipt_role_gate_metrics(
        role_metrics, execution_validity=execution_validity
    )
    corrected_gate = _evaluate_static_gate(
        metrics,
        protocol["shared_absolute_gate"],
        protocol["execution_validity_hard_fail"],
    )
    run_nonce = uuid.uuid4().hex
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during formal reclassification")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during formal reclassification")
    _assert_runtime_layout(set())
    _assert_evidence_layout(set())
    _verify_closure()
    _require_head_pushed_and_ci_success(head)
    value: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_policy_operational_train_reclassification",
        "release": RELEASE,
        "status": (
            "train_reclassification_passed"
            if corrected_gate["passed"] is True
            else "train_reclassification_failed"
        ),
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "reclassification_execution_commit": head,
        "run_nonce": run_nonce,
        "source_receipt": {
            "path": PRIOR_RECEIPT_PATH.as_posix(),
            "file_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "payload_sha256": receipt["payload_sha256"],
        },
        "role_gate_metrics": role_metrics,
        "metrics": metrics,
        "corrected_gate": corrected_gate,
        "execution_validity": execution_validity,
        "post_hoc_non_independent": True,
        "new_market_data_queried": False,
        "retained_8_0_train_artifacts_accessed": True,
        "runtime_created": False,
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
        "claim_contract": protocol["claim_contract"],
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    _create_only(TRAIN_RECLASSIFICATION_PATH, value)
    _assert_runtime_layout(set())
    _assert_evidence_layout({TRAIN_RECLASSIFICATION_PATH.name})
    print(
        f"train reclassification status={value['status']} "
        f"payload={value['payload_sha256']}",
        flush=True,
    )
    return 0


def run_validation() -> int:
    head = _require_clean_main()
    closure, protocol, selection = _verify_closure()
    if (ROOT / WINNER_FREEZE_PATH).exists():
        raise FileExistsError("8.1 winner freeze is create-only")
    if any((ROOT / path).exists() for path in (AUDIT_PATH, RESULT_PATH)):
        raise RuntimeError("8.1 validation forbids audit or result evidence")
    reclassification = _read_json(TRAIN_RECLASSIFICATION_PATH)
    reclassification_bytes = _require_committed(TRAIN_RECLASSIFICATION_PATH)
    _verify_train_reclassification_contract(
        reclassification,
        closure=closure,
        protocol=protocol,
        selection=selection,
    )
    if head == reclassification.get("reclassification_execution_commit"):
        raise RuntimeError(
            "8.1 validation requires a later commit containing reclassification"
        )
    _assert_runtime_layout(set())
    _assert_evidence_layout({TRAIN_RECLASSIFICATION_PATH.name})
    validation: dict[str, Any] | None = None
    selected: str | None = None
    run_nonce = uuid.uuid4().hex
    if run_nonce == reclassification.get("run_nonce"):
        raise RuntimeError("8.1 validation nonce must differ from reclassification nonce")
    if reclassification["corrected_gate"]["passed"] is True:
        validation = _evaluate_stage(
            "validation",
            gate_config=protocol["shared_absolute_gate"],
            validity_config=protocol["execution_validity_hard_fail"],
            closure_payload=closure["payload_sha256"],
            execution_commit=head,
            run_nonce=run_nonce,
            predecessor={
                "kind": "train_reclassification",
                "payload_sha256": reclassification["payload_sha256"],
            },
        )
        if validation["gate"]["passed"] is True:
            selected = PRIMARY_ID
    status = (
        "selected_policy_frozen"
        if selected is not None
        else "selected_null_frozen_validation_failed"
        if validation is not None
        else "selected_null_frozen_reclassification_failed"
    )
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during formal validation")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during formal validation")
    allowed_stages: set[str] = set()
    if validation is not None:
        allowed_stages.add("validation")
    _assert_runtime_layout(allowed_stages)
    _assert_evidence_layout({TRAIN_RECLASSIFICATION_PATH.name})
    _verify_closure()
    _require_head_pushed_and_ci_success(head)
    freeze: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_winner_freeze",
        "release": RELEASE,
        "status": status,
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selection_execution_commit": head,
        "run_nonce": run_nonce,
        "candidate_registry": [PRIMARY_ID],
        "selected_candidate_id": selected,
        "train_reclassification": {
            "path": TRAIN_RECLASSIFICATION_PATH.as_posix(),
            "file_sha256": hashlib.sha256(reclassification_bytes).hexdigest(),
            "payload_sha256": reclassification["payload_sha256"],
        },
        "validation": validation,
        "validation_market_outcomes_opened": validation is not None,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = canonical_payload_sha256(freeze)
    _create_only(WINNER_FREEZE_PATH, freeze)
    _assert_evidence_layout(
        {TRAIN_RECLASSIFICATION_PATH.name, WINNER_FREEZE_PATH.name}
    )
    print(
        f"winner freeze selected={selected} payload={freeze['payload_sha256']}",
        flush=True,
    )
    return 0


def run_audit() -> int:
    head = _require_clean_main()
    closure, protocol, selection = _verify_closure()
    freeze = _read_json(WINNER_FREEZE_PATH)
    _require_committed(WINNER_FREEZE_PATH)
    _verify_winner_freeze_contract(
        freeze,
        closure=closure,
        protocol=protocol,
        selection=selection,
    )
    if (
        freeze.get("selected_candidate_id") != PRIMARY_ID
        or freeze.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or freeze.get("asset_selection_payload_sha256")
        != selection.get("payload_sha256")
        or freeze.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
        or freeze.get("audit_market_outcomes_opened") is not False
    ):
        raise RuntimeError("audit requires the frozen non-null 8.1 policy")
    if (ROOT / AUDIT_PATH).exists():
        raise FileExistsError("8.1 historical audit is create-only")
    _assert_runtime_layout({"validation"})
    _assert_evidence_layout(
        {TRAIN_RECLASSIFICATION_PATH.name, WINNER_FREEZE_PATH.name}
    )
    run_nonce = uuid.uuid4().hex
    if run_nonce == freeze.get("run_nonce"):
        raise RuntimeError("8.1 audit nonce must differ from validation nonce")
    audit = _evaluate_stage(
        "audit",
        gate_config=protocol["shared_absolute_gate"],
        validity_config=protocol["execution_validity_hard_fail"],
        closure_payload=closure["payload_sha256"],
        execution_commit=head,
        run_nonce=run_nonce,
        predecessor={
            "kind": "winner_freeze",
            "payload_sha256": freeze["payload_sha256"],
        },
    )
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during historical audit")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during historical audit")
    _assert_runtime_layout({"validation", "audit"})
    _assert_evidence_layout(
        {TRAIN_RECLASSIFICATION_PATH.name, WINNER_FREEZE_PATH.name}
    )
    _verify_closure()
    _require_head_pushed_and_ci_success(head)
    value: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_historical_audit",
        "release": RELEASE,
        "status": (
            "historical_audit_passed"
            if audit["gate"]["passed"] is True
            else "historical_audit_failed"
        ),
        "selected_candidate_id": PRIMARY_ID,
        "winner_freeze_payload_sha256": freeze["payload_sha256"],
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "audit_execution_commit": head,
        "run_nonce": run_nonce,
        "audit": audit,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    _create_only(AUDIT_PATH, value)
    print(
        f"audit status={value['status']} payload={value['payload_sha256']}",
        flush=True,
    )
    return 0


def run_finalize() -> int:
    _require_clean_main()
    closure, protocol, selection = _verify_closure()
    if (ROOT / RESULT_PATH).exists():
        raise FileExistsError(f"{RELEASE} terminal result is create-only")
    _assert_evidence_layout(
        {TRAIN_RECLASSIFICATION_PATH.name, WINNER_FREEZE_PATH.name, AUDIT_PATH.name}
    )
    _assert_runtime_layout({"validation", "audit"})
    freeze = _read_json(WINNER_FREEZE_PATH)
    freeze_bytes = _require_committed(WINNER_FREEZE_PATH)
    _verify_winner_freeze_contract(
        freeze,
        closure=closure,
        protocol=protocol,
        selection=selection,
    )
    if (
        freeze.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or freeze.get("asset_selection_payload_sha256") != selection.get("payload_sha256")
        or freeze.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
    ):
        raise ValueError(f"winner freeze does not bind the active {RELEASE} closure")
    selected = freeze.get("selected_candidate_id")
    audit: dict[str, Any] | None = None
    allowed_stages: set[str] = set()
    if freeze.get("validation") is not None:
        allowed_stages.add("validation")
    if selected is None:
        if (ROOT / AUDIT_PATH).exists():
            raise RuntimeError("null 8.1 selection cannot have an audit artifact")
        status = "selection_falsified_no_candidate"
    else:
        if selected != PRIMARY_ID:
            raise RuntimeError("8.1 finalize received an unknown selected policy")
        audit = _read_json(AUDIT_PATH)
        audit_bytes = _require_committed(AUDIT_PATH)
        _verify_audit_contract(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            selection=selection,
        )
        allowed_stages.add("audit")
        status = (
            "historical_strategic_beta_diagnostic_passed_fresh_evidence_required"
            if audit.get("status") == "historical_audit_passed"
            else "historical_strategic_beta_diagnostic_failed"
        )
        if hashlib.sha256(audit_bytes).hexdigest() != file_sha256(ROOT / AUDIT_PATH):
            raise RuntimeError("audit bytes changed while finalizing")
    _assert_runtime_layout(allowed_stages)
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_terminal_result",
        "release": RELEASE,
        "status": status,
        "selected_candidate_id": selected,
        "audit_status": audit.get("status") if audit is not None else "not_opened",
        "winner_freeze": {
            "path": WINNER_FREEZE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(freeze_bytes).hexdigest(),
            "payload_sha256": freeze["payload_sha256"],
        },
        "historical_audit": (
            {
                "path": AUDIT_PATH.as_posix(),
                "file_sha256": file_sha256(ROOT / AUDIT_PATH),
                "payload_sha256": audit["payload_sha256"],
            }
            if audit is not None
            else None
        ),
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "claim_contract": protocol["claim_contract"],
    }
    result["payload_sha256"] = canonical_payload_sha256(result)
    _create_only(RESULT_PATH, result)
    allowed_evidence = {
        TRAIN_RECLASSIFICATION_PATH.name,
        WINNER_FREEZE_PATH.name,
        RESULT_PATH.name,
    }
    if audit is not None:
        allowed_evidence.add(AUDIT_PATH.name)
    _assert_evidence_layout(allowed_evidence)
    print(f"terminal result status={status} payload={result['payload_sha256']}", flush=True)
    return 0


def _verify_result_contract(
    result: Mapping[str, Any],
    *,
    freeze: Mapping[str, Any],
    audit: Mapping[str, Any] | None,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    selection: Mapping[str, Any],
) -> None:
    if (
        set(result) != _RESULT_FIELDS
        or result.get("payload_sha256") != canonical_payload_sha256(result)
        or result.get("schema_version") != 1
        or result.get("kind") != "factor_lab_multi_asset_terminal_result"
        or result.get("release") != RELEASE
        or result.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or result.get("asset_selection_payload_sha256") != selection.get("payload_sha256")
        or result.get("implementation_closure_payload_sha256") != closure.get("payload_sha256")
        or result.get("claim_contract") != protocol.get("claim_contract")
    ):
        raise ValueError("terminal result contract differs")
    freeze_binding = result.get("winner_freeze")
    if (
        not isinstance(freeze_binding, Mapping)
        or set(freeze_binding) != {"path", "file_sha256", "payload_sha256"}
        or freeze_binding.get("path") != WINNER_FREEZE_PATH.as_posix()
        or freeze_binding.get("file_sha256") != file_sha256(ROOT / WINNER_FREEZE_PATH)
        or freeze_binding.get("payload_sha256") != freeze.get("payload_sha256")
        or result.get("selected_candidate_id") != freeze.get("selected_candidate_id")
    ):
        raise ValueError("terminal winner-freeze binding differs")
    selected = freeze.get("selected_candidate_id")
    audit_binding = result.get("historical_audit")
    if selected is None:
        if (
            audit is not None
            or audit_binding is not None
            or result.get("status") != "selection_falsified_no_candidate"
            or result.get("audit_status") != "not_opened"
        ):
            raise ValueError("null-selection terminal result differs")
        return
    if audit is None or not isinstance(audit_binding, Mapping):
        raise ValueError("selected terminal result lacks its historical audit")
    if (
        set(audit_binding) != {"path", "file_sha256", "payload_sha256"}
        or audit_binding.get("path") != AUDIT_PATH.as_posix()
        or audit_binding.get("file_sha256") != file_sha256(ROOT / AUDIT_PATH)
        or audit_binding.get("payload_sha256") != audit.get("payload_sha256")
        or result.get("audit_status") != audit.get("status")
    ):
        raise ValueError("terminal historical-audit binding differs")
    expected_status = (
        "historical_strategic_beta_diagnostic_passed_fresh_evidence_required"
        if audit.get("status") == "historical_audit_passed"
        else "historical_strategic_beta_diagnostic_failed"
    )
    if result.get("status") != expected_status:
        raise ValueError("terminal result status differs from historical audit")


def verify_release_state(
    *, verify_data: bool = False, verify_runtime: bool = False
) -> dict[str, Any]:
    """Verify the complete committed 8.1 closure/evidence chain for CLI and CI."""

    closure, protocol, selection = _verify_closure(verify_runtime=verify_runtime)
    freeze_path = ROOT / WINNER_FREEZE_PATH
    reclassification_path = ROOT / TRAIN_RECLASSIFICATION_PATH
    audit_path = ROOT / AUDIT_PATH
    result_path = ROOT / RESULT_PATH
    if not freeze_path.is_file():
        if audit_path.exists() or result_path.exists():
            raise ValueError("audit/result exists without a winner freeze")
        reclassification: dict[str, Any] | None = None
        if reclassification_path.is_file():
            reclassification = _read_json(TRAIN_RECLASSIFICATION_PATH)
            _require_committed(TRAIN_RECLASSIFICATION_PATH)
            _verify_train_reclassification_contract(
                reclassification,
                closure=closure,
                protocol=protocol,
                selection=selection,
                verify_data=verify_data,
            )
            _assert_evidence_layout({TRAIN_RECLASSIFICATION_PATH.name})
            _assert_runtime_layout(set())
            status = (
                "train_reclassification_passed_pending_validation"
                if reclassification["corrected_gate"]["passed"] is True
                else "train_reclassification_failed_pending_null_freeze"
            )
        else:
            _assert_evidence_layout(set())
            _assert_runtime_layout(set())
            status = str(closure["status"])
        return {
            "status": status,
            "closure": closure,
            "protocol": protocol,
            "selection": selection,
            "train_reclassification": reclassification,
            "freeze": None,
            "audit": None,
            "result": None,
        }
    if not reclassification_path.is_file():
        raise ValueError("winner freeze exists without a train reclassification")
    freeze = _read_json(WINNER_FREEZE_PATH)
    _require_committed(WINNER_FREEZE_PATH)
    _verify_winner_freeze_contract(
        freeze,
        closure=closure,
        protocol=protocol,
        selection=selection,
        verify_data=verify_data,
    )
    audit: dict[str, Any] | None = None
    if audit_path.is_file():
        if freeze.get("selected_candidate_id") != PRIMARY_ID:
            raise ValueError("null 8.1 selection cannot have historical audit evidence")
        audit = _read_json(AUDIT_PATH)
        _require_committed(AUDIT_PATH)
        _verify_audit_contract(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            selection=selection,
            verify_data=verify_data,
        )
    result: dict[str, Any] | None = None
    if result_path.is_file():
        result = _read_json(RESULT_PATH)
        _require_committed(RESULT_PATH)
        _verify_result_contract(
            result,
            freeze=freeze,
            audit=audit,
            closure=closure,
            protocol=protocol,
            selection=selection,
        )
    allowed_stages: set[str] = set()
    if freeze.get("validation") is not None:
        allowed_stages.add("validation")
    if audit is not None:
        allowed_stages.add("audit")
    _assert_runtime_layout(allowed_stages)
    allowed_evidence = {
        TRAIN_RECLASSIFICATION_PATH.name,
        WINNER_FREEZE_PATH.name,
    }
    if result is not None:
        allowed_evidence.add(RESULT_PATH.name)
    if audit is not None:
        allowed_evidence.add(AUDIT_PATH.name)
    _assert_evidence_layout(allowed_evidence)
    if result is not None:
        status = str(result["status"])
    elif audit is not None:
        status = str(audit["status"])
    elif freeze.get("selected_candidate_id") is None:
        status = "selection_frozen_no_candidate_pending_finalize"
    else:
        status = "selection_frozen_pending_historical_audit"
    return {
        "status": status,
        "closure": closure,
        "protocol": protocol,
        "selection": selection,
        "train_reclassification": _read_json(TRAIN_RECLASSIFICATION_PATH),
        "freeze": freeze,
        "audit": audit,
        "result": result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=("reclassify", "validation", "audit", "finalize"),
    )
    args = parser.parse_args(argv)
    if args.mode == "reclassify":
        return run_reclassify()
    if args.mode == "validation":
        return run_validation()
    if args.mode == "audit":
        return run_audit()
    return run_finalize()


if __name__ == "__main__":
    raise SystemExit(main())
