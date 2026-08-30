#!/usr/bin/env python
"""Run the physically staged 7.1 corrective multi-asset train replay."""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import importlib.metadata
import io
import json
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
    CANDIDATE_ID,
    CONTROL_ID,
    SimulationConfig,
    build_monthly_targets,
    combine_phase_metrics,
    evaluate_phase_gate,
    phase_metrics,
    simulate_targets,
)


RELEASE = "7.1"
PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
ASSET_SELECTION_PATH = Path("protocols/7.0-asset-selection.json")
AMENDMENT_PATH = Path("protocols/7.1-corrective-amendment-1.json")
CLOSURE_PATH = Path("protocols/7.1-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/7.1")
WINNER_FREEZE_PATH = EVIDENCE_ROOT / "winner-freeze.json"
AUDIT_PATH = EVIDENCE_ROOT / "historical-audit.json"
RESULT_PATH = EVIDENCE_ROOT / "result.json"
PRECLOSURE_TRAIN_PATH = Path("protocols/evidence/7.0/preclosure-train.json")
PRIOR_CLOSURE_PATH = Path("protocols/7.0-release.json")
PRIOR_FAILURE_PATH = Path("protocols/evidence/7.0/execution-failure.json")
PRIOR_TAG = "7.0"
PRIOR_TAG_OBJECT = "25bbc306e8842feab923380416f8329e0dd81100"
PRIOR_COMMIT = "412026ca0370d53ca704adfd1122a811e768842e"
PRIOR_CLOSURE_PAYLOAD = "d0b6072234d45363144a47517c8c4c535e4c9550ea36925a4b7cc54216110009"
PRIOR_FAILURE_PAYLOAD = "04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9"
AMENDMENT_PAYLOAD = "7335cdbb61cd0d7b9c3e6f6896ec576c7e403b87d83cfa3d6679965691984c86"
WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-7.1"
PRIOR_WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-7.0"
SOURCE_ROOT = WORK_ROOT / "sources"
EVALUATION_ROOT = WORK_ROOT / "evaluations"
BINDING_ROOT = WORK_ROOT / "stage-bindings"
EXPECTED_IMPLEMENTATION_PATHS = {
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
}
RUNTIME_DISTRIBUTIONS = ("numpy", "pandas", "pyarrow", "scipy", "tushare")
NATIVE_SUFFIXES = (".dll", ".dylib", ".pyd", ".so")
EVALUATION_ROLES = ("candidate", "control", "stress")
EVALUATION_ARTIFACTS = ("targets", "orders", "daily_nav", "holdings", "trades")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
STAGES: dict[str, dict[str, str]] = {
    "train": {
        "source_start": "2014-01-15",
        "source_end": "2019-12-31",
        "performance_start": "2015-03-02",
        "performance_end": "2019-12-31",
    }
}
_CLOSURE_FIELDS = {
    "schema_version", "kind", "release", "closure_role", "direction_change",
    "route", "status", "prior_train_returns_opened",
    "corrective_train_returns_opened", "selected_candidate_id",
    "audit_status", "protocol", "asset_selection", "corrective_amendment",
    "prior_release",
    "implementation_commit", "implementation_tree", "implementation", "runtime",
    "formal_data", "claim_contract", "payload_sha256",
}
_FREEZE_FIELDS = {
    "schema_version", "kind", "release", "status", "protocol_payload_sha256",
    "asset_selection_payload_sha256", "implementation_closure_payload_sha256",
    "selection_execution_commit", "run_nonce", "candidate_registry",
    "selected_candidate_id", "train", "validation",
    "validation_market_outcomes_opened", "audit_market_outcomes_opened",
    "runner_up_fallback", "claim_contract", "payload_sha256",
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


def _verify_disclosed_outcome_boundary(disclosed: Mapping[str, Any]) -> None:
    if (
        disclosed.get("status") != "train_falsified_before_preselection_closure"
        or disclosed.get("selection", {}).get("validation_opened") is not False
        or disclosed.get("selection", {}).get("audit_opened") is not False
        or disclosed.get("disclosure", {}).get(
            "validation_market_outcomes_opened"
        )
        is not False
        or disclosed.get("disclosure", {}).get(
            "audit_market_outcomes_opened"
        )
        is not False
    ):
        raise ValueError("preclosure disclosure opened a forbidden downstream phase")


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
    remote_head = remote.stdout.decode("ascii", errors="replace").split()
    if remote.returncode != 0 or not remote_head or remote_head[0] != head:
        raise RuntimeError("formal HEAD is not the current pushed origin/main commit")
    completed = subprocess.run(
        [
            "gh",
            "run",
            "list",
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
        or freeze.get("candidate_registry") != [CANDIDATE_ID]
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
            ASSET_SELECTION_PATH,
            PRECLOSURE_TRAIN_PATH,
        ),
    )
    closure_predecessor = {
        "kind": "preselection_closure",
        "payload_sha256": closure["payload_sha256"],
    }
    train = freeze.get("train")
    if not isinstance(train, Mapping):
        raise ValueError("winner freeze must contain a complete train phase")
    _verify_phase_reference(
        train,
        stage_name="train",
        gate_config=protocol["train_gate"],
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=execution_commit,
        run_nonce=str(freeze["run_nonce"]),
        predecessor=closure_predecessor,
        verify_data=verify_data,
    )
    _verify_disclosed_train_replay(train, _read_json(PRECLOSURE_TRAIN_PATH))
    if (
        train["gate"]["passed"] is not False
        or freeze.get("validation") is not None
        or freeze.get("status") != "selected_null_frozen_train_failed"
        or freeze.get("selected_candidate_id") is not None
        or freeze.get("validation_market_outcomes_opened") is not False
    ):
        raise ValueError("7.1 corrective winner freeze is not the exact train-failed null")


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
        or audit.get("selected_candidate_id") != CANDIDATE_ID
        or audit.get("winner_freeze_payload_sha256") != freeze.get("payload_sha256")
        or audit.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or audit.get("asset_selection_payload_sha256") != selection.get("payload_sha256")
        or audit.get("implementation_closure_payload_sha256") != closure.get("payload_sha256")
        or audit.get("runner_up_fallback") is not False
        or audit.get("claim_contract") != protocol.get("claim_contract")
        or not re.fullmatch(r"[0-9a-f]{32}", str(audit.get("run_nonce") or ""))
    ):
        raise ValueError("historical audit contract bindings differ")
    execution_commit = str(audit["audit_execution_commit"])
    _verify_execution_lineage(
        execution_commit,
        evidence_path=AUDIT_PATH,
        required_files=(CLOSURE_PATH, PROTOCOL_PATH, ASSET_SELECTION_PATH, WINNER_FREEZE_PATH),
    )
    phase = audit.get("audit")
    if not isinstance(phase, Mapping):
        raise ValueError("historical audit lacks a complete audit phase")
    _verify_phase_reference(
        phase,
        stage_name="audit",
        gate_config=protocol["audit_gate"],
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


def _verify_corrective_amendment(amendment: Mapping[str, Any]) -> None:
    prior = amendment.get("prior_release")
    correction = amendment.get("correction")
    sole = correction.get("sole_permitted_change") if isinstance(correction, Mapping) else None
    runtime = amendment.get("runtime_and_stage_contract")
    phase = amendment.get("phase_contract")
    unchanged = amendment.get("unchanged_contract")
    if (
        amendment.get("payload_sha256") != AMENDMENT_PAYLOAD
        or amendment.get("release") != RELEASE
        or amendment.get("direction_change") is not False
        or amendment.get("route") != "fixed_multi_asset_causal_trend_budget"
        or not isinstance(prior, Mapping)
        or prior.get("tag") != PRIOR_TAG
        or prior.get("annotated_tag_object") != PRIOR_TAG_OBJECT
        or prior.get("peeled_commit") != PRIOR_COMMIT
        or not isinstance(sole, Mapping)
        or sole.get("path") != "scripts/run-multi-asset-evidence.py"
        or sole.get("function") != "_replay_evaluation"
        or sole.get("sort_kind") != "mergesort"
        or sole.get("sort_key") != ["signal_date", "code"]
        or sole.get("comparison_dtype_check_unchanged") is not True
        or sole.get("comparison_exact_value_check_unchanged") is not True
        or not isinstance(runtime, Mapping)
        or runtime.get("runtime_root") != "runtime/data/multi-asset-7.1"
        or runtime.get("reuse_7_0_derived_stages") is not False
        or runtime.get("reuse_7_0_evaluations") is not False
        or runtime.get("reuse_7_0_gate_or_status_views") is not False
        or not isinstance(phase, Mapping)
        or phase.get("formal_7_1_scope") != "train corrective replay only"
        or phase.get("selected_candidate_id") is not None
        or phase.get("validation_market_outcomes_opened") is not False
        or phase.get("validation_stage_created") is not False
        or phase.get("audit_market_outcomes_opened") is not False
        or phase.get("audit_stage_created") is not False
        or not isinstance(unchanged, Mapping)
        or not unchanged
        or any(value is not True for value in unchanged.values())
    ):
        raise ValueError("7.1 corrective amendment differs from its exact whitelist")
    bound_files = (
        ("protocol", PROTOCOL_PATH, "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"),
        (
            "asset_selection",
            ASSET_SELECTION_PATH,
            "b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b",
        ),
        (
            "preclosure_train_disclosure",
            PRECLOSURE_TRAIN_PATH,
            "6bd2909ddc97ec84d3535d15e8f13330a5752831aead82d8fb50afdd16ac6775",
        ),
        ("preselection_closure", PRIOR_CLOSURE_PATH, PRIOR_CLOSURE_PAYLOAD),
        ("execution_failure", PRIOR_FAILURE_PATH, PRIOR_FAILURE_PAYLOAD),
    )
    for name, path, expected_payload in bound_files:
        binding = prior.get(name)
        current = (ROOT / path).read_bytes()
        value = json.loads(current.decode("utf-8"))
        if (
            not isinstance(binding, Mapping)
            or binding.get("path") != path.as_posix()
            or binding.get("file_sha256") != hashlib.sha256(current).hexdigest()
            or binding.get("payload_sha256") != expected_payload
            or not isinstance(value, Mapping)
            or value.get("payload_sha256") != expected_payload
            or canonical_payload_sha256(value) != expected_payload
            or _git("show", f"{PRIOR_COMMIT}:{path.as_posix()}") != current
        ):
            raise ValueError(f"7.1 amendment does not bind the published 7.0 blob: {path}")


def _verify_closure(
    *, verify_runtime: bool = True
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    closure = _read_json(CLOSURE_PATH)
    protocol = _read_json(PROTOCOL_PATH)
    selection = _read_json(ASSET_SELECTION_PATH)
    amendment = _read_json(AMENDMENT_PATH)
    disclosed = _read_json(PRECLOSURE_TRAIN_PATH)
    prior_closure = _read_json(PRIOR_CLOSURE_PATH)
    prior_failure = _read_json(PRIOR_FAILURE_PATH)
    _verify_corrective_amendment(amendment)
    _verify_disclosed_outcome_boundary(disclosed)
    if (
        set(closure) != _CLOSURE_FIELDS
        or closure.get("schema_version") != 1
        or closure.get("kind") != "factor_lab_release_closure"
        or closure.get("release") != RELEASE
        or closure.get("closure_role")
        != "corrective_train_replay_after_7_0_execution_failure"
        or closure.get("direction_change") is not False
        or closure.get("route") != "fixed_multi_asset_causal_trend_budget"
        or closure.get("status")
        != "corrective_implementation_frozen_for_exact_failed_train_replay"
        or closure.get("prior_train_returns_opened") is not True
        or closure.get("corrective_train_returns_opened") is not False
        or closure.get("selected_candidate_id") is not None
        or closure.get("audit_status") != "not_opened"
        or closure.get("claim_contract") != protocol.get("claim_contract")
        or closure.get("protocol", {}).get("payload_sha256")
        != protocol.get("payload_sha256")
        or closure.get("asset_selection", {}).get("payload_sha256")
        != selection.get("payload_sha256")
        or closure.get("corrective_amendment", {}).get("payload_sha256")
        != amendment.get("payload_sha256")
    ):
        raise ValueError("7.1 corrective preselection closure contract differs")
    if (
        protocol.get("protocol_id")
        != "factor-lab/7.0/fixed-multi-asset-trend-budget-v1"
        or protocol.get("release") != "7.0"
        or [item.get("candidate_id") for item in protocol.get("candidate_registry", [])]
        != [CANDIDATE_ID]
        or selection.get("selected_codes")
        != [
            "510300.SH",
            "159920.SZ",
            "513100.SH",
            "518880.SH",
            "511010.SH",
            "511880.SH",
        ]
        or set(closure.get("implementation", {})) != EXPECTED_IMPLEMENTATION_PATHS
    ):
        raise ValueError("7.1 closure registry or implementation boundary differs")
    formal_data = closure.get("formal_data")
    disclosure_binding = (
        formal_data.get("preclosure_train_disclosure")
        if isinstance(formal_data, Mapping)
        else None
    )
    failure_binding = (
        formal_data.get("prior_execution_failure")
        if isinstance(formal_data, Mapping)
        else None
    )
    if (
        not isinstance(disclosure_binding, Mapping)
        or disclosure_binding.get("path") != PRECLOSURE_TRAIN_PATH.as_posix()
        or disclosure_binding.get("file_sha256")
        != file_sha256(ROOT / PRECLOSURE_TRAIN_PATH)
        or disclosure_binding.get("payload_sha256") != disclosed.get("payload_sha256")
        or disclosure_binding.get("status")
        != "train_falsified_before_preselection_closure"
        or disclosure_binding.get("validation_market_outcomes_opened") is not False
        or disclosure_binding.get("audit_market_outcomes_opened") is not False
        or not isinstance(failure_binding, Mapping)
        or failure_binding.get("path") != PRIOR_FAILURE_PATH.as_posix()
        or failure_binding.get("file_sha256") != file_sha256(ROOT / PRIOR_FAILURE_PATH)
        or failure_binding.get("payload_sha256") != PRIOR_FAILURE_PAYLOAD
        or failure_binding.get("status") != "selection_inconclusive_software_failure"
        or failure_binding.get("classification") != "target_order_replay_false_negative"
        or any(
            failure_binding.get(key) is not False
            for key in (
                "validation_market_outcomes_opened",
                "winner_freeze_created",
                "audit_market_outcomes_opened",
                "terminal_result_created",
            )
        )
    ):
        raise ValueError("7.1 closure does not bind the disclosed train and 7.0 failure")
    for key, path in (
        ("protocol", PROTOCOL_PATH),
        ("asset_selection", ASSET_SELECTION_PATH),
        ("corrective_amendment", AMENDMENT_PATH),
    ):
        if closure[key].get("file_sha256") != file_sha256(ROOT / path):
            raise ValueError(f"7.1 closure bytes differ: {path}")
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
    implementation = closure.get("implementation")
    if not isinstance(implementation, Mapping) or set(implementation) != EXPECTED_IMPLEMENTATION_PATHS:
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
        current_sha = file_sha256(path)
        committed = _git("show", f"{implementation_commit}:{key}")
        if (
            current_sha != binding["sha256"]
            or hashlib.sha256(committed).hexdigest() != binding["sha256"]
        ):
            raise ValueError(f"frozen implementation bytes differ: {key}")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if ancestor.returncode != 0:
        raise ValueError("current HEAD does not descend from the frozen implementation")
    for relative in (
        PROTOCOL_PATH,
        ASSET_SELECTION_PATH,
        AMENDMENT_PATH,
        PRECLOSURE_TRAIN_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_FAILURE_PATH,
    ):
        if _git("show", f"{implementation_commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(f"implementation commit lacks frozen contract: {relative}")
    current_prior_closure = (ROOT / PRIOR_CLOSURE_PATH).read_bytes()
    current_prior_failure = (ROOT / PRIOR_FAILURE_PATH).read_bytes()
    prior = closure.get("prior_release")
    expected_prior = {
        "release": "7.0",
        "tag": PRIOR_TAG,
        "annotated_tag_object": PRIOR_TAG_OBJECT,
        "peeled_commit": PRIOR_COMMIT,
        "preselection_closure": {
            "path": PRIOR_CLOSURE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(current_prior_closure).hexdigest(),
            "payload_sha256": PRIOR_CLOSURE_PAYLOAD,
        },
        "execution_failure": {
            "path": PRIOR_FAILURE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(current_prior_failure).hexdigest(),
            "payload_sha256": PRIOR_FAILURE_PAYLOAD,
            "status": "selection_inconclusive_software_failure",
            "classification": "target_order_replay_false_negative",
        },
    }
    if (
        prior != expected_prior
        or _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_CLOSURE_PATH.as_posix()}")
        != current_prior_closure
        or _git("show", f"{PRIOR_COMMIT}:{PRIOR_FAILURE_PATH.as_posix()}")
        != current_prior_failure
        or prior_closure.get("payload_sha256") != PRIOR_CLOSURE_PAYLOAD
        or prior_failure.get("payload_sha256") != PRIOR_FAILURE_PAYLOAD
    ):
        raise ValueError("7.1 closure prior-release lineage differs")
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


def _phase_predecessor(kind: str, phase: Mapping[str, Any]) -> dict[str, str]:
    return {
        "kind": kind,
        "evaluation_payload_sha256": str(phase["evaluation_payload_sha256"]),
        "gate_sha256": canonical_payload_sha256({"gate": phase["gate"]}),
    }


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
    if WORK_ROOT.is_symlink() or (WORK_ROOT.exists() and not WORK_ROOT.is_dir()):
        raise RuntimeError("7.1 runtime root must be a regular local directory")
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
                f"unexpected entry under the 7.1 runtime root: {unexpected_roots}"
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
                    raise RuntimeError("could not verify 7.1/7.0 physical isolation") from exc
                if same:
                    raise RuntimeError(
                        f"7.1 runtime file reuses a 7.0 physical file: {current}"
                    )


def _assert_evidence_layout(allowed_names: set[str]) -> None:
    root = ROOT / EVIDENCE_ROOT
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise RuntimeError("7.1 evidence root must be a regular local directory")
    if not root.exists():
        return
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed_names)
    if unexpected:
        raise RuntimeError(f"unexpected 7.1 evidence artifact: {unexpected}")
    for path in root.iterdir():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"7.1 evidence artifact is indirect or not a file: {path}")


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
            f"7.1 corrective replay forbids a pre-existing {stage_name} source stage"
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
) -> None:
    directory = EVALUATION_ROOT / f"stage={stage_name}"
    sessions = tuple(pd.to_datetime(stage.calendar["trade_date"]).dt.normalize())
    regenerated: dict[str, Mapping[str, Any]] = {}
    for role, strategy_id, cost_bps in (
        ("candidate", CANDIDATE_ID, 8.0),
        ("control", CONTROL_ID, 8.0),
        ("stress", CANDIDATE_ID, 16.0),
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
    candidate_metrics = phase_metrics(
        regenerated["candidate"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    stress_metrics = phase_metrics(
        regenerated["stress"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    control_metrics = phase_metrics(
        regenerated["control"],
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    metrics = combine_phase_metrics(candidate_metrics, stress_metrics, control_metrics)
    gate = evaluate_phase_gate(metrics, gate_config)
    if evaluation.get("metrics") != metrics or evaluation.get("gate") != gate:
        raise ValueError(f"{stage_name} evaluation metrics do not replay")


def _verify_phase_reference(
    phase: Mapping[str, Any],
    *,
    stage_name: str,
    gate_config: Mapping[str, Any],
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
    recomputed_gate = evaluate_phase_gate(metrics, gate_config)
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
    )


def _evaluate_stage(
    stage_name: str,
    *,
    gate_config: Mapping[str, Any],
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
            f"7.1 corrective replay forbids a pre-existing {stage_name} evaluation"
        )
    candidate = _run_one(
        stage,
        strategy_id=CANDIDATE_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=8.0,
    )
    stress = _run_one(
        stage,
        strategy_id=CANDIDATE_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=16.0,
    )
    control = _run_one(
        stage,
        strategy_id=CONTROL_ID,
        start=spec["performance_start"],
        end=spec["performance_end"],
        cost_bps=8.0,
    )
    candidate_metrics = phase_metrics(
        candidate,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    stress_metrics = phase_metrics(
        stress,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    control_metrics = phase_metrics(
        control,
        start=spec["performance_start"],
        end=spec["performance_end"],
    )
    combined = combine_phase_metrics(
        candidate_metrics,
        stress_metrics,
        control_metrics,
    )
    if (
        combined.get("start_date") != spec["performance_start"]
        or combined.get("end_date") != spec["performance_end"]
    ):
        raise RuntimeError(
            f"{stage_name} metrics do not cover the exact protocol boundary"
        )
    gate = evaluate_phase_gate(combined, gate_config)
    evaluation = _persist_evaluation(
        stage_name,
        source_manifest_payload=source_payload,
        results={"candidate": candidate, "stress": stress, "control": control},
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
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
        verify_data=True,
    )
    return phase


def _verify_disclosed_train_replay(
    train: Mapping[str, Any], disclosure: Mapping[str, Any]
) -> None:
    metrics = train.get("metrics")
    if (
        not isinstance(metrics, Mapping)
        or train.get("gate", {}).get("passed") is not False
    ):
        raise ValueError("formal train does not reproduce the disclosed failed boundary")
    candidate = disclosure.get("candidate") or {}
    stress = disclosure.get("stress") or {}
    control = disclosure.get("control") or {}
    relative = disclosure.get("relative") or {}
    expected = {
        "cagr": candidate.get("cagr"),
        "sharpe": candidate.get("sharpe"),
        "max_drawdown": candidate.get("max_drawdown"),
        "stress_cagr": stress.get("cagr"),
        "relative_cagr": relative.get("cagr"),
        "relative_sharpe": relative.get("sharpe"),
        "relative_max_drawdown": relative.get("max_drawdown"),
    }
    if any(metrics.get(key) != value for key, value in expected.items()):
        raise ValueError("formal train metrics differ from the disclosed probe")
    nested_control = metrics.get("control") or {}
    if any(
        nested_control.get(key) != control.get(key)
        for key in ("cagr", "sharpe", "max_drawdown")
    ):
        raise ValueError("formal control metrics differ from the disclosed probe")
    candidate_metrics = dict(metrics)
    for key in (
        "stress_cagr",
        "stress_cost_cagr",
        "relative_cagr",
        "relative_sharpe",
        "relative_max_drawdown",
        "stress",
        "control",
    ):
        candidate_metrics.pop(key, None)
    if (
        canonical_payload_sha256(candidate_metrics)
        != candidate.get("canonical_metrics_sha256")
        or canonical_payload_sha256(metrics)
        != relative.get("combined_metrics_sha256")
        or canonical_payload_sha256(metrics.get("stress") or {})
        != stress.get("canonical_metrics_sha256")
        or canonical_payload_sha256(metrics.get("control") or {})
        != control.get("canonical_metrics_sha256")
        or canonical_payload_sha256(train.get("gate") or {})
        != disclosure.get("train_gate", {}).get("canonical_gate_sha256")
    ):
        raise ValueError("formal train hashes differ from the complete disclosed replay")


def run_selection() -> int:
    head = _require_clean_main()
    closure, protocol, selection = _verify_closure()
    if (ROOT / WINNER_FREEZE_PATH).exists():
        raise FileExistsError(f"{RELEASE} winner freeze is create-only")
    if any((ROOT / path).exists() for path in (AUDIT_PATH, RESULT_PATH)):
        raise RuntimeError("7.1 corrective selection forbids audit or result evidence")
    _assert_runtime_layout({"train"})
    _assert_evidence_layout(set())
    if WORK_ROOT.exists() or WORK_ROOT.is_symlink():
        raise RuntimeError(
            "7.1 corrective selection requires an absent fresh runtime; archive any execution failure and do not retry within 7.1"
        )
    run_nonce = uuid.uuid4().hex
    closure_predecessor = {
        "kind": "preselection_closure",
        "payload_sha256": closure["payload_sha256"],
    }

    train = _evaluate_stage(
        "train",
        gate_config=protocol["train_gate"],
        closure_payload=closure["payload_sha256"],
        execution_commit=head,
        run_nonce=run_nonce,
        predecessor=closure_predecessor,
    )
    disclosed = _read_json(PRECLOSURE_TRAIN_PATH)
    _verify_disclosed_train_replay(train, disclosed)
    if train["gate"]["passed"] is not False:
        raise RuntimeError("7.1 corrective train unexpectedly passed its bound failed gate")
    validation: dict[str, Any] | None = None
    selected: str | None = None
    status = "selected_null_frozen_train_failed"
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during formal selection")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during formal selection")
    _assert_runtime_layout({"train"})
    _assert_evidence_layout(set())
    if any((ROOT / path).exists() for path in (AUDIT_PATH, RESULT_PATH)):
        raise RuntimeError("7.1 corrective replay created forbidden downstream evidence")
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
        "candidate_registry": [CANDIDATE_ID],
        "selected_candidate_id": selected,
        "train": train,
        "validation": validation,
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = canonical_payload_sha256(freeze)
    _create_only(WINNER_FREEZE_PATH, freeze)
    print(
        f"winner freeze selected={selected} payload={freeze['payload_sha256']}",
        flush=True,
    )
    return 0


def run_audit() -> int:
    raise RuntimeError("7.1 corrective release is train-only and forbids audit")


def run_finalize() -> int:
    _require_clean_main()
    closure, protocol, selection = _verify_closure()
    if (ROOT / RESULT_PATH).exists():
        raise FileExistsError(f"{RELEASE} terminal result is create-only")
    _assert_evidence_layout({WINNER_FREEZE_PATH.name})
    _assert_runtime_layout({"train"})
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
    if (
        selected is not None
        or freeze.get("validation") is not None
        or freeze.get("validation_market_outcomes_opened") is not False
        or (ROOT / AUDIT_PATH).exists()
    ):
        raise RuntimeError("7.1 corrective finalize accepts only the committed null freeze")
    _assert_runtime_layout({"train"})
    status = "selection_falsified_no_candidate"
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_terminal_result",
        "release": RELEASE,
        "status": status,
        "selected_candidate_id": selected,
        "audit_status": "not_opened",
        "winner_freeze": {
            "path": WINNER_FREEZE_PATH.as_posix(),
            "file_sha256": hashlib.sha256(freeze_bytes).hexdigest(),
            "payload_sha256": freeze["payload_sha256"],
        },
        "historical_audit": None,
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "claim_contract": protocol["claim_contract"],
    }
    result["payload_sha256"] = canonical_payload_sha256(result)
    _create_only(RESULT_PATH, result)
    _assert_evidence_layout({WINNER_FREEZE_PATH.name, RESULT_PATH.name})
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
        "historical_audit_passed_fresh_evidence_required"
        if audit.get("status") == "historical_audit_passed"
        else "historical_audit_failed"
    )
    if result.get("status") != expected_status:
        raise ValueError("terminal result status differs from historical audit")


def verify_release_state(
    *, verify_data: bool = False, verify_runtime: bool = False
) -> dict[str, Any]:
    """Verify the complete committed 7.1 closure/evidence chain for CLI and CI."""

    closure, protocol, selection = _verify_closure(verify_runtime=verify_runtime)
    freeze_path = ROOT / WINNER_FREEZE_PATH
    audit_path = ROOT / AUDIT_PATH
    result_path = ROOT / RESULT_PATH
    if not freeze_path.is_file():
        if audit_path.exists() or result_path.exists():
            raise ValueError("audit/result exists without a winner freeze")
        _assert_evidence_layout(set())
        _assert_runtime_layout(set())
        return {
            "status": str(closure["status"]),
            "closure": closure,
            "protocol": protocol,
            "selection": selection,
            "freeze": None,
            "audit": None,
            "result": None,
        }
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
    if audit_path.exists():
        raise ValueError("7.1 corrective release forbids historical audit evidence")
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
    _assert_runtime_layout({"train"})
    allowed_evidence = {WINNER_FREEZE_PATH.name}
    if result is not None:
        allowed_evidence.add(RESULT_PATH.name)
    _assert_evidence_layout(allowed_evidence)
    if result is not None:
        status = str(result["status"])
    elif freeze.get("selected_candidate_id") is None:
        status = "selection_frozen_no_candidate_pending_finalize"
    else:
        status = "selection_frozen_pending_historical_audit"
    return {
        "status": status,
        "closure": closure,
        "protocol": protocol,
        "selection": selection,
        "freeze": freeze,
        "audit": audit,
        "result": result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("selection", "finalize"))
    args = parser.parse_args(argv)
    if args.mode == "selection":
        return run_selection()
    return run_finalize()


if __name__ == "__main__":
    raise SystemExit(main())
