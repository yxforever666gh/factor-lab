#!/usr/bin/env python
"""Run the staged 9.0 causal volatility-balanced development and audit."""

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
    _frame_content_sha256,
    capture_multi_asset_stage,
    load_multi_asset_stage,
)
from factor_lab.data.sources import _configured_tushare_client  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)
from factor_lab.research.multi_asset import (  # noqa: E402
    ALL_CODES,
    BASE_COST_BPS_PER_SIDE,
    CASH_CODE,
    CASH_ONLY_ID,
    CONTROL_ID,
    INITIAL_CAPITAL_RMB,
    LOT_SIZE,
    MAX_SIGNAL_ADV_PARTICIPATION,
    RISK_BUDGETS,
    RISK_CODES,
    SimulationConfig,
    STRESS_COST_BPS_PER_SIDE,
    VOLATILITY_BALANCED_ID,
    VOLATILITY_FLOOR,
    VOLATILITY_LEVEL_LOOKBACK,
    VOLATILITY_RETURN_COUNT,
    build_monthly_targets,
    phase_metrics,
    simulate_targets,
)


RELEASE = "9.0"
ROUTE = "causal_monthly_volatility_balanced_budget"
PROTOCOL_ID = "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
PROTOCOL_PATH = Path("protocols/9.0-causal-volatility-balanced-budget.json")
SCOUT_PATH = Path("protocols/9.0-preprotocol-scout.json")
ASSET_SELECTION_PATH = Path("protocols/7.0-asset-selection.json")
INHERITED_PROTOCOL_PATH = Path("protocols/7.0-multi-asset.json")
ECONOMIC_GATE_PROTOCOL_PATH = Path("protocols/8.0-static-capital-budget.json")
CLOSURE_PATH = Path("protocols/9.0-release.json")
EVIDENCE_ROOT = Path("protocols/evidence/9.0")
WINNER_FREEZE_PATH = EVIDENCE_ROOT / "winner-freeze.json"
AUDIT_PATH = EVIDENCE_ROOT / "historical-audit.json"
RESULT_PATH = EVIDENCE_ROOT / "result.json"
PRIOR_PROTOCOL_PATH = Path("protocols/8.1-policy-operational-metric-reclassification.json")
PRIOR_CLOSURE_PATH = Path("protocols/8.1-release.json")
PRIOR_RECLASSIFICATION_PATH = Path("protocols/evidence/8.1/train-reclassification.json")
PRIOR_FREEZE_PATH = Path("protocols/evidence/8.1/winner-freeze.json")
PRIOR_RESULT_PATH = Path("protocols/evidence/8.1/result.json")
PRIOR_TAG = "8.1"
PRIOR_TAG_OBJECT = "8f575ed3833c8cc01f89e7a951d4234bd7ee6622"
PRIOR_COMMIT = "a4c0d36f727e99f6b2353facf24fd3cdedba958e"
PRIOR_PROTOCOL_PAYLOAD = "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5"
PRIOR_PROTOCOL_FILE_SHA256 = "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583"
PRIOR_CLOSURE_PAYLOAD = "f4a47421d08ca77eca6b27fd6417909a04c3eaf789c11d9ca069366412440ef5"
PRIOR_CLOSURE_FILE_SHA256 = "ef1596fa5cfbfdfd0c27d74c2747dcc852b7f209a4e27de2b7c01c6d8dbcc557"
PRIOR_RECLASSIFICATION_PAYLOAD = "4f498ffc12deac61144c77c56ba89cb9abccc034d2d73df4f1df8a6c50184c79"
PRIOR_RECLASSIFICATION_FILE_SHA256 = "bfd2c0c801259394861eba000a8e34bc9617cba3adcf6629d7e8b501ccf3c51b"
PRIOR_FREEZE_PAYLOAD = "d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76"
PRIOR_FREEZE_FILE_SHA256 = "b865e80cb899f7e5274d72b46ab1e0d88dad64b0ab2eb4e46750c5cec2167387"
PRIOR_RESULT_PAYLOAD = "d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4"
PRIOR_RESULT_FILE_SHA256 = "bcbcb09974e6314190de7a835560c4abbc1cde79734ed4fcef759061653cd95d"
PRIOR_VALIDATION_MANIFEST_PAYLOAD = "f5903d2b24b47662a9ba4ea3d2d127c9b5dee385d5b927140b25eda68b3ff060"
PRIOR_VALIDATION_MANIFEST_FILE_SHA256 = "e4b995095af7015c7b5380340e0f761337335ea4b1a70dca56de3aefed402553"
PRIOR_VALIDATION_BINDING_PAYLOAD = "7479aa06071d34544b6ce880d6a2986a09988e3905853dbab7127eaeb0e13d5b"
PRIOR_VALIDATION_BINDING_FILE_SHA256 = "d6adfe439984574dda1ef1c3ceab20a51a64a964d80d13a24bb505ef79f7697e"
PRIOR_VALIDATION_EVALUATION_PAYLOAD = "7794ee8c81cc784d262a464c55d37f3017b1e75cbc4bb421b5e4b8eb85685981"
PRIOR_VALIDATION_EVALUATION_FILE_SHA256 = "f5e630ff2c881bae179c884714b29a592456ced50c154ca38bcde3d43fab2fd5"
PRIOR_VALIDATION_PARQUET_COUNT = 20
PRIOR_VALIDATION_ROW_COUNT = 62654
PROTOCOL_PAYLOAD = "f6c7cce39e8b9a1ae5df10965a2dd607916095b2caf24fcf0a29b625c5bafc3e"
PROTOCOL_FILE_SHA256 = "19ecf56b5bd9c8b42b9f4df50761f719e2ca544eaea959a88c62d0ea4178d620"
SCOUT_PAYLOAD = "71926f08ce5ca2ab1b6470f7d3ee385371c4bfaf3243c5f942a891f63a8075a0"
SCOUT_FILE_SHA256 = "44b90b964ecca9a30029b1dfad45ae313ae4a5c12a91d82ba885ceecb826b857"
INHERITED_PROTOCOL_FILE_SHA256 = (
    "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
)
INHERITED_PROTOCOL_PAYLOAD = (
    "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
)
ASSET_SELECTION_FILE_SHA256 = "6d2d819db2579db76f8e7830a5de090d8d471c7fdc657abd8aba626cd1b065ec"
ASSET_SELECTION_PAYLOAD = "b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b"
ECONOMIC_GATE_PROTOCOL_FILE_SHA256 = "ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc"
ECONOMIC_GATE_PROTOCOL_PAYLOAD = "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
PRIMARY_ID = VOLATILITY_BALANCED_ID
WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-9.0"
PRIOR_WORK_ROOT = ROOT / "runtime" / "data" / "multi-asset-8.1"
PRIOR_SOURCE_ROOT = PRIOR_WORK_ROOT / "sources"
SOURCE_ROOT = WORK_ROOT / "sources"
EVALUATION_ROOT = WORK_ROOT / "evaluations"
BINDING_ROOT = WORK_ROOT / "stage-bindings"
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
FREEZE_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "status",
    "protocol_payload_sha256",
    "implementation_closure_payload_sha256",
    "development_execution_commit",
    "run_nonce",
    "candidate_registry",
    "selected_candidate_id",
    "development_source",
    "development",
    "audit_market_outcomes_opened",
    "runner_up_fallback",
    "claim_contract",
    "payload_sha256",
}
AUDIT_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "status",
    "selected_candidate_id",
    "winner_freeze_payload_sha256",
    "protocol_payload_sha256",
    "implementation_closure_payload_sha256",
    "audit_execution_commit",
    "run_nonce",
    "audit",
    "pre_2023_source_prefix",
    "runner_up_fallback",
    "claim_contract",
    "payload_sha256",
}
RESULT_FIELDS = {
    "schema_version",
    "kind",
    "release",
    "status",
    "selected_candidate_id",
    "audit_status",
    "winner_freeze",
    "historical_audit",
    "protocol_payload_sha256",
    "implementation_closure_payload_sha256",
    "claim_contract",
    "payload_sha256",
}
EVIDENCE_REFERENCE_FIELDS = {"path", "file_sha256", "payload_sha256"}
RUNTIME_DISTRIBUTIONS = ("numpy", "pandas", "pyarrow", "scipy", "tushare")
NATIVE_SUFFIXES = (".dll", ".dylib", ".pyd", ".so")
EVALUATION_ROLES = (
    "candidate", "candidate_stress", "static", "static_stress", "cash", "cash_stress"
)
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
    "development": {
        "source_start": "2014-01-15",
        "source_end": "2022-12-30",
        "performance_start": "2015-03-02",
        "performance_end": "2022-12-30",
    },
    "audit": {
        "source_start": "2014-01-15",
        "source_end": "2026-08-28",
        "performance_start": "2023-01-03",
        "performance_end": "2026-08-28",
    },
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


def _client() -> Any:
    config_path = ROOT / "configs" / "data.json"
    config = load_data_config(config_path)
    layout = RuntimeLayout.from_config(
        config,
        config_path=config_path,
        repo_root=ROOT,
    )
    return _configured_tushare_client(dict(config.get("sync") or {}), layout)


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
    if not allowed_stages.issubset({"development", "audit"}):
        raise RuntimeError("9.0 runtime may contain only development and audit stages")
    if WORK_ROOT.is_symlink() or (WORK_ROOT.exists() and not WORK_ROOT.is_dir()):
        raise RuntimeError("9.0 runtime root must be a regular local directory")
    if not allowed_stages and WORK_ROOT.exists():
        raise RuntimeError("9.0 pre-development state requires an absent runtime root")
    expected = {
        # Development must consume the retained 8.1 validation source in place;
        # only the first unopened 2023+ audit may create a new source stage.
        SOURCE_ROOT: ({"stage=audit"} if "audit" in allowed_stages else set()),
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
                f"unexpected entry under the 9.0 runtime root: {unexpected_roots}"
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
                    raise RuntimeError("could not verify 9.0/8.1 physical isolation") from exc
                if same:
                    raise RuntimeError(
                        f"9.0 runtime file reuses an 8.1 physical file: {current}"
                    )


def _assert_evidence_layout(allowed_names: set[str]) -> None:
    root = ROOT / EVIDENCE_ROOT
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise RuntimeError("9.0 evidence root must be a regular local directory")
    if not root.exists():
        return
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed_names)
    if unexpected:
        raise RuntimeError(f"unexpected 9.0 evidence artifact: {unexpected}")
    for path in root.iterdir():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"9.0 evidence artifact is indirect or not a file: {path}")


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
            f"9.0 formal run forbids a pre-existing {stage_name} source stage"
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


def _v9_verify_result(
    result: Mapping[str, Any],
    *,
    freeze: Mapping[str, Any],
    audit: Mapping[str, Any] | None,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> None:
    freeze_reference = result.get("winner_freeze")
    audit_reference = result.get("historical_audit")
    if (
        set(result) != RESULT_FIELDS
        or result.get("payload_sha256") != canonical_payload_sha256(result)
        or result.get("schema_version") != 1
        or result.get("kind") != "factor_lab_multi_asset_terminal_result"
        or result.get("release") != RELEASE
        or result.get("selected_candidate_id") != freeze.get("selected_candidate_id")
        or result.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or result.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
        or result.get("claim_contract") != protocol.get("claim_contract")
        or not isinstance(freeze_reference, Mapping)
        or set(freeze_reference) != EVIDENCE_REFERENCE_FIELDS
        or freeze_reference.get("payload_sha256")
        != freeze.get("payload_sha256")
        or freeze_reference.get("path") != WINNER_FREEZE_PATH.as_posix()
        or freeze_reference.get("file_sha256")
        != file_sha256(ROOT / WINNER_FREEZE_PATH)
    ):
        raise ValueError("9.0 terminal result contract differs")
    if freeze.get("selected_candidate_id") is None:
        if (
            audit is not None
            or result.get("historical_audit") is not None
            or result.get("status") != "selection_falsified_no_candidate"
            or result.get("audit_status") != "not_opened"
        ):
            raise ValueError("9.0 null terminal result differs")
        return
    if audit is None:
        raise ValueError("9.0 selected terminal result lacks audit evidence")
    expected = (
        "historical_adaptive_beta_diagnostic_passed_fresh_evidence_required"
        if audit.get("status") == "historical_audit_passed"
        else "historical_adaptive_beta_diagnostic_failed"
    )
    if (
        result.get("status") != expected
        or result.get("audit_status") != audit.get("status")
        or not isinstance(audit_reference, Mapping)
        or set(audit_reference) != EVIDENCE_REFERENCE_FIELDS
        or audit_reference.get("path") != AUDIT_PATH.as_posix()
        or audit_reference.get("payload_sha256")
        != audit.get("payload_sha256")
        or audit_reference.get("file_sha256")
        != file_sha256(ROOT / AUDIT_PATH)
    ):
        raise ValueError("9.0 selected terminal result differs")


def verify_release_state(
    *, verify_data: bool = False, verify_runtime: bool = False
) -> dict[str, Any]:
    """Verify the committed 9.0 closure and staged evidence chain."""

    closure, protocol = _v9_verify_closure(verify_runtime=verify_runtime)
    freeze_path = ROOT / WINNER_FREEZE_PATH
    audit_path = ROOT / AUDIT_PATH
    result_path = ROOT / RESULT_PATH
    if not freeze_path.is_file():
        if audit_path.exists() or result_path.exists():
            raise ValueError("9.0 audit/result exists without winner freeze")
        _assert_evidence_layout(set())
        _assert_runtime_layout(set())
        return {
            "status": str(closure["status"]),
            "closure": closure,
            "protocol": protocol,
            "freeze": None,
            "audit": None,
            "result": None,
        }
    freeze = _read_json(WINNER_FREEZE_PATH)
    _require_committed(WINNER_FREEZE_PATH)
    _v9_verify_freeze(
        freeze,
        closure=closure,
        protocol=protocol,
        verify_data=verify_data,
    )
    audit: dict[str, Any] | None = None
    if audit_path.is_file():
        if freeze.get("selected_candidate_id") != PRIMARY_ID:
            raise ValueError("null 9.0 selection cannot have audit evidence")
        audit = _read_json(AUDIT_PATH)
        _require_committed(AUDIT_PATH)
        _v9_verify_audit(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=verify_data,
        )
    result: dict[str, Any] | None = None
    if result_path.is_file():
        result = _read_json(RESULT_PATH)
        _require_committed(RESULT_PATH)
        _v9_verify_result(
            result,
            freeze=freeze,
            audit=audit,
            closure=closure,
            protocol=protocol,
        )
    allowed_stages = {"development"}
    allowed_evidence = {WINNER_FREEZE_PATH.name}
    if audit is not None:
        allowed_stages.add("audit")
        allowed_evidence.add(AUDIT_PATH.name)
    if result is not None:
        allowed_evidence.add(RESULT_PATH.name)
    _assert_runtime_layout(allowed_stages)
    _assert_evidence_layout(allowed_evidence)
    status = (
        str(result["status"])
        if result is not None
        else str(audit["status"])
        if audit is not None
        else "selection_frozen_no_candidate_pending_finalize"
        if freeze.get("selected_candidate_id") is None
        else "selection_frozen_pending_historical_audit"
    )
    return {
        "status": status,
        "closure": closure,
        "protocol": protocol,
        "freeze": freeze,
        "audit": audit,
        "result": result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", required=True, choices=("development", "audit", "finalize")
    )
    args = parser.parse_args(argv)
    if args.mode == "development":
        return run_development()
    if args.mode == "audit":
        return run_audit()
    return run_finalize()


def _v9_remote_prior_tag_refs() -> dict[str, str]:
    remote = subprocess.run(
        [
            "git",
            "ls-remote",
            "--exit-code",
            "origin",
            f"refs/tags/{PRIOR_TAG}",
            f"refs/tags/{PRIOR_TAG}^{{}}",
        ],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if remote.returncode != 0:
        raise RuntimeError("could not verify the exact published 8.1 tag")
    try:
        return {
            ref: object_id
            for object_id, ref in (
                line.split() for line in remote.stdout.decode("ascii").splitlines()
            )
        }
    except ValueError as exc:
        raise ValueError("published 8.1 tag response is malformed") from exc


def _v9_tagged_json(
    path: Path, *, expected_file: str, expected_payload: str
) -> dict[str, Any]:
    current = (ROOT / path).read_bytes()
    tagged = _git("show", f"{PRIOR_COMMIT}:{path.as_posix()}")
    value = json.loads(current.decode("utf-8"))
    if (
        current != tagged
        or hashlib.sha256(current).hexdigest() != expected_file
        or not isinstance(value, dict)
        or value.get("payload_sha256") != expected_payload
        or canonical_payload_sha256(value) != expected_payload
    ):
        raise ValueError(f"published 8.1 artifact differs: {path}")
    return value


def _v9_assert_frame_exact(actual: pd.DataFrame, expected: pd.DataFrame, *, role: str) -> None:
    try:
        pd.testing.assert_frame_equal(
            actual.reset_index(drop=True),
            expected.reset_index(drop=True),
            check_dtype=True,
            check_exact=True,
        )
    except AssertionError as exc:
        raise ValueError(f"{role} does not replay exactly") from exc


def _verify_prior_8_1_archive(
    *, verify_data: bool = True, verify_runtime: bool = True
) -> dict[str, Any]:
    """Verify the published null 8.1 chain and its retained validation stage."""

    if verify_data and not verify_runtime:
        raise ValueError("deep prior-data verification requires the retained runtime")
    if (
        _git("cat-file", "-t", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != "tag"
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}").decode("ascii").strip()
        != PRIOR_TAG_OBJECT
        or _git("rev-parse", f"refs/tags/{PRIOR_TAG}^{{}}").decode("ascii").strip()
        != PRIOR_COMMIT
        or _v9_remote_prior_tag_refs()
        != {
            f"refs/tags/{PRIOR_TAG}": PRIOR_TAG_OBJECT,
            f"refs/tags/{PRIOR_TAG}^{{}}": PRIOR_COMMIT,
        }
    ):
        raise ValueError("published 8.1 annotated tag identity differs")
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", PRIOR_COMMIT, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("current HEAD does not descend from published 8.1")

    protocol = _v9_tagged_json(
        PRIOR_PROTOCOL_PATH,
        expected_file=PRIOR_PROTOCOL_FILE_SHA256,
        expected_payload=PRIOR_PROTOCOL_PAYLOAD,
    )
    closure = _v9_tagged_json(
        PRIOR_CLOSURE_PATH,
        expected_file=PRIOR_CLOSURE_FILE_SHA256,
        expected_payload=PRIOR_CLOSURE_PAYLOAD,
    )
    reclassification = _v9_tagged_json(
        PRIOR_RECLASSIFICATION_PATH,
        expected_file=PRIOR_RECLASSIFICATION_FILE_SHA256,
        expected_payload=PRIOR_RECLASSIFICATION_PAYLOAD,
    )
    freeze = _v9_tagged_json(
        PRIOR_FREEZE_PATH,
        expected_file=PRIOR_FREEZE_FILE_SHA256,
        expected_payload=PRIOR_FREEZE_PAYLOAD,
    )
    result = _v9_tagged_json(
        PRIOR_RESULT_PATH,
        expected_file=PRIOR_RESULT_FILE_SHA256,
        expected_payload=PRIOR_RESULT_PAYLOAD,
    )
    validation = freeze.get("validation")
    if (
        protocol.get("release") != "8.1"
        or closure.get("release") != "8.1"
        or reclassification.get("status") != "train_reclassification_passed"
        or freeze.get("status") != "selected_null_frozen_validation_failed"
        or freeze.get("selected_candidate_id") is not None
        or freeze.get("validation_market_outcomes_opened") is not True
        or freeze.get("audit_market_outcomes_opened") is not False
        or not isinstance(validation, Mapping)
        or result.get("status") != "selection_falsified_no_candidate"
        or result.get("selected_candidate_id") is not None
        or result.get("audit_status") != "not_opened"
    ):
        raise ValueError("published 8.1 terminal null boundary differs")

    projection: dict[str, Any] = {
        "release": "8.1",
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "tag_object": PRIOR_TAG_OBJECT,
        "tag_commit": PRIOR_COMMIT,
        "protocol_payload_sha256": PRIOR_PROTOCOL_PAYLOAD,
        "closure_payload_sha256": PRIOR_CLOSURE_PAYLOAD,
        "reclassification_payload_sha256": PRIOR_RECLASSIFICATION_PAYLOAD,
        "freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD,
        "result_payload_sha256": PRIOR_RESULT_PAYLOAD,
        "validation_manifest_payload_sha256": PRIOR_VALIDATION_MANIFEST_PAYLOAD,
        "validation_binding_payload_sha256": PRIOR_VALIDATION_BINDING_PAYLOAD,
        "validation_evaluation_payload_sha256": PRIOR_VALIDATION_EVALUATION_PAYLOAD,
        "artifact_parquet_count": PRIOR_VALIDATION_PARQUET_COUNT,
        "artifact_row_count": PRIOR_VALIDATION_ROW_COUNT,
        "deep_data_verified": bool(verify_data),
        "deep_runtime_verified": bool(verify_runtime),
    }
    if verify_runtime:
        stage = load_multi_asset_stage(PRIOR_SOURCE_ROOT, "validation")
        manifest_path = stage.path / "manifest.json"
        binding_path = PRIOR_WORK_ROOT / "stage-bindings" / "validation.json"
        evaluation_path = (
            PRIOR_WORK_ROOT / "evaluations" / "stage=validation" / "evaluation.json"
        )
        binding = _read_json(binding_path)
        evaluation = _read_json(evaluation_path)
        if (
            stage.manifest.get("payload_sha256") != PRIOR_VALIDATION_MANIFEST_PAYLOAD
            or file_sha256(manifest_path) != PRIOR_VALIDATION_MANIFEST_FILE_SHA256
            or binding.get("payload_sha256") != PRIOR_VALIDATION_BINDING_PAYLOAD
            or file_sha256(binding_path) != PRIOR_VALIDATION_BINDING_FILE_SHA256
            or evaluation.get("payload_sha256") != PRIOR_VALIDATION_EVALUATION_PAYLOAD
            or file_sha256(evaluation_path) != PRIOR_VALIDATION_EVALUATION_FILE_SHA256
            or validation.get("source_manifest_payload_sha256")
            != PRIOR_VALIDATION_MANIFEST_PAYLOAD
            or validation.get("stage_binding_payload_sha256")
            != PRIOR_VALIDATION_BINDING_PAYLOAD
            or validation.get("evaluation_payload_sha256")
            != PRIOR_VALIDATION_EVALUATION_PAYLOAD
            or validation.get("evaluation_file_sha256")
            != PRIOR_VALIDATION_EVALUATION_FILE_SHA256
        ):
            raise ValueError("retained 8.1 validation identities differ")
        artifacts = evaluation.get("artifacts")
        if not isinstance(artifacts, Mapping) or set(artifacts) != {
            "primary", "stress", "cash", "cash_stress"
        }:
            raise ValueError("retained 8.1 evaluation role set differs")
        parquet_count = 0
        row_count = 0
        for role, values in artifacts.items():
            if not isinstance(values, Mapping) or set(values) != set(EVALUATION_ARTIFACTS):
                raise ValueError(f"retained 8.1 {role} artifact set differs")
            for artifact, entry in values.items():
                path = evaluation_path.parent / str(entry.get("path") or "")
                if (
                    path.is_symlink()
                    or not path.is_file()
                    or file_sha256(path) != entry.get("file_sha256")
                ):
                    raise ValueError(f"retained 8.1 artifact differs: {path}")
                rows = len(pd.read_parquet(path))
                if rows != entry.get("rows"):
                    raise ValueError(f"retained 8.1 artifact row count differs: {path}")
                parquet_count += 1
                row_count += rows
        if (
            parquet_count != PRIOR_VALIDATION_PARQUET_COUNT
            or row_count != PRIOR_VALIDATION_ROW_COUNT
        ):
            raise ValueError("retained 8.1 evaluation totals differ")
        if verify_data:
            sessions = tuple(pd.to_datetime(stage.calendar["trade_date"]).dt.normalize())
            role_specs = {
                "primary": (CONTROL_ID, 8.0),
                "stress": (CONTROL_ID, 16.0),
                "cash": (CASH_ONLY_ID, 8.0),
                "cash_stress": (CASH_ONLY_ID, 16.0),
            }
            for role, (strategy_id, cost) in role_specs.items():
                expected_targets = _v9_filter_targets(
                    build_monthly_targets(stage.assets, sessions, strategy_id),
                    start="2020-01-02",
                    end="2022-12-30",
                )
                replay = simulate_targets(
                    stage.assets,
                    expected_targets,
                    sessions,
                    SimulationConfig(cost_bps_per_side=cost),
                )
                for artifact in EVALUATION_ARTIFACTS:
                    persisted = pd.read_parquet(
                        evaluation_path.parent / artifacts[role][artifact]["path"]
                    )
                    _v9_assert_frame_exact(
                        persisted, replay[artifact], role=f"8.1 {role} {artifact}"
                    )
    immutable_identity = {
        key: value
        for key, value in projection.items()
        if key not in {"deep_data_verified", "deep_runtime_verified"}
    }
    projection["archive_identity_sha256"] = canonical_payload_sha256(
        immutable_identity
    )
    return projection


def _v9_verify_protocol_contract(protocol: Mapping[str, Any]) -> None:
    if not _is_sha256(PROTOCOL_PAYLOAD) or not _is_sha256(PROTOCOL_FILE_SHA256):
        raise RuntimeError("9.0 protocol hash constants are pending and fail closed")
    scout = _read_json(SCOUT_PATH)
    asset_selection = _read_json(ASSET_SELECTION_PATH)
    inherited = _read_json(INHERITED_PROTOCOL_PATH)
    economic_gate = _read_json(ECONOMIC_GATE_PROTOCOL_PATH)
    frozen = protocol.get("frozen_source_and_execution_contract")
    strategy = protocol.get("candidate_registry", {}).get("strategy")
    claim = protocol.get("claim_contract")
    phases = protocol.get("physical_phases")
    relative = protocol.get("relative_stability_gate")
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(protocol) != PROTOCOL_PAYLOAD
        or file_sha256(ROOT / PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
        or protocol.get("release") != RELEASE
        or protocol.get("direction_change") is not True
        or protocol.get("route") != ROUTE
        or protocol.get("protocol_id") != PROTOCOL_ID
        or protocol.get("strategy_id") != PRIMARY_ID
        or not isinstance(strategy, Mapping)
        or strategy.get("risk_assets") != list(RISK_CODES)
        or strategy.get("cash_asset") != CASH_CODE
        or strategy.get("frozen_base_budget")
        != {**RISK_BUDGETS, CASH_CODE: 0.0}
        or strategy.get("required_observed_total_return_levels")
        != VOLATILITY_LEVEL_LOOKBACK
        or strategy.get("volatility_return_count") != VOLATILITY_RETURN_COUNT
        or asset_selection.get("payload_sha256") != ASSET_SELECTION_PAYLOAD
        or canonical_payload_sha256(asset_selection) != ASSET_SELECTION_PAYLOAD
        or file_sha256(ROOT / ASSET_SELECTION_PATH)
        != ASSET_SELECTION_FILE_SHA256
        or inherited.get("payload_sha256") != INHERITED_PROTOCOL_PAYLOAD
        or canonical_payload_sha256(inherited) != INHERITED_PROTOCOL_PAYLOAD
        or file_sha256(ROOT / INHERITED_PROTOCOL_PATH)
        != INHERITED_PROTOCOL_FILE_SHA256
        or economic_gate.get("payload_sha256")
        != ECONOMIC_GATE_PROTOCOL_PAYLOAD
        or canonical_payload_sha256(economic_gate)
        != ECONOMIC_GATE_PROTOCOL_PAYLOAD
        or file_sha256(ROOT / ECONOMIC_GATE_PROTOCOL_PATH)
        != ECONOMIC_GATE_PROTOCOL_FILE_SHA256
        or not isinstance(frozen, Mapping)
        or frozen.get("asset_selection")
        != {
            "path": ASSET_SELECTION_PATH.as_posix(),
            "file_sha256": ASSET_SELECTION_FILE_SHA256,
            "payload_sha256": ASSET_SELECTION_PAYLOAD,
        }
        or frozen.get("data_and_execution_protocol")
        != {
            "path": INHERITED_PROTOCOL_PATH.as_posix(),
            "file_sha256": INHERITED_PROTOCOL_FILE_SHA256,
            "payload_sha256": INHERITED_PROTOCOL_PAYLOAD,
        }
        or frozen.get("economic_gate_source")
        != {
            "path": ECONOMIC_GATE_PROTOCOL_PATH.as_posix(),
            "file_sha256": ECONOMIC_GATE_PROTOCOL_FILE_SHA256,
            "payload_sha256": ECONOMIC_GATE_PROTOCOL_PAYLOAD,
        }
        or frozen.get("initial_capital_rmb") != INITIAL_CAPITAL_RMB
        or frozen.get("lot_size_shares") != LOT_SIZE
        or frozen.get("maximum_signal_adv20_fraction")
        != MAX_SIGNAL_ADV_PARTICIPATION
        or frozen.get("base_cost_bps_per_side") != BASE_COST_BPS_PER_SIDE
        or frozen.get("stress_cost_bps_per_side")
        != STRESS_COST_BPS_PER_SIDE
        or VOLATILITY_FLOOR != 1e-12
        or protocol.get("candidate_registry", {}).get("strategy", {}).get(
            "volatility_floor"
        )
        != VOLATILITY_FLOOR
        or scout.get("payload_sha256") != SCOUT_PAYLOAD
        or canonical_payload_sha256(scout) != SCOUT_PAYLOAD
        or file_sha256(ROOT / SCOUT_PATH) != SCOUT_FILE_SHA256
        or protocol.get("preprotocol_scout", {}).get("payload_sha256")
        != SCOUT_PAYLOAD
        or protocol.get("preprotocol_scout", {}).get("file_sha256")
        != SCOUT_FILE_SHA256
        or not isinstance(protocol.get("shared_absolute_gate"), Mapping)
        or not isinstance(relative, Mapping)
        or relative
        != {
            "sharpe_delta_at_least": 0.0,
            "max_drawdown_delta_at_least": 0.0,
            "positive_complete_year_ratio_delta_at_least": 0.0,
        }
        or not isinstance(phases, Mapping)
        or phases.get("development", {}).get("source_release") != "8.1"
        or phases.get("development", {}).get("source_root")
        != "runtime/data/multi-asset-8.1/sources/stage=validation"
        or phases.get("development", {}).get("runtime_stage") is not None
        or phases.get("development", {}).get("performance_start") != "2015-03-02"
        or phases.get("development", {}).get("performance_end") != "2022-12-30"
        or phases.get("audit", {}).get("performance_start") != "2023-01-03"
        or phases.get("audit", {}).get("market_outcome_opened") is not False
        or not isinstance(claim, Mapping)
        or claim.get("research_object") != "causal_volatility_balanced_strategic_beta"
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
        raise ValueError("9.0 protocol differs from its exact runner contract")


def _v9_verify_implementation_map(
    closure: Mapping[str, Any], implementation_commit: str
) -> None:
    implementation = closure.get("implementation")
    if not isinstance(implementation, Mapping) or set(implementation) != EXPECTED_IMPLEMENTATION_PATHS:
        raise ValueError("9.0 closure implementation path set differs")
    for relative, binding in implementation.items():
        path = _safe_repo_file(str(relative))
        committed = _git("show", f"{implementation_commit}:{relative}")
        if (
            not isinstance(binding, Mapping)
            or binding.get("path") != relative
            or file_sha256(path) != binding.get("sha256")
            or hashlib.sha256(committed).hexdigest() != binding.get("sha256")
        ):
            raise ValueError(f"9.0 frozen implementation differs: {relative}")


def _v9_verify_closure(
    *, verify_runtime: bool = True
) -> tuple[dict[str, Any], dict[str, Any]]:
    closure = _read_json(CLOSURE_PATH)
    protocol = _read_json(PROTOCOL_PATH)
    _v9_verify_protocol_contract(protocol)
    prior = closure.get("prior_8_1_archive")
    implementation_commit = str(closure.get("implementation_commit") or "")
    expected_protocol_reference = {
        "path": PROTOCOL_PATH.as_posix(),
        "file_sha256": PROTOCOL_FILE_SHA256,
        "payload_sha256": PROTOCOL_PAYLOAD,
        "protocol_id": PROTOCOL_ID,
    }
    expected_scout_reference = {
        "path": SCOUT_PATH.as_posix(),
        "file_sha256": SCOUT_FILE_SHA256,
        "payload_sha256": SCOUT_PAYLOAD,
        "status": "selected_volatility_balanced_after_fully_exposed_development",
    }
    if (
        set(closure) != CLOSURE_FIELDS
        or closure.get("payload_sha256") != canonical_payload_sha256(closure)
        or closure.get("schema_version") != 1
        or closure.get("kind") != "factor_lab_release_closure"
        or closure.get("release") != RELEASE
        or closure.get("closure_role") != "causal_volatility_balanced_preselection_root"
        or closure.get("status") != "implementation_frozen_before_formal_development_replay"
        or closure.get("direction_change") is not True
        or closure.get("route") != ROUTE
        or closure.get("development_outcomes_opened") is not True
        or closure.get("audit_market_outcomes_opened") is not False
        or closure.get("protocol") != expected_protocol_reference
        or closure.get("preprotocol_scout") != expected_scout_reference
        or closure.get("formal_data") != {}
        or not isinstance(prior, Mapping)
        or prior.get("tag_object") != PRIOR_TAG_OBJECT
        or prior.get("tag_commit") != PRIOR_COMMIT
        or not _is_commit(implementation_commit)
        or closure.get("claim_contract") != protocol.get("claim_contract")
    ):
        raise ValueError("9.0 preselection closure contract differs")
    implementation_tree = _git(
        "rev-parse", f"{implementation_commit}^{{tree}}"
    ).decode("ascii").strip()
    if closure.get("implementation_tree") != implementation_tree:
        raise ValueError("9.0 preselection closure implementation tree differs")
    archived = _verify_prior_8_1_archive(
        verify_data=verify_runtime, verify_runtime=verify_runtime
    )
    expected_prior = {
        "release": "8.1",
        "tag": PRIOR_TAG,
        "tag_object": PRIOR_TAG_OBJECT,
        "tag_commit": PRIOR_COMMIT,
        "files": {
            PRIOR_PROTOCOL_PATH.as_posix(): {
                "path": PRIOR_PROTOCOL_PATH.as_posix(),
                "file_sha256": PRIOR_PROTOCOL_FILE_SHA256,
                "payload_sha256": PRIOR_PROTOCOL_PAYLOAD,
                "status": "frozen_after_8_0_train_evaluation_before_8_1_reclassification",
            },
            PRIOR_CLOSURE_PATH.as_posix(): {
                "path": PRIOR_CLOSURE_PATH.as_posix(),
                "file_sha256": PRIOR_CLOSURE_FILE_SHA256,
                "payload_sha256": PRIOR_CLOSURE_PAYLOAD,
                "status": "implementation_frozen_before_8_1_reclassification",
            },
            PRIOR_RECLASSIFICATION_PATH.as_posix(): {
                "path": PRIOR_RECLASSIFICATION_PATH.as_posix(),
                "file_sha256": PRIOR_RECLASSIFICATION_FILE_SHA256,
                "payload_sha256": PRIOR_RECLASSIFICATION_PAYLOAD,
                "status": "train_reclassification_passed",
            },
            PRIOR_FREEZE_PATH.as_posix(): {
                "path": PRIOR_FREEZE_PATH.as_posix(),
                "file_sha256": PRIOR_FREEZE_FILE_SHA256,
                "payload_sha256": PRIOR_FREEZE_PAYLOAD,
                "status": "selected_null_frozen_validation_failed",
            },
            PRIOR_RESULT_PATH.as_posix(): {
                "path": PRIOR_RESULT_PATH.as_posix(),
                "file_sha256": PRIOR_RESULT_FILE_SHA256,
                "payload_sha256": PRIOR_RESULT_PAYLOAD,
                "status": "selection_falsified_no_candidate",
            },
        },
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "protocol_payload_sha256": PRIOR_PROTOCOL_PAYLOAD,
        "closure_payload_sha256": PRIOR_CLOSURE_PAYLOAD,
        "reclassification_payload_sha256": PRIOR_RECLASSIFICATION_PAYLOAD,
        "freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD,
        "result_payload_sha256": PRIOR_RESULT_PAYLOAD,
        "validation_manifest_payload_sha256": PRIOR_VALIDATION_MANIFEST_PAYLOAD,
        "validation_binding_payload_sha256": PRIOR_VALIDATION_BINDING_PAYLOAD,
        "validation_evaluation_payload_sha256": PRIOR_VALIDATION_EVALUATION_PAYLOAD,
        "artifact_parquet_count": PRIOR_VALIDATION_PARQUET_COUNT,
        "artifact_row_count": PRIOR_VALIDATION_ROW_COUNT,
        "deep_data_verified": True,
        "deep_runtime_verified": True,
        "archive_identity_sha256": archived["archive_identity_sha256"],
    }
    if prior != expected_prior:
        raise ValueError("9.0 closure prior-8.1 archive identity differs")
    _v9_verify_implementation_map(closure, implementation_commit)
    for relative in (
        PROTOCOL_PATH,
        SCOUT_PATH,
        ASSET_SELECTION_PATH,
        INHERITED_PROTOCOL_PATH,
        ECONOMIC_GATE_PROTOCOL_PATH,
        PRIOR_PROTOCOL_PATH,
        PRIOR_CLOSURE_PATH,
        PRIOR_RECLASSIFICATION_PATH,
        PRIOR_FREEZE_PATH,
        PRIOR_RESULT_PATH,
    ):
        if _git("show", f"{implementation_commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(
                f"9.0 implementation commit lacks exact contract: {relative}"
            )
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("current HEAD does not descend from the 9.0 implementation")
    if verify_runtime:
        _require_source_imports()
        if closure.get("runtime") != _runtime_identity():
            raise ValueError("9.0 formal runtime differs from closure")
    _require_committed(CLOSURE_PATH)
    return closure, protocol


def _v9_filter_targets(targets: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    executions = pd.to_datetime(targets["execution_date"], errors="coerce").dt.normalize()
    selected = targets.loc[
        executions.between(pd.Timestamp(start), pd.Timestamp(end))
    ].copy()
    if selected.empty:
        raise ValueError(f"9.0 phase has no targets between {start} and {end}")
    return selected.reset_index(drop=True)


def _v9_prefix_replay_count(
    stage: MultiAssetStage, targets: pd.DataFrame, sessions: tuple[pd.Timestamp, ...]
) -> int:
    count = 0
    for signal_date, expected in targets.groupby("signal_date", sort=True):
        signal = pd.Timestamp(signal_date).normalize()
        execution = pd.Timestamp(expected["execution_date"].iloc[0]).normalize()
        prefix_assets = {
            code: frame.loc[
                pd.to_datetime(frame["trade_date"]).dt.normalize().le(signal)
            ].copy()
            for code, frame in stage.assets.items()
        }
        prefix_sessions = tuple(value for value in sessions if value <= execution)
        rebuilt = build_monthly_targets(prefix_assets, prefix_sessions, PRIMARY_ID)
        actual = rebuilt.loc[
            pd.to_datetime(rebuilt["signal_date"]).dt.normalize().eq(signal)
        ]
        if actual.empty:
            raise ValueError(f"9.0 target prefix omitted signal {signal.date()}")
        _v9_assert_frame_exact(
            actual.sort_values("code", kind="mergesort"),
            expected.sort_values("code", kind="mergesort"),
            role=f"9.0 target prefix {signal.date()}",
        )
        count += 1
    return count


def _v9_role_metrics(
    result: Mapping[str, Any], *, start: str, end: str
) -> dict[str, Any]:
    values = phase_metrics(result, start=start, end=end)
    # Each D1/D2 result is a separate fresh-cash account whose market/calendar
    # view was physically cut at that phase end before target construction.
    validity = _role_execution_validity(result)
    # The account-wide artifact validity is shared by D1/D2, while accounting
    # reconciliation is an exact metric of the reported phase slice.
    validity["maximum_nav_reconciliation_error"] = values[
        "nav_reconciliation_error"
    ]
    values["execution_validity"] = validity
    return values


def _v9_absolute_gate(
    candidate: Mapping[str, Any],
    cash: Mapping[str, Any],
    *,
    thresholds: Mapping[str, Any],
    operational: Mapping[str, float],
) -> dict[str, Any]:
    values = {
        "net_cagr_strictly_positive": {
            "metric": float(candidate["cagr"]), "threshold": 0.0
        },
        "net_sharpe_at_least": {
            "metric": float(candidate["sharpe"]),
            "threshold": float(thresholds["net_sharpe_at_least"]),
        },
        "daily_max_drawdown_at_least": {
            "metric": float(candidate["max_drawdown"]),
            "threshold": float(thresholds["daily_max_drawdown_at_least"]),
        },
        "positive_complete_year_ratio_at_least": {
            "metric": float(candidate["positive_complete_year_ratio"]),
            "threshold": float(thresholds["positive_complete_year_ratio_at_least"]),
        },
        "cash_excess_cagr_strictly_positive": {
            "metric": float(candidate["cagr"]) - float(cash["cagr"]),
            "threshold": 0.0,
        },
        "annualized_turnover_at_most": {
            "metric": float(operational["annualized_turnover"]),
            "threshold": float(operational["annualized_turnover_at_most"]),
        },
        "requested_notional_fill_ratio_at_least": {
            "metric": float(operational["requested_notional_fill_ratio"]),
            "threshold": float(operational["requested_notional_fill_ratio_at_least"]),
        },
        "capacity_limited_requested_notional_ratio_at_most": {
            "metric": float(operational["capacity_limited_requested_notional_ratio"]),
            "threshold": float(
                operational["capacity_limited_requested_notional_ratio_at_most"]
            ),
        },
        "nav_reconciliation_error_at_most": {
            "metric": float(operational["nav_reconciliation_error"]),
            "threshold": float(operational["nav_reconciliation_error_at_most"]),
        },
    }
    checks = {
        key: value["metric"] > value["threshold"]
        if key in {"net_cagr_strictly_positive", "cash_excess_cagr_strictly_positive"}
        else value["metric"] <= value["threshold"]
        if key.endswith("_at_most")
        else value["metric"] >= value["threshold"]
        for key, value in values.items()
    }
    return {"passed": all(checks.values()), "checks": checks, "values": values}


def _v9_relative_gate(
    candidate: Mapping[str, Any],
    static: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "sharpe_delta_at_least": {
            "metric": float(candidate["sharpe"]) - float(static["sharpe"]),
            "threshold": float(config["sharpe_delta_at_least"]),
        },
        "max_drawdown_delta_at_least": {
            "metric": float(candidate["max_drawdown"])
            - float(static["max_drawdown"]),
            "threshold": float(config["max_drawdown_delta_at_least"]),
        },
        "positive_complete_year_ratio_delta_at_least": {
            "metric": float(candidate["positive_complete_year_ratio"])
            - float(static["positive_complete_year_ratio"]),
            "threshold": float(
                config["positive_complete_year_ratio_delta_at_least"]
            ),
        },
    }
    checks = {
        key: value["metric"] >= value["threshold"] for key, value in values.items()
    }
    return {"passed": all(checks.values()), "checks": checks, "values": values}


def _v9_require_hard_validity(roles: Mapping[str, Mapping[str, Any]]) -> None:
    if set(roles) != set(EVALUATION_ROLES):
        raise RuntimeError("9.0 hard-validity role set differs")
    for role, metrics in roles.items():
        validity = metrics.get("execution_validity")
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
        counts = (
            "blocked_missing_open_trade_count",
            "blocked_capacity_trade_count",
            "capacity_violation_count",
            "negative_cash_observation_count",
            "leverage_observation_count",
        )
        if (
            not isinstance(validity, Mapping)
            or set(validity) != required
            or validity.get("artifact_set_complete") is not True
            or validity.get("status_values_allowed") is not True
            or validity.get("status_execution_identity_exact") is not True
            or not all(_is_exact_nonnegative_int(validity.get(key)) for key in counts)
            or validity.get("capacity_violation_count") != 0
            or validity.get("negative_cash_observation_count") != 0
            or validity.get("leverage_observation_count") != 0
            or not math.isfinite(float(validity.get("minimum_cash", float("nan"))))
            or float(validity["minimum_cash"]) < -1e-8
            or not math.isfinite(
                float(validity.get("maximum_gross_exposure_ratio", float("nan")))
            )
            or float(validity["maximum_gross_exposure_ratio"]) < 0.0
            or float(validity["maximum_gross_exposure_ratio"]) > 1.0 + 1e-8
            or not math.isfinite(
                float(validity.get("maximum_nav_reconciliation_error", float("nan")))
            )
            or float(validity["maximum_nav_reconciliation_error"]) < 0.0
            or float(validity["maximum_nav_reconciliation_error"])
            != float(metrics["nav_reconciliation_error"])
            or not math.isfinite(
                float(validity.get("requested_notional_total", float("nan")))
            )
            or float(validity["requested_notional_total"]) < 0.0
            or not math.isfinite(
                float(validity.get("executed_notional_total", float("nan")))
            )
            or float(validity["executed_notional_total"]) < 0.0
            or float(validity["executed_notional_total"])
            > float(validity["requested_notional_total"]) + NOTIONAL_ABS_TOL_RMB
            or not math.isfinite(
                float(
                    validity.get(
                        "capacity_limited_requested_notional", float("nan")
                    )
                )
            )
            or float(validity["capacity_limited_requested_notional"]) < 0.0
            or float(validity["capacity_limited_requested_notional"])
            > float(validity["requested_notional_total"]) + NOTIONAL_ABS_TOL_RMB
            or validity.get("capacity_fields_finite_and_nonnegative") is not True
            or validity.get("executed_notional_not_above_requested") is not True
            or validity.get("capacity_limited_notional_not_above_requested") is not True
            or validity.get("capacity_aggregation_identity_exact") is not True
            or validity.get("requested_fill_identity_exact") is not True
            or validity.get("daily_trade_notional_identity_exact") is not True
        ):
            raise RuntimeError(f"9.0 {role} execution validity hard fail")


def _v9_phase_bundle(
    results: Mapping[str, Mapping[str, Any]],
    *,
    start: str,
    end: str,
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    roles = {
        role: _v9_role_metrics(result, start=start, end=end)
        for role, result in results.items()
    }
    return _v9_bundle_from_roles(roles, start=start, end=end, protocol=protocol)


def _v9_bundle_from_roles(
    roles: Mapping[str, Mapping[str, Any]],
    *,
    start: str,
    end: str,
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    if set(roles) != set(EVALUATION_ROLES):
        raise ValueError("9.0 phase result role set differs")
    _v9_require_hard_validity(roles)
    is_audit = pd.Timestamp(start) >= pd.Timestamp("2023-01-01")
    operational_config = protocol["shared_absolute_gate"]["operational"]
    operational = {
        "annualized_turnover": max(
            float(roles[role]["annualized_turnover"])
            for role in ("candidate", "candidate_stress")
        ),
        "requested_notional_fill_ratio": min(
            float(roles[role]["requested_notional_fill_ratio"])
            for role in ("candidate", "candidate_stress")
        ),
        "capacity_limited_requested_notional_ratio": max(
            float(roles[role]["capacity_limited_requested_notional_ratio"])
            for role in ("candidate", "candidate_stress")
        ),
        "nav_reconciliation_error": max(
            float(values["nav_reconciliation_error"]) for values in roles.values()
        ),
        **{key: float(value) for key, value in operational_config.items()},
    }
    base_thresholds = dict(protocol["shared_absolute_gate"]["base"])
    stress_thresholds = dict(protocol["shared_absolute_gate"]["stress_16bp"])
    if is_audit:
        audit_gate = protocol["audit_gate"]
        base_thresholds.update(
            {
                "net_sharpe_at_least": audit_gate["base_sharpe_at_least"],
                "daily_max_drawdown_at_least": audit_gate[
                    "daily_max_drawdown_at_least"
                ],
                "positive_complete_year_ratio_at_least": audit_gate[
                    "positive_complete_year_ratio_at_least"
                ],
            }
        )
        stress_thresholds.update(
            {
                "net_sharpe_at_least": audit_gate["stress_sharpe_at_least"],
                "daily_max_drawdown_at_least": audit_gate[
                    "daily_max_drawdown_at_least"
                ],
                "positive_complete_year_ratio_at_least": audit_gate[
                    "positive_complete_year_ratio_at_least"
                ],
            }
        )
    base_absolute = _v9_absolute_gate(
        roles["candidate"],
        roles["cash"],
        thresholds=base_thresholds,
        operational=operational,
    )
    stress_absolute = _v9_absolute_gate(
        roles["candidate_stress"],
        roles["cash_stress"],
        thresholds=stress_thresholds,
        operational=operational,
    )
    base_relative = _v9_relative_gate(
        roles["candidate"], roles["static"], protocol["relative_stability_gate"]
    )
    stress_relative = _v9_relative_gate(
        roles["candidate_stress"],
        roles["static_stress"],
        protocol["relative_stability_gate"],
    )
    audit_years_passed = (
        not is_audit
        or (
            int(roles["candidate"]["complete_year_count"])
            == int(protocol["audit_gate"]["complete_year_count"])
            and int(roles["candidate"]["positive_complete_year_count"])
            >= int(protocol["audit_gate"]["minimum_positive_complete_year_count"])
            and int(roles["candidate_stress"]["complete_year_count"])
            == int(protocol["audit_gate"]["complete_year_count"])
            and int(roles["candidate_stress"]["positive_complete_year_count"])
            >= int(protocol["audit_gate"]["minimum_positive_complete_year_count"])
        )
    )
    passed = audit_years_passed and all(
        value["passed"]
        for value in (base_absolute, stress_absolute, base_relative, stress_relative)
    )
    return {
        "start_date": start,
        "end_date": end,
        "roles": roles,
        "operational": operational,
        "base_absolute_gate": base_absolute,
        "stress_absolute_gate": stress_absolute,
        "base_relative_stability_gate": base_relative,
        "stress_relative_stability_gate": stress_relative,
        "audit_complete_year_count_gate_passed": audit_years_passed,
        "passed": passed,
    }


def _v9_run_roles(
    stage: MultiAssetStage, *, start: str, end: str
) -> tuple[dict[str, Mapping[str, Any]], int]:
    sessions = tuple(pd.to_datetime(stage.calendar["trade_date"]).dt.normalize())
    candidate_targets = _v9_filter_targets(
        build_monthly_targets(stage.assets, sessions, PRIMARY_ID), start=start, end=end
    )
    static_targets = _v9_filter_targets(
        build_monthly_targets(stage.assets, sessions, CONTROL_ID), start=start, end=end
    )
    cash_targets = _v9_filter_targets(
        build_monthly_targets(stage.assets, sessions, CASH_ONLY_ID), start=start, end=end
    )
    prefix_count = _v9_prefix_replay_count(stage, candidate_targets, sessions)
    specs = {
        "candidate": (candidate_targets, 8.0),
        "candidate_stress": (candidate_targets, 16.0),
        "static": (static_targets, 8.0),
        "static_stress": (static_targets, 16.0),
        "cash": (cash_targets, 8.0),
        "cash_stress": (cash_targets, 16.0),
    }
    results = {
        role: simulate_targets(
            stage.assets,
            targets,
            sessions,
            SimulationConfig(cost_bps_per_side=cost),
        )
        for role, (targets, cost) in specs.items()
    }
    for first, second in (
        ("candidate", "candidate_stress"),
        ("static", "static_stress"),
        ("cash", "cash_stress"),
    ):
        _v9_assert_frame_exact(
            results[first]["targets"],
            results[second]["targets"],
            role=f"9.0 {first}/{second} targets",
        )
    return results, prefix_count


def _v9_trim_development_result(
    result: Mapping[str, Any],
    *,
    phase_name: str,
    start: str,
    end: str,
    sessions: tuple[pd.Timestamp, ...],
) -> dict[str, pd.DataFrame]:
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp(end)
    baseline = max(value for value in sessions if value < start_date)
    trimmed: dict[str, pd.DataFrame] = {}
    for artifact in EVALUATION_ARTIFACTS:
        frame = result[artifact].copy()
        if artifact in {"daily_nav", "holdings"}:
            dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
            frame = frame.loc[dates.between(baseline, end_date)].copy()
        frame["development_phase"] = phase_name
        trimmed[artifact] = frame.reset_index(drop=True)
    return trimmed


def _v9_causal_stage_through(
    stage: MultiAssetStage, *, end: str
) -> MultiAssetStage:
    cutoff = pd.Timestamp(end).normalize()

    def through(frame: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
        selected = frame.loc[dates.le(cutoff)].copy().reset_index(drop=True)
        if selected.empty or pd.to_datetime(selected["trade_date"]).max().normalize() > cutoff:
            raise ValueError(f"9.0 causal stage could not enforce cutoff {end}")
        return selected

    assets = {code: through(frame) for code, frame in stage.assets.items()}
    calendar = through(stage.calendar)
    manifest = dict(stage.manifest)
    manifest["price_end_date"] = cutoff.date().isoformat()
    return MultiAssetStage(
        path=stage.path,
        manifest=manifest,
        calendar=calendar,
        assets=assets,
    )


def _v9_run_development_roles(
    stage: MultiAssetStage,
) -> tuple[
    dict[str, Mapping[str, Any]],
    dict[str, dict[str, Mapping[str, Any]]],
    int,
]:
    phase_specs = {
        "D1": ("2015-03-02", "2019-12-31"),
        "D2": ("2020-01-02", "2022-12-30"),
    }
    phases: dict[str, dict[str, Mapping[str, Any]]] = {}
    persisted_parts = {
        role: {artifact: [] for artifact in EVALUATION_ARTIFACTS}
        for role in EVALUATION_ROLES
    }
    prefix_count = 0
    for phase_name, (start, end) in phase_specs.items():
        phase_stage = _v9_causal_stage_through(stage, end=end)
        sessions = tuple(
            pd.to_datetime(phase_stage.calendar["trade_date"]).dt.normalize()
        )
        all_targets = {
            "candidate": build_monthly_targets(
                phase_stage.assets, sessions, PRIMARY_ID
            ),
            "static": build_monthly_targets(
                phase_stage.assets, sessions, CONTROL_ID
            ),
            "cash": build_monthly_targets(
                phase_stage.assets, sessions, CASH_ONLY_ID
            ),
        }
        candidate_targets = _v9_filter_targets(
            all_targets["candidate"], start=start, end=end
        )
        static_targets = _v9_filter_targets(all_targets["static"], start=start, end=end)
        cash_targets = _v9_filter_targets(all_targets["cash"], start=start, end=end)
        prefix_count += _v9_prefix_replay_count(
            phase_stage, candidate_targets, sessions
        )
        role_specs = {
            "candidate": (candidate_targets, 8.0),
            "candidate_stress": (candidate_targets, 16.0),
            "static": (static_targets, 8.0),
            "static_stress": (static_targets, 16.0),
            "cash": (cash_targets, 8.0),
            "cash_stress": (cash_targets, 16.0),
        }
        results = {
            role: simulate_targets(
                phase_stage.assets,
                targets,
                sessions,
                SimulationConfig(cost_bps_per_side=cost),
            )
            for role, (targets, cost) in role_specs.items()
        }
        for first, second in (
            ("candidate", "candidate_stress"),
            ("static", "static_stress"),
            ("cash", "cash_stress"),
        ):
            _v9_assert_frame_exact(
                results[first]["targets"],
                results[second]["targets"],
                role=f"9.0 {phase_name} {first}/{second} targets",
            )
        phases[phase_name] = results
        for role, result in results.items():
            trimmed = _v9_trim_development_result(
                result,
                phase_name=phase_name,
                start=start,
                end=end,
                sessions=sessions,
            )
            for artifact, frame in trimmed.items():
                persisted_parts[role][artifact].append(frame)
    persisted = {
        role: {
            artifact: pd.concat(parts, ignore_index=True)
            for artifact, parts in artifacts.items()
        }
        for role, artifacts in persisted_parts.items()
    }
    return persisted, phases, prefix_count


def _v9_development_phases(
    phase_results: Mapping[str, Mapping[str, Mapping[str, Any]]],
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "D1": _v9_phase_bundle(
            phase_results["D1"],
            start="2015-03-02",
            end="2019-12-31",
            protocol=protocol,
        ),
        "D2": _v9_phase_bundle(
            phase_results["D2"],
            start="2020-01-02",
            end="2022-12-30",
            protocol=protocol,
        ),
    }


def _v9_verify_scout_development(phases: Mapping[str, Mapping[str, Any]]) -> None:
    scout = _read_json(SCOUT_PATH)
    expected = scout["prototypes"]["causal_volatility_balanced_budget_v0"][
        "development"
    ]
    actual: dict[str, Any] = {}
    for phase_name in ("D1", "D2"):
        phase = phases[phase_name]
        roles = phase["roles"]
        values: dict[str, Any] = {}
        for label, candidate_role, static_role, cash_role, absolute_key, relative_key in (
            (
                "base",
                "candidate",
                "static",
                "cash",
                "base_absolute_gate",
                "base_relative_stability_gate",
            ),
            (
                "stress_16bp",
                "candidate_stress",
                "static_stress",
                "cash_stress",
                "stress_absolute_gate",
                "stress_relative_stability_gate",
            ),
        ):
            candidate = roles[candidate_role]
            static = roles[static_role]
            cash = roles[cash_role]
            values[label] = {
                "cagr": candidate["cagr"],
                "cash_cagr": cash["cagr"],
                "cash_excess_cagr": candidate["cagr"] - cash["cagr"],
                "sharpe": candidate["sharpe"],
                "max_drawdown": candidate["max_drawdown"],
                "positive_complete_year_ratio": candidate[
                    "positive_complete_year_ratio"
                ],
                "annualized_turnover": candidate["annualized_turnover"],
                "requested_notional_fill_ratio": candidate[
                    "requested_notional_fill_ratio"
                ],
                "capacity_limited_requested_notional_ratio": candidate[
                    "capacity_limited_requested_notional_ratio"
                ],
                "nav_reconciliation_error": candidate[
                    "nav_reconciliation_error"
                ],
                "cagr_minus_static": candidate["cagr"] - static["cagr"],
                "sharpe_minus_static": candidate["sharpe"] - static["sharpe"],
                "max_drawdown_minus_static_positive_is_better": candidate[
                    "max_drawdown"
                ]
                - static["max_drawdown"],
                "positive_complete_year_ratio_minus_static": candidate[
                    "positive_complete_year_ratio"
                ]
                - static["positive_complete_year_ratio"],
                "gate_passed": bool(
                    phase[absolute_key]["passed"]
                    and phase[relative_key]["passed"]
                ),
            }
        actual[phase_name] = values
    if actual != expected:
        raise ValueError("9.0 formal development does not exactly replay scout metrics")


def _v9_create_development_binding(
    stage: MultiAssetStage,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
) -> dict[str, Any]:
    path = _binding_path("development")
    if path.exists() or path.is_symlink():
        raise FileExistsError("9.0 development binding is create-only")
    value = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_stage_binding",
        "release": RELEASE,
        "stage": "development",
        "stage_manifest_payload_sha256": stage.manifest["payload_sha256"],
        "stage_manifest_file_sha256": file_sha256(stage.path / "manifest.json"),
        "implementation_closure_payload_sha256": closure_payload,
        "execution_commit": execution_commit,
        "predecessor": {
            "kind": "published_8_1_validation_source",
            "tag_object": PRIOR_TAG_OBJECT,
            "tag_commit": PRIOR_COMMIT,
        },
        "run_nonce": run_nonce,
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    _create_only(path, value)
    return value


def _v9_load_stage_and_binding(
    stage_name: str,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
) -> tuple[MultiAssetStage, dict[str, Any]]:
    stage = (
        load_multi_asset_stage(PRIOR_SOURCE_ROOT, "validation")
        if stage_name == "development"
        else load_multi_asset_stage(SOURCE_ROOT, stage_name)
    )
    binding = _verify_stage_binding(
        stage_name,
        stage,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    return stage, binding


def _v9_evaluate_stage(
    stage_name: str,
    *,
    stage: MultiAssetStage,
    binding: Mapping[str, Any],
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    spec = STAGES[stage_name]
    if stage_name == "development":
        results, phase_results, prefix_count = _v9_run_development_roles(stage)
        phases = _v9_development_phases(phase_results, protocol)
        _v9_verify_scout_development(phases)
    else:
        results, prefix_count = _v9_run_roles(
            stage, start=spec["performance_start"], end=spec["performance_end"]
        )
        phases = {
            "audit": _v9_phase_bundle(
                results,
                start=spec["performance_start"],
                end=spec["performance_end"],
                protocol=protocol,
            )
        }
    metrics = {
        "prefix_replay_signal_count": prefix_count,
        "phases": phases,
    }
    gate = {
        "passed": all(value["passed"] for value in phases.values()),
        "phase_passes": {key: value["passed"] for key, value in phases.items()},
    }
    evaluation = _persist_evaluation(
        stage_name,
        source_manifest_payload=stage.manifest["payload_sha256"],
        results=results,
        metrics=metrics,
        gate=gate,
        closure_payload=closure_payload,
        stage_binding_payload=binding["payload_sha256"],
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    evaluation_file = file_sha256(
        EVALUATION_ROOT / f"stage={stage_name}" / "evaluation.json"
    )
    _v9_replay_evaluation(
        stage_name,
        stage=stage,
        evaluation=evaluation,
        protocol=protocol,
    )
    return _phase_from_evaluation(evaluation, evaluation_file)


def _v9_replay_evaluation(
    stage_name: str,
    *,
    stage: MultiAssetStage,
    evaluation: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> None:
    spec = STAGES[stage_name]
    if stage_name == "development":
        results, phase_results, prefix_count = _v9_run_development_roles(stage)
        phases = _v9_development_phases(phase_results, protocol)
        _v9_verify_scout_development(phases)
    else:
        results, prefix_count = _v9_run_roles(
            stage, start=spec["performance_start"], end=spec["performance_end"]
        )
        phases = {
            "audit": _v9_phase_bundle(
                results,
                start=spec["performance_start"],
                end=spec["performance_end"],
                protocol=protocol,
            )
        }
    expected_metrics = {"prefix_replay_signal_count": prefix_count, "phases": phases}
    expected_gate = {
        "passed": all(value["passed"] for value in phases.values()),
        "phase_passes": {key: value["passed"] for key, value in phases.items()},
    }
    if evaluation.get("metrics") != expected_metrics or evaluation.get("gate") != expected_gate:
        raise ValueError(f"9.0 {stage_name} evaluation metrics do not replay")
    directory = EVALUATION_ROOT / f"stage={stage_name}"
    for role in EVALUATION_ROLES:
        for artifact in EVALUATION_ARTIFACTS:
            persisted = pd.read_parquet(directory / f"{role}-{artifact}.parquet")
            _v9_assert_frame_exact(
                persisted,
                results[role][artifact],
                role=f"9.0 {stage_name} {role} {artifact}",
            )


def _v9_reference_from_runtime(
    stage_name: str,
    *,
    closure_payload: str,
    execution_commit: str,
    run_nonce: str,
    predecessor: Mapping[str, Any],
    protocol: Mapping[str, Any],
    verify_data: bool,
) -> dict[str, Any]:
    stage, binding = _v9_load_stage_and_binding(
        stage_name,
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    evaluation, evaluation_file = _load_evaluation(
        stage_name,
        source_manifest_payload=stage.manifest["payload_sha256"],
        stage_binding_payload=binding["payload_sha256"],
        closure_payload=closure_payload,
        execution_commit=execution_commit,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    if verify_data:
        _v9_replay_evaluation(
            stage_name, stage=stage, evaluation=evaluation, protocol=protocol
        )
    return _phase_from_evaluation(evaluation, evaluation_file)


def _v9_verify_development_reference(
    reference: Any,
    *,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    execution_commit: str,
    run_nonce: str,
    verify_data: bool,
) -> None:
    predecessor = {
        "kind": "published_8_1_validation_source",
        "tag_object": PRIOR_TAG_OBJECT,
        "tag_commit": PRIOR_COMMIT,
    }
    if (
        not isinstance(reference, Mapping)
        or set(reference) != _PHASE_FIELDS
        or reference.get("source_manifest_payload_sha256")
        != PRIOR_VALIDATION_MANIFEST_PAYLOAD
        or not _is_sha256(reference.get("stage_binding_payload_sha256"))
        or not _is_sha256(reference.get("evaluation_payload_sha256"))
        or not _is_sha256(reference.get("evaluation_file_sha256"))
        or not isinstance(reference.get("metrics"), Mapping)
        or set(reference["metrics"].get("phases", {})) != {"D1", "D2"}
        or reference.get("gate", {}).get("phase_passes")
        != {
            "D1": bool(reference["metrics"]["phases"]["D1"].get("passed")),
            "D2": bool(reference["metrics"]["phases"]["D2"].get("passed")),
        }
        or reference.get("gate", {}).get("passed")
        is not all(
            bool(reference["metrics"]["phases"][name].get("passed"))
            for name in ("D1", "D2")
        )
    ):
        raise ValueError("9.0 development reference is incomplete or inconsistent")
    for name, start, end in (
        ("D1", "2015-03-02", "2019-12-31"),
        ("D2", "2020-01-02", "2022-12-30"),
    ):
        stored = reference["metrics"]["phases"][name]
        recomputed = _v9_bundle_from_roles(
            stored.get("roles", {}), start=start, end=end, protocol=protocol
        )
        if stored != recomputed:
            raise ValueError(f"9.0 development {name} gate does not replay")
    if verify_data:
        actual = _v9_reference_from_runtime(
            "development",
            closure_payload=str(closure["payload_sha256"]),
            execution_commit=execution_commit,
            run_nonce=run_nonce,
            predecessor=predecessor,
            protocol=protocol,
            verify_data=True,
        )
        if actual != reference:
            raise ValueError("9.0 development reference differs from runtime")


def _v9_verify_execution_lineage(
    commit: str,
    *,
    evidence_path: Path,
    required_files: tuple[Path, ...],
) -> None:
    if not _is_commit(commit):
        raise ValueError(f"invalid 9.0 execution commit: {commit!r}")
    resolved = _git("rev-parse", "--verify", f"{commit}^{{commit}}").decode(
        "ascii"
    ).strip()
    if resolved != commit or subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode != 0:
        raise ValueError("9.0 execution commit is not an ancestor of HEAD")
    for relative in required_files:
        if _git("show", f"{commit}:{relative.as_posix()}") != (
            ROOT / relative
        ).read_bytes():
            raise ValueError(
                f"9.0 execution commit lacks exact predecessor: {relative}"
            )
    if subprocess.run(
        ["git", "cat-file", "-e", f"{commit}:{evidence_path.as_posix()}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode == 0:
        raise ValueError(f"9.0 evidence predates its execution: {evidence_path}")


def _v9_verify_freeze(
    freeze: Mapping[str, Any],
    *,
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    verify_data: bool,
) -> None:
    commit = str(freeze.get("development_execution_commit") or "")
    run_nonce = str(freeze.get("run_nonce") or "")
    development = freeze.get("development")
    development_source = freeze.get("development_source")
    if (
        set(freeze) != FREEZE_FIELDS
        or freeze.get("payload_sha256") != canonical_payload_sha256(freeze)
        or freeze.get("schema_version") != 1
        or freeze.get("kind") != "factor_lab_multi_asset_winner_freeze"
        or freeze.get("release") != RELEASE
        or freeze.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or freeze.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
        or freeze.get("candidate_registry") != [PRIMARY_ID]
        or development_source
        != {
            "source_manifest_payload_sha256": PRIOR_VALIDATION_MANIFEST_PAYLOAD,
            "tag_object": PRIOR_TAG_OBJECT,
            "tag_commit": PRIOR_COMMIT,
        }
        or freeze.get("runner_up_fallback") is not False
        or freeze.get("audit_market_outcomes_opened") is not False
        or freeze.get("claim_contract") != protocol.get("claim_contract")
        or not _is_commit(commit)
        or not re.fullmatch(r"[0-9a-f]{32}", run_nonce)
    ):
        raise ValueError("9.0 winner freeze contract differs")
    _v9_verify_execution_lineage(
        commit,
        evidence_path=WINNER_FREEZE_PATH,
        required_files=(
            CLOSURE_PATH,
            PROTOCOL_PATH,
            SCOUT_PATH,
            ASSET_SELECTION_PATH,
            INHERITED_PROTOCOL_PATH,
            ECONOMIC_GATE_PROTOCOL_PATH,
            PRIOR_PROTOCOL_PATH,
            PRIOR_CLOSURE_PATH,
            PRIOR_RECLASSIFICATION_PATH,
            PRIOR_FREEZE_PATH,
            PRIOR_RESULT_PATH,
        ),
    )
    _v9_verify_development_reference(
        development,
        closure=closure,
        protocol=protocol,
        execution_commit=commit,
        run_nonce=run_nonce,
        verify_data=verify_data,
    )
    passed = bool(development["gate"]["passed"])
    expected_status = (
        "selected_policy_frozen"
        if passed
        else "selected_null_frozen_development_failed"
    )
    expected_selected = PRIMARY_ID if passed else None
    if (
        freeze.get("status") != expected_status
        or freeze.get("selected_candidate_id") != expected_selected
    ):
        raise ValueError("9.0 winner freeze selection differs from development gate")


def run_development() -> int:
    head = _require_clean_main()
    closure, protocol = _v9_verify_closure()
    if any((ROOT / path).exists() for path in (WINNER_FREEZE_PATH, AUDIT_PATH, RESULT_PATH)):
        raise RuntimeError("9.0 development requires an empty evidence root")
    _assert_runtime_layout(set())
    _assert_evidence_layout(set())
    _require_head_pushed_and_ci_success(head)
    archived = _verify_prior_8_1_archive(verify_data=True, verify_runtime=True)
    stage = load_multi_asset_stage(PRIOR_SOURCE_ROOT, "validation")
    if (
        stage.manifest.get("price_end_date") != "2022-12-30"
        or archived.get("validation_manifest_payload_sha256")
        != stage.manifest.get("payload_sha256")
    ):
        raise ValueError("9.0 development source is not the retained 8.1 validation stage")
    run_nonce = uuid.uuid4().hex
    binding = _v9_create_development_binding(
        stage,
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=head,
        run_nonce=run_nonce,
    )
    predecessor = binding["predecessor"]
    development = _v9_evaluate_stage(
        "development",
        stage=stage,
        binding=binding,
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=head,
        run_nonce=run_nonce,
        predecessor=predecessor,
        protocol=protocol,
    )
    _assert_runtime_layout({"development"})
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during 9.0 development")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during 9.0 development")
    _require_head_pushed_and_ci_success(head)
    passed = bool(development["gate"]["passed"])
    freeze: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_winner_freeze",
        "release": RELEASE,
        "status": (
            "selected_policy_frozen"
            if passed
            else "selected_null_frozen_development_failed"
        ),
        "protocol_payload_sha256": protocol["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "development_execution_commit": head,
        "run_nonce": run_nonce,
        "candidate_registry": [PRIMARY_ID],
        "selected_candidate_id": PRIMARY_ID if passed else None,
        "development_source": {
            "source_manifest_payload_sha256": PRIOR_VALIDATION_MANIFEST_PAYLOAD,
            "tag_object": PRIOR_TAG_OBJECT,
            "tag_commit": PRIOR_COMMIT,
        },
        "development": development,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = canonical_payload_sha256(freeze)
    _create_only(WINNER_FREEZE_PATH, freeze)
    _assert_evidence_layout({WINNER_FREEZE_PATH.name})
    print(
        f"development selected={freeze['selected_candidate_id']} "
        f"payload={freeze['payload_sha256']}",
        flush=True,
    )
    return 0


def _v9_verify_audit(
    audit: Mapping[str, Any],
    *,
    freeze: Mapping[str, Any],
    closure: Mapping[str, Any],
    protocol: Mapping[str, Any],
    verify_data: bool,
) -> None:
    commit = str(audit.get("audit_execution_commit") or "")
    nonce = str(audit.get("run_nonce") or "")
    reference = audit.get("audit")
    source_prefix = audit.get("pre_2023_source_prefix")
    prefix_hashes = (
        source_prefix.get("prefix_content_sha256")
        if isinstance(source_prefix, Mapping)
        else None
    )
    predecessor = {"kind": "winner_freeze", "payload_sha256": freeze["payload_sha256"]}
    if (
        set(audit) != AUDIT_FIELDS
        or audit.get("payload_sha256") != canonical_payload_sha256(audit)
        or audit.get("schema_version") != 1
        or audit.get("kind") != "factor_lab_multi_asset_historical_audit"
        or audit.get("release") != RELEASE
        or audit.get("selected_candidate_id") != PRIMARY_ID
        or audit.get("winner_freeze_payload_sha256") != freeze.get("payload_sha256")
        or audit.get("protocol_payload_sha256") != protocol.get("payload_sha256")
        or audit.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
        or audit.get("runner_up_fallback") is not False
        or audit.get("claim_contract") != protocol.get("claim_contract")
        or not _is_commit(commit)
        or not re.fullmatch(r"[0-9a-f]{32}", nonce)
        or nonce == freeze.get("run_nonce")
        or not isinstance(reference, Mapping)
        or set(reference) != _PHASE_FIELDS
        or set(reference.get("metrics", {}).get("phases", {})) != {"audit"}
        or not isinstance(source_prefix, Mapping)
        or set(source_prefix)
        != {
            "cutoff",
            "retained_source_manifest_payload_sha256",
            "audit_source_manifest_payload_sha256",
            "prefix_content_sha256",
            "payload_sha256",
        }
        or source_prefix.get("payload_sha256")
        != canonical_payload_sha256(source_prefix)
        or source_prefix.get("cutoff") != "2022-12-30"
        or source_prefix.get("retained_source_manifest_payload_sha256")
        != PRIOR_VALIDATION_MANIFEST_PAYLOAD
        or not _is_sha256(source_prefix.get("audit_source_manifest_payload_sha256"))
        or reference.get("source_manifest_payload_sha256")
        != source_prefix.get("audit_source_manifest_payload_sha256")
        or not isinstance(prefix_hashes, Mapping)
        or set(prefix_hashes) != {"calendar", *ALL_CODES}
        or any(not _is_sha256(value) for value in prefix_hashes.values())
    ):
        raise ValueError("9.0 historical audit contract differs")
    _v9_verify_execution_lineage(
        commit,
        evidence_path=AUDIT_PATH,
        required_files=(
            CLOSURE_PATH,
            PROTOCOL_PATH,
            SCOUT_PATH,
            ASSET_SELECTION_PATH,
            INHERITED_PROTOCOL_PATH,
            ECONOMIC_GATE_PROTOCOL_PATH,
            PRIOR_PROTOCOL_PATH,
            PRIOR_CLOSURE_PATH,
            PRIOR_RECLASSIFICATION_PATH,
            PRIOR_FREEZE_PATH,
            PRIOR_RESULT_PATH,
            WINNER_FREEZE_PATH,
        ),
    )
    stored_phase = reference["metrics"]["phases"]["audit"]
    recomputed_phase = _v9_bundle_from_roles(
        stored_phase.get("roles", {}),
        start="2023-01-03",
        end="2026-08-28",
        protocol=protocol,
    )
    if stored_phase != recomputed_phase:
        raise ValueError("9.0 historical audit gate does not replay")
    stored_passed = bool(stored_phase.get("passed"))
    if reference.get("gate") != {
        "passed": stored_passed,
        "phase_passes": {"audit": stored_passed},
    }:
        raise ValueError("9.0 historical audit envelope gate differs")
    passed = stored_passed
    if audit.get("status") != (
        "historical_audit_passed" if passed else "historical_audit_failed"
    ):
        raise ValueError("9.0 historical audit status differs from gate")
    if verify_data:
        audit_stage = load_multi_asset_stage(SOURCE_ROOT, "audit")
        if _v9_verify_audit_history_prefix(audit_stage) != source_prefix:
            raise ValueError("9.0 audit pre-2023 source prefix differs")
        actual = _v9_reference_from_runtime(
            "audit",
            closure_payload=str(closure["payload_sha256"]),
            execution_commit=commit,
            run_nonce=nonce,
            predecessor=predecessor,
            protocol=protocol,
            verify_data=True,
        )
        if actual != reference:
            raise ValueError("9.0 audit reference differs from runtime")


def _v9_verify_audit_history_prefix(audit_stage: MultiAssetStage) -> dict[str, Any]:
    """Reject provider history rewrites before any 2023+ target is generated."""

    prior = load_multi_asset_stage(PRIOR_SOURCE_ROOT, "validation")
    cutoff = pd.Timestamp("2022-12-30")

    def prefix(frame: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
        return frame.loc[dates.le(cutoff)].reset_index(drop=True)

    prior_calendar = prefix(prior.calendar)
    audit_calendar = prefix(audit_stage.calendar)
    _v9_assert_frame_exact(
        audit_calendar, prior_calendar, role="9.0 audit calendar <=2022 prefix"
    )
    hashes: dict[str, str] = {
        "calendar": _frame_content_sha256(audit_calendar)
    }
    if hashes["calendar"] != _frame_content_sha256(prior_calendar):
        raise ValueError("9.0 audit calendar prefix content hash differs")
    if set(audit_stage.assets) != set(prior.assets):
        raise ValueError("9.0 audit asset set differs from retained 8.1 source")
    for code in sorted(prior.assets):
        expected = prefix(prior.assets[code])
        actual = prefix(audit_stage.assets[code])
        _v9_assert_frame_exact(
            actual, expected, role=f"9.0 audit {code} <=2022 prefix"
        )
        actual_hash = _frame_content_sha256(actual)
        if actual_hash != _frame_content_sha256(expected):
            raise ValueError(f"9.0 audit {code} prefix content hash differs")
        hashes[code] = actual_hash
    identity = {
        "cutoff": cutoff.date().isoformat(),
        "retained_source_manifest_payload_sha256": prior.manifest["payload_sha256"],
        "audit_source_manifest_payload_sha256": audit_stage.manifest["payload_sha256"],
        "prefix_content_sha256": hashes,
    }
    identity["payload_sha256"] = canonical_payload_sha256(identity)
    return identity


def run_audit() -> int:
    head = _require_clean_main()
    closure, protocol = _v9_verify_closure()
    freeze = _read_json(WINNER_FREEZE_PATH)
    _require_committed(WINNER_FREEZE_PATH)
    _v9_verify_freeze(
        freeze, closure=closure, protocol=protocol, verify_data=True
    )
    if freeze.get("selected_candidate_id") != PRIMARY_ID:
        raise RuntimeError("9.0 audit requires a committed non-null winner freeze")
    if head == freeze.get("development_execution_commit"):
        raise RuntimeError("9.0 audit requires a later commit containing the freeze")
    if any((ROOT / path).exists() for path in (AUDIT_PATH, RESULT_PATH)):
        raise RuntimeError("9.0 audit/result evidence must be absent before audit")
    _assert_runtime_layout({"development"})
    _assert_evidence_layout({WINNER_FREEZE_PATH.name})
    _require_head_pushed_and_ci_success(head)
    run_nonce = uuid.uuid4().hex
    if run_nonce == freeze.get("run_nonce"):
        raise RuntimeError("9.0 audit nonce must differ from development nonce")
    predecessor = {"kind": "winner_freeze", "payload_sha256": freeze["payload_sha256"]}
    stage, binding = _stage(
        "audit",
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=head,
        run_nonce=run_nonce,
        predecessor=predecessor,
    )
    prefix_identity = _v9_verify_audit_history_prefix(stage)
    reference = _v9_evaluate_stage(
        "audit",
        stage=stage,
        binding=binding,
        closure_payload=str(closure["payload_sha256"]),
        execution_commit=head,
        run_nonce=run_nonce,
        predecessor=predecessor,
        protocol=protocol,
    )
    _assert_runtime_layout({"development", "audit"})
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during 9.0 audit")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during 9.0 audit")
    _require_head_pushed_and_ci_success(head)
    passed = bool(reference["gate"]["passed"])
    value: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_historical_audit",
        "release": RELEASE,
        "status": "historical_audit_passed" if passed else "historical_audit_failed",
        "selected_candidate_id": PRIMARY_ID,
        "winner_freeze_payload_sha256": freeze["payload_sha256"],
        "protocol_payload_sha256": protocol["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "audit_execution_commit": head,
        "run_nonce": run_nonce,
        "audit": reference,
        "pre_2023_source_prefix": prefix_identity,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    _create_only(AUDIT_PATH, value)
    _assert_evidence_layout({WINNER_FREEZE_PATH.name, AUDIT_PATH.name})
    print(f"audit status={value['status']} payload={value['payload_sha256']}", flush=True)
    return 0


def run_finalize() -> int:
    head = _require_clean_main()
    _require_head_pushed_and_ci_success(head)
    closure, protocol = _v9_verify_closure()
    if (ROOT / RESULT_PATH).exists():
        raise FileExistsError("9.0 terminal result is create-only")
    freeze = _read_json(WINNER_FREEZE_PATH)
    freeze_bytes = _require_committed(WINNER_FREEZE_PATH)
    _v9_verify_freeze(
        freeze, closure=closure, protocol=protocol, verify_data=True
    )
    selected = freeze.get("selected_candidate_id")
    audit: dict[str, Any] | None = None
    audit_bytes: bytes | None = None
    if selected is None:
        if (ROOT / AUDIT_PATH).exists():
            raise RuntimeError("null 9.0 selection cannot have audit evidence")
        status = "selection_falsified_no_candidate"
        audit_status = "not_opened"
        _assert_runtime_layout({"development"})
    else:
        audit = _read_json(AUDIT_PATH)
        audit_bytes = _require_committed(AUDIT_PATH)
        _v9_verify_audit(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=True,
        )
        audit_status = str(audit["status"])
        status = (
            "historical_adaptive_beta_diagnostic_passed_fresh_evidence_required"
            if audit_status == "historical_audit_passed"
            else "historical_adaptive_beta_diagnostic_failed"
        )
        _assert_runtime_layout({"development", "audit"})
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_terminal_result",
        "release": RELEASE,
        "status": status,
        "selected_candidate_id": selected,
        "audit_status": audit_status,
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
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "claim_contract": protocol["claim_contract"],
    }
    result["payload_sha256"] = canonical_payload_sha256(result)
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during 9.0 finalize")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during 9.0 finalize")
    _v9_verify_closure()
    _require_head_pushed_and_ci_success(head)
    if _git("rev-parse", "HEAD").decode("ascii").strip() != head:
        raise RuntimeError("HEAD changed during final 9.0 result publication")
    if _git("status", "--porcelain").strip():
        raise RuntimeError("tracked worktree changed during final 9.0 result publication")
    if _require_committed(WINNER_FREEZE_PATH) != freeze_bytes:
        raise RuntimeError("winner freeze changed during 9.0 finalize")
    if audit_bytes is not None and _require_committed(AUDIT_PATH) != audit_bytes:
        raise RuntimeError("historical audit changed during 9.0 finalize")
    _create_only(RESULT_PATH, result)
    allowed = {WINNER_FREEZE_PATH.name, RESULT_PATH.name}
    if audit is not None:
        allowed.add(AUDIT_PATH.name)
    _assert_evidence_layout(allowed)
    print(f"terminal result status={status} payload={result['payload_sha256']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
