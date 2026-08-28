from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
from pathlib import Path
import subprocess
import sys
import sysconfig

import pytest

from factor_lab import prospective_release_runner as release_runner


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _release_sources() -> dict[str, bytes]:
    runner_path = Path(release_runner.__file__).resolve()
    return {
        "configs/data.json": b"{}\n",
        "src/factor_lab/__init__.py": b"",
        "src/factor_lab/data/__init__.py": b"",
        "src/factor_lab/prospective_release_runner.py": runner_path.read_bytes(),
        "src/factor_lab/data/prospective.py": b'''\
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

CANONICAL_CALENDAR_ANCHOR = "2026-08-21"
CANONICAL_CALENDAR_COUNT = 1
CANONICAL_CALENDAR_SHA256 = "a" * 64
FROZEN_BRIDGE_END = datetime(2026, 8, 21)
PROSPECTIVE_RELATIVE_ROOT = Path("runtime/prospective/5.0")

def load_prospective_input_snapshot(path):
    requested = Path(path).name
    return SimpleNamespace(
        snapshot_sha256=requested,
        signal_date="2026-08-24",
        trade_date="2026-08-25",
        manifest={"protocol_release": "5.0"},
        target_adapter={"calendar_next_trade_date": "2026-08-25"},
        calendar_sessions=("2026-08-21", "2026-08-24", "2026-08-25"),
        target_frame=[{"date": "2026-08-24", "ticker": "000001.SZ"}],
        target_rows_sha256="b" * 64,
        input_sources_sha256="c" * 64,
        membership_artifact_sha256="d" * 64,
        inputs_available_at_utc="2026-08-24T07:01:00Z",
        build_completed_at_utc="2026-08-24T07:02:00Z",
    )

def build_prospective_input_snapshot(
        root, signal_date, *, available_at_utc, membership_snapshot_path):
    base = Path(root) / "runtime/prospective/5.0/inputs" / ("e" * 64)
    return SimpleNamespace(
        signal_date=str(signal_date), trade_date="2026-08-25",
        snapshot_sha256="e" * 64, directory=base,
        manifest_path=base / "manifest.json", rows_path=base / "rows.parquet",
        build_receipt_path=base / "build-receipt.json",
        build_completed_at_utc="2026-08-24T07:02:00Z",
        inputs_available_at_utc=str(available_at_utc),
    )
''',
        "src/factor_lab/prospective_targets.py": b'''\
from types import SimpleNamespace

class DeploymentSpec:
    def __init__(self, **values):
        self.values = values
        self.deployment_sha256 = "1" * 64
        for key, value in values.items():
            setattr(self, key, value)
    def to_dict(self):
        return {**self.values, "deployment_sha256": self.deployment_sha256}

class TenSleeveState:
    @classmethod
    def genesis(cls, deployment):
        return SimpleNamespace(last_processed_calendar_index=0)
    @classmethod
    def from_mapping(cls, value):
        return SimpleNamespace(last_processed_calendar_index=value["last_processed_calendar_index"])

class InputSnapshot:
    def __init__(self, **values):
        self.values = values
    def to_dict(self):
        rows = self.values["rows"]
        return {**self.values, "rows": list(rows), "snapshot_sha256": "2" * 64}

class GenerationResult:
    @classmethod
    def from_mapping(cls, value):
        return SimpleNamespace(**dict(value))

class _Generated:
    def __init__(self, deployment):
        self.deployment = deployment
    def to_dict(self):
        return {
            "deployment_sha256": self.deployment.deployment_sha256,
            "due_offset": 1,
            "result_sha256": "3" * 64,
            "signal_date": "2026-08-24",
            "trade_date": "2026-08-25",
            "released_marker": "target",
        }

def generate_fixed_core_targets(*, deployment, input_snapshot, previous_state):
    return _Generated(deployment)
''',
        "src/factor_lab/prospective_execution.py": b'''\
from types import SimpleNamespace

class SleeveAccountState:
    @classmethod
    def genesis(cls, *, deployment_sha256, offset):
        return SimpleNamespace(
            deployment_sha256=deployment_sha256, offset=offset,
            state_sha256="4" * 64,
        )
    @classmethod
    def from_mapping(cls, value):
        return SimpleNamespace(**dict(value))

class _Outcome:
    def __init__(self, generation, snapshot):
        self.generation = generation
        self.snapshot = snapshot
    def to_dict(self):
        return {
            "released_marker": "outcome",
            "generation_result_sha256": self.generation.result_sha256,
            "execution_snapshot_sha256": self.snapshot.snapshot_sha256,
            "outcome_sha256": "5" * 64,
        }

def evaluate_due_sleeve_cycle(*, generation_result, execution_snapshot,
                              previous_account_state):
    return _Outcome(generation_result, execution_snapshot)
''',
        "src/factor_lab/prospective_evaluation.py": b'''\
EVALUATOR_ID = "factor-lab/prospective-evaluation/5.2"
EVALUATION_CONTRACT_SHA256 = "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"

def evaluate_prospective_outcomes(outcomes):
    return {
        "released_marker": "evaluation",
        "outcome_count": len(outcomes),
        "hash_seed_probe": list({
            "seed-00", "seed-01", "seed-02", "seed-03", "seed-04", "seed-05",
            "seed-06", "seed-07", "seed-08", "seed-09", "seed-10", "seed-11",
        }),
        "evaluation_sha256": "7" * 64,
    }
''',
        "src/factor_lab/data/prospective_execution.py": b'''\
from pathlib import Path
from types import SimpleNamespace

def load_prospective_execution_snapshot(path, generation, *, previous_account_state):
    snapshot = SimpleNamespace(snapshot_sha256=Path(path).name)
    return SimpleNamespace(snapshot_sha256=Path(path).name, snapshot=snapshot)

def build_prospective_execution_snapshot(root, generation, *,
        source_data_snapshot_sha256, previous_account_state, available_at_utc):
    base = Path(root) / "runtime/prospective/5.0/executions" / ("6" * 64)
    snapshot = SimpleNamespace(
        holding_start_date="2026-08-25", holding_end_date="2026-09-08",
        observation_available_at_utc=str(available_at_utc),
    )
    return SimpleNamespace(
        snapshot_sha256="6" * 64, execution_source_sha256="7" * 64,
        directory=base, snapshot_path=base / "snapshot.json",
        sources_path=base / "sources.json", snapshot=snapshot,
        source_contract={"previous_account_state_sha256": None},
    )
''',
        "src/factor_lab/data/prospective_membership.py": b'''\
from pathlib import Path
from types import SimpleNamespace

def build_prospective_membership_snapshot(root, membership_month, *,
        available_at_utc, config_path):
    assert Path(config_path).read_bytes() == b"{}\\n"
    base = Path(root) / "runtime/prospective/5.0/membership" / membership_month / ("8" * 64)
    return SimpleNamespace(
        membership_month=membership_month, as_of_date="2026-08-31",
        artifact_sha256="8" * 64, directory=base,
        membership_path=base / "membership.parquet",
        manifest_path=base / "manifest.json",
        source_contract_path=base / "source-contract.json",
        reference_raw_path=base / "bak-basic-raw.json",
        completed_at_utc=str(available_at_utc),
    )
''',
    }


@pytest.fixture()
def published_capsule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[release_runner.ReleaseCapsule, Path, str, str]:
    project = tmp_path / "published-project"
    project.mkdir()
    sources = _release_sources()
    for relative, raw in sources.items():
        path = project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    frozen_distributions = {"pytest": importlib.metadata.version("pytest")}
    monkeypatch.setattr(
        release_runner,
        "_running_distribution_versions",
        lambda: dict(frozen_distributions),
    )
    closure_payload = {
        "schema_version": 1,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_runtime": sys.version,
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "platform_tag": sysconfig.get_platform(),
        "distributions": frozen_distributions,
        "files": [
            {"path": relative, "sha256": hashlib.sha256(raw).hexdigest()}
            for relative, raw in sorted(sources.items())
        ],
    }
    manifest = {
        "release_runner": {
            "runner_id": release_runner.RUNNER_ID,
            "capsule_pattern": (
                "runtime/prospective/5.0/release-runners/"
                "<implementation_commit_oid>"
            ),
            "source_origin": "exact_git_blob_bytes_from_published_annotated_tag_commit",
            "process_isolation": (
                "python_-B_-s_with_python_env_reset_and_capsule_src_first"
            ),
            "operations": list(release_runner.OPERATIONS),
            "timeout_policy": {
                "single_operation_seconds": 120,
                "replay_history_base_seconds": 120,
                "replay_history_per_operation_seconds": 5,
                "replay_history_max_seconds": 3600,
            },
            "audit_missing_capsule_policy": "fail_without_materialization",
            "daily_replay_policy": (
                "validate_structural_bundle_artifact_and_recursive_cas_bindings_"
                "then_replay_uncached_suffix"
            ),
            "full_audit_policy": (
                "bypass_cache_replay_complete_history_and_refresh_current_head_prefix"
            ),
        },
        "runtime_closure": {
            **closure_payload,
            "payload_sha256": hashlib.sha256(_canonical(closure_payload)).hexdigest(),
        },
    }
    manifest_path = project / "protocols/release.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_raw = json.dumps(manifest, indent=2).encode("utf-8") + b"\n"
    manifest_path.write_bytes(manifest_raw)

    _git(project, "init")
    _git(project, "config", "user.name", "Release Test")
    _git(project, "config", "user.email", "release@example.invalid")
    _git(project, "add", ".")
    _git(project, "commit", "-m", "published implementation")
    commit = _git(project, "rev-parse", "HEAD")
    _git(project, "tag", "-a", "5.2", "-m", "release 5.2")
    tag_oid = _git(project, "rev-parse", "refs/tags/5.2")
    manifest_sha = hashlib.sha256(manifest_raw).hexdigest()
    store = tmp_path / "release-runners"

    with pytest.raises(release_runner.ReleaseRunnerError, match="not installed"):
        release_runner.verify_release_capsule(
            project,
            store,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag="5.2",
            implementation_release_tag_object_oid=tag_oid,
            implementation_commit_oid=commit,
        )
    assert not store.exists()
    capsule = release_runner.materialize_release_capsule(
        project,
        store,
        manifest_path="protocols/release.json",
        manifest_sha256=manifest_sha,
        implementation_release_tag="5.2",
        implementation_release_tag_object_oid=tag_oid,
        implementation_commit_oid=commit,
    )
    return capsule, project, commit, manifest_sha


def _target_payload(project: Path) -> dict[str, object]:
    return {
        "project_root": str(project.resolve()),
        "source_data_snapshot_sha256": SHA_A,
        "deployment_bindings": {
            "activation_record_sha256": SHA_B,
            "implementation_upgrade_record_sha256": SHA_C,
            "deployment_protocol_sha256": SHA_D,
        },
        "previous_state": None,
        "admission_deadline_utc": None,
    }


def test_capsule_runs_all_operations_from_published_bytes(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule, project, _commit, _manifest_sha = published_capsule
    shadow = project.parent / "shadow"
    (shadow / "factor_lab").mkdir(parents=True)
    (shadow / "factor_lab/__init__.py").write_text(
        "raise RuntimeError('PYTHONPATH shadow imported')\n", encoding="utf-8"
    )
    monkeypatch.setenv("PYTHONPATH", str(shadow))

    target = release_runner.run_release_operation(
        capsule, "replay_target", _target_payload(project)
    )
    assert target["generation_result"]["released_marker"] == "target"
    assert target["deployment"]["activation_record_sha256"] == SHA_B
    assert (
        target["input_snapshot"]["admission_deadline_utc"]
        == "2026-08-25T01:15:00Z"
    )

    exact_deadline = _target_payload(project)
    exact_deadline["admission_deadline_utc"] = "2026-08-25T01:15:00Z"
    exact_target = release_runner.run_release_operation(
        capsule, "replay_target", exact_deadline
    )
    assert exact_target == target

    bad_deadline = _target_payload(project)
    bad_deadline["admission_deadline_utc"] = "2026-08-25T01:15:01Z"
    with pytest.raises(release_runner.ReleaseRunnerError, match="deadline"):
        release_runner.run_release_operation(
            capsule, "replay_target", bad_deadline
        )

    outcome = release_runner.run_release_operation(
        capsule,
        "replay_outcome",
        {
            "project_root": str(project.resolve()),
            "generation_result": target["generation_result"],
            "previous_account_state": None,
            "execution_snapshot_sha256": SHA_E,
        },
    )
    assert outcome["cycle_outcome"]["released_marker"] == "outcome"
    assert outcome["cycle_outcome"]["execution_snapshot_sha256"] == SHA_E

    membership = release_runner.run_release_operation(
        capsule,
        "build_membership",
        {
            "project_root": str(project.resolve()),
            "membership_month": "2026-09",
            "available_at_utc": "2026-08-31T10:00:00Z",
        },
    )
    assert membership["artifact_sha256"] == "8" * 64

    decision = release_runner.run_release_operation(
        capsule,
        "build_input",
        {
            "project_root": str(project.resolve()),
            "signal_date": "2026-08-24",
            "available_at_utc": "2026-08-24T07:01:00Z",
            "membership_snapshot_path": membership["membership_path"],
        },
    )
    assert decision["source_data_snapshot_sha256"] == SHA_E
    assert decision["build_completed_at_utc"] == "2026-08-24T07:02:00Z"

    execution = release_runner.run_release_operation(
        capsule,
        "build_execution",
        {
            "project_root": str(project.resolve()),
            "generation_result": target["generation_result"],
            "source_data_snapshot_sha256": SHA_A,
            "previous_account_state": None,
            "available_at_utc": "2026-09-08T08:00:00Z",
        },
    )
    assert execution["execution_snapshot_sha256"] == "6" * 64
    assert execution["previous_account_state_sha256"] is None
    assert execution["holding_start_date"] == "2026-08-25"
    assert execution["holding_end_date"] == "2026-09-08"

    evaluation = release_runner.run_release_operation(
        capsule,
        "evaluate",
        {
            "outcomes": [outcome],
            "evaluator_id": "factor-lab/prospective-evaluation/5.2",
            "evaluation_contract_sha256": (
                "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
            ),
            "ledger_id": "factor-lab/prospective/5.0",
            "ledger_head_record_sha256": SHA_B,
            "implementation_upgrade_record_sha256": SHA_C,
        },
    )
    released_evaluation = evaluation["evaluation_envelope"]["evaluation"]
    assert released_evaluation["released_marker"] == "evaluation"
    assert released_evaluation["outcome_count"] == 1
    assert released_evaluation["evaluation_sha256"] == "7" * 64
    assert sorted(released_evaluation["hash_seed_probe"]) == [
        f"seed-{index:02d}" for index in range(12)
    ]
    assert evaluation["evaluation_envelope"]["binding"] == {
        "evaluator_id": "factor-lab/prospective-evaluation/5.2",
        "evaluation_contract_sha256": (
            "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
        ),
        "ledger_id": "factor-lab/prospective/5.0",
        "ledger_head_record_sha256": SHA_B,
        "implementation_upgrade_record_sha256": SHA_C,
        "outcome_count": 1,
        "outcomes_sha256": hashlib.sha256(_canonical([outcome])).hexdigest(),
    }

    repeated_evaluation = release_runner.run_release_operation(
        capsule,
        "evaluate",
        {
            "outcomes": [outcome],
            "evaluator_id": "factor-lab/prospective-evaluation/5.2",
            "evaluation_contract_sha256": (
                "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
            ),
            "ledger_id": "factor-lab/prospective/5.0",
            "ledger_head_record_sha256": SHA_B,
            "implementation_upgrade_record_sha256": SHA_C,
        },
    )
    assert repeated_evaluation == evaluation


def test_capsule_tampering_is_rejected(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
) -> None:
    capsule, project, commit, manifest_sha = published_capsule
    target = capsule.root / "src/factor_lab/prospective_targets.py"
    target.write_bytes(target.read_bytes() + b"\n# tampered\n")
    with pytest.raises(release_runner.ReleaseRunnerError, match="artifact differs"):
        release_runner.verify_release_capsule(
            project,
            capsule.store_root,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag=capsule.implementation_release_tag,
            implementation_release_tag_object_oid=(
                capsule.implementation_release_tag_object_oid
            ),
            implementation_commit_oid=commit,
        )


def test_capsule_requires_exact_annotated_tag_object_and_peeled_commit(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
) -> None:
    capsule, project, commit, manifest_sha = published_capsule
    with pytest.raises(release_runner.ReleaseRunnerError, match="tag object oid differs"):
        release_runner.verify_release_capsule(
            project,
            capsule.store_root,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag="5.2",
            implementation_release_tag_object_oid=commit,
            implementation_commit_oid=commit,
        )


def test_capsule_rejects_running_platform_outside_runtime_closure(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule, project, commit, manifest_sha = published_capsule
    monkeypatch.setattr(release_runner.platform, "machine", lambda: "other-machine")
    with pytest.raises(release_runner.ReleaseRunnerError, match="platform_machine"):
        release_runner.verify_release_capsule(
            project,
            capsule.store_root,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag=capsule.implementation_release_tag,
            implementation_release_tag_object_oid=(
                capsule.implementation_release_tag_object_oid
            ),
            implementation_commit_oid=commit,
        )


def test_capsule_rejects_an_extra_running_distribution(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule, project, commit, manifest_sha = published_capsule
    monkeypatch.setattr(
        release_runner,
        "_running_distribution_versions",
        lambda: {
            "pytest": importlib.metadata.version("pytest"),
            "undeclared-extra": "1.0",
        },
    )
    with pytest.raises(release_runner.ReleaseRunnerError, match="distribution set"):
        release_runner.verify_release_capsule(
            project,
            capsule.store_root,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag=capsule.implementation_release_tag,
            implementation_release_tag_object_oid=(
                capsule.implementation_release_tag_object_oid
            ),
            implementation_commit_oid=commit,
        )


def test_replay_history_freezes_multiple_replays_in_one_rpc(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
) -> None:
    capsule, project, _commit, _manifest_sha = published_capsule
    second_payload = _target_payload(project)
    second_payload["admission_deadline_utc"] = "2026-08-25T01:15:00Z"
    payloads = [_target_payload(project), second_payload]
    operations = []
    for payload in payloads:
        operation_id = hashlib.sha256(
            _canonical(
                {"operation": "replay_target", "payload": payload}
            )
        ).hexdigest()
        operations.append(
            {
                "operation_id": operation_id,
                "operation": "replay_target",
                "payload": payload,
            }
        )
    result = release_runner.run_release_operation(
        capsule, "replay_history", {"operations": operations}
    )
    assert [row["operation_id"] for row in result["results"]] == [
        row["operation_id"] for row in operations
    ]
    assert all(
        row["result"]["generation_result"]["released_marker"] == "target"
        for row in result["results"]
    )


def test_rpc_schema_rejects_boolean_as_integer() -> None:
    base = {
        "schema_version": True,
        "runner_id": release_runner.RUNNER_ID,
        "operation": "replay_target",
        "payload": {},
    }
    request = {
        **base,
        "request_sha256": hashlib.sha256(
            release_runner.canonical_json_bytes(base)
        ).hexdigest(),
    }
    with pytest.raises(release_runner.ReleaseRunnerError, match="binding differs"):
        release_runner._validate_request(request)


def test_capsule_rejects_unlisted_shadow_file(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
) -> None:
    capsule, project, commit, manifest_sha = published_capsule
    (capsule.source_root / "pandas.py").write_text(
        "raise RuntimeError('shadow')\n", encoding="utf-8"
    )
    with pytest.raises(release_runner.ReleaseRunnerError, match="file set differs"):
        release_runner.verify_release_capsule(
            project,
            capsule.store_root,
            manifest_path="protocols/release.json",
            manifest_sha256=manifest_sha,
            implementation_release_tag=capsule.implementation_release_tag,
            implementation_release_tag_object_oid=(
                capsule.implementation_release_tag_object_oid
            ),
            implementation_commit_oid=commit,
        )


def test_rpc_rejects_floats_and_reports_timeout(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule, _project, _commit, _manifest_sha = published_capsule
    with pytest.raises(release_runner.ReleaseRunnerError, match="floating-point"):
        release_runner.canonical_json_bytes({"value": 1.0})

    monkeypatch.setattr(release_runner, "verify_release_capsule", lambda *a, **k: capsule)

    def timeout(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd="python", timeout=1)

    monkeypatch.setattr(release_runner.subprocess, "run", timeout)
    with pytest.raises(release_runner.ReleaseRunnerError, match="timed out"):
        release_runner.run_release_operation(capsule, "replay_target", {})


def test_replay_history_scales_timeout_and_resets_python_environment(
    published_capsule: tuple[release_runner.ReleaseCapsule, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule, _project, _commit, _manifest_sha = published_capsule
    monkeypatch.setattr(
        release_runner, "verify_release_capsule", lambda *args, **kwargs: capsule
    )
    monkeypatch.setenv("PYTHONPATH", "untrusted-shadow")
    monkeypatch.setenv("PYTHONHOME", "untrusted-home")
    monkeypatch.setenv("PYTHONWARNINGS", "error")
    monkeypatch.setenv("TUSHARE_TOKEN", "kept-for-production-data-ops")
    captured: dict[str, object] = {}

    def timeout(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured["argv"] = args[0]
        captured["timeout"] = kwargs["timeout"]
        captured["env"] = kwargs["env"]
        raise subprocess.TimeoutExpired(cmd="python", timeout=1)

    monkeypatch.setattr(release_runner.subprocess, "run", timeout)
    operations = [
        {
            "operation_id": hashlib.sha256(str(index).encode()).hexdigest(),
            "operation": "replay_target",
            "payload": {},
        }
        for index in range(250)
    ]
    with pytest.raises(release_runner.ReleaseRunnerError, match="timed out"):
        release_runner.run_release_operation(
            capsule, "replay_history", {"operations": operations}
        )
    assert captured["timeout"] == 1370.0
    assert captured["argv"][1:3] == ["-B", "-s"]  # type: ignore[index]
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["PYTHONHASHSEED"] == "0"
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert "PYTHONPATH" not in environment
    assert "PYTHONHOME" not in environment
    assert "PYTHONWARNINGS" not in environment
    assert environment["TUSHARE_TOKEN"] == "kept-for-production-data-ops"
