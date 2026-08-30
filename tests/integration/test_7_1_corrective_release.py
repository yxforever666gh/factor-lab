from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts" / "run-multi-asset-evidence.py"
BUILDER_PATH = ROOT / "scripts" / "build-7.1-preselection-closure.py"


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_7_1_runner_and_builder_use_only_the_corrective_namespace() -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_runner_contract")
    builder = _load(BUILDER_PATH, "factor_lab_v71_builder_contract")

    assert runner.RELEASE == "7.1"
    assert runner.WORK_ROOT == ROOT / "runtime/data/multi-asset-7.1"
    assert runner.EVIDENCE_ROOT == Path("protocols/evidence/7.1")
    assert runner.CLOSURE_PATH == Path("protocols/7.1-release.json")
    assert runner.STAGES == {
        "train": {
            "source_start": "2014-01-15",
            "source_end": "2019-12-31",
            "performance_start": "2015-03-02",
            "performance_end": "2019-12-31",
        }
    }
    assert set(builder.IMPLEMENTATION_PATHS) == runner.EXPECTED_IMPLEMENTATION_PATHS
    assert builder.WORK_ROOT == Path("runtime/data/multi-asset-7.1")
    assert builder.EVIDENCE_ROOT == Path("protocols/evidence/7.1")
    assert "corrective_amendment" in runner._CLOSURE_FIELDS
    assert builder.WORK_ROOT in builder.FORBIDDEN_BEFORE_CLOSURE
    assert builder.CLOSURE_PATH in builder.FORBIDDEN_BEFORE_CLOSURE


def test_7_1_runner_does_not_expose_or_execute_audit() -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_no_audit")

    with pytest.raises(SystemExit) as raised:
        runner.main(["--mode", "audit"])
    assert raised.value.code == 2
    with pytest.raises(RuntimeError, match="train-only"):
        runner.run_audit()


def test_7_1_runtime_rejects_validation_stage_injection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_validation_isolation")
    work = tmp_path / "multi-asset-7.1"
    monkeypatch.setattr(runner, "WORK_ROOT", work)
    monkeypatch.setattr(runner, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(runner, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(runner, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(runner, "PRIOR_WORK_ROOT", tmp_path / "multi-asset-7.0")
    (runner.SOURCE_ROOT / "stage=validation").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="unexpected or renamed"):
        runner._assert_runtime_layout({"train"})


def test_7_1_runtime_rejects_hardlinked_7_0_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_hardlink_isolation")
    work = tmp_path / "multi-asset-7.1"
    prior = tmp_path / "multi-asset-7.0"
    monkeypatch.setattr(runner, "WORK_ROOT", work)
    monkeypatch.setattr(runner, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(runner, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(runner, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(runner, "PRIOR_WORK_ROOT", prior)
    old_file = prior / "sources/stage=train/510300.SH.parquet"
    new_file = work / "sources/stage=train/510300.SH.parquet"
    old_file.parent.mkdir(parents=True)
    new_file.parent.mkdir(parents=True)
    old_file.write_bytes(b"same physical bytes")
    os.link(old_file, new_file)

    with pytest.raises(RuntimeError, match="reuses a 7.0 physical file"):
        runner._assert_runtime_layout({"train"})


def test_7_1_rejects_copied_or_rebound_preexisting_stage_and_evaluation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_copy_isolation")
    work = tmp_path / "multi-asset-7.1"
    monkeypatch.setattr(runner, "WORK_ROOT", work)
    monkeypatch.setattr(runner, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(runner, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(runner, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(runner, "PRIOR_WORK_ROOT", tmp_path / "multi-asset-7.0")
    (runner.SOURCE_ROOT / "stage=train").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="pre-existing train source stage"):
        runner._stage(
            "train",
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "preselection_closure", "payload_sha256": "c" * 64},
        )

    (runner.SOURCE_ROOT / "stage=train").rmdir()
    (runner.EVALUATION_ROOT / "stage=train").mkdir(parents=True)
    fake_stage = runner.MultiAssetStage(
        path=runner.SOURCE_ROOT / "stage=train",
        manifest={"payload_sha256": "m" * 64},
        calendar=None,
        assets={},
    )
    monkeypatch.setattr(
        runner,
        "_stage",
        lambda *args, **kwargs: (fake_stage, {"payload_sha256": "b" * 64}),
    )
    with pytest.raises(RuntimeError, match="pre-existing train evaluation"):
        runner._evaluate_stage(
            "train",
            gate_config={},
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "preselection_closure", "payload_sha256": "c" * 64},
        )


def test_7_1_runtime_and_evidence_roots_have_exact_top_level_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_top_level_layout")
    work = tmp_path / "multi-asset-7.1"
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "WORK_ROOT", work)
    monkeypatch.setattr(runner, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(runner, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(runner, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(runner, "PRIOR_WORK_ROOT", tmp_path / "multi-asset-7.0")
    (work / "stage=validation").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="unexpected entry"):
        runner._assert_runtime_layout({"train"})

    (work / "stage=validation").rmdir()
    evidence = tmp_path / runner.EVIDENCE_ROOT
    evidence.mkdir(parents=True)
    (evidence / "historical-audit.json").write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unexpected 7.1 evidence"):
        runner._assert_evidence_layout(set())


def test_7_1_builder_emits_the_runner_exact_closure_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _load(RUNNER_PATH, "factor_lab_v71_schema_runner")
    builder = _load(BUILDER_PATH, "factor_lab_v71_schema_builder")
    commit = "d" * 40
    prior = {
        "release": "7.0",
        "tag": "7.0",
        "annotated_tag_object": builder.PRIOR_TAG_OBJECT,
        "peeled_commit": builder.PRIOR_COMMIT,
        "preselection_closure": {
            "path": builder.PRIOR_CLOSURE_PATH.as_posix(),
            "file_sha256": "a" * 64,
            "payload_sha256": builder.PRIOR_CLOSURE_PAYLOAD,
        },
        "execution_failure": {
            "path": builder.PRIOR_FAILURE_PATH.as_posix(),
            "file_sha256": "b" * 64,
            "payload_sha256": builder.PRIOR_FAILURE_PAYLOAD,
            "status": "selection_inconclusive_software_failure",
            "classification": "target_order_replay_false_negative",
        },
    }

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("branch", "--show-current"):
            return b"main\n"
        if args == ("status", "--porcelain"):
            return b""
        if args == ("rev-parse", "HEAD"):
            return commit.encode("ascii") + b"\n"
        if args == ("rev-parse", "HEAD^{tree}"):
            return b"tree\n"
        if len(args) == 2 and args[0] == "show" and args[1].startswith(commit + ":"):
            relative = args[1].split(":", 1)[1]
            return (ROOT / relative).read_bytes()
        raise AssertionError(args)

    helpers = SimpleNamespace(
        EXPECTED_IMPLEMENTATION_PATHS=runner.EXPECTED_IMPLEMENTATION_PATHS,
        _require_source_imports=lambda: None,
        _require_head_pushed_and_ci_success=lambda _head: None,
        _runtime_identity=lambda: {"runtime": "exact"},
        _verify_corrective_amendment=lambda _value: None,
        _verify_disclosed_outcome_boundary=lambda _value: None,
    )
    captured: dict = {}
    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(builder, "_runner_helpers", lambda: helpers)
    monkeypatch.setattr(builder, "_verify_prior_release", lambda: prior)
    monkeypatch.setattr(builder, "FORBIDDEN_BEFORE_CLOSURE", ())
    monkeypatch.setattr(builder, "CLOSURE_PATH", Path("protocols/test-7.1-release.json"))
    monkeypatch.setattr(builder, "_create_only", lambda _path, payload: captured.update(payload))

    assert builder.main() == 0
    assert set(captured) == runner._CLOSURE_FIELDS
    assert captured["prior_release"] == prior
    assert captured["release"] == "7.1"
    assert captured["prior_train_returns_opened"] is True
    assert captured["corrective_train_returns_opened"] is False
    assert captured["selected_candidate_id"] is None
    assert captured["audit_status"] == "not_opened"


@pytest.mark.parametrize(
    "remote",
    [
        "",
        "0" * 40 + "\trefs/tags/7.0\n" + "1" * 40 + "\trefs/tags/7.0^{}\n",
        "25bbc306e8842feab923380416f8329e0dd81100\trefs/tags/7.0\n",
    ],
)
def test_7_1_builder_rejects_missing_or_mismatched_remote_7_0_tag(
    remote: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load(BUILDER_PATH, "factor_lab_v71_remote_tag_gate")

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("cat-file", "-t", "refs/tags/7.0"):
            return b"tag\n"
        if args == ("rev-parse", "refs/tags/7.0"):
            return builder.PRIOR_TAG_OBJECT.encode("ascii") + b"\n"
        if args == ("rev-parse", "refs/tags/7.0^{}"):
            return builder.PRIOR_COMMIT.encode("ascii") + b"\n"
        if args == ("merge-base", "--is-ancestor", builder.PRIOR_COMMIT, "HEAD"):
            return b""
        if args[:4] == ("ls-remote", "--exit-code", "origin", "refs/tags/7.0"):
            return remote.encode("ascii")
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    with pytest.raises(ValueError, match="remote 7.0 annotated tag"):
        builder._verify_prior_release()
