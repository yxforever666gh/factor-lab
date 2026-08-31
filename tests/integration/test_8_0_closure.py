from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = ROOT / "scripts" / "build-8.0-preselection-closure.py"


def _load_builder(name: str = "factor_lab_v8_closure_builder") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, BUILDER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _payload(**values: object) -> dict[str, object]:
    output = dict(values)
    output["payload_sha256"] = canonical_payload_sha256(output)
    return output


def test_8_0_builder_uses_fresh_namespace_and_create_only_roots() -> None:
    builder = _load_builder()

    assert builder.RELEASE == "8.0"
    assert builder.PROTOCOL_PATH == Path("protocols/8.0-static-capital-budget.json")
    assert builder.INHERITED_PROTOCOL_PATH == Path("protocols/7.0-multi-asset.json")
    assert builder.CLOSURE_PATH == Path("protocols/8.0-release.json")
    assert builder.WORK_ROOT == Path("runtime/data/multi-asset-8.0")
    assert builder.EVIDENCE_ROOT == Path("protocols/evidence/8.0")
    assert set(builder.FORBIDDEN_BEFORE_CLOSURE) == {
        builder.WORK_ROOT,
        builder.EVIDENCE_ROOT,
        builder.CLOSURE_PATH,
    }
    assert "prior_release" in builder.CLOSURE_FIELDS
    assert "prior_train_exposure" in builder.CLOSURE_FIELDS


def test_8_0_protocol_is_self_hashed_and_binds_prior_release_and_claims() -> None:
    builder = _load_builder("factor_lab_v8_protocol_gate")
    protocol = builder._json(builder.PROTOCOL_PATH)

    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    builder._validate_protocol(protocol)
    assert protocol["inherited_data_execution_contract"]["source"] == {
        "path": builder.INHERITED_PROTOCOL_PATH.as_posix(),
        "file_sha256": builder.INHERITED_PROTOCOL_FILE_SHA256,
        "payload_sha256": builder.INHERITED_PROTOCOL_PAYLOAD,
    }

    wrong_prior = copy.deepcopy(protocol)
    wrong_prior["prior_release"]["tag"] = "7.0"
    wrong_prior["payload_sha256"] = canonical_payload_sha256(wrong_prior)
    with pytest.raises(ValueError, match="unexpected 8.0 protocol"):
        builder._validate_protocol(wrong_prior)

    relaxed_claim = copy.deepcopy(protocol)
    relaxed_claim["claim_contract"]["profit_claim_allowed"] = True
    relaxed_claim["payload_sha256"] = canonical_payload_sha256(relaxed_claim)
    with pytest.raises(ValueError, match="unexpected 8.0 protocol"):
        builder._validate_protocol(relaxed_claim)

    wrong_inherited = copy.deepcopy(protocol)
    wrong_inherited["inherited_data_execution_contract"]["source"][
        "file_sha256"
    ] = "0" * 64
    wrong_inherited["payload_sha256"] = canonical_payload_sha256(wrong_inherited)
    with pytest.raises(ValueError, match="unexpected 8.0 protocol"):
        builder._validate_protocol(wrong_inherited)


def test_8_0_builder_rejects_tampered_inherited_protocol_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v8_inherited_protocol_gate")
    protocol = builder._json(builder.PROTOCOL_PATH)
    inherited = {
        "schema_version": 1,
        "release": "7.0-tampered",
    }
    inherited["payload_sha256"] = canonical_payload_sha256(inherited)
    inherited_path = tmp_path / builder.INHERITED_PROTOCOL_PATH
    inherited_path.parent.mkdir(parents=True, exist_ok=True)
    inherited_path.write_text(
        json.dumps(inherited, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(builder, "ROOT", tmp_path)

    with pytest.raises(
        ValueError, match="inherited 7.0 data/execution protocol bytes differ"
    ):
        builder._validate_protocol(protocol)


def test_8_0_builder_requires_runner_to_have_migrated_exact_namespace(
    tmp_path: Path,
) -> None:
    builder = _load_builder("factor_lab_v8_runner_gate")
    valid = SimpleNamespace(
        RELEASE="8.0",
        PROTOCOL_PATH=builder.PROTOCOL_PATH,
        INHERITED_PROTOCOL_PATH=builder.INHERITED_PROTOCOL_PATH,
        INHERITED_PROTOCOL_FILE_SHA256=builder.INHERITED_PROTOCOL_FILE_SHA256,
        INHERITED_PROTOCOL_PAYLOAD=builder.INHERITED_PROTOCOL_PAYLOAD,
        CLOSURE_PATH=builder.CLOSURE_PATH,
        EVIDENCE_ROOT=builder.EVIDENCE_ROOT,
        WORK_ROOT=tmp_path / builder.WORK_ROOT,
        EXPECTED_IMPLEMENTATION_PATHS={
            builder.RUNNER_PATH.as_posix(),
            "scripts/build-8.0-preselection-closure.py",
        },
    )
    builder.ROOT = tmp_path
    assert set(builder._verify_runner_contract(valid)) == set(
        valid.EXPECTED_IMPLEMENTATION_PATHS
    )

    for attribute, replacement in (
        ("RELEASE", "7.1"),
        ("PROTOCOL_PATH", Path("protocols/7.0-multi-asset.json")),
        ("INHERITED_PROTOCOL_PATH", Path("protocols/tampered.json")),
        ("CLOSURE_PATH", Path("protocols/7.1-release.json")),
        ("EVIDENCE_ROOT", Path("protocols/evidence/7.1")),
        ("WORK_ROOT", tmp_path / "runtime/data/multi-asset-7.1"),
    ):
        changed = SimpleNamespace(**vars(valid))
        setattr(changed, attribute, replacement)
        with pytest.raises(ValueError, match="exact 8.0 namespace"):
            builder._verify_runner_contract(changed)


@pytest.mark.parametrize(
    "remote",
    [
        "",
        "0" * 40 + "\trefs/tags/7.1\n" + "1" * 40 + "\trefs/tags/7.1^{}\n",
        "15ea8e8de95638fdc0786ff0f35177b0ecba878d\trefs/tags/7.1\n",
    ],
)
def test_8_0_builder_rejects_missing_or_mismatched_remote_7_1_tag(
    remote: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v8_remote_tag_gate")

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("cat-file", "-t", "refs/tags/7.1"):
            return b"tag\n"
        if args == ("rev-parse", "refs/tags/7.1"):
            return builder.PRIOR_TAG_OBJECT.encode("ascii") + b"\n"
        if args == ("rev-parse", "refs/tags/7.1^{}"):
            return builder.PRIOR_COMMIT.encode("ascii") + b"\n"
        if args == ("merge-base", "--is-ancestor", builder.PRIOR_COMMIT, "HEAD"):
            return b""
        if args[:4] == ("ls-remote", "--exit-code", "origin", "refs/tags/7.1"):
            return remote.encode("ascii")
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    with pytest.raises(ValueError, match="remote 7.1 annotated tag"):
        builder._verify_prior_release()


@pytest.mark.parametrize("forbidden_role", ["runtime", "evidence", "closure"])
def test_8_0_builder_rejects_any_preexisting_formal_artifact(
    forbidden_role: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder(f"factor_lab_v8_absence_{forbidden_role}")
    monkeypatch.setattr(builder, "ROOT", tmp_path)

    target = {
        "runtime": tmp_path / builder.WORK_ROOT,
        "evidence": tmp_path / builder.EVIDENCE_ROOT,
        "closure": tmp_path / builder.CLOSURE_PATH,
    }[forbidden_role]
    if forbidden_role == "closure":
        target.parent.mkdir(parents=True)
        target.write_text("{}\n", encoding="utf-8")
    else:
        target.mkdir(parents=True)

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("branch", "--show-current"):
            return b"main\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    error = FileExistsError if forbidden_role == "closure" else RuntimeError
    with pytest.raises(error):
        builder.main()


def test_8_0_builder_emits_exact_schema_from_dynamic_runner_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v8_schema")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    monkeypatch.setattr(builder, "SRC", tmp_path / "src")
    monkeypatch.setattr(builder, "FORBIDDEN_BEFORE_CLOSURE", ())
    commit = "d" * 40
    implementation_paths = {
        builder.RUNNER_PATH.as_posix(),
        "scripts/build-8.0-preselection-closure.py",
    }
    for relative in implementation_paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative + "\n", encoding="utf-8")
    for relative in (
        builder.PROTOCOL_PATH,
        builder.INHERITED_PROTOCOL_PATH,
        builder.PRIOR_CLOSURE_PATH,
        builder.PRIOR_FREEZE_PATH,
        builder.PRIOR_RESULT_PATH,
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")

    protocol = _payload(
        schema_version=1,
        release="8.0",
        direction_change=True,
        protocol_id="factor-lab/8.0/strategic-static-capital-budget-beta-v1",
        route="strategic_static_capital_budget_beta",
        prior_release={
            "release": "7.1",
            "tag": builder.PRIOR_TAG,
            "annotated_tag_object": builder.PRIOR_TAG_OBJECT,
            "peeled_commit": builder.PRIOR_COMMIT,
            "preselection_closure": {
                "path": builder.PRIOR_CLOSURE_PATH.as_posix(),
                "file_sha256": builder.PRIOR_CLOSURE_FILE_SHA256,
                "payload_sha256": builder.PRIOR_CLOSURE_PAYLOAD,
            },
            "winner_freeze": {
                "path": builder.PRIOR_FREEZE_PATH.as_posix(),
                "file_sha256": builder.PRIOR_FREEZE_FILE_SHA256,
                "payload_sha256": builder.PRIOR_FREEZE_PAYLOAD,
                "status": "selected_null_frozen_train_failed",
                "selected_candidate_id": None,
            },
            "terminal_result": {
                "path": builder.PRIOR_RESULT_PATH.as_posix(),
                "file_sha256": builder.PRIOR_RESULT_FILE_SHA256,
                "payload_sha256": builder.PRIOR_RESULT_PAYLOAD,
                "status": "selection_falsified_no_candidate",
                "selected_candidate_id": None,
                "audit_status": "not_opened",
            },
        },
        claim_contract={
            "alpha_claim_allowed": False,
            "profit_claim_allowed": False,
            "stable_future_profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
            "minimum_fresh_sessions": 252,
            "minimum_fresh_monthly_executions": 12,
            "investment_recommendation_allowed": False,
        },
    )
    prior_release = {
        "release": "7.1",
        "tag": "7.1",
        "annotated_tag_object": builder.PRIOR_TAG_OBJECT,
        "peeled_commit": builder.PRIOR_COMMIT,
        "preselection_closure": {},
        "winner_freeze": {},
        "terminal_result": {},
    }
    exposure = {
        "source_release": "7.1",
        "train_gate_passed": False,
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
    }
    helpers = SimpleNamespace(
        RELEASE="8.0",
        PROTOCOL_PATH=builder.PROTOCOL_PATH,
        INHERITED_PROTOCOL_PATH=builder.INHERITED_PROTOCOL_PATH,
        INHERITED_PROTOCOL_FILE_SHA256=builder.INHERITED_PROTOCOL_FILE_SHA256,
        INHERITED_PROTOCOL_PAYLOAD=builder.INHERITED_PROTOCOL_PAYLOAD,
        CLOSURE_PATH=builder.CLOSURE_PATH,
        EVIDENCE_ROOT=builder.EVIDENCE_ROOT,
        WORK_ROOT=tmp_path / builder.WORK_ROOT,
        EXPECTED_IMPLEMENTATION_PATHS=implementation_paths,
        _require_source_imports=lambda: None,
        _require_head_pushed_and_ci_success=lambda _head: None,
        _runtime_identity=lambda: {"runtime": "exact"},
    )
    captured: dict[str, object] = {}

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
            return (tmp_path / relative).read_bytes()
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(builder, "_runner_helpers", lambda: helpers)
    monkeypatch.setattr(builder, "_json", lambda path: protocol if path == builder.PROTOCOL_PATH else {})
    monkeypatch.setattr(builder, "_validate_protocol", lambda _protocol: None)
    monkeypatch.setattr(
        builder, "_verify_prior_release", lambda: (prior_release, exposure)
    )
    monkeypatch.setattr(
        builder, "_create_only", lambda _path, payload: captured.update(payload)
    )

    assert builder.main() == 0
    assert set(captured) == builder.CLOSURE_FIELDS
    assert captured["release"] == "8.0"
    assert captured["route"] == "strategic_static_capital_budget_beta"
    assert captured["protocol"]["payload_sha256"] == protocol["payload_sha256"]
    assert captured["prior_release"] == prior_release
    assert captured["prior_train_exposure"] == exposure
    assert captured["implementation_commit"] == commit
    assert set(captured["implementation"]) == implementation_paths
    assert captured["runtime"] == {"runtime": "exact"}
    assert captured["validation_market_outcomes_opened"] is False
    assert captured["audit_status"] == "not_opened"
    assert captured["claim_contract"] == protocol["claim_contract"]
    assert captured["payload_sha256"] == canonical_payload_sha256(captured)


def test_8_0_closure_writer_is_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v8_create_only")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    path = Path("protocols/test-8.0-release.json")
    builder._create_only(path, {"schema_version": 1})
    with pytest.raises(FileExistsError):
        builder._create_only(path, {"schema_version": 1})
