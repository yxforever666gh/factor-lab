from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = ROOT / "scripts" / "build-8.1-preselection-closure.py"


def _load_builder(name: str = "factor_lab_v81_closure_builder") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, BUILDER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _actual_protocol() -> dict[str, object]:
    return json.loads(
        (ROOT / "protocols/8.1-policy-operational-metric-reclassification.json").read_text(
            encoding="utf-8"
        )
    )


def test_8_1_builder_uses_fresh_namespace_and_create_only_roots() -> None:
    builder = _load_builder()

    assert builder.RELEASE == "8.1"
    assert builder.PROTOCOL_PATH == Path(
        "protocols/8.1-policy-operational-metric-reclassification.json"
    )
    assert builder.CLOSURE_PATH == Path("protocols/8.1-release.json")
    assert builder.WORK_ROOT == Path("runtime/data/multi-asset-8.1")
    assert builder.EVIDENCE_ROOT == Path("protocols/evidence/8.1")
    assert builder.PRIOR_RECEIPT_PATH == Path(
        "protocols/evidence/8.0/execution-failure.json"
    )
    assert set(builder.FORBIDDEN_BEFORE_CLOSURE) == {
        builder.WORK_ROOT,
        builder.EVIDENCE_ROOT,
        builder.CLOSURE_PATH,
    }
    assert "train_reclassification_source" in builder.CLOSURE_FIELDS
    assert "train_reclassification_status" in builder.CLOSURE_FIELDS
    assert "post_hoc_reclassification" in builder.CLOSURE_FIELDS


def test_8_1_builder_validates_exact_protocol_and_published_sources() -> None:
    builder = _load_builder("factor_lab_v81_protocol_gate")
    protocol = builder._json(builder.PROTOCOL_PATH)

    builder._validate_protocol(protocol)
    assert protocol["payload_sha256"] == builder.PROTOCOL_PAYLOAD
    assert protocol["prior_release"]["execution_failure_receipt"] == {
        "path": builder.PRIOR_RECEIPT_PATH.as_posix(),
        "file_sha256": builder.PRIOR_RECEIPT_FILE_SHA256,
        "payload_sha256": builder.PRIOR_RECEIPT_PAYLOAD,
        "status": "selection_inconclusive_execution_failure",
        "classification": "post_evaluation_github_ci_transport_failure",
    }

    changed = copy.deepcopy(protocol)
    changed["metric_role_contract"]["policy_operational_metrics"]["roles"] = [
        "primary",
        "stress",
        "cash",
        "cash_stress",
    ]
    changed["payload_sha256"] = canonical_payload_sha256(changed)
    with pytest.raises(ValueError, match="unexpected 8.1 protocol"):
        builder._validate_protocol(changed)


def test_8_1_prior_release_projection_matches_the_protocol_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_prior_projection")

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("cat-file", "-t", "refs/tags/8.0"):
            return b"tag\n"
        if args == ("rev-parse", "refs/tags/8.0"):
            return builder.PRIOR_TAG_OBJECT.encode("ascii") + b"\n"
        if args == ("rev-parse", "refs/tags/8.0^{}"):
            return builder.PRIOR_COMMIT.encode("ascii") + b"\n"
        if args == ("merge-base", "--is-ancestor", builder.PRIOR_COMMIT, "HEAD"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(
        builder,
        "_remote_tag_refs",
        lambda: {
            "refs/tags/8.0": builder.PRIOR_TAG_OBJECT,
            "refs/tags/8.0^{}": builder.PRIOR_COMMIT,
        },
    )
    monkeypatch.setattr(
        builder, "_tag_blob", lambda path: (ROOT / path).read_bytes()
    )
    prior_release, source = builder._verify_prior_release()

    assert prior_release == _actual_protocol()["prior_release"]
    assert source["economic_role_metrics_source"] == "published_receipt_only"
    assert source["validity_source"] == (
        "receipt_bound_retained_8_0_artifacts_read_only"
    )


def test_8_1_builder_rejects_tampered_receipt_even_when_rehashed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v81_receipt_gate")
    for relative in (
        builder.PROTOCOL_PATH,
        builder.PRIOR_PROTOCOL_PATH,
        builder.PRIOR_RECEIPT_PATH,
    ):
        source = ROOT / relative
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    receipt_path = tmp_path / builder.PRIOR_RECEIPT_PATH
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["train_stage"]["role_gate_metrics"]["cash"][
        "requested_notional_fill_ratio"
    ] = 1.0
    receipt["payload_sha256"] = canonical_payload_sha256(receipt)
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    protocol = builder._json(builder.PROTOCOL_PATH)

    with pytest.raises(ValueError, match="frozen source bytes or receipt metrics"):
        builder._validate_protocol(protocol)


def test_8_1_builder_requires_deep_prior_runtime_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_prior_runtime_admission")
    protocol = builder._json(builder.PROTOCOL_PATH)
    receipt = builder._json(builder.PRIOR_RECEIPT_PATH)
    validity = {
        "source": "receipt_bound_8_0_train_artifacts",
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
        "roles": {"deep": "verified"},
    }
    required: list[tuple[object, object]] = []
    helpers = SimpleNamespace(
        _verify_prior_train_artifacts=lambda value: validity
        if value == receipt
        else pytest.fail("wrong receipt"),
        _combine_receipt_role_gate_metrics=lambda metrics, **kwargs: {
            "role_metrics": metrics,
            "execution_validity": kwargs["execution_validity"],
        },
        _require_execution_validity=lambda metrics, config: required.append(
            (metrics, config)
        ),
    )
    monkeypatch.setattr(
        builder,
        "_json",
        lambda path: receipt if path == builder.PRIOR_RECEIPT_PATH else protocol,
    )

    assert builder._verify_prior_runtime_admission(helpers, protocol) == validity
    assert len(required) == 1
    assert required[0][1] == protocol["execution_validity_hard_fail"]

    helpers._verify_prior_train_artifacts = lambda _value: (_ for _ in ()).throw(
        FileNotFoundError("retained runtime absent")
    )
    with pytest.raises(FileNotFoundError, match="retained runtime absent"):
        builder._verify_prior_runtime_admission(helpers, protocol)

    helpers._verify_prior_train_artifacts = lambda _value: {
        **validity,
        "artifact_row_count": 1,
    }
    with pytest.raises(ValueError, match="admission identity differs"):
        builder._verify_prior_runtime_admission(helpers, protocol)


def test_8_1_remote_tag_transport_fallback_requires_exact_github_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_tag_api_fallback")

    def git(*_args: str, **_kwargs: object) -> bytes:
        raise RuntimeError("transport unavailable")

    def github(path: str) -> dict[str, object]:
        if path.endswith("/git/ref/tags/8.0"):
            return {
                "ref": "refs/tags/8.0",
                "object": {"type": "tag", "sha": builder.PRIOR_TAG_OBJECT},
            }
        if path.endswith(f"/git/tags/{builder.PRIOR_TAG_OBJECT}"):
            return {
                "sha": builder.PRIOR_TAG_OBJECT,
                "object": {"type": "commit", "sha": builder.PRIOR_COMMIT},
            }
        raise AssertionError(path)

    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(builder, "_github_api", github)
    assert builder._remote_tag_refs() == {
        "refs/tags/8.0": builder.PRIOR_TAG_OBJECT,
        "refs/tags/8.0^{}": builder.PRIOR_COMMIT,
    }

    def lightweight(_path: str) -> dict[str, object]:
        return {
            "ref": "refs/tags/8.0",
            "object": {"type": "commit", "sha": builder.PRIOR_COMMIT},
        }

    monkeypatch.setattr(builder, "_github_api", lightweight)
    with pytest.raises(ValueError, match="tag object binding differs"):
        builder._remote_tag_refs()


def test_8_1_missing_remote_tag_is_not_misclassified_as_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_missing_remote_tag")
    monkeypatch.setattr(builder, "_git", lambda *_args, **_kwargs: b"")
    monkeypatch.setattr(
        builder,
        "_github_api",
        lambda _path: pytest.fail("a successful empty result is not transport failure"),
    )

    assert builder._remote_tag_refs() == {}


def test_8_1_builder_does_not_hide_a_nontransport_remote_tag_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_remote_mismatch")

    def git(*args: str, **_kwargs: object) -> bytes:
        if args == ("cat-file", "-t", "refs/tags/8.0"):
            return b"tag\n"
        if args == ("rev-parse", "refs/tags/8.0"):
            return builder.PRIOR_TAG_OBJECT.encode("ascii") + b"\n"
        if args == ("rev-parse", "refs/tags/8.0^{}"):
            return builder.PRIOR_COMMIT.encode("ascii") + b"\n"
        if args == ("merge-base", "--is-ancestor", builder.PRIOR_COMMIT, "HEAD"):
            return b""
        if args[:4] == ("ls-remote", "--exit-code", "origin", "refs/tags/8.0"):
            return (
                ("0" * 40 + "\trefs/tags/8.0\n")
                + (builder.PRIOR_COMMIT + "\trefs/tags/8.0^{}\n")
            ).encode("ascii")
        raise AssertionError(args)

    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(
        builder,
        "_github_api",
        lambda _path: pytest.fail("API fallback must not mask a returned mismatch"),
    )
    with pytest.raises(ValueError, match="remote 8.0 annotated tag binding differs"):
        builder._verify_prior_release()


def test_8_1_builder_rejects_malformed_nonempty_remote_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v81_remote_malformed")
    monkeypatch.setattr(builder, "_git", lambda *_args, **_kwargs: b"malformed\n")
    monkeypatch.setattr(
        builder,
        "_github_api",
        lambda _path: pytest.fail("malformed remote data is not a transport failure"),
    )

    with pytest.raises(ValueError, match="response is malformed"):
        builder._remote_tag_refs()


def test_8_1_builder_requires_runner_to_have_exact_namespace(tmp_path: Path) -> None:
    builder = _load_builder("factor_lab_v81_runner_gate")
    valid = SimpleNamespace(
        RELEASE="8.1",
        PROTOCOL_PATH=builder.PROTOCOL_PATH,
        PROTOCOL_PAYLOAD=builder.PROTOCOL_PAYLOAD,
        PROTOCOL_FILE_SHA256=builder.PROTOCOL_FILE_SHA256,
        CLOSURE_PATH=builder.CLOSURE_PATH,
        EVIDENCE_ROOT=builder.EVIDENCE_ROOT,
        WORK_ROOT=tmp_path / builder.WORK_ROOT,
        PRIOR_RECEIPT_PATH=builder.PRIOR_RECEIPT_PATH,
        EXPECTED_IMPLEMENTATION_PATHS={
            builder.RUNNER_PATH.as_posix(),
            "scripts/build-8.1-preselection-closure.py",
        },
    )
    builder.ROOT = tmp_path
    assert set(builder._verify_runner_contract(valid)) == set(
        valid.EXPECTED_IMPLEMENTATION_PATHS
    )

    for attribute, replacement in (
        ("RELEASE", "8.0"),
        ("PROTOCOL_PATH", Path("protocols/8.0-static-capital-budget.json")),
        ("PROTOCOL_PAYLOAD", "0" * 64),
        ("PROTOCOL_FILE_SHA256", "1" * 64),
        ("CLOSURE_PATH", Path("protocols/8.0-release.json")),
        ("EVIDENCE_ROOT", Path("protocols/evidence/8.0")),
        ("WORK_ROOT", tmp_path / "runtime/data/multi-asset-8.0"),
        ("PRIOR_RECEIPT_PATH", Path("protocols/evidence/7.1/result.json")),
    ):
        changed = SimpleNamespace(**vars(valid))
        setattr(changed, attribute, replacement)
        with pytest.raises(ValueError, match="exact 8.1 namespace"):
            builder._verify_runner_contract(changed)


@pytest.mark.parametrize("forbidden_role", ["runtime", "evidence", "closure"])
def test_8_1_builder_rejects_any_preexisting_formal_artifact(
    forbidden_role: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder(f"factor_lab_v81_absence_{forbidden_role}")
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
    if forbidden_role == "closure":
        with pytest.raises(FileExistsError, match="create-only"):
            builder.main()
    else:
        with pytest.raises(RuntimeError, match="already exists"):
            builder.main()


def test_8_1_builder_emits_exact_schema_from_dynamic_runner_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v81_schema")
    protocol = _actual_protocol()
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    monkeypatch.setattr(builder, "SRC", tmp_path / "src")
    monkeypatch.setattr(builder, "FORBIDDEN_BEFORE_CLOSURE", ())
    commit = "d" * 40
    implementation_paths = {
        builder.RUNNER_PATH.as_posix(),
        "scripts/build-8.1-preselection-closure.py",
    }
    for relative in implementation_paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative + "\n", encoding="utf-8")
    for relative in (
        builder.PROTOCOL_PATH,
        builder.PRIOR_PROTOCOL_PATH,
        builder.PRIOR_CLOSURE_PATH,
        builder.PRIOR_RECEIPT_PATH,
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")

    prior_release = {
        "release": "8.0",
        "tag": "8.0",
        "annotated_tag_object": builder.PRIOR_TAG_OBJECT,
        "peeled_commit": builder.PRIOR_COMMIT,
    }
    train_source = {
        "source_release": "8.0",
        "receipt_payload_sha256": builder.PRIOR_RECEIPT_PAYLOAD,
        "economic_role_metrics_source": "published_receipt_only",
        "validity_source": "receipt_bound_retained_8_0_artifacts_read_only",
    }
    execution_validity = {
        "source": "receipt_bound_8_0_train_artifacts",
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
        "roles": {"deep": "verified"},
    }
    helpers = SimpleNamespace(
        RELEASE="8.1",
        PROTOCOL_PATH=builder.PROTOCOL_PATH,
        PROTOCOL_PAYLOAD=builder.PROTOCOL_PAYLOAD,
        PROTOCOL_FILE_SHA256=builder.PROTOCOL_FILE_SHA256,
        CLOSURE_PATH=builder.CLOSURE_PATH,
        EVIDENCE_ROOT=builder.EVIDENCE_ROOT,
        WORK_ROOT=tmp_path / builder.WORK_ROOT,
        PRIOR_RECEIPT_PATH=builder.PRIOR_RECEIPT_PATH,
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
    monkeypatch.setattr(builder, "_json", lambda _path: protocol)
    monkeypatch.setattr(builder, "_validate_protocol", lambda _protocol: None)
    monkeypatch.setattr(
        builder, "_verify_prior_release", lambda: (prior_release, train_source)
    )
    monkeypatch.setattr(
        builder,
        "_verify_prior_runtime_admission",
        lambda _helpers, _protocol: execution_validity,
    )
    monkeypatch.setattr(
        builder, "_create_only", lambda _path, payload: captured.update(payload)
    )

    assert builder.main() == 0
    assert set(captured) == builder.CLOSURE_FIELDS
    assert captured["release"] == "8.1"
    assert captured["direction_change"] is False
    assert captured["route"] == "policy_operational_metric_reclassification"
    assert captured["status"] == "implementation_frozen_before_8_1_reclassification"
    assert captured["post_hoc_reclassification"] is True
    assert captured["prior_train_returns_opened"] is True
    assert captured["train_reexecution_allowed"] is False
    assert captured["train_reclassification_status"] == "pending"
    assert captured["validation_market_outcomes_opened"] is False
    assert captured["audit_status"] == "not_opened"
    assert captured["prior_release"] == prior_release
    assert captured["train_reclassification_source"] == {
        **train_source,
        "execution_validity_sha256": canonical_payload_sha256(execution_validity),
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
    }
    assert captured["implementation_commit"] == commit
    assert set(captured["implementation"]) == implementation_paths
    assert captured["runtime"] == {"runtime": "exact"}
    assert captured["formal_data"] == {}
    assert captured["claim_contract"] == protocol["claim_contract"]
    assert captured["payload_sha256"] == canonical_payload_sha256(captured)


def test_8_1_closure_writer_is_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v81_create_only")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    path = Path("protocols/test-8.1-release.json")
    builder._create_only(path, {"schema_version": 1})
    with pytest.raises(FileExistsError):
        builder._create_only(path, {"schema_version": 1})
