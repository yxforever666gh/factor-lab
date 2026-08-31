import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = ROOT / "scripts" / "build-9.0-preselection-closure.py"


def _load_builder(name: str = "factor_lab_v90_closure_builder") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, BUILDER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read(relative: str) -> dict[str, Any]:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def _archive_projection(builder: ModuleType) -> dict[str, Any]:
    value = {
        "release": "8.1",
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "tag_object": builder.PRIOR_TAG_OBJECT,
        "tag_commit": builder.PRIOR_COMMIT,
        "protocol_payload_sha256": builder.PRIOR_FILES[
            Path("protocols/8.1-policy-operational-metric-reclassification.json")
        ]["payload_sha256"],
        "closure_payload_sha256": builder.PRIOR_FILES[
            Path("protocols/8.1-release.json")
        ]["payload_sha256"],
        "reclassification_payload_sha256": builder.PRIOR_FILES[
            Path("protocols/evidence/8.1/train-reclassification.json")
        ]["payload_sha256"],
        "freeze_payload_sha256": builder.PRIOR_FILES[
            Path("protocols/evidence/8.1/winner-freeze.json")
        ]["payload_sha256"],
        "result_payload_sha256": builder.PRIOR_FILES[
            Path("protocols/evidence/8.1/result.json")
        ]["payload_sha256"],
        **builder.PRIOR_VALIDATION_IDENTITIES,
        "deep_data_verified": True,
        "deep_runtime_verified": True,
    }
    value["archive_identity_sha256"] = canonical_payload_sha256(value)
    return value


def test_9_0_builder_namespace_and_create_only_roots_are_exact() -> None:
    builder = _load_builder()
    assert builder.RELEASE == "9.0"
    assert builder.ROUTE == builder.STRATEGY_ID == (
        "causal_monthly_volatility_balanced_budget"
    )
    assert builder.PROTOCOL_ID == (
        "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
    )
    assert builder.SCOUT_PATH == Path("protocols/9.0-preprotocol-scout.json")
    assert builder.PROTOCOL_PATH == Path(
        "protocols/9.0-causal-volatility-balanced-budget.json"
    )
    assert builder.CLOSURE_PATH == Path("protocols/9.0-release.json")
    assert builder.EVIDENCE_ROOT == Path("protocols/evidence/9.0")
    assert builder.WORK_ROOT == Path("runtime/data/multi-asset-9.0")
    assert builder.FORBIDDEN_BEFORE_CLOSURE == (
        builder.WORK_ROOT,
        builder.EVIDENCE_ROOT,
        builder.CLOSURE_PATH,
    )
    assert "prior_8_1_archive" in builder.CLOSURE_FIELDS
    assert "preprotocol_scout" in builder.CLOSURE_FIELDS


def test_9_0_builder_accepts_only_the_exact_scout_and_protocol() -> None:
    builder = _load_builder("factor_lab_v90_contract_validation")
    scout = _read(builder.SCOUT_PATH.as_posix())
    protocol = _read(builder.PROTOCOL_PATH.as_posix())
    builder._validate_scout(scout)
    builder._validate_protocol(protocol, scout)
    assert scout["payload_sha256"] == builder.SCOUT_PAYLOAD
    assert protocol["payload_sha256"] == builder.PROTOCOL_PAYLOAD
    assert hashlib.sha256((ROOT / builder.SCOUT_PATH).read_bytes()).hexdigest() == (
        builder.SCOUT_FILE_SHA256
    )
    assert hashlib.sha256((ROOT / builder.PROTOCOL_PATH).read_bytes()).hexdigest() == (
        builder.PROTOCOL_FILE_SHA256
    )


def test_9_0_runner_contract_requires_exact_namespace_and_prior_archive_helper() -> None:
    builder = _load_builder("factor_lab_v90_runner_contract")
    paths = set(builder.EXPECTED_IMPLEMENTATION_PATHS)
    helper = SimpleNamespace(
        EXPECTED_IMPLEMENTATION_PATHS=paths,
        RELEASE=builder.RELEASE,
        ROUTE=builder.ROUTE,
        PROTOCOL_ID=builder.PROTOCOL_ID,
        SCOUT_PATH=builder.SCOUT_PATH,
        PROTOCOL_PATH=builder.PROTOCOL_PATH,
        PROTOCOL_PAYLOAD=builder.PROTOCOL_PAYLOAD,
        PROTOCOL_FILE_SHA256=builder.PROTOCOL_FILE_SHA256,
        CLOSURE_PATH=builder.CLOSURE_PATH,
        EVIDENCE_ROOT=builder.EVIDENCE_ROOT,
        WORK_ROOT=builder.ROOT / builder.WORK_ROOT,
        PRIOR_TAG=builder.PRIOR_TAG,
        PRIMARY_ID=builder.STRATEGY_ID,
        STAGES={
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
        },
        _verify_prior_8_1_archive=lambda **_: {},
        _v9_verify_protocol_contract=lambda _protocol: None,
    )
    assert builder._verify_runner_contract(helper) == tuple(sorted(paths))
    for missing in ("configs/data.json", ".github/workflows/ci.yml"):
        helper.EXPECTED_IMPLEMENTATION_PATHS = paths - {missing}
        with pytest.raises(ValueError, match="exact 9.0 namespace"):
            builder._verify_runner_contract(helper)
    helper.EXPECTED_IMPLEMENTATION_PATHS = paths
    helper.PROTOCOL_PAYLOAD = "0" * 64
    with pytest.raises(ValueError, match="exact 9.0 namespace"):
        builder._verify_runner_contract(helper)


def test_actual_9_0_runner_and_builder_namespaces_are_aligned() -> None:
    builder = _load_builder("factor_lab_v90_actual_runner_contract")
    helper = builder._runner_helpers()
    paths = builder._verify_runner_contract(helper)
    assert builder.RUNNER_PATH.as_posix() in paths
    assert "scripts/build-9.0-preselection-closure.py" in paths
    assert "src/factor_lab/research/multi_asset.py" in paths


def test_9_0_builder_requires_deep_prior_runtime_admission() -> None:
    builder = _load_builder("factor_lab_v90_deep_archive")
    value = _archive_projection(builder)
    calls: list[dict[str, bool]] = []

    def verify(**kwargs: bool) -> dict[str, Any]:
        calls.append(kwargs)
        return value

    helpers = SimpleNamespace(_verify_prior_8_1_archive=verify)
    assert builder._verify_prior_runtime_admission(helpers) == value
    assert calls == [{"verify_data": True, "verify_runtime": True}]
    bad = {**value, "artifact_row_count": value["artifact_row_count"] - 1}
    helpers._verify_prior_8_1_archive = lambda **_: bad
    with pytest.raises(ValueError, match="retained 8.1 validation admission differs"):
        builder._verify_prior_runtime_admission(helpers)
    helpers._verify_prior_8_1_archive = lambda **_: (_ for _ in ()).throw(
        FileNotFoundError("retained runtime missing")
    )
    with pytest.raises(FileNotFoundError, match="retained runtime missing"):
        builder._verify_prior_runtime_admission(helpers)


def test_9_0_prior_release_projection_matches_local_tagged_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v90_prior_projection")
    monkeypatch.setattr(
        builder,
        "_remote_prior_tag_refs",
        lambda: {
            "refs/tags/8.1": builder.PRIOR_TAG_OBJECT,
            "refs/tags/8.1^{}": builder.PRIOR_COMMIT,
        },
    )
    projection = builder._verify_prior_release()
    assert projection["tag_object"] == builder.PRIOR_TAG_OBJECT
    assert projection["tag_commit"] == builder.PRIOR_COMMIT
    assert projection["status"] == "selection_falsified_no_candidate"
    assert projection["selected_candidate_id"] is None
    assert projection["audit_status"] == "not_opened"
    assert set(projection["files"]) == {path.as_posix() for path in builder.PRIOR_FILES}


def test_9_0_remote_tag_fallback_requires_exact_github_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v90_remote_fallback")

    def failing_git(*_args: str, **_kwargs: bool) -> bytes:
        raise builder._GitCommandError(
            "transport",
            returncode=128,
            stdout=b"",
            stderr=b"fatal: unable to access: Could not resolve host: github.com",
        )

    def github(path: str) -> dict[str, Any]:
        if path.endswith("/git/ref/tags/8.1"):
            return {
                "ref": "refs/tags/8.1",
                "object": {"type": "tag", "sha": builder.PRIOR_TAG_OBJECT},
            }
        return {
            "sha": builder.PRIOR_TAG_OBJECT,
            "object": {"type": "commit", "sha": builder.PRIOR_COMMIT},
        }

    monkeypatch.setattr(builder, "_git", failing_git)
    monkeypatch.setattr(builder, "_github_api", github)
    assert builder._remote_prior_tag_refs() == {
        "refs/tags/8.1": builder.PRIOR_TAG_OBJECT,
        "refs/tags/8.1^{}": builder.PRIOR_COMMIT,
    }
    monkeypatch.setattr(
        builder,
        "_github_api",
        lambda _path: {
            "ref": "refs/tags/8.1",
            "object": {"type": "commit", "sha": builder.PRIOR_COMMIT},
        },
    )
    with pytest.raises(ValueError, match="tag object differs"):
        builder._remote_prior_tag_refs()


def test_9_0_missing_remote_tag_is_not_an_api_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _load_builder("factor_lab_v90_missing_tag")
    monkeypatch.setattr(
        builder,
        "_git",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            builder._GitCommandError(
                "missing", returncode=2, stdout=b"", stderr=b""
            )
        ),
    )
    monkeypatch.setattr(
        builder,
        "_github_prior_tag_refs",
        lambda: pytest.fail("missing ref must not use API fallback"),
    )
    assert builder._remote_prior_tag_refs() == {}


@pytest.mark.parametrize("forbidden", ["runtime", "evidence", "closure"])
def test_9_0_builder_rejects_preexisting_formal_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, forbidden: str
) -> None:
    builder = _load_builder(f"factor_lab_v90_forbidden_{forbidden}")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    paths = {
        "runtime": builder.WORK_ROOT,
        "evidence": builder.EVIDENCE_ROOT,
        "closure": builder.CLOSURE_PATH,
    }
    target = tmp_path / paths[forbidden]
    if forbidden == "closure":
        target.parent.mkdir(parents=True)
        target.write_text("{}\n", encoding="utf-8")
    else:
        target.mkdir(parents=True)
    monkeypatch.setattr(builder, "_git", lambda *args, **_: b"main\n" if args == ("branch", "--show-current") else b"")
    expected = "create-only" if forbidden == "closure" else "already exists"
    with pytest.raises((FileExistsError, RuntimeError), match=expected):
        builder.main()


def test_9_0_builder_emits_exact_preselection_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v90_emit")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    for relative in (
        *builder.PRIOR_FILES,
        *builder.INHERITED_CONTRACT_PATHS,
        builder.SCOUT_PATH,
        builder.PROTOCOL_PATH,
    ):
        source = ROOT / relative
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    implementation_paths = (
        builder.RUNNER_PATH.as_posix(),
        "scripts/build-9.0-preselection-closure.py",
        "src/factor_lab/research/multi_asset.py",
    )
    for relative in implementation_paths:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"frozen {relative}\n", encoding="utf-8")
    commit = "d" * 40
    tree = "e" * 40

    def git(*args: str, **_: bool) -> bytes:
        if args == ("branch", "--show-current"):
            return b"main\n"
        if args == ("status", "--porcelain"):
            return b""
        if args == ("rev-parse", "HEAD"):
            return (commit + "\n").encode()
        if args == ("rev-parse", "HEAD^{tree}"):
            return (tree + "\n").encode()
        if len(args) == 2 and args[0] == "show" and args[1].startswith(commit + ":"):
            return (tmp_path / args[1].split(":", 1)[1]).read_bytes()
        raise AssertionError(args)

    archive = _archive_projection(builder)
    prior = {
        "release": "8.1",
        "tag": "8.1",
        "tag_object": builder.PRIOR_TAG_OBJECT,
        "tag_commit": builder.PRIOR_COMMIT,
        "files": {},
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
    }
    verified_protocols = []
    helper = SimpleNamespace(
        _require_source_imports=lambda: None,
        _require_head_pushed_and_ci_success=lambda _commit: None,
        _runtime_identity=lambda: {"runtime": "exact"},
        _v9_verify_protocol_contract=lambda value: verified_protocols.append(value),
    )
    monkeypatch.setattr(builder, "_git", git)
    monkeypatch.setattr(builder, "_runner_helpers", lambda: helper)
    monkeypatch.setattr(builder, "_verify_runner_contract", lambda _helpers: implementation_paths)
    monkeypatch.setattr(builder, "_verify_prior_release", lambda: prior)
    monkeypatch.setattr(builder, "_verify_prior_runtime_admission", lambda _helpers: archive)
    assert builder.main() == 0
    value = json.loads((tmp_path / builder.CLOSURE_PATH).read_text(encoding="utf-8"))
    assert set(value) == builder.CLOSURE_FIELDS
    assert value["payload_sha256"] == canonical_payload_sha256(value)
    assert value["closure_role"] == "causal_volatility_balanced_preselection_root"
    assert value["status"] == "implementation_frozen_before_formal_development_replay"
    assert value["development_outcomes_opened"] is True
    assert value["audit_market_outcomes_opened"] is False
    assert value["protocol"]["payload_sha256"] == builder.PROTOCOL_PAYLOAD
    assert value["preprotocol_scout"]["payload_sha256"] == builder.SCOUT_PAYLOAD
    assert value["prior_8_1_archive"]["archive_identity_sha256"] == (
        archive["archive_identity_sha256"]
    )
    assert value["implementation_commit"] == commit
    assert value["implementation_tree"] == tree
    assert value["runtime"] == {"runtime": "exact"}
    assert value["formal_data"] == {}
    assert verified_protocols == [_read(builder.PROTOCOL_PATH.as_posix())]


def test_9_0_closure_writer_is_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder("factor_lab_v90_create_only")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    payload = {"payload_sha256": "a" * 64}
    builder._create_only(Path("protocols/9.0-release.json"), payload)
    with pytest.raises(FileExistsError):
        builder._create_only(Path("protocols/9.0-release.json"), payload)


def test_no_formal_9_0_artifact_exists_during_protocol_implementation() -> None:
    builder = _load_builder("factor_lab_v90_absence")
    assert not (ROOT / builder.CLOSURE_PATH).exists()
    assert not (ROOT / builder.EVIDENCE_ROOT).exists()
    assert not (ROOT / builder.WORK_ROOT).exists()
