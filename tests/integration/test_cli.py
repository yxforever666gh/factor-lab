import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

import factor_lab.cli as cli
from factor_lab.cli import build_parser


def _copy_8_0_failure_archive_inputs(destination: Path) -> Path:
    source_root = Path(__file__).resolve().parents[2]
    for relative in (
        cli.V8_PROTOCOL_PATH,
        cli.V8_ASSET_SELECTION_PATH,
        cli.V7_PRECLOSURE_TRAIN_PATH,
        cli.V8_CLOSURE_PATH,
        cli.V8_FAILURE_PATH,
    ):
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((source_root / relative).read_bytes())
    return source_root


def _mock_v9_preclosure_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = SimpleNamespace(
        _verify_prior_8_1_archive=lambda **_: {
            "status": "selection_falsified_no_candidate",
            "deep_data_verified": True,
            "deep_runtime_verified": True,
            "artifact_parquet_count": 20,
            "artifact_row_count": 62654,
            "archive_identity_sha256": "a" * 64,
        }
    )
    monkeypatch.setattr(cli, "_load_v9_runner", lambda _root: runner)
    monkeypatch.setattr(
        cli,
        "_verify_published_8_1_archive",
        lambda _root: {"result": {"payload_sha256": cli.V81_RESULT_PAYLOAD_SHA256}},
    )


def test_cli_exposes_only_lightweight_mainline_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["data", "status"]).data_command == "status"
    research = parser.parse_args(["research", "run", "--suite", "next", "--canary"])
    assert research.research_command == "run"
    assert research.canary is True
    assert parser.parse_args(["report", "--run", "latest"]).command == "report"
    strategy = parser.parse_args(["strategy", "status", "--verify-data"])
    assert strategy.strategy_command == "status"
    assert strategy.verify_data is True
    assert parser.parse_args(
        ["strategy", "status", "--release", "6.3"]
    ).release == "6.3"
    assert parser.parse_args(
        ["strategy", "status", "--release", "7.0"]
    ).release == "7.0"
    assert parser.parse_args(
        ["strategy", "status", "--release", "7.1"]
    ).release == "7.1"
    assert parser.parse_args(
        ["strategy", "status", "--release", "8.0"]
    ).release == "8.0"
    assert parser.parse_args(
        ["strategy", "status", "--release", "8.1"]
    ).release == "8.1"
    assert parser.parse_args(
        ["strategy", "status", "--release", "9.0"]
    ).release == "9.0"
    assert parser.parse_args(
        ["strategy", "status", "--release", "10.0"]
    ).release == "10.0"
    targets = parser.parse_args(["strategy", "targets", "--signal-date", "latest"])
    assert targets.strategy_command == "targets"

    enrich = parser.parse_args(
        ["data", "enrich", "--from", "2017-01-01", "--to", "2026-08-13"]
    )
    assert enrich.data_command == "enrich"
    assert enrich.resume is True

    suspensions = parser.parse_args(
        ["data", "suspensions", "--from", "2017-01-01", "--to", "2026-08-21"]
    )
    assert suspensions.data_command == "suspensions"
    assert suspensions.resume is True

    reference = parser.parse_args(
        ["data", "reference", "--trade-date", "2026-08-31"]
    )
    assert reference.data_command == "reference"
    assert reference.trade_date == "2026-08-31"


@pytest.mark.parametrize("command", ["prospective", "adaptive-shadow"])
def test_retired_runtime_commands_are_rejected(command: str) -> None:
    with pytest.raises(SystemExit) as raised:
        build_parser().parse_args([command])

    assert raised.value.code == 2


def test_help_imports_without_retired_runtime(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["--help"])

    assert raised.value.code == 0
    help_text = capsys.readouterr().out
    assert "data" in help_text
    assert "research" in help_text
    assert "strategy" in help_text
    assert "report" in help_text
    assert "prospective" not in help_text
    assert "adaptive-shadow" not in help_text


def test_walk_forward_is_the_default_research_suite() -> None:
    research = build_parser().parse_args(["research", "run", "--canary"])

    assert research.suite == "walk-forward"


def test_retired_adaptive_research_suite_is_rejected() -> None:
    with pytest.raises(SystemExit) as raised:
        build_parser().parse_args(
            ["research", "run", "--suite", "adaptive", "--canary"]
        )

    assert raised.value.code == 2


def test_default_root_finds_checkout_above_noneditable_wheel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "factor-lab"
    (project / ".git").mkdir(parents=True)
    (project / "configs").mkdir()
    (project / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (project / "configs" / "data.json").write_text("{}\n", encoding="utf-8")
    installed = project / "runtime/environments/6.0/Lib/site-packages/factor_lab/cli.py"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.setattr(cli, "__file__", str(installed))
    monkeypatch.chdir(elsewhere)

    assert cli._root() == project.resolve()


def test_explicit_root_does_not_require_implicit_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[Path] = []

    def fail_discovery() -> Path:
        raise AssertionError("explicit --root must bypass discovery")

    def data_command(arguments) -> int:
        captured.append(arguments.root)
        return 0

    monkeypatch.setattr(cli, "_root", fail_discovery)
    monkeypatch.setattr(cli, "_data_command", data_command)

    assert cli.main(["--root", str(tmp_path), "data", "status"]) == 0
    assert captured == [tmp_path]


def test_strategy_status_verifies_tracked_implementation_and_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    # This test validates the checked-out evidence chain.  A workflow cannot
    # require its own still-running GitHub job to have completed successfully;
    # the remote-CI and worktree-state contracts are covered by dedicated tests
    # below.
    monkeypatch.setattr(cli, "_v9_require_head_ci", lambda _root: "a" * 40)
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)

    def is_committed(relative: str) -> bool:
        return subprocess.run(
            ["git", "cat-file", "-e", f"HEAD:{relative}"],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).returncode == 0

    closure_exists = (root / cli.V9_CLOSURE_PATH).is_file()
    closure_committed = closure_exists and is_committed(cli.V9_CLOSURE_PATH)
    pending_formal_artifact = any(
        (root / relative).is_file() and not is_committed(relative)
        for relative in (
            cli.V9_CLOSURE_PATH,
            cli.V9_WINNER_FREEZE_PATH,
            cli.V9_AUDIT_PATH,
            cli.V9_RESULT_PATH,
        )
    )
    if not closure_exists:
        _mock_v9_preclosure_readiness(monkeypatch)
    result, exit_code = cli._strategy_status(root, verify_data=False, release="9.0")
    if pending_formal_artifact:
        assert exit_code == 3
        assert result["status"] == "integrity_mismatch"
    elif closure_committed:
        assert exit_code == 0
    else:
        assert exit_code in {0, 2}
        assert result["status"] in {
            "implementation_ready_for_preselection_closure",
            "implementation_pending_clean_commit",
        }
    assert result["version"] == "9.0"
    assert result["route"] == cli.V9_ROUTE
    assert result["profit_claim_allowed"] is False
    if not closure_exists:
        assert result["audit_status"] == "not_opened"
    allowed = {
        "match",
        "not_verified",
        "not_retained",
        "not_applicable",
        "pending_clean_commit",
    }
    if pending_formal_artifact:
        assert any(check["status"] == "mismatch" for check in result["checks"])
    else:
        assert all(check["status"] in allowed for check in result["checks"])
    categories = {check["category"] for check in result["checks"]}
    if closure_exists:
        assert "release_evidence_chain" in categories
    else:
        assert "protocol_payload" in categories
        assert "published_8_1_archive" in categories
        assert "retained_8_1_development_readiness" in categories


def test_default_strategy_status_tracks_10_0_results_first_stage() -> None:
    root = Path(__file__).resolve().parents[2]
    evidence_path = root / cli.V10_EVIDENCE_PATH
    clean = cli._working_tree_is_clean(root)
    committed = (
        evidence_path.is_file()
        and subprocess.run(
            ["git", "cat-file", "-e", f"HEAD:{cli.V10_EVIDENCE_PATH}"],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).returncode
        == 0
    )
    result, exit_code = cli._strategy_status(root, verify_data=False)
    assert result["version"] == "10.0"
    assert result["route"] == cli.V10_ROUTE
    assert result["profit_claim_allowed"] is False
    if not evidence_path.is_file():
        assert exit_code == 2
        assert result["status"] == "implementation_pending_results_first_replay"
        assert result["selected_candidate_id"] is None
    elif not committed or not clean:
        assert exit_code == 3
        assert result["status"] == "integrity_mismatch"
    else:
        assert exit_code == 0
        assert result["status"] == "candidate_passed_all_results_first_gates"
        assert result["selected_candidate_id"] == cli.V10_ROUTE


def test_10_0_status_delegates_exact_and_deep_evidence_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    protocol_target = tmp_path / cli.V10_PROTOCOL_PATH
    protocol_target.parent.mkdir(parents=True, exist_ok=True)
    protocol_target.write_bytes((root / cli.V10_PROTOCOL_PATH).read_bytes())
    evidence = {
        "status": "candidate_passed_all_results_first_gates",
        "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
        "selection": {"selected_candidate_id": cli.V10_ROUTE},
        "periods": {
            "full": {
                "metrics": {
                    "candidate": {"cagr": 0.10, "max_drawdown": -0.25},
                    "static": {"cagr": 0.08},
                }
            }
        },
    }
    evidence["payload_sha256"] = cli._canonical_payload_sha256(evidence)
    evidence_target = tmp_path / cli.V10_EVIDENCE_PATH
    evidence_target.parent.mkdir(parents=True, exist_ok=True)
    evidence_target.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    evidence_bytes = evidence_target.read_bytes()
    calls: list[bool] = []
    runner = SimpleNamespace(
        verify_evidence=lambda _value, *, verify_data: calls.append(verify_data)
    )
    monkeypatch.setattr(cli, "_load_v10_runner", lambda _root: runner)
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout=evidence_bytes, stderr=b""
        ),
    )
    result, exit_code = cli._strategy_status_10_0(tmp_path, verify_data=True)
    assert exit_code == 0
    assert result["canonical_data_hashes_verified"] is True
    assert calls == [True]

    runner.verify_evidence = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        ValueError("forged evidence")
    )
    result, exit_code = cli._strategy_status_10_0(tmp_path, verify_data=False)
    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any("forged evidence" in check.get("error", "") for check in result["checks"])


def test_8_0_real_execution_failure_receipt_has_valid_shallow_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if not (root / cli.V8_RUNTIME_PATH).is_dir():
        pytest.skip("pre-tag archive runtime is not retained in this checkout")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_8_0(root, verify_data=False)

    assert exit_code == 0
    assert result["status"] == "selection_inconclusive_execution_failure"
    assert result["observed_train_gate_passed"] is False
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["profit_claim_allowed"] is False
    assert result["canonical_data_hashes_verified"] is False
    assert result["execution_failure_payload_sha256"] == (
        cli.V8_FAILURE_PAYLOAD_SHA256
    )
    assert any(
        check["category"] == "execution_failure_archive"
        and check["status"] == "match"
        for check in result["checks"]
    )
    stage_check = next(
        check
        for check in result["checks"]
        if check["category"] == "canonical_stage_artifacts"
    )
    assert stage_check["status"] == "not_verified"


def test_8_0_real_execution_failure_receipt_deep_verifies_only_train_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    runtime_root = root / cli.V8_RUNTIME_PATH
    if not runtime_root.is_dir():
        pytest.skip("the archived 2015-2019 train runtime is not retained")
    assert {path.name for path in (runtime_root / "sources").iterdir()} == {
        "stage=train"
    }
    assert {path.name for path in (runtime_root / "evaluations").iterdir()} == {
        "stage=train"
    }
    assert {path.name for path in (runtime_root / "stage-bindings").iterdir()} == {
        "train.json"
    }
    evaluation = json.loads(
        (
            runtime_root
            / "evaluations"
            / "stage=train"
            / "evaluation.json"
        ).read_text(encoding="utf-8")
    )
    assert evaluation["stage"] == "train"
    assert evaluation["metrics"]["start_date"] == "2015-03-02"
    assert evaluation["metrics"]["end_date"] == "2019-12-31"
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "b" * 40)

    result, exit_code = cli._strategy_status_8_0(root, verify_data=True)

    assert exit_code == 0
    assert result["status"] == "selection_inconclusive_execution_failure"
    assert result["canonical_data_hashes_verified"] is True
    assert any(
        check["category"] == "canonical_stage_artifacts"
        and check["status"] == "match"
        for check in result["checks"]
    )


def test_8_0_execution_failure_rejects_internally_rehashed_receipt_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _copy_8_0_failure_archive_inputs(tmp_path)
    receipt_path = tmp_path / cli.V8_FAILURE_PATH
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["calibration_execution"]["canonical_failure_code"] = "forged_failure"
    receipt["payload_sha256"] = cli._canonical_payload_sha256(receipt)
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert "receipt identity differs" in result["checks"][-1]["error"]


def test_8_0_execution_failure_rejects_frozen_input_file_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _copy_8_0_failure_archive_inputs(tmp_path)
    protocol_path = tmp_path / cli.V8_PROTOCOL_PATH
    protocol_path.write_bytes(protocol_path.read_bytes() + b"\n")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert f"frozen input differs: {cli.V8_PROTOCOL_PATH}" in (
        result["checks"][-1]["error"]
    )


def test_8_0_execution_failure_rejects_normal_evidence_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _copy_8_0_failure_archive_inputs(tmp_path)
    admission_path = tmp_path / cli.V8_TRAIN_ADMISSION_PATH
    admission_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert "mutually exclusive with normal evidence" in (
        result["checks"][-1]["error"]
    )


def test_8_0_execution_failure_rejects_missing_runtime_before_remote_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _copy_8_0_failure_archive_inputs(tmp_path)
    actual_archive_git = cli._v8_archive_git

    def archive_git(_root: Path, *args: str, **kwargs):
        if args == (
            "show-ref",
            "--verify",
            "--quiet",
            "refs/tags/8.0",
        ):
            return 1, b"", b""
        return actual_archive_git(source_root, *args, **kwargs)

    ci_roots: list[Path] = []

    def require_head_ci(root: Path) -> str:
        ci_roots.append(root)
        return "c" * 40

    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_archive_git", archive_git)
    monkeypatch.setattr(cli, "_v8_require_head_ci", require_head_ci)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert ci_roots == []
    assert result["status"] == "integrity_mismatch"
    assert result["canonical_data_hashes_verified"] is False
    assert "must be retained until the GitHub tag is verified" in (
        result["checks"][-1]["error"]
    )


def test_8_0_execution_failure_rejects_empty_runtime_before_remote_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _copy_8_0_failure_archive_inputs(tmp_path)
    (tmp_path / cli.V8_RUNTIME_PATH).mkdir(parents=True)
    actual_archive_git = cli._v8_archive_git

    def archive_git(_root: Path, *args: str, **kwargs):
        if args == ("show-ref", "--verify", "--quiet", "refs/tags/8.0"):
            return 1, b"", b""
        return actual_archive_git(source_root, *args, **kwargs)

    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_archive_git", archive_git)
    monkeypatch.setattr(
        cli,
        "_v8_require_head_ci",
        lambda _root: (_ for _ in ()).throw(
            AssertionError("invalid runtime must fail before CI lookup")
        ),
    )

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert "archived runtime layout differs" in result["checks"][-1]["error"]


def test_8_0_execution_failure_rejects_local_only_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _copy_8_0_failure_archive_inputs(tmp_path)
    actual_archive_git = cli._v8_archive_git
    tag_object = cli.V8_TAG_OBJECT
    tag_commit = cli.V8_TAG_COMMIT

    def archive_git(_root: Path, *args: str, **kwargs):
        if args == ("show-ref", "--verify", "--quiet", "refs/tags/8.0"):
            return 0, b"", b""
        if args == ("cat-file", "-t", "refs/tags/8.0"):
            return 0, b"tag\n", b""
        if args == ("rev-parse", "refs/tags/8.0"):
            return 0, (tag_object + "\n").encode("ascii"), b""
        if args == ("rev-parse", "refs/tags/8.0^{}"):
            return 0, (tag_commit + "\n").encode("ascii"), b""
        if args[:4] == ("ls-remote", "--exit-code", "origin", "refs/tags/8.0"):
            remote = (
                "c" * 40
                + "\trefs/tags/8.0\n"
                + tag_commit
                + "\trefs/tags/8.0^{}\n"
            )
            return 0, remote.encode("ascii"), b""
        return actual_archive_git(source_root, *args, **kwargs)

    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_archive_git", archive_git)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert "local and GitHub 8.0 tag identities differ" in (
        result["checks"][-1]["error"]
    )


def test_8_0_execution_failure_allows_missing_runtime_after_remote_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _copy_8_0_failure_archive_inputs(tmp_path)
    actual_archive_git = cli._v8_archive_git
    tag_object = cli.V8_TAG_OBJECT
    tag_commit = cli.V8_TAG_COMMIT
    normal_paths = {
        cli.V8_TRAIN_ADMISSION_PATH,
        cli.V8_WINNER_FREEZE_PATH,
        cli.V8_AUDIT_PATH,
        cli.V8_RESULT_PATH,
    }

    def archive_git(_root: Path, *args: str, **kwargs):
        if args == ("show-ref", "--verify", "--quiet", "refs/tags/8.0"):
            return 0, b"", b""
        if args == ("cat-file", "-t", "refs/tags/8.0"):
            return 0, b"tag\n", b""
        if args == ("rev-parse", "refs/tags/8.0"):
            return 0, (tag_object + "\n").encode("ascii"), b""
        if args == ("rev-parse", "refs/tags/8.0^{}"):
            return 0, (tag_commit + "\n").encode("ascii"), b""
        if args[:4] == ("ls-remote", "--exit-code", "origin", "refs/tags/8.0"):
            remote = (
                tag_object
                + "\trefs/tags/8.0\n"
                + tag_commit
                + "\trefs/tags/8.0^{}\n"
            )
            return 0, remote.encode("ascii"), b""
        if args == (
            "merge-base",
            "--is-ancestor",
            "644840a4967d69f6acc8903549705370bffdcba1",
            tag_commit,
        ):
            return 0, b"", b""
        if len(args) == 2 and args[0] == "show" and args[1].startswith(
            tag_commit + ":"
        ):
            relative = args[1].split(":", 1)[1]
            return 0, (tmp_path / relative).read_bytes(), b""
        if (
            len(args) == 3
            and args[:2] == ("cat-file", "-e")
            and args[2].startswith(tag_commit + ":")
            and args[2].split(":", 1)[1] in normal_paths
        ):
            return 1, b"", b"missing"
        return actual_archive_git(source_root, *args, **kwargs)

    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_archive_git", archive_git)
    monkeypatch.setattr(
        cli,
        "_v8_require_head_ci",
        lambda _root: (_ for _ in ()).throw(
            AssertionError("published archive must not require current-main CI")
        ),
    )

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=False)

    assert exit_code == 0
    assert result["status"] == "selection_inconclusive_execution_failure"
    assert result["canonical_data_hashes_verified"] is False
    assert any(
        check["category"] == "local_archived_annotated_tag"
        and check["status"] == "match"
        for check in result["checks"]
    )
    assert any(
        check["category"] == "canonical_stage_artifacts"
        and check["status"] == "not_retained"
        for check in result["checks"]
    )


def test_explicit_7_0_status_reports_published_execution_failure() -> None:
    root = Path(__file__).resolve().parents[2]

    result, exit_code = cli._strategy_status(
        root, verify_data=True, release="7.0"
    )

    assert exit_code == 0
    assert result["version"] == "7.0"
    assert result["status"] == "selection_inconclusive_software_failure"
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["terminal_result_payload_sha256"] is None
    assert result["canonical_data_hashes_verified"] is False
    assert any(
        check["category"] == "selection_execution_failure"
        and check["status"] == "match"
        for check in result["checks"]
    )


def test_explicit_7_1_status_reports_published_null_result() -> None:
    root = Path(__file__).resolve().parents[2]

    result, exit_code = cli._strategy_status(
        root, verify_data=True, release="7.1"
    )

    assert exit_code == 0
    assert result["version"] == "7.1"
    assert result["status"] == "selection_falsified_no_candidate"
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["terminal_result_payload_sha256"] == (
        "869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9"
    )
    assert result["canonical_data_hashes_verified"] is False


def test_explicit_8_1_status_reports_published_validation_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)

    result, exit_code = cli._strategy_status(
        root, verify_data=True, release="8.1"
    )

    assert exit_code == 0
    assert result["version"] == "8.1"
    assert result["status"] == "selection_falsified_no_candidate"
    assert result["train_reclassification_status"] == (
        "train_reclassification_passed"
    )
    assert result["winner_freeze_status"] == (
        "selected_null_frozen_validation_failed"
    )
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["terminal_result_payload_sha256"] == (
        cli.V81_RESULT_PAYLOAD_SHA256
    )
    assert result["canonical_data_hashes_verified"] is False


def test_default_strategy_status_reports_the_9_0_preclosure_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V9_CLOSURE_PATH).is_file():
        pytest.skip("9.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v9_require_head_ci", lambda _root: "a" * 40)
    _mock_v9_preclosure_readiness(monkeypatch)

    result, exit_code = cli._strategy_status(root, verify_data=False, release="9.0")

    assert exit_code == 0
    assert result["status"] == "implementation_ready_for_preselection_closure"
    assert result["version"] == "9.0"
    assert result["route"] == cli.V9_ROUTE
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["profit_claim_allowed"] is False
    assert all(check["status"] == "match" for check in result["checks"])


def test_default_preclosure_status_does_not_claim_dirty_tree_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V9_CLOSURE_PATH).is_file():
        pytest.skip("9.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: False)
    _mock_v9_preclosure_readiness(monkeypatch)

    result, exit_code = cli._strategy_status(root, verify_data=False, release="9.0")

    assert exit_code == 2
    assert result["status"] == "implementation_pending_clean_commit"
    assert any(
        check["category"] == "preclosure_working_tree"
        and check["status"] == "pending_clean_commit"
        for check in result["checks"]
    )


def test_8_0_preclosure_status_rejects_strategy_contract_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    protocol = json.loads((root / cli.V8_PROTOCOL_PATH).read_text(encoding="utf-8"))
    protocol["strategy_registry"][0]["strategy_id"] = "forged_policy"
    protocol["payload_sha256"] = cli._canonical_payload_sha256(protocol)
    protocol_path = tmp_path / cli.V8_PROTOCOL_PATH
    protocol_path.parent.mkdir(parents=True)
    protocol_path.write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for relative in (
        cli.V8_ASSET_SELECTION_PATH,
        cli.V7_PROTOCOL_PATH,
        cli.V71_CLOSURE_PATH,
        cli.V71_WINNER_FREEZE_PATH,
        cli.V71_RESULT_PATH,
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((root / relative).read_bytes())
    monkeypatch.setattr(cli, "_verify_published_7_1_result", lambda _root: {})
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_8_0_pending(
        tmp_path, verify_data=False
    )

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] in {"protocol_payload", "strategic_policy_contract"}
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_8_0_preclosure_status_rejects_orphan_train_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    for relative in (
        cli.V8_PROTOCOL_PATH,
        cli.V8_ASSET_SELECTION_PATH,
        cli.V7_PROTOCOL_PATH,
        cli.V71_CLOSURE_PATH,
        cli.V71_WINNER_FREEZE_PATH,
        cli.V71_RESULT_PATH,
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((root / relative).read_bytes())
    admission_path = tmp_path / cli.V8_TRAIN_ADMISSION_PATH
    admission_path.parent.mkdir(parents=True, exist_ok=True)
    admission_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "_verify_published_7_1_result", lambda _root: {})
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_8_0_pending(
        tmp_path, verify_data=False
    )

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "preclosure_absence"
        and check["path"] == cli.V8_EVIDENCE_ROOT
        and check["status"] == "unexpected"
        for check in result["checks"]
    )


def test_default_preclosure_status_requires_pushed_successful_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V9_CLOSURE_PATH).is_file():
        pytest.skip("9.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    _mock_v9_preclosure_readiness(monkeypatch)
    monkeypatch.setattr(
        cli,
        "_v9_require_head_ci",
        lambda _root: (_ for _ in ()).throw(RuntimeError("push CI missing")),
    )

    result, exit_code = cli._strategy_status(root, verify_data=False, release="9.0")

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "preclosure_head_push_ci"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_9_0_pending_status_loads_the_9_0_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    for relative in (cli.V9_PROTOCOL_PATH, cli.V9_SCOUT_PATH):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((root / relative).read_bytes())
    monkeypatch.setattr(
        cli,
        "_load_v9_runner",
        lambda _root: (_ for _ in ()).throw(ValueError("stale 8.1 runner")),
    )
    monkeypatch.setattr(
        cli,
        "_verify_published_8_1_archive",
        lambda _root: (_ for _ in ()).throw(ValueError("runner unavailable")),
    )
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: False)

    result, exit_code = cli._strategy_status_9_0(tmp_path, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert result["version"] == "9.0"
    assert any(
        check["category"] == "formal_runner_namespace"
        and "stale 8.1 runner" in check["error"]
        for check in result["checks"]
    )


def test_9_0_freeze_audit_and_result_status_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure_path = tmp_path / cli.V9_CLOSURE_PATH
    closure_path.parent.mkdir(parents=True)
    closure_path.write_text("{}", encoding="utf-8")
    freeze = {
        "status": "selected_policy_frozen",
        "selected_candidate_id": cli.V9_ROUTE,
        "payload_sha256": "1" * 64,
    }
    audit = {"status": "historical_audit_passed", "payload_sha256": "2" * 64}
    result = {
        "status": "historical_adaptive_beta_diagnostic_passed_fresh_evidence_required",
        "selected_candidate_id": cli.V9_ROUTE,
        "audit_status": "historical_audit_passed",
        "payload_sha256": "3" * 64,
    }
    verifier = SimpleNamespace(
        verify_release_state=lambda **_: {
            "status": result["status"],
            "closure": {"route": cli.V9_ROUTE},
            "protocol": {"protocol_id": cli.V9_PROTOCOL_ID},
            "freeze": freeze,
            "audit": audit,
            "result": result,
        }
    )
    monkeypatch.setattr(cli, "_load_v9_runner", lambda _root: verifier)
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v9_require_head_ci", lambda _root: "a" * 40)

    value, exit_code = cli._strategy_status_9_0(tmp_path, verify_data=True)

    assert exit_code == 0
    assert value["status"] == result["status"]
    assert value["selected_candidate_id"] == cli.V9_ROUTE
    assert value["winner_freeze_status"] == "selected_policy_frozen"
    assert value["winner_freeze_payload_sha256"] == "1" * 64
    assert value["audit_status"] == "historical_audit_passed"
    assert value["historical_audit_payload_sha256"] == "2" * 64
    assert value["terminal_result_status"] == result["status"]
    assert value["terminal_result_payload_sha256"] == "3" * 64
    assert value["canonical_data_hashes_verified"] is True
    assert value["profit_claim_allowed"] is False


def test_8_0_status_rejects_orphan_audit_from_full_chain_verifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure = tmp_path / cli.V8_CLOSURE_PATH
    closure.parent.mkdir(parents=True)
    closure.write_text("{}", encoding="utf-8")
    verifier = SimpleNamespace(
        verify_release_state=lambda **_: (_ for _ in ()).throw(
            ValueError("audit/result exists without a winner freeze")
        )
    )
    monkeypatch.setattr(cli, "_load_v8_runner", lambda _root: verifier)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=True)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert result["canonical_data_hashes_verified"] is False
    assert "without a winner freeze" in result["checks"][-1]["error"]


def test_8_0_verify_data_is_forwarded_to_stage_verifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure_path = tmp_path / cli.V8_CLOSURE_PATH
    closure_path.parent.mkdir(parents=True)
    closure_path.write_text("{}", encoding="utf-8")
    calls: list[tuple[bool, bool]] = []

    def verify_release_state(*, verify_data: bool, verify_runtime: bool) -> dict:
        calls.append((verify_data, verify_runtime))
        return {
            "status": "selection_frozen_pending_historical_audit",
            "closure": {"route": "strategic_static_capital_budget_beta"},
            "protocol": {
                "protocol_id": "factor-lab/8.0/strategic-static-capital-budget-beta-v1",
                "claim_contract": {
                    "historical_evidence_class": "pre_registered_historical_diagnostic_only",
                    "profit_claim_allowed": False,
                },
            },
            "selection": {},
            "freeze": {"selected_candidate_id": "static_risk_budget"},
            "audit": None,
            "result": None,
        }

    monkeypatch.setattr(
        cli,
        "_load_v8_runner",
        lambda _root: SimpleNamespace(verify_release_state=verify_release_state),
    )
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_8_0(tmp_path, verify_data=True)

    assert exit_code == 0
    assert calls == [(True, False)]
    assert result["canonical_data_hashes_verified"] is True
    assert any(
        check["category"] == "head_push_ci" and check["status"] == "match"
        for check in result["checks"]
    )
    assert any(
        check["category"] == "canonical_stage_artifacts"
        and check["status"] == "match"
        for check in result["checks"]
    )


def test_8_0_train_admission_pending_validation_shallow_and_deep_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure_path = tmp_path / cli.V8_CLOSURE_PATH
    closure_path.parent.mkdir(parents=True)
    closure_path.write_text("{}", encoding="utf-8")
    calls: list[tuple[bool, bool]] = []

    def verify_release_state(*, verify_data: bool, verify_runtime: bool) -> dict:
        calls.append((verify_data, verify_runtime))
        return {
            "status": "train_admission_passed_pending_validation",
            "closure": {"route": "strategic_static_capital_budget_beta"},
            "protocol": {
                "protocol_id": "factor-lab/8.0/strategic-static-capital-budget-beta-v1",
                "claim_contract": {
                    "historical_pass_interpretation": "historical beta diagnostic only",
                    "profit_claim_allowed": False,
                },
            },
            "selection": {},
            "train_admission": {
                "status": "train_admission_passed",
                "payload_sha256": "a" * 64,
            },
            "freeze": None,
            "audit": None,
            "result": None,
        }

    monkeypatch.setattr(
        cli,
        "_load_v8_runner",
        lambda _root: SimpleNamespace(verify_release_state=verify_release_state),
    )
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v8_require_head_ci", lambda _root: "b" * 40)

    shallow, shallow_exit = cli._strategy_status_8_0(tmp_path, verify_data=False)
    deep, deep_exit = cli._strategy_status_8_0(tmp_path, verify_data=True)

    assert calls == [(False, False), (True, False)]
    assert (shallow_exit, deep_exit) == (0, 0)
    for result in (shallow, deep):
        assert result["status"] == "train_admission_passed_pending_validation"
        assert result["selected_candidate_id"] is None
        assert result["audit_status"] == "not_opened"
        assert result["terminal_result_payload_sha256"] is None
        assert result["profit_claim_allowed"] is False
    assert shallow["canonical_data_hashes_verified"] is False
    assert deep["canonical_data_hashes_verified"] is True
    assert next(
        check for check in shallow["checks"]
        if check["category"] == "canonical_stage_artifacts"
    )["status"] == "not_verified"
    assert next(
        check for check in deep["checks"]
        if check["category"] == "canonical_stage_artifacts"
    )["status"] == "match"


def test_strategy_status_detects_implementation_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if not (root / cli.PRESELECTION_CLOSURE_PATH).is_file():
        pytest.skip("6.3 closure is created from the clean implementation commit")
    actual_sha256 = cli._file_sha256

    def tampered_sha256(path: Path) -> str:
        if path.name == "wide_pricing.py":
            return "0" * 64
        return actual_sha256(path)

    monkeypatch.setattr(cli, "_file_sha256", tampered_sha256)

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.3"
    )

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "implementation:wide_pricing"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_status_detects_protocol_file_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if not (root / cli.PRESELECTION_CLOSURE_PATH).is_file():
        pytest.skip("6.3 closure is created from the clean implementation commit")
    actual_sha256 = cli._file_sha256

    def tampered_sha256(path: Path) -> str:
        if path.name == "6.2-wide-universe.json":
            return "0" * 64
        return actual_sha256(path)

    monkeypatch.setattr(cli, "_file_sha256", tampered_sha256)

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.3"
    )

    assert exit_code == 3
    assert any(
        check["category"] == "protocol_file:wide_universe"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_status_can_explicitly_audit_legacy_6_0() -> None:
    root = Path(__file__).resolve().parents[2]

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.0"
    )

    assert exit_code == 3
    assert result["version"] == "6.0"
    assert any(
        check["category"] == "implementation:cli"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_targets_rebuilds_absolute_sleeve_state(tmp_path: Path) -> None:
    data_root = tmp_path / "runtime" / "data" / "top500"
    data_root.mkdir(parents=True)
    calendar = pd.bdate_range("2017-01-03", periods=12)
    tickers = [f"T{value:02d}" for value in range(1, 31)]
    rows: list[dict[str, object]] = []
    for date_index, date in enumerate(calendar):
        ranking = tickers[date_index:] + tickers[:date_index]
        strength = {
            ticker: float(len(tickers) - rank)
            for rank, ticker in enumerate(ranking)
        }
        for ticker in tickers:
            value = strength[ticker]
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "eligible": True,
                    "universe_member": True,
                    "earnings_yield": value,
                    "pb": 1.0,
                    "book_yield": value,
                    "volatility_20": 31.0 - value,
                }
            )
    pd.DataFrame(rows).to_parquet(data_root / "features.parquet", index=False)
    pd.DataFrame({"date": calendar}).to_parquet(
        data_root / "execution.parquet", index=False
    )

    result = cli._strategy_targets(tmp_path, "latest")

    assert result["requested_signal_date"] == calendar[-1].date().isoformat()
    assert result["decision"]["calendar_index"] == 11
    assert result["decision"]["sleeve"] == 1
    assert result["decision"]["status"] == "ok"
    assert len(result["decision"]["selected_tickers"]) == 10
